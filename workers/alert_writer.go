package workers

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// AlertEventRow는 alert_event 에 넣는 열이다. status, created_at 등은 DB 기본값을 쓴다.
type AlertEventRow struct {
	RuleID      int64
	SensorID    int
	Seq         int64
	DataTime    time.Time
	TriggeredAt time.Time
	Value       float64
}

func (AlertEventRow) TableName() string { return "alert_event" }

// AlertStore는 AlertWriter가 쓰는 저장소다. 테스트에서 대체할 수 있도록 분리했다.
type AlertStore interface {
	// InsertAlerts는 한 문장으로 경보를 넣는다. (rule_id, sensor_id, seq) 중복은 건너뛰고 실제 삽입 수를 돌려준다.
	InsertAlerts(ctx context.Context, rows []AlertEventRow) (inserted int64, err error)
}

type GormAlertStore struct{ DB *gorm.DB }

func (s GormAlertStore) InsertAlerts(ctx context.Context, rows []AlertEventRow) (int64, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	res := s.DB.WithContext(ctx).
		Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "rule_id"}, {Name: "sensor_id"}, {Name: "seq"}},
			DoNothing: true,
		}).
		Create(&rows)
	return res.RowsAffected, res.Error
}

// AlertWriter는 AlertEngine 후보 큐를 비우며 alert_event 에 기록한다.
// DbPushWorker 와 별개의 고루틴이므로 센서 데이터 저장(불변식 5의 단일 저장 워커)에 영향을 주지 않는다.
type AlertWriter struct {
	Store AlertStore
	In    <-chan AlertCandidate

	MaxBatch        int           // 기본 200
	RetryInitial    time.Duration // 기본 100ms
	RetryMax        time.Duration // 기본 5s
	ShutdownTimeout time.Duration // 기본 5s
	Clock           Clock
}

func (w *AlertWriter) applyDefaults() {
	if w.MaxBatch <= 0 {
		w.MaxBatch = 200
	}
	if w.RetryInitial <= 0 {
		w.RetryInitial = 100 * time.Millisecond
	}
	if w.RetryMax <= 0 {
		w.RetryMax = 5 * time.Second
	}
	if w.ShutdownTimeout <= 0 {
		w.ShutdownTimeout = 5 * time.Second
	}
	if w.Clock == nil {
		w.Clock = realClock{}
	}
}

func (w *AlertWriter) Run(ctx context.Context) error {
	w.applyDefaults()
	for {
		select {
		case c := <-w.In:
			batch := w.collect(c)
			w.write(ctx, batch)
		case <-ctx.Done():
			// 종료: 큐에 남은 후보를 마지막으로 기록한다. 못 쓴 경보는 재시작 후 재전달 메시지로 다시 평가된다.
			shutdownCtx, cancel := context.WithTimeout(context.Background(), w.ShutdownTimeout)
			defer cancel()
			for {
				select {
				case c := <-w.In:
					w.write(shutdownCtx, w.collect(c))
				default:
					return nil
				}
			}
		}
	}
}

// collect는 첫 후보와 지금 큐에 있는 후보를 MaxBatch까지 묶는다. 기다리지 않아 경보 지연이 늘지 않는다.
func (w *AlertWriter) collect(first AlertCandidate) []AlertEventRow {
	batch := []AlertEventRow{toRow(first)}
	for len(batch) < w.MaxBatch {
		select {
		case c := <-w.In:
			batch = append(batch, toRow(c))
		default:
			return batch
		}
	}
	return batch
}

func toRow(c AlertCandidate) AlertEventRow {
	return AlertEventRow{RuleID: c.RuleID, SensorID: c.SensorID, Seq: c.Seq, DataTime: c.DataTime, TriggeredAt: c.TriggeredAt, Value: c.Value}
}

// write는 일시적 오류면 성공할 때까지(ctx 취소 전까지) 재시도하고, 영구 오류면 문제 행을 찾아 버린다.
// 규칙이 삭제돼 FK 위반이 난 경보처럼 다시 시도해도 소용없는 행 하나 때문에 나머지를 잃지 않기 위해서다.
func (w *AlertWriter) write(ctx context.Context, rows []AlertEventRow) {
	if len(rows) == 0 {
		return
	}
	backoff := w.RetryInitial
	for {
		inserted, err := w.Store.InsertAlerts(ctx, rows)
		if err == nil {
			now := time.Now()
			for _, r := range rows {
				alertLatencySeconds.Observe(now.Sub(r.TriggeredAt).Seconds())
			}
			alertsCreated.Add(float64(inserted))
			alertsDeduped.Add(float64(int64(len(rows)) - inserted))
			return
		}
		if ClassifyDBError(err) == KindPermanent {
			if len(rows) == 1 {
				alertsDropped.WithLabelValues("rejected").Inc()
				slog.Error("alert rejected", "rule_id", rows[0].RuleID, "sensor_id", rows[0].SensorID, "seq", rows[0].Seq, "code", sqlStateOf(err), "error", err)
				return
			}
			mid := len(rows) / 2
			w.write(ctx, rows[:mid])
			w.write(ctx, rows[mid:])
			return
		}
		if ctx.Err() != nil {
			alertsDropped.WithLabelValues("shutdown").Add(float64(len(rows)))
			slog.Error("alerts not persisted before shutdown; they will be re-evaluated from redelivered messages", "count", len(rows), "error", err)
			return
		}
		dbErrors.WithLabelValues(string(KindTransient)).Inc()
		slog.Warn("alert insert failed; retrying", "count", len(rows), "backoff", backoff, "error", err)
		select {
		case <-w.Clock.After(backoff):
		case <-ctx.Done():
		}
		backoff = min(backoff*2, w.RetryMax)
	}
}

// AlertRuleLoader는 활성 규칙과 센서 목록을 주기적으로 다시 읽어 엔진에 반영한다.
type AlertRuleLoader struct {
	DB      *gorm.DB
	Engine  *AlertEngine
	Refresh time.Duration // 기본 10s
}

type sensorTypeRow struct {
	ID         int
	TenantID   int
	SensorType string
}

func (sensorTypeRow) TableName() string { return "sensor" }

func (l *AlertRuleLoader) load(ctx context.Context) error {
	var rules []AlertRule
	if err := l.DB.WithContext(ctx).Where("enabled").Find(&rules).Error; err != nil {
		return err
	}
	var sensors []sensorTypeRow
	if err := l.DB.WithContext(ctx).Find(&sensors).Error; err != nil {
		return err
	}
	infos := make(map[int]SensorInfo, len(sensors))
	for _, s := range sensors {
		infos[s.ID] = SensorInfo{TenantID: s.TenantID, Type: s.SensorType}
	}
	l.Engine.SetSensors(infos)
	l.Engine.SetRules(rules)
	return nil
}

func (l *AlertRuleLoader) Run(ctx context.Context) error {
	refresh := l.Refresh
	if refresh <= 0 {
		refresh = 10 * time.Second
	}
	// 첫 로딩이 실패해도 수집을 막지 않는다. 규칙이 없는 동안은 경보만 나오지 않는다.
	if err := l.load(ctx); err != nil {
		slog.Error("failed to load alert rules; retrying", "error", err)
	}
	tick := time.NewTicker(refresh)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			if err := l.load(ctx); err != nil {
				slog.Error("failed to refresh alert rules; keeping previous rules", "error", err)
			}
		case <-ctx.Done():
			return nil
		}
	}
}

// 경보 상태: 미확인(open) → 조치 중(in_progress) → 조치 완료(resolved)
const (
	AlertOpen       = "open"
	AlertInProgress = "in_progress"
	AlertResolved   = "resolved"
)

var alertTransitions = map[[2]string]bool{
	{AlertOpen, AlertInProgress}:     true,
	{AlertInProgress, AlertResolved}: true,
}

// ErrInvalidAlertTransition은 허용되지 않은 상태 전이 요청이다.
var ErrInvalidAlertTransition = errors.New("invalid alert status transition")

// TransitionAlert는 경보 상태를 from → to 로 바꾼다. 조건부 UPDATE 이므로 같은 경보를 동시에 처리하려는
// 요청 중 정확히 하나만 changed=true 를 받는다. 이미 다른 상태이거나 없는 경보면 changed=false 다.
func TransitionAlert(ctx context.Context, db *gorm.DB, id int64, from, to, by string) (changed bool, err error) {
	if !alertTransitions[[2]string{from, to}] {
		return false, ErrInvalidAlertTransition
	}
	res := db.WithContext(ctx).Exec(
		`UPDATE alert_event SET status = ?, status_changed_at = now(), changed_by = ? WHERE id = ? AND status = ?`,
		to, by, id, from)
	if res.Error != nil {
		return false, res.Error
	}
	return res.RowsAffected == 1, nil
}
