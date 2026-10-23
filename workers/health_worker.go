package workers

import (
	"context"
	"log/slog"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// HealthWorker는 tracker의 타임아웃을 주기적으로 평가하고 전이를 DB에 기록한다.
// 읽는 테이블은 sensor뿐이며 sensor_data는 읽지 않는다.
type HealthWorker struct {
	DB      *gorm.DB
	Tracker *HealthTracker

	Tick         time.Duration // 타임아웃 평가/전이 기록 주기
	SeenInterval time.Duration // last_seen_at 일괄 반영 주기
	Refresh      time.Duration // 센서 목록 재로딩 주기
}

type sensorRow struct {
	ID                 int
	ExpectedIntervalMs int
}

func (sensorRow) TableName() string { return "sensor" }

func (w *HealthWorker) loadSensors(ctx context.Context) error {
	var rows []sensorRow
	if err := w.DB.WithContext(ctx).Find(&rows).Error; err != nil {
		return err
	}
	intervals := make(map[int]time.Duration, len(rows))
	for _, r := range rows {
		intervals[r.ID] = time.Duration(r.ExpectedIntervalMs) * time.Millisecond
	}
	w.Tracker.SyncSensors(intervals, time.Now())
	return nil
}

func (w *HealthWorker) Run(ctx context.Context) error {
	if err := w.loadSensors(ctx); err != nil {
		return err
	}
	// 시작 시점의 unknown 상태를 DB에 반영 (이전 실행의 stale한 online 제거)
	w.persist(ctx, false)

	tick := time.NewTicker(w.Tick)
	seen := time.NewTicker(w.SeenInterval)
	refresh := time.NewTicker(w.Refresh)
	defer tick.Stop()
	defer seen.Stop()
	defer refresh.Stop()

	for {
		select {
		case now := <-tick.C:
			w.Tracker.CheckTimeouts(now)
			w.persist(ctx, false)
		case <-seen.C:
			w.persist(ctx, true)
		case <-refresh.C:
			if err := w.loadSensors(ctx); err != nil {
				slog.Error("failed to refresh sensors", "error", err)
			}
		case <-ctx.Done():
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			w.persist(shutdownCtx, true)
			cancel()
			return nil
		}
	}
}

func (w *HealthWorker) persist(ctx context.Context, includeSeen bool) {
	events, rows := w.Tracker.Drain(includeSeen)
	if len(events) == 0 && len(rows) == 0 {
		return
	}

	err := w.DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if len(events) > 0 {
			if err := tx.Create(&events).Error; err != nil {
				return err
			}
		}
		if len(rows) > 0 {
			return tx.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "sensor_id"}},
				DoUpdates: clause.AssignmentColumns([]string{"status", "last_seen_at", "last_seq", "status_changed_at"}),
			}).Create(&rows).Error
		}
		return nil
	})
	if err != nil {
		slog.Error("failed to persist health state", "events", len(events), "rows", len(rows), "error", err)
		w.Tracker.Requeue(events, rows)
		return
	}
	for _, e := range events {
		slog.Info("sensor state change", "sensor_id", e.SensorID, "event", e.EventType, "source", e.Source, "message", e.Message)
	}
}
