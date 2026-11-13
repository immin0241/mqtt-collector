package report

import (
	"context"
	"fmt"
	"slices"
	"time"

	"gorm.io/gorm"

	"github.com/immin0241/webframework-project-mqtt-collector/workers"
)

// 조회 상한. LLM 컨텍스트와 DB 부하를 함께 제한한다.
const (
	MaxAlerts       = 100
	MaxSensors      = 500
	MaxSilentListed = 100
)

// Window는 한 날짜의 [Start, End) 구간이다.
type Window struct {
	Date       string    `json:"date"`
	Start, End time.Time `json:"-"`
	Timezone   string    `json:"timezone"`
}

// ParseDate는 "YYYY-MM-DD" 를 loc 의 하루 구간으로 바꾼다. 빈 문자열이면 now 기준 어제다.
// 2000-01-01 이전이거나 내일 이후 날짜는 거부한다(데이터가 있을 수 없는 구간으로 쿼리하지 않는다).
func ParseDate(s string, loc *time.Location, now time.Time) (Window, error) {
	var day time.Time
	if s == "" {
		y, m, d := now.In(loc).AddDate(0, 0, -1).Date()
		day = time.Date(y, m, d, 0, 0, 0, 0, loc)
	} else {
		t, err := time.ParseInLocation("2006-01-02", s, loc)
		if err != nil {
			return Window{}, fmt.Errorf("date must be YYYY-MM-DD: %q", s)
		}
		day = t
	}
	if day.Before(time.Date(2000, 1, 1, 0, 0, 0, 0, loc)) {
		return Window{}, fmt.Errorf("date %s is too old", day.Format("2006-01-02"))
	}
	y, m, d := now.In(loc).Date()
	if day.After(time.Date(y, m, d, 0, 0, 0, 0, loc)) {
		return Window{}, fmt.Errorf("date %s is in the future", day.Format("2006-01-02"))
	}
	return Window{Date: day.Format("2006-01-02"), Start: day, End: day.AddDate(0, 0, 1), Timezone: loc.String()}, nil
}

// Reader는 테넌트의 읽기 전용 연결(뷰만 조회 가능한 role)로 리포트 데이터를 읽는다.
type Reader struct{ DB *gorm.DB }

type Count struct {
	Key   string `json:"key"`
	Count int64  `json:"count"`
}

type MetricStat struct {
	Metric string   `json:"metric"`
	Unit   string   `json:"unit"`
	Count  int64    `json:"sample_count"`
	Min    *float64 `json:"min"`
	Max    *float64 `json:"max"`
	Avg    *float64 `json:"avg"`
}

type AlertCount struct {
	Severity string `json:"severity"`
	Status   string `json:"status"`
	Count    int64  `json:"count"`
}

type Summary struct {
	Window        Window       `json:"window"`
	SensorsByStat []Count      `json:"sensors_by_status"`
	Metrics       []MetricStat `json:"metrics"`
	Alerts        []AlertCount `json:"alerts"`
	SensorEvents  []Count      `json:"sensor_events"`
	SilentSensors []int        `json:"sensors_without_data"`
	SilentTotal   int          `json:"sensors_without_data_total"`
}

func (r Reader) Summary(ctx context.Context, w Window) (*Summary, error) {
	db := r.DB.WithContext(ctx)
	s := &Summary{Window: w}
	if err := db.Raw(`SELECT coalesce(st.status, 'unknown') AS key, count(*) AS count
		FROM v_sensor s LEFT JOIN v_sensor_status st ON st.sensor_id = s.id
		GROUP BY 1 ORDER BY 1`).Scan(&s.SensorsByStat).Error; err != nil {
		return nil, err
	}
	if err := db.Raw(`SELECT metric, unit, sum(sample_count)::bigint AS count, min(value_min) AS min, max(value_max) AS max,
			sum(value_avg * sample_count) / nullif(sum(sample_count), 0) AS avg
		FROM v_sensor_data_1m WHERE bucket >= ? AND bucket < ?
		GROUP BY metric, unit ORDER BY metric`, w.Start, w.End).Scan(&s.Metrics).Error; err != nil {
		return nil, err
	}
	if err := db.Raw(`SELECT severity, status, count(*) AS count FROM v_alert_event
		WHERE triggered_at >= ? AND triggered_at < ? GROUP BY 1, 2 ORDER BY 1, 2`, w.Start, w.End).Scan(&s.Alerts).Error; err != nil {
		return nil, err
	}
	if err := db.Raw(`SELECT event_type AS key, count(*) AS count FROM v_sensor_event
		WHERE time >= ? AND time < ? GROUP BY 1 ORDER BY 1`, w.Start, w.End).Scan(&s.SensorEvents).Error; err != nil {
		return nil, err
	}
	var silent []int
	if err := db.Raw(`SELECT s.id FROM v_sensor s
		WHERE NOT EXISTS (SELECT 1 FROM v_sensor_data_1m a WHERE a.sensor_id = s.id AND a.bucket >= ? AND a.bucket < ?)
		ORDER BY s.id`, w.Start, w.End).Scan(&silent).Error; err != nil {
		return nil, err
	}
	s.SilentTotal = len(silent)
	if len(silent) > MaxSilentListed {
		silent = silent[:MaxSilentListed]
	}
	s.SilentSensors = silent
	s.nonNil()
	return s, nil
}

// nonNil은 빈 목록을 JSON 에서 null 이 아니라 []로 나오게 한다. 도구 응답을 읽는 쪽(LLM·클라이언트)이 null 과 빈 목록을 구분하지 않아도 된다.
func (s *Summary) nonNil() {
	if s.SensorsByStat == nil {
		s.SensorsByStat = []Count{}
	}
	if s.Metrics == nil {
		s.Metrics = []MetricStat{}
	}
	if s.Alerts == nil {
		s.Alerts = []AlertCount{}
	}
	if s.SensorEvents == nil {
		s.SensorEvents = []Count{}
	}
	if s.SilentSensors == nil {
		s.SilentSensors = []int{}
	}
}

type Alert struct {
	ID              int64      `json:"id"`
	Rule            string     `json:"rule"`
	Severity        string     `json:"severity"`
	SensorID        int        `json:"sensor_id"`
	Value           float64    `json:"value"`
	Status          string     `json:"status"`
	TriggeredAt     time.Time  `json:"triggered_at"`
	StatusChangedAt *time.Time `json:"status_changed_at"`
}

// AlertStatuses는 list_alerts 가 받는 상태 값이다.
var AlertStatuses = []string{"open", "in_progress", "resolved"}

func (r Reader) Alerts(ctx context.Context, w Window, status string, limit int) ([]Alert, error) {
	if status != "" && !slices.Contains(AlertStatuses, status) {
		return nil, fmt.Errorf("status must be one of %v", AlertStatuses)
	}
	if limit <= 0 || limit > MaxAlerts {
		limit = MaxAlerts
	}
	var out []Alert
	err := r.DB.WithContext(ctx).Raw(`SELECT id, rule_name AS rule, severity, sensor_id, value, status, triggered_at, status_changed_at
		FROM v_alert_event WHERE triggered_at >= ? AND triggered_at < ? AND (? = '' OR status = ?)
		ORDER BY triggered_at DESC LIMIT ?`, w.Start, w.End, status, status, limit).Scan(&out).Error
	return out, err
}

type SensorState struct {
	ID         int        `json:"sensor_id"`
	Type       string     `json:"sensor_type"`
	FacilityID *int       `json:"facility_id"`
	Status     string     `json:"status"`
	LastSeenAt *time.Time `json:"last_seen_at"`
}

func (r Reader) Sensors(ctx context.Context) ([]SensorState, error) {
	var out []SensorState
	err := r.DB.WithContext(ctx).Raw(`SELECT s.id, s.sensor_type AS type, s.facility_id, coalesce(st.status, 'unknown') AS status, st.last_seen_at
		FROM v_sensor s LEFT JOIN v_sensor_status st ON st.sensor_id = s.id
		ORDER BY s.id LIMIT ?`, MaxSensors).Scan(&out).Error
	return out, err
}

type TrendPoint struct {
	Hour  time.Time `json:"hour"`
	Count int64     `json:"sample_count"`
	Avg   *float64  `json:"avg"`
	Min   *float64  `json:"min"`
	Max   *float64  `json:"max"`
}

// Trend는 한 센서·지표의 하루를 1시간 단위로 요약한다(최대 24점). 이 테넌트의 센서가 아니면 빈 결과다.
func (r Reader) Trend(ctx context.Context, w Window, sensorID int, metric string) ([]TrendPoint, error) {
	if !slices.Contains(workers.KnownMetrics, metric) {
		return nil, fmt.Errorf("metric must be one of %v", workers.KnownMetrics)
	}
	var out []TrendPoint
	err := r.DB.WithContext(ctx).Raw(`SELECT date_trunc('hour', bucket, ?) AS hour, sum(sample_count)::bigint AS count,
			sum(value_avg * sample_count) / nullif(sum(sample_count), 0) AS avg, min(value_min) AS min, max(value_max) AS max
		FROM v_sensor_data_1m WHERE sensor_id = ? AND metric = ? AND bucket >= ? AND bucket < ?
		GROUP BY 1 ORDER BY 1`, w.Timezone, sensorID, metric, w.Start, w.End).Scan(&out).Error
	return out, err
}
