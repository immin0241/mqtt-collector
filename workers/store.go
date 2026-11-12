package workers

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"gorm.io/gorm"
)

// RejectedRow는 sensor_data_rejected 한 행이다. rejected_at은 DB 기본값(now())을 쓴다.
type RejectedRow struct {
	ReceivedAt   time.Time
	Topic        string
	Payload      []byte
	SensorID     *int
	Seq          *int64
	ErrorCode    string
	ErrorMessage string
}

func (RejectedRow) TableName() string { return "sensor_data_rejected" }

// Store는 DbPushWorker가 쓰는 저장소다. 테스트에서 대체할 수 있도록 분리했다.
type Store interface {
	// InsertRows는 한 문장(트랜잭션)으로 rows를 넣는다. 중복 키는 건너뛰며, 새로 저장된 메시지 수를 돌려준다.
	// (메시지 단위다. 한 메시지가 측정 행 여러 개로 펼쳐져도 하나로 센다.)
	InsertRows(ctx context.Context, rows []SensorData) (inserted int64, err error)
	InsertRejected(ctx context.Context, row RejectedRow) error
}

// NoMeasurementCode는 측정값이 하나도 없는 메시지를 격리할 때 쓰는 SQLSTATE다.
// 22(data exception)라 영구 오류로 분류되어 이분법으로 그 메시지만 격리 테이블에 간다.
const NoMeasurementCode = "22023"

// 데이터 검증에 실패한 메시지를 격리할 때 쓰는 SQLSTATE다. 모두 22(data exception)라 영구 오류로 분류된다. (ADR-0013)
const (
	OutOfRangeCode = "22003" // numeric_value_out_of_range: 지표의 정상 범위(metric.min_value/max_value) 밖
	BadTimeCode    = "22008" // datetime_field_overflow: time 이 없거나(0) 비현실적
)

// 측정 시각의 허용 범위. 늦게 도착한 데이터(통신 두절 뒤 일괄 전송)는 정상이므로 과거 쪽은 느슨하게 둔다. (ADR-0013)
var (
	minValidTime  = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	maxFutureSkew = 24 * time.Hour // received_at 보다 이만큼 넘게 미래면 센서 시계 오류로 본다
)

// maxRowsPerStatement는 한 INSERT에 넣는 측정 행의 상한이다. 파라미터가 행당 6개라 PostgreSQL의 65,535개 제한 안에 둔다.
const maxRowsPerStatement = 10000

type GormStore struct {
	DB      *gorm.DB
	metrics map[string]int16         // metric.name -> metric.id
	ranges  map[string]PhysicalRange // metric.name -> 정상 범위 (NULL 은 ±Inf)
}

// Ranges는 카탈로그의 지표별 정상 범위다. 센서 상태 판정(HealthConfig.Ranges)도 같은 값을 써야 정의가 갈라지지 않는다.
func (s *GormStore) Ranges() map[string]PhysicalRange {
	out := make(map[string]PhysicalRange, len(s.ranges))
	for k, v := range s.ranges {
		out[k] = v
	}
	return out
}

// validate는 메시지 하나의 시각과 측정값이 정상인지 본다. 문제가 있으면 영구 오류(SQLSTATE 22)를 돌려준다.
func (s *GormStore) validate(d SensorData, ms []Measurement) error {
	if d.Time.Before(minValidTime) || (!d.ReceivedAt.IsZero() && d.Time.After(d.ReceivedAt.Add(maxFutureSkew))) {
		return &pgconn.PgError{Code: BadTimeCode, Message: fmt.Sprintf("time %s is outside the valid window", d.Time.UTC().Format(time.RFC3339))}
	}
	for _, m := range ms {
		if r, ok := s.ranges[m.Metric]; ok && (m.Value < r.Min || m.Value > r.Max) {
			return &pgconn.PgError{Code: OutOfRangeCode, Message: fmt.Sprintf("%s=%g is outside the range %g..%g", m.Metric, m.Value, r.Min, r.Max)}
		}
	}
	return nil
}

// NewGormStore는 metric 카탈로그를 읽고 KnownMetrics가 모두 있는지 확인한다. 없으면 기동하지 않는다.
// (없는 채로 두면 해당 메시지가 영원히 저장되지 못한다)
func NewGormStore(ctx context.Context, db *gorm.DB) (*GormStore, error) {
	type metricRow struct {
		ID       int16
		Name     string
		MinValue *float64
		MaxValue *float64
	}
	var rows []metricRow
	if err := db.WithContext(ctx).Raw(`SELECT id, name, min_value, max_value FROM metric`).Scan(&rows).Error; err != nil {
		return nil, fmt.Errorf("load metric catalog: %w", err)
	}
	s := &GormStore{DB: db, metrics: make(map[string]int16, len(rows)), ranges: make(map[string]PhysicalRange, len(rows))}
	for _, r := range rows {
		s.metrics[r.Name] = r.ID
		lo, hi := math.Inf(-1), math.Inf(1)
		if r.MinValue != nil {
			lo = *r.MinValue
		}
		if r.MaxValue != nil {
			hi = *r.MaxValue
		}
		s.ranges[r.Name] = PhysicalRange{Min: lo, Max: hi}
	}
	for _, name := range KnownMetrics {
		if _, ok := s.metrics[name]; !ok {
			return nil, fmt.Errorf("metric %q is missing from the metric catalog", name)
		}
	}
	return s, nil
}

func (s *GormStore) InsertRows(ctx context.Context, rows []SensorData) (int64, error) {
	if len(rows) == 0 {
		return 0, nil
	}

	type row struct {
		time       time.Time
		sensorID   int
		seq        int64
		metricID   int16
		value      float64
		receivedAt time.Time
	}
	flat := make([]row, 0, len(rows)*2)
	for _, r := range rows {
		ms := r.Measurements()
		if len(ms) == 0 {
			return 0, &pgconn.PgError{Code: NoMeasurementCode, Message: "message has no measurement"}
		}
		if err := s.validate(r, ms); err != nil {
			return 0, err
		}
		for _, m := range ms {
			flat = append(flat, row{r.Time, r.SensorID, r.Seq, s.metrics[m.Metric], m.Value, r.ReceivedAt})
		}
	}

	type key struct {
		sensorID int
		seq      int64
		nano     int64
	}
	stored := make(map[key]struct{}, len(rows))

	err := s.DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for start := 0; start < len(flat); start += maxRowsPerStatement {
			chunk := flat[start:min(start+maxRowsPerStatement, len(flat))]
			var sb strings.Builder
			sb.WriteString("INSERT INTO sensor_data (time, sensor_id, seq, metric_id, value, received_at) VALUES ")
			args := make([]any, 0, len(chunk)*6)
			for i, r := range chunk {
				if i > 0 {
					sb.WriteByte(',')
				}
				sb.WriteString("(?,?,?,?,?,?)")
				args = append(args, r.time, r.sensorID, r.seq, r.metricID, r.value, r.receivedAt)
			}
			sb.WriteString(" ON CONFLICT DO NOTHING RETURNING sensor_id, seq, time")

			res, err := tx.Raw(sb.String(), args...).Rows()
			if err != nil {
				return err
			}
			for res.Next() {
				var id int
				var seq int64
				var t time.Time
				if err := res.Scan(&id, &seq, &t); err != nil {
					_ = res.Close()
					return err
				}
				stored[key{id, seq, t.UnixNano()}] = struct{}{}
			}
			if err := res.Err(); err != nil {
				_ = res.Close()
				return err
			}
			_ = res.Close()
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return int64(len(stored)), nil
}

func (s *GormStore) InsertRejected(ctx context.Context, row RejectedRow) error {
	return s.DB.WithContext(ctx).Create(&row).Error
}
