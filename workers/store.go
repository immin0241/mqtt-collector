package workers

import (
	"context"
	"fmt"
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

// maxRowsPerStatement는 한 INSERT에 넣는 측정 행의 상한이다. 파라미터가 행당 6개라 PostgreSQL의 65,535개 제한 안에 둔다.
const maxRowsPerStatement = 10000

type GormStore struct {
	DB      *gorm.DB
	metrics map[string]int16 // metric.name -> metric.id
}

// NewGormStore는 metric 카탈로그를 읽고 KnownMetrics가 모두 있는지 확인한다. 없으면 기동하지 않는다.
// (없는 채로 두면 해당 메시지가 영원히 저장되지 못한다)
func NewGormStore(ctx context.Context, db *gorm.DB) (*GormStore, error) {
	type metricRow struct {
		ID   int16
		Name string
	}
	var rows []metricRow
	if err := db.WithContext(ctx).Raw(`SELECT id, name FROM metric`).Scan(&rows).Error; err != nil {
		return nil, fmt.Errorf("load metric catalog: %w", err)
	}
	s := &GormStore{DB: db, metrics: make(map[string]int16, len(rows))}
	for _, r := range rows {
		s.metrics[r.Name] = r.ID
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
