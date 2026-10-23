package workers

import (
	"context"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
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
	// InsertRows는 한 문장(트랜잭션)으로 rows를 넣는다. 중복 키는 건너뛴다.
	InsertRows(ctx context.Context, rows []SensorData) error
	InsertRejected(ctx context.Context, row RejectedRow) error
}

type GormStore struct {
	DB *gorm.DB
}

func (s GormStore) InsertRows(ctx context.Context, rows []SensorData) error {
	if len(rows) == 0 {
		return nil
	}
	return s.DB.WithContext(ctx).
		Clauses(clause.OnConflict{DoNothing: true}).
		CreateInBatches(rows, len(rows)).Error
}

func (s GormStore) InsertRejected(ctx context.Context, row RejectedRow) error {
	return s.DB.WithContext(ctx).Create(&row).Error
}
