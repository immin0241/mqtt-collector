package workers

import (
	"context"
	"log"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	batchSize     = 300
	flushInterval = 2 * time.Second
)

type SensorData struct {
	Time        time.Time `gorm:"column:time" json:"time"`
	SensorID    int       `gorm:"column:sensor_id" json:"sensor_id"`
	Seq         int64     `json:"seq"`
	TiltX       *float64  `json:"tilt_x"`
	TiltY       *float64  `json:"tilt_y"`
	CrackWidth  *float64  `json:"crack_width"`
	Temperature *float64  `json:"temperature"`
	ReceivedAt  time.Time `json:"-"`
}

func (SensorData) TableName() string { return "sensor_data" }

type DbPushWorker struct {
	DB          *gorm.DB
	DataChannel chan SensorData
}

func (worker *DbPushWorker) Run(ctx context.Context) error {
	batch := make([]SensorData, 0, batchSize)

	// 배치의 첫 메시지가 들어온 시점부터 flushInterval 경과 시 flush
	timer := time.NewTimer(flushInterval)
	if !timer.Stop() {
		<-timer.C
	}

	flush := func(flushCtx context.Context) {
		if timer.Stop() == false {
			select {
			case <-timer.C:
			default:
			}
		}
		if len(batch) == 0 {
			return
		}
		err := worker.DB.WithContext(flushCtx).
			Clauses(clause.OnConflict{DoNothing: true}).
			CreateInBatches(batch, batchSize).Error
		if err != nil {
			log.Printf("Failed to bulk insert %d rows: %v", len(batch), err)
		}
		batch = batch[:0]
	}

	for {
		select {
		case data := <-worker.DataChannel:
			if len(batch) == 0 {
				timer.Reset(flushInterval)
			}
			batch = append(batch, data)

			if len(batch) >= batchSize {
				flush(ctx)
			}
		case <-timer.C:
			flush(ctx)
		case <-ctx.Done():
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			flush(shutdownCtx)
			cancel()
			return nil
		}
	}
}
