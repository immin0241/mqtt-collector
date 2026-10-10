package workers

import (
	"context"
	"fmt"
	"log"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"gorm.io/gorm"
)

type SensorData struct {
	gorm.Model
	Topic string
	Value string
}

type DbPushWorker struct {
	DB             *gorm.DB
	MessageChannel chan mqtt.Message
	BufferChannel  chan mqtt.Message
}

func (worker *DbPushWorker) Run(ctx context.Context) error {
	for {
		select {
		case message := <-worker.MessageChannel:
			fmt.Printf("%s | Received payload: %s\n", time.Now().Format(time.RFC3339Nano), message.Payload())

			data := SensorData{Topic: message.Topic(), Value: string(message.Payload())}
			if err := worker.DB.WithContext(ctx).Create(&data).Error; err != nil {
				log.Printf("Failed to save sensor data: %v", err)
			}
		case <-ctx.Done():
			return nil
		}
	}
}
