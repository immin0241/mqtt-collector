package workers

import (
	"context"
	"fmt"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type DbPushWorker struct {
	messageChannel chan mqtt.Message
}

func (worker *DbPushWorker) Run(ctx context.Context) error {
	for {
		select {
		case message := <-worker.messageChannel:
			fmt.Printf("%s | Received payload: %s", time.Now().Format(time.RFC3339Nano), message.Payload())
		case <-ctx.Done():
			return nil
		}
	}
}
