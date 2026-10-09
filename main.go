package main

import (
	"context"
	"fmt"
	"log"
	"mosquitto-playground/workers"
	"os"
	"os/signal"
	"syscall"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"gorm.io/gorm"
)

type SensorData struct {
	gorm.Model
	Topic string
	Value string
}

var messagePubHandler mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
	fmt.Printf("TOPIC: %s, PAYLOAD: %s\n", msg.Topic(), msg.Payload())
}

func NewMessageHandler(db *gorm.DB) mqtt.MessageHandler {
	return func(client mqtt.Client, msg mqtt.Message) {
		fmt.Printf("TOPIC: %s, MESSAGE: %s\n", msg.Topic(), msg.Payload())

		var sensorData = SensorData{Topic: msg.Topic(), Value: string(msg.Payload())}
		db.Create(&sensorData)
	}
}

var connectHandler mqtt.OnConnectHandler = func(client mqtt.Client) {
	fmt.Println("Connected successfully to the broker!")
}

var connectLostHandler mqtt.ConnectionLostHandler = func(client mqtt.Client, err error) {
	log.Printf("Connection lost: %v", err)
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	sensorCollectionWorker := &workers.SensorCollectionWorker{BrokerAddress: os.Getenv("MQTT_BROKER")}
	dbPushWorker := &workers.DbPushWorker{}

	_ = sensorCollectionWorker.Run(ctx, []string{"sensor/tilt/*"})
	_ = dbPushWorker.Run(ctx)
}
