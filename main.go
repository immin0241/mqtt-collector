package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"mosquitto-playground/workers"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"golang.org/x/sync/errgroup"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := gorm.Open(postgres.Open(os.Getenv("DATABASE_URL")), &gorm.Config{})
	if err != nil {
		log.Fatalf("failed to connect database: %v", err)
	}

	messages := make(chan mqtt.Message, 600)

	sensorCollectionWorker := &workers.SensorCollectionWorker{
		BrokerAddress:  os.Getenv("MQTT_BROKER"),
		Username:       os.Getenv("MQTT_USERNAME"),
		Password:       os.Getenv("MQTT_PASSWORD"),
		MessageChannel: messages,
	}
	sensorCollectionWorker.CreateClient()

	dbPushWorker := &workers.DbPushWorker{DB: db, MessageChannel: messages}

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return sensorCollectionWorker.Run(gctx, []string{"sensors/#"}) })
	g.Go(func() error { return dbPushWorker.Run(gctx) })

	if err := g.Wait(); err != nil {
		log.Fatalf("worker stopped: %v", err)
	}
}
