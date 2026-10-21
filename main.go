package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"mosquitto-playground/workers"

	"golang.org/x/sync/errgroup"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func durationEnv(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		log.Fatalf("invalid %s=%q", key, v)
	}
	return d
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := gorm.Open(postgres.Open(os.Getenv("DATABASE_URL")), &gorm.Config{})
	if err != nil {
		log.Fatalf("failed to connect database: %v", err)
	}

	tracker := workers.NewHealthTracker(durationEnv("HEALTH_MIN_TIMEOUT", 2*time.Second))
	data := make(chan workers.SensorData, 600)

	sensorCollectionWorker := &workers.SensorCollectionWorker{
		BrokerAddress: os.Getenv("MQTT_BROKER"),
		Username:      os.Getenv("MQTT_USERNAME"),
		Password:      os.Getenv("MQTT_PASSWORD"),
		DataChannel:   data,
		Tracker:       tracker,
	}
	sensorCollectionWorker.CreateClient(ctx)

	dbPushWorker := &workers.DbPushWorker{DB: db, DataChannel: data}

	healthWorker := &workers.HealthWorker{
		DB:           db,
		Tracker:      tracker,
		Tick:         durationEnv("HEALTH_TICK", time.Second),
		SeenInterval: 5 * time.Second,
		Refresh:      time.Minute,
	}

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return sensorCollectionWorker.Run(gctx) })
	g.Go(func() error { return dbPushWorker.Run(gctx) })
	g.Go(func() error { return healthWorker.Run(gctx) })

	if err := g.Wait(); err != nil {
		log.Fatalf("worker stopped: %v", err)
	}
}
