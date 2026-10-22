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
	// 지속 세션은 고정 ClientID가 필요하다. 기본값을 두면 인스턴스끼리 세션을 빼앗으므로 필수로 받는다.
	clientID := os.Getenv("MQTT_CLIENT_ID")
	if clientID == "" {
		log.Fatal("MQTT_CLIENT_ID is required (persistent session needs a stable client id)")
	}

	// 채널 상한 600: 메모리 상한. 가득 차면 수신 핸들러가 막혀 백프레셔가 걸린다.
	data := make(chan workers.Envelope, 600)
	unsubscribed := make(chan struct{})
	flushed := make(chan struct{})

	sensorCollectionWorker := &workers.SensorCollectionWorker{
		BrokerAddress: os.Getenv("MQTT_BROKER"),
		ClientID:      clientID,
		Username:      os.Getenv("MQTT_USERNAME"),
		Password:      os.Getenv("MQTT_PASSWORD"),
		DataChannel:   data,
		Tracker:       tracker,
		Unsubscribed:  unsubscribed,
		Flushed:       flushed,
	}
	sensorCollectionWorker.CreateClient(ctx)

	dbPushWorker := &workers.DbPushWorker{
		Store:         workers.GormStore{DB: db},
		DataChannel:   data,
		BatchSize:     300,
		FlushInterval: durationEnv("FLUSH_INTERVAL", 2*time.Second),
		Unsubscribed:  unsubscribed,
		Flushed:       flushed,
	}

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
