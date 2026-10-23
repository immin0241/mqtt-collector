package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/immin0241/webframework-project-mqtt-collector/workers"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/sync/errgroup"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// fatal은 구조화 로그를 남기고 종료한다.
func fatal(msg string, args ...any) {
	slog.Error(msg, args...)
	os.Exit(1)
}

func durationEnv(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		fatal("invalid duration env", "key", key, "value", v)
	}
	return d
}

func main() {
	// 모든 로그를 JSON으로 출력한다. 표준 log 패키지 출력도 이 핸들러로 전달된다.
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := gorm.Open(postgres.Open(os.Getenv("DATABASE_URL")), &gorm.Config{})
	if err != nil {
		fatal("failed to connect database", "error", err)
	}

	tracker := workers.NewHealthTracker(durationEnv("HEALTH_MIN_TIMEOUT", 2*time.Second))
	// 지속 세션은 고정 ClientID가 필요하다. 기본값을 두면 인스턴스끼리 세션을 빼앗으므로 필수로 받는다.
	clientID := os.Getenv("MQTT_CLIENT_ID")
	if clientID == "" {
		fatal("MQTT_CLIENT_ID is required (persistent session needs a stable client id)")
	}

	// 채널 상한 600: 메모리 상한. 가득 차면 수신 핸들러가 막혀 백프레셔가 걸린다.
	data := make(chan workers.Envelope, 600)
	workers.RegisterChannelDepth(data)
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
	go serveMetrics(gctx, os.Getenv("METRICS_ADDR"))
	g.Go(func() error { return sensorCollectionWorker.Run(gctx) })
	g.Go(func() error { return dbPushWorker.Run(gctx) })
	g.Go(func() error { return healthWorker.Run(gctx) })

	if err := g.Wait(); err != nil {
		fatal("worker stopped", "error", err)
	}
}

// serveMetrics는 /metrics 를 노출한다. 실패해도 수집은 계속해야 하므로 오류는 로그만 남긴다.
func serveMetrics(ctx context.Context, addr string) {
	if addr == "" {
		addr = ":9090"
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	slog.Info("metrics server listening", "addr", addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("metrics server stopped", "error", err)
	}
}
