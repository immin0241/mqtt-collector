// Package app은 수집 워커들을 조립해 실행한다. main은 환경 변수를 Config로 바꿔 Run만 호출한다.
// 통합 테스트도 같은 Run을 사용한다.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/sync/errgroup"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/immin0241/webframework-project-mqtt-collector/workers"
)

// Config는 Run에 필요한 모든 설정이다. 0값은 기본값을 뜻한다.
type Config struct {
	DatabaseURL string

	BrokerAddress string
	ClientID      string // 필수. 지속 세션 식별자
	Username      string
	Password      string

	// MetricsAddr가 비어 있으면 지표 서버를 띄우지 않는다.
	MetricsAddr string

	ChannelSize      int // 기본 600. 메모리 상한이다.
	BatchSize        int // 기본 300
	FlushInterval    time.Duration
	RetryInitial     time.Duration
	RetryMax         time.Duration
	ShutdownTimeout  time.Duration
	HealthMinTimeout time.Duration // 기본 2s
	HealthTick       time.Duration // 기본 1s
}

// Run은 ctx가 취소될 때까지 수집을 수행하고, 종료 절차(수신 중단 → drain → 최종 flush → ack → 연결 종료)를 마친 뒤 반환한다.
func Run(ctx context.Context, cfg Config) error {
	if cfg.ClientID == "" {
		return errors.New("ClientID is required (persistent session needs a stable client id)")
	}
	if cfg.ChannelSize <= 0 {
		cfg.ChannelSize = 600
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 300
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 2 * time.Second
	}
	if cfg.HealthMinTimeout <= 0 {
		cfg.HealthMinTimeout = 2 * time.Second
	}
	if cfg.HealthTick <= 0 {
		cfg.HealthTick = time.Second
	}

	db, err := gorm.Open(postgres.Open(cfg.DatabaseURL), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		return fmt.Errorf("connect database: %w", err)
	}
	if sqlDB, err := db.DB(); err == nil {
		defer sqlDB.Close()
	}

	tracker := workers.NewHealthTracker(cfg.HealthMinTimeout)

	// 채널 상한: 가득 차면 수신 핸들러가 막혀 백프레셔가 걸린다.
	data := make(chan workers.Envelope, cfg.ChannelSize)
	workers.RegisterChannelDepth(data)
	unsubscribed := make(chan struct{})
	flushed := make(chan struct{})

	collector := &workers.SensorCollectionWorker{
		BrokerAddress: cfg.BrokerAddress,
		ClientID:      cfg.ClientID,
		Username:      cfg.Username,
		Password:      cfg.Password,
		DataChannel:   data,
		Tracker:       tracker,
		Unsubscribed:  unsubscribed,
		Flushed:       flushed,
	}
	collector.CreateClient(ctx)

	pusher := &workers.DbPushWorker{
		Store:           workers.GormStore{DB: db},
		DataChannel:     data,
		BatchSize:       cfg.BatchSize,
		FlushInterval:   cfg.FlushInterval,
		RetryInitial:    cfg.RetryInitial,
		RetryMax:        cfg.RetryMax,
		ShutdownTimeout: cfg.ShutdownTimeout,
		Unsubscribed:    unsubscribed,
		Flushed:         flushed,
	}

	health := &workers.HealthWorker{
		DB:           db,
		Tracker:      tracker,
		Tick:         cfg.HealthTick,
		SeenInterval: 5 * time.Second,
		Refresh:      time.Minute,
	}

	g, gctx := errgroup.WithContext(ctx)
	if cfg.MetricsAddr != "" {
		g.Go(func() error { serveMetrics(gctx, cfg.MetricsAddr); return nil })
	}
	g.Go(func() error { return collector.Run(gctx) })
	g.Go(func() error { return pusher.Run(gctx) })
	g.Go(func() error { return health.Run(gctx) })
	return g.Wait()
}

// serveMetrics는 /metrics 를 노출한다. 실패해도 수집은 계속해야 하므로 오류는 로그만 남긴다.
func serveMetrics(ctx context.Context, addr string) {
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
