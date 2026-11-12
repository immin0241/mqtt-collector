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

	// 센서 상태·경보 정책 (0 이면 기본값)
	HealthTimeoutFactor float64       // 통신 오류: 예상 주기 × k 무수신. 기본 2
	HealthGrace         time.Duration // 시작 직후 통신 오류 판정 유예
	HealthSeenInterval  time.Duration // last_seen 스냅샷 반영 주기. 기본 60s
	FlatlineCount       int           // 같은 값 연속 N회면 장애. 기본 10
	OutOfRangeCount     int           // 범위 밖 값 연속 N회면 장애. 기본 3
	AlertQueue          int           // 경보 후보 큐 크기. 기본 1000
	AlertRefresh        time.Duration // 경보 규칙 재로딩 주기. 기본 10s

	// 테넌트별 MQTT 세션 (ADR-0011)
	TenantRefresh     time.Duration // tenant 목록 재조회 주기. 기본 10s
	TenantChannelSize int           // 테넌트별 채널 크기. 기본 100
	MaxTenants        int           // 동시 세션 상한. 기본 50
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

	// metric 카탈로그에 KnownMetrics가 모두 있어야 기동한다. (마이그레이션 000011)
	store, err := workers.NewGormStore(ctx, db)
	if err != nil {
		return fmt.Errorf("init store: %w", err)
	}

	tracker := workers.NewHealthTrackerWithConfig(workers.HealthConfig{
		MinTimeout:      cfg.HealthMinTimeout,
		TimeoutFactor:   cfg.HealthTimeoutFactor,
		Grace:           cfg.HealthGrace,
		FlatlineCount:   cfg.FlatlineCount,
		OutOfRangeCount: cfg.OutOfRangeCount,
	})
	workers.RegisterSensorStates(tracker)

	if cfg.AlertQueue <= 0 {
		cfg.AlertQueue = 1000
	}
	alerts := workers.NewAlertEngine(cfg.AlertQueue)

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
		Alerts:        alerts,

		Tenants:           workers.GormTenantSource{DB: db},
		TenantRefresh:     cfg.TenantRefresh,
		TenantChannelSize: cfg.TenantChannelSize,
		MaxTenants:        cfg.MaxTenants,

		Unsubscribed: unsubscribed,
		Flushed:      flushed,
	}
	collector.CreateClient(ctx)

	pusher := &workers.DbPushWorker{
		Store:           store,
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
		SeenInterval: cfg.HealthSeenInterval,
		Refresh:      time.Minute,
	}
	if health.SeenInterval <= 0 {
		health.SeenInterval = time.Minute
	}

	alertWriter := &workers.AlertWriter{Store: workers.GormAlertStore{DB: db}, In: alerts.Out}
	alertLoader := &workers.AlertRuleLoader{DB: db, Engine: alerts, Refresh: cfg.AlertRefresh}

	g, gctx := errgroup.WithContext(ctx)
	if cfg.MetricsAddr != "" {
		g.Go(func() error { serveMetrics(gctx, cfg.MetricsAddr); return nil })
	}
	g.Go(func() error { return collector.Run(gctx) })
	g.Go(func() error { return pusher.Run(gctx) })
	g.Go(func() error { return health.Run(gctx) })
	g.Go(func() error { return alertWriter.Run(gctx) })
	g.Go(func() error { return alertLoader.Run(gctx) })
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
