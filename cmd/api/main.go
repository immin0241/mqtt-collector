// api 는 경보 규칙 등록과 경보 상태 변경 HTTP 서비스다. 수집 바이너리(루트 main.go)와 별도 프로세스다. (ADR-0010)
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

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/immin0241/webframework-project-mqtt-collector/internal/api"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(log)

	if err := run(log); err != nil {
		log.Error("api exited", "error", err)
		os.Exit(1)
	}
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func must(key string) (string, error) {
	v := os.Getenv(key)
	if v == "" {
		return "", errors.New(key + " is required")
	}
	return v, nil
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	dsn, err := must("API_DATABASE_URL")
	if err != nil {
		return err
	}
	jwksURL, err := must("API_JWKS_URL")
	if err != nil {
		return err
	}
	issuer, err := must("API_ISSUER")
	if err != nil {
		return err
	}

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		return err
	}

	verifier, err := api.NewJWKSVerifier(ctx, jwksURL, issuer, env("API_AUDIENCE_PREFIX", "urn:logto:organization:"))
	if err != nil {
		return err
	}

	srv := &api.Server{
		DB:       db,
		Verifier: verifier,
		Log:      log,
		Scopes: api.Scopes{
			RulesWrite:       env("API_SCOPE_RULES_WRITE", api.DefaultScopes.RulesWrite),
			AlertsTransition: env("API_SCOPE_ALERTS_TRANSITION", api.DefaultScopes.AlertsTransition),
		},
	}
	httpSrv := &http.Server{
		Addr:              env("API_ADDR", ":8080"),
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
	}

	errc := make(chan error, 1)
	go func() { errc <- httpSrv.ListenAndServe() }()
	log.Info("api listening", "addr", httpSrv.Addr)

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return httpSrv.Shutdown(shutdownCtx)
}
