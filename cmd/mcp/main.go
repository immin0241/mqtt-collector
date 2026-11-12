// mcp는 테넌트 일일 리포트용 읽기 전용 MCP 서버다. 수집 바이너리·api 와 별도 프로세스다. (ADR-0014)
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/immin0241/webframework-project-mqtt-collector/internal/api"
	"github.com/immin0241/webframework-project-mqtt-collector/internal/report"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(log)
	if err := run(log); err != nil {
		log.Error("mcp exited", "error", err)
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

func envInt(key string, def int) int {
	if n, err := strconv.Atoi(os.Getenv(key)); err == nil && n > 0 {
		return n
	}
	return def
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var req [7]string
	for i, k := range []string{"MCP_DATABASE_URL", "MCP_ROLE_SECRET", "MCP_DB_HOST", "MCP_JWKS_URL", "MCP_ISSUER", "MCP_RESOURCE_URL", "MCP_METADATA_URL"} {
		v, err := must(k)
		if err != nil {
			return err
		}
		req[i] = v
	}
	dsn, secret, dbHost, jwksURL, issuer, resource, metaURL := req[0], req[1], req[2], req[3], req[4], req[5], req[6]

	loc, err := time.LoadLocation(env("MCP_REPORT_TZ", "Asia/Seoul"))
	if err != nil {
		return err
	}
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		return err
	}
	verifier, err := api.NewJWKSVerifier(ctx, jwksURL, issuer, env("MCP_AUDIENCE_PREFIX", "urn:logto:organization:"))
	if err != nil {
		return err
	}

	pools := &report.Pools{
		Host: dbHost, Port: env("MCP_DB_PORT", "5432"), DBName: env("MCP_DB_NAME", "go"), SSLMode: env("MCP_DB_SSLMODE", "disable"),
		Secret: secret, Max: envInt("MCP_MAX_TENANT_POOLS", 50), ConnsPerPool: envInt("MCP_CONNS_PER_POOL", 2),
	}
	defer pools.Close()

	svc := &report.Service{Tenants: report.GormTenants{DB: db}, DB: pools, Loc: loc, Log: log}
	httpSrv := &http.Server{
		Addr:              env("MCP_ADDR", ":8090"),
		Handler:           svc.Handler(verifier, resource, issuer, metaURL, env("MCP_SCOPE_REPORTS_READ", report.DefaultScope)),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	errc := make(chan error, 1)
	go func() { errc <- httpSrv.ListenAndServe() }()
	log.Info("mcp listening", "addr", httpSrv.Addr, "tz", loc.String())

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return httpSrv.Shutdown(shutdownCtx)
}
