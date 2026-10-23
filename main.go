package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/immin0241/webframework-project-mqtt-collector/internal/app"
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

func intEnv(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		fatal("invalid int env", "key", key, "value", v)
	}
	return n
}

func floatEnv(key string, fallback float64) float64 {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil || f <= 0 {
		fatal("invalid float env", "key", key, "value", v)
	}
	return f
}

func main() {
	// 모든 로그를 JSON으로 출력한다. 표준 log 패키지 출력도 이 핸들러로 전달된다.
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	metricsAddr := os.Getenv("METRICS_ADDR")
	if metricsAddr == "" {
		metricsAddr = ":9090"
	}

	err := app.Run(ctx, app.Config{
		DatabaseURL:      os.Getenv("DATABASE_URL"),
		BrokerAddress:    os.Getenv("MQTT_BROKER"),
		ClientID:         os.Getenv("MQTT_CLIENT_ID"),
		Username:         os.Getenv("MQTT_USERNAME"),
		Password:         os.Getenv("MQTT_PASSWORD"),
		MetricsAddr:      metricsAddr,
		FlushInterval:    durationEnv("FLUSH_INTERVAL", 2*time.Second),
		HealthMinTimeout: durationEnv("HEALTH_MIN_TIMEOUT", 2*time.Second),
		HealthTick:       durationEnv("HEALTH_TICK", time.Second),

		HealthTimeoutFactor: floatEnv("HEALTH_TIMEOUT_FACTOR", 2),
		HealthGrace:         durationEnv("HEALTH_GRACE", 0),
		HealthSeenInterval:  durationEnv("HEALTH_SEEN_INTERVAL", time.Minute),
		FlatlineCount:       intEnv("FLATLINE_COUNT", 10),
		OutOfRangeCount:     intEnv("OUT_OF_RANGE_COUNT", 3),
		AlertRefresh:        durationEnv("ALERT_REFRESH", 10*time.Second),
	})
	if err != nil {
		fatal("collector stopped", "error", err)
	}
}
