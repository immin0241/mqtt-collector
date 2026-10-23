package integration

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/immin0241/webframework-project-mqtt-collector/internal/app"
	"github.com/immin0241/webframework-project-mqtt-collector/internal/reconcile"
	"github.com/immin0241/webframework-project-mqtt-collector/internal/testenv"
)

// harness는 테스트 하나가 쓰는 TimescaleDB + Mosquitto 이다.
type harness struct {
	pg *testenv.Postgres
	mq *testenv.Mosquitto
}

func newHarness(t *testing.T, sensorIDs ...int) *harness {
	t.Helper()
	testenv.RequireDocker(t)
	h := &harness{pg: testenv.StartPostgres(t), mq: testenv.StartMosquitto(t)}
	h.pg.SeedSensors(t, sensorIDs...)
	return h
}

// runningApp은 app.Run을 고루틴으로 실행한 인스턴스다.
type runningApp struct {
	cancel context.CancelFunc
	done   chan error
}

func (h *harness) startApp(t *testing.T, clientID string) *runningApp {
	t.Helper()
	return h.startAppWith(t, clientID, nil)
}

// startAppWith는 기본 설정을 mutate로 바꿔서 앱을 띄운다.
func (h *harness) startAppWith(t *testing.T, clientID string, mutate func(*app.Config)) *runningApp {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	a := &runningApp{cancel: cancel, done: make(chan error, 1)}
	cfg := app.Config{
		DatabaseURL:   h.pg.DSN,
		BrokerAddress: "tcp://" + h.mq.Addr(t),
		ClientID:      clientID,
		FlushInterval: 200 * time.Millisecond,
		RetryInitial:  50 * time.Millisecond,
		RetryMax:      500 * time.Millisecond,
	}
	if mutate != nil {
		mutate(&cfg)
	}
	go func() {
		err := app.Run(ctx, cfg)
		if err != nil {
			slog.Error("app.Run returned", "client_id", clientID, "error", err)
		}
		a.done <- err
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-a.done:
		case <-time.After(30 * time.Second):
		}
	})
	return a
}

// stop은 SIGTERM과 같은 정상 종료를 요청하고 Run이 반환할 때까지 기다린다.
func (a *runningApp) stop(t *testing.T) time.Duration {
	t.Helper()
	start := time.Now()
	a.cancel()
	select {
	case err := <-a.done:
		if err != nil {
			t.Errorf("app.Run returned error on graceful shutdown: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Error("app.Run did not return within 30s of shutdown")
	}
	return time.Since(start)
}

// waitForSubscribe는 앱이 연결·구독을 마칠 때까지 기다린다. 지속 세션은 첫 구독 이후부터 메시지를 보관하므로
// 모든 테스트는 이 뒤에 발행을 시작한다.
func (h *harness) waitForSubscribe(t *testing.T) {
	t.Helper()
	time.Sleep(3 * time.Second)
}

// reconcileNow는 since 이후의 DB 현황을 발행 목록과 대조한다.
func (h *harness) reconcileNow(t *testing.T, pub reconcile.SeqCounts, since time.Time) ([]reconcile.SensorReport, reconcile.Snapshot) {
	t.Helper()
	ctx := context.Background()
	conn := h.pg.Conn(t)
	defer conn.Close(ctx)
	return reconcileWith(t, conn, pub, since)
}

func reconcileWith(t *testing.T, conn *pgx.Conn, pub reconcile.SeqCounts, since time.Time) ([]reconcile.SensorReport, reconcile.Snapshot) {
	t.Helper()
	snap, err := reconcile.LoadSnapshot(context.Background(), conn, since, time.Now().Add(time.Minute))
	if err != nil {
		t.Fatalf("load snapshot: %v", err)
	}
	return reconcile.Compare(pub, snap.Stored, snap.Rejected), snap
}

func allOK(reports []reconcile.SensorReport) bool {
	for _, r := range reports {
		if !r.OK() {
			return false
		}
	}
	return true
}

// waitReconciled는 대조 결과가 모두 OK가 될 때까지 기다린다. 시간 안에 되지 않으면 마지막 결과와 함께 실패한다.
func (h *harness) waitReconciled(t *testing.T, pub *testenv.Publisher, since time.Time, timeout time.Duration) []reconcile.SensorReport {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		reports, _ := h.reconcileNow(t, pub.Published(), since)
		if allOK(reports) {
			return reports
		}
		if time.Now().After(deadline) {
			if os.Getenv("MOSQUITTO_DEBUG") == "1" {
				t.Logf("broker log:\n%s", h.mq.Logs(t))
			}
			t.Fatalf("not reconciled within %s: %+v", timeout, reports)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// counter는 프로세스 전역 Prometheus 카운터의 현재 값(모든 라벨 합)이다.
func counter(t *testing.T, name string) float64 {
	t.Helper()
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	var sum float64
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			sum += m.GetCounter().GetValue()
		}
	}
	return sum
}
