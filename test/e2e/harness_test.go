//go:build e2e

// Package e2e는 장애 주입 테스트(TC-02, 03, 04, 06)다. `go test -tags e2e ./test/e2e` 로 실행한다.
// DB와 브로커 앞에 testenv.FaultProxy를 두어 연결 차단·지연을 주입하고, 판정은 internal/reconcile 을 쓴다.
package e2e

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/immin0241/webframework-project-mqtt-collector/internal/app"
	"github.com/immin0241/webframework-project-mqtt-collector/internal/reconcile"
	"github.com/immin0241/webframework-project-mqtt-collector/internal/testenv"
)

// env는 DB·브로커 컨테이너와 각각의 앞단 프록시다. 앱과 발행기는 항상 프록시로 접속한다.
type env struct {
	pg      *testenv.Postgres
	mq      *testenv.Mosquitto
	pgProxy *testenv.FaultProxy
	mqProxy *testenv.FaultProxy
}

func newEnv(t *testing.T, sensorIDs ...int) *env {
	t.Helper()
	testenv.RequireDocker(t)
	e := &env{pg: testenv.StartPostgres(t), mq: testenv.StartMosquitto(t)}
	e.pg.SeedSensors(t, sensorIDs...)
	e.pgProxy = testenv.NewFaultProxy(t, e.pg.Addr)
	e.mqProxy = testenv.NewFaultProxy(t, e.mq.Addr(t))
	return e
}

func (e *env) appConfig(clientID string) app.Config {
	return app.Config{
		DatabaseURL:   e.pg.DSNFor(e.pgProxy.Addr()),
		BrokerAddress: "tcp://" + e.mqProxy.Addr(),
		ClientID:      clientID,
		RetryInitial:  200 * time.Millisecond,
		RetryMax:      2 * time.Second,
	}
}

// startApp은 app.Run을 같은 프로세스에서 실행한다. 반환된 함수로 정상 종료한다.
func (e *env) startApp(t *testing.T, cfg app.Config) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- app.Run(ctx, cfg) }()
	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			select {
			case <-done:
			case <-time.After(60 * time.Second):
				t.Error("app.Run did not return within 60s")
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

// publisher는 브로커 프록시로 접속한 발행기다.
func (e *env) publisher(t *testing.T, id string) *testenv.Publisher {
	return testenv.NewPublisher(t, e.mqProxy.Addr(), id)
}

// load는 센서마다 초당 perSensorRate 건을 발행하는 부하다. PUBACK을 받은 것만 발행 목록에 기록된다.
type load struct {
	cancel context.CancelFunc
	wg     sync.WaitGroup
	sent   atomic.Int64
}

func startLoad(t *testing.T, pub *testenv.Publisher, ids []int, perSensorRate int) *load {
	ctx, cancel := context.WithCancel(context.Background())
	l := &load{cancel: cancel}
	interval := time.Second / time.Duration(perSensorRate)
	for _, id := range ids {
		l.wg.Add(1)
		go func(id int) {
			defer l.wg.Done()
			tick := time.NewTicker(interval)
			defer tick.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-tick.C:
				}
				if _, err := pub.Next(ctx, id); err == nil {
					l.sent.Add(1)
				}
			}
		}(id)
	}
	t.Cleanup(l.stop)
	return l
}

func (l *load) stop() { l.cancel(); l.wg.Wait() }

// sampler는 channel_depth 와 힙 사용량의 최댓값을 주기적으로 기록한다.
type sampler struct {
	cancel   context.CancelFunc
	done     chan struct{}
	maxDepth float64
	baseHeap uint64
	maxHeap  uint64
}

func startSampler() *sampler {
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	ctx, cancel := context.WithCancel(context.Background())
	s := &sampler{cancel: cancel, done: make(chan struct{}), baseHeap: ms.HeapInuse}
	go func() {
		defer close(s.done)
		tick := time.NewTicker(100 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
			if d, ok := gather("channel_depth"); ok && d > s.maxDepth {
				s.maxDepth = d
			}
			runtime.ReadMemStats(&ms)
			if ms.HeapInuse > s.maxHeap {
				s.maxHeap = ms.HeapInuse
			}
		}
	}()
	return s
}

func (s *sampler) stop() { s.cancel(); <-s.done }

// gather는 이름이 같은 카운터·게이지의 합이다. 없으면 found=false.
func gather(name string) (sum float64, found bool) {
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		return 0, false
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			found = true
			switch {
			case m.Counter != nil:
				sum += m.GetCounter().GetValue()
			case m.Gauge != nil:
				sum += m.GetGauge().GetValue()
			}
		}
	}
	return sum, found
}

func counter(name string) float64 { v, _ := gather(name); return v }

// waitReconciled는 대조 결과가 모두 OK가 될 때까지 기다리고 마지막 결과를 돌려준다.
func (e *env) waitReconciled(t *testing.T, pub *testenv.Publisher, since time.Time, timeout time.Duration) []reconcile.SensorReport {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		conn := e.pg.Conn(t)
		snap, err := reconcile.LoadSnapshot(context.Background(), conn, since, time.Now().Add(time.Minute))
		conn.Close(context.Background())
		if err != nil {
			t.Fatalf("load snapshot: %v", err)
		}
		reports := reconcile.Compare(pub.Published(), snap.Stored, snap.Rejected)
		ok := len(reports) > 0
		for _, r := range reports {
			ok = ok && r.OK()
		}
		if ok {
			return reports
		}
		if time.Now().After(deadline) {
			for _, r := range reports {
				t.Errorf("sensor %d: published=%d stored=%d missing=%d unexpected=%d dbdup=%d",
					r.SensorID, r.Published, r.Stored, r.Missing, r.Unexpected, r.DBDuplicates)
			}
			t.Fatalf("not reconciled within %s", timeout)
		}
		time.Sleep(time.Second)
	}
}

// buildCollector는 수집기 바이너리를 빌드한다. TC-04가 하위 프로세스를 SIGKILL 하는 데 쓴다.
func buildCollector(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "collector")
	cmd := exec.Command("go", "build", "-o", bin, ".")
	cmd.Dir = testenv.RepoRoot()
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}
	return bin
}

func collectorCmd(bin, dbURL, brokerAddr, clientID, metricsAddr string) *exec.Cmd {
	cmd := exec.Command(bin)
	cmd.Env = append(os.Environ(),
		"DATABASE_URL="+dbURL,
		"MQTT_BROKER=tcp://"+brokerAddr,
		"MQTT_CLIENT_ID="+clientID,
		"METRICS_ADDR="+metricsAddr,
	)
	return cmd
}
