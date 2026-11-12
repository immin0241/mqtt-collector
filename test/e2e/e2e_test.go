//go:build e2e

package e2e

import (
	"os"
	"strconv"
	"testing"
	"time"
)

const channelCap = 600 // internal/app 기본 채널 상한

func envDuration(t *testing.T, key string, fallback time.Duration) time.Duration {
	t.Helper()
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		t.Fatalf("%s: %v", key, err)
	}
	return d
}

var sensors = []int{1, 2, 3, 4, 5}

// TC-02: DB를 10초 끊어도 panic이 없고, 재시도로 복구되며, 누락·중복이 0이고 메모리가 상한 안에 있다.
func TestTC02_DBOutage(t *testing.T) {
	e := newEnv(t, sensors...)
	e.startApp(t, e.appConfig("e2e-tc02"))
	time.Sleep(3 * time.Second)

	since := time.Now().Add(-time.Minute)
	pub := e.publisher(t, "e2e-tc02-pub")
	errsBefore := counter("db_errors_total")
	s := startSampler()
	l := startLoad(t, pub, sensors, 100) // 500 msg/s

	time.Sleep(5 * time.Second)
	e.pgProxy.Cut()
	t.Log("db cut")
	time.Sleep(10 * time.Second)
	e.pgProxy.Restore()
	t.Log("db restored")
	time.Sleep(5 * time.Second)

	l.stop()
	reports := e.waitReconciled(t, pub, since, 90*time.Second)
	s.stop()

	if got := counter("db_errors_total") - errsBefore; got <= 0 {
		t.Errorf("db_errors_total did not increase during outage")
	}
	if s.maxDepth > channelCap {
		t.Errorf("channel_depth max = %v, want <= %d", s.maxDepth, channelCap)
	}
	t.Logf("sent=%d channel_depth max=%v heap base=%dMiB max=%dMiB",
		l.sent.Load(), s.maxDepth, s.baseHeap>>20, s.maxHeap>>20)
	if grow := int64(s.maxHeap) - int64(s.baseHeap); grow > 256<<20 {
		t.Errorf("heap grew %d MiB during outage", grow>>20)
	}
	for _, r := range reports {
		if r.Missing != 0 || r.DBDuplicates != 0 {
			t.Errorf("sensor %d: %+v", r.SensorID, r)
		}
	}
}

// TC-03: 브로커를 10초 중지해도 앱이 재연결·재구독하고, 누락 0, DB 중복 0이다.
// 발행기는 PUBACK을 받은 메시지만 기록하고 실패분은 재시도하므로 발행 측 유실은 누락에 섞이지 않는다.
func TestTC03_BrokerOutage(t *testing.T) {
	e := newEnv(t, sensors...)
	e.startApp(t, e.appConfig("e2e-tc03"))
	time.Sleep(3 * time.Second)

	since := time.Now().Add(-time.Minute)
	pub := e.publisher(t, "e2e-tc03-pub")
	l := startLoad(t, pub, sensors, 100)

	time.Sleep(5 * time.Second)
	e.mqProxy.Cut()
	e.mq.Stop(t)
	t.Log("broker stopped")
	time.Sleep(10 * time.Second)
	e.mq.Start(t)
	e.mqProxy.SetUpstream(e.mq.Addr(t))
	e.mqProxy.Restore()
	t.Log("broker restarted")
	time.Sleep(10 * time.Second)

	l.stop()
	reports := e.waitReconciled(t, pub, since, 90*time.Second)
	t.Logf("sent=%d", l.sent.Load())
	for _, r := range reports {
		if r.Missing != 0 || r.DBDuplicates != 0 {
			t.Errorf("sensor %d: %+v", r.SensorID, r)
		}
	}
}

// TC-04: 부하 중 수집기를 SIGKILL 하고 재시작해도 누락이 0이고 DB 중복이 없다.
func TestTC04_WorkerSIGKILL(t *testing.T) {
	e := newEnv(t, sensors...)
	bin := buildCollector(t)
	dbURL := e.pg.DSNFor(e.pgProxy.Addr())

	start := func(port int) interface{ Kill() error } {
		cmd := collectorCmd(bin, dbURL, e.mqProxy.Addr(), "e2e-tc04", "127.0.0.1:"+strconv.Itoa(port))
		cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
		if err := cmd.Start(); err != nil {
			t.Fatalf("start collector: %v", err)
		}
		t.Cleanup(func() { _ = cmd.Process.Kill(); _, _ = cmd.Process.Wait() })
		return killer{cmd.Process}
	}

	first := start(19091)
	time.Sleep(4 * time.Second)

	since := time.Now().Add(-time.Minute)
	pub := e.publisher(t, "e2e-tc04-pub")
	l := startLoad(t, pub, sensors, 100)

	time.Sleep(5 * time.Second)
	if err := first.Kill(); err != nil { // SIGKILL
		t.Fatalf("kill: %v", err)
	}
	t.Log("collector killed")
	time.Sleep(3 * time.Second)
	start(19092)
	time.Sleep(8 * time.Second)

	l.stop()
	reports := e.waitReconciled(t, pub, since, 90*time.Second)
	t.Logf("sent=%d", l.sent.Load())
	for _, r := range reports {
		if r.Missing != 0 || r.DBDuplicates != 0 {
			t.Errorf("sensor %d: %+v", r.SensorID, r)
		}
	}
}

type killer struct{ p *os.Process }

func (k killer) Kill() error { return k.p.Kill() }

// TC-06: 1,000 msg/s 부하에 DB 지연을 주면 메모리가 상한 안에 머물고, 지연 해소 후 누락·중복이 0이다.
// 기본 지속 시간은 30초이며 E2E_TC06_DURATION=5m 으로 RFP 조건(5분)을 재현한다.
// DB 지연은 E2E_TC06_DB_LATENCY(기본 100ms, 방향당)로 바꾼다.
func TestTC06_SustainedLoadWithDBLatency(t *testing.T) {
	duration := envDuration(t, "E2E_TC06_DURATION", 30*time.Second)
	latency := envDuration(t, "E2E_TC06_DB_LATENCY", 100*time.Millisecond)

	ids := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	e := newEnv(t, ids...)
	e.startApp(t, e.appConfig("e2e-tc06"))
	time.Sleep(3 * time.Second)

	since := time.Now().Add(-time.Minute)
	pub := e.publisher(t, "e2e-tc06-pub")
	s := startSampler()
	dropped := brokerDropped(t, e.mq.Addr(t))
	// 대조가 실패(Fatal)해도 메모리·큐 지표와 브로커의 드롭 수는 남긴다. $SYS 값은 10초마다 갱신되어 12초 기다린 뒤 읽는다.
	defer func() {
		time.Sleep(12 * time.Second)
		s.stop()
		t.Logf("on exit: broker dropped=%s channel_depth max=%v heap base=%dMiB max=%dMiB",
			dropped(), s.maxDepth, s.baseHeap>>20, s.maxHeap>>20)
	}()

	e.pgProxy.SetLatency(latency)
	started := time.Now()
	l := startLoad(t, pub, ids, 100) // 1,000 msg/s
	time.Sleep(duration)
	l.stop()
	loadElapsed := time.Since(started)
	e.pgProxy.SetLatency(0)

	reports := e.waitReconciled(t, pub, since, 10*time.Minute)
	s.stop()

	t.Logf("conditions: sensors=%d target=1000msg/s duration=%s db_latency=%s/direction batch=300 flush=2s channel=%d",
		len(ids), duration, latency, channelCap)
	t.Logf("result: sent=%d (%.0f msg/s achieved) channel_depth max=%v heap base=%dMiB max=%dMiB drained_in=%s",
		l.sent.Load(), float64(l.sent.Load())/loadElapsed.Seconds(), s.maxDepth,
		s.baseHeap>>20, s.maxHeap>>20, time.Since(started)-loadElapsed)

	if s.maxDepth > channelCap {
		t.Errorf("channel_depth max = %v, want <= %d", s.maxDepth, channelCap)
	}
	if grow := int64(s.maxHeap) - int64(s.baseHeap); grow > 256<<20 {
		t.Errorf("heap grew %d MiB", grow>>20)
	}
	for _, r := range reports {
		if r.Missing != 0 || r.DBDuplicates != 0 {
			t.Errorf("sensor %d: %+v", r.SensorID, r)
		}
	}
}
