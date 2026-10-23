package workers

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

func sp(s string) *string { return &s }
func ip(i int) *int       { return &i }

func thresholdRule(id int64, sensor int, metric, op string, th float64) AlertRule {
	return AlertRule{ID: id, TenantID: 1, SensorID: ip(sensor), Metric: metric, Kind: RuleThreshold, Operator: sp(op), Threshold: th, Enabled: true}
}

func crack(id int, seq int64, at time.Time, v float64) SensorData {
	return SensorData{SensorID: id, Seq: seq, Time: at, ReceivedAt: at, CrackWidth: f64(v)}
}

func drain(e *AlertEngine) []AlertCandidate {
	var out []AlertCandidate
	for {
		select {
		case c := <-e.Out:
			out = append(out, c)
		default:
			return out
		}
	}
}

func seqs(cs []AlertCandidate) []int64 {
	var out []int64
	for _, c := range cs {
		out = append(out, c.Seq)
	}
	return out
}

func equalSeqs(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestThresholdFiresOnceWhileBreachContinues(t *testing.T) {
	e := NewAlertEngine(100)
	e.SetRules([]AlertRule{thresholdRule(1, 7, "crack_width", "gt", 3.0)})

	for i, v := range []float64{1, 5, 6, 7, 2, 4} { // 정상, 위반(발화), 계속, 계속, 해소, 재위반(발화)
		e.Evaluate(crack(7, int64(i), t0.Add(time.Duration(i)*time.Second), v))
	}
	got := seqs(drain(e))
	if !equalSeqs(got, []int64{1, 5}) {
		t.Fatalf("fired at seq %v, want [1 5]", got)
	}
}

func TestLessThanOperator(t *testing.T) {
	e := NewAlertEngine(10)
	e.SetRules([]AlertRule{thresholdRule(1, 7, "crack_width", "lt", 0.5)})
	e.Evaluate(crack(7, 0, t0, 1.0))
	e.Evaluate(crack(7, 1, t0, 0.2))
	if got := seqs(drain(e)); !equalSeqs(got, []int64{1}) {
		t.Fatalf("fired at %v", got)
	}
}

func TestCandidateCarriesReceiveTimeAndValue(t *testing.T) {
	e := NewAlertEngine(10)
	e.SetRules([]AlertRule{thresholdRule(9, 7, "crack_width", "gt", 3.0)})
	d := crack(7, 4, t0, 8.5)
	d.ReceivedAt = t0.Add(123 * time.Millisecond)
	e.Evaluate(d)
	c := drain(e)[0]
	if c.RuleID != 9 || c.SensorID != 7 || c.Seq != 4 || c.Value != 8.5 || !c.TriggeredAt.Equal(d.ReceivedAt) || !c.DataTime.Equal(t0) {
		t.Fatalf("candidate = %+v", c)
	}
}

func TestRedeliveredMessageDoesNotFireTwice(t *testing.T) {
	e := NewAlertEngine(10)
	e.SetRules([]AlertRule{thresholdRule(1, 7, "crack_width", "gt", 3.0)})
	d := crack(7, 1, t0, 9.0)
	e.Evaluate(d)
	e.Evaluate(d) // 브로커 재전달 / 중복 전송
	e.Evaluate(d)
	if n := len(drain(e)); n != 1 {
		t.Fatalf("fired %d times, want 1", n)
	}
}

func TestRateOfChangeUsesWindow(t *testing.T) {
	e := NewAlertEngine(10)
	e.SetRules([]AlertRule{{ID: 2, TenantID: 1, SensorID: ip(7), Metric: "crack_width", Kind: RuleRateOfChange,
		Threshold: 1.0, WindowSeconds: ip(10), Enabled: true}})

	// 초당 0.2 증가: 10초 창 안에서 처음 변화량이 1.0 을 넘는 것은 seq 6 (1.2)
	for i := 0; i < 12; i++ {
		e.Evaluate(crack(7, int64(i), t0.Add(time.Duration(i)*time.Second), 0.2*float64(i)))
	}
	if got := seqs(drain(e)); !equalSeqs(got, []int64{6}) {
		t.Fatalf("fired at %v, want [6]", got)
	}
}

func TestRateOfChangeIgnoresSlowDrift(t *testing.T) {
	e := NewAlertEngine(10)
	e.SetRules([]AlertRule{{ID: 2, TenantID: 1, SensorID: ip(7), Metric: "crack_width", Kind: RuleRateOfChange,
		Threshold: 1.0, WindowSeconds: ip(10), Enabled: true}})
	// 초당 0.05: 10초 창 안 변화는 0.5 를 넘지 않는다. 오래 지속돼도 창 밖 값은 비교하지 않는다.
	for i := 0; i < 300; i++ {
		e.Evaluate(crack(7, int64(i), t0.Add(time.Duration(i)*time.Second), 0.05*float64(i)))
	}
	if n := len(drain(e)); n != 0 {
		t.Fatalf("slow drift fired %d alerts", n)
	}
}

func TestRateOfChangeFallingCounts(t *testing.T) {
	e := NewAlertEngine(10)
	e.SetRules([]AlertRule{{ID: 2, TenantID: 1, SensorID: ip(7), Metric: "crack_width", Kind: RuleRateOfChange,
		Threshold: 1.0, WindowSeconds: ip(5), Enabled: true}})
	e.Evaluate(crack(7, 0, t0, 5.0))
	e.Evaluate(crack(7, 1, t0.Add(time.Second), 3.0)) // -2.0
	if got := seqs(drain(e)); !equalSeqs(got, []int64{1}) {
		t.Fatalf("fired at %v", got)
	}
}

func TestRateOfChangeSamplesAreBounded(t *testing.T) {
	e := NewAlertEngine(10)
	e.SetRules([]AlertRule{{ID: 2, TenantID: 1, SensorID: ip(7), Metric: "crack_width", Kind: RuleRateOfChange,
		Threshold: 1e9, WindowSeconds: ip(86400), Enabled: true}})
	for i := 0; i < maxSamples*3; i++ {
		e.Evaluate(crack(7, int64(i), t0.Add(time.Duration(i)*time.Millisecond), 1))
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if n := len(e.state[stateKey{2, 7}].samples); n > maxSamples {
		t.Fatalf("samples = %d, want <= %d", n, maxSamples)
	}
}

func TestRuleTargeting(t *testing.T) {
	e := NewAlertEngine(100)
	e.SetSensors(map[int]SensorInfo{
		1: {TenantID: 1, Type: "crack"},
		2: {TenantID: 1, Type: "crack"},
		3: {TenantID: 2, Type: "crack"}, // 다른 테넌트
		4: {TenantID: 1, Type: "env"},   // 다른 타입
	})
	disabled := thresholdRule(30, 1, "crack_width", "gt", 0)
	disabled.Enabled = false
	e.SetRules([]AlertRule{
		{ID: 10, TenantID: 1, SensorType: sp("crack"), Metric: "crack_width", Kind: RuleThreshold, Operator: sp("gt"), Threshold: 3, Enabled: true},
		thresholdRule(20, 2, "crack_width", "gt", 100), // 센서 2 전용, 임계치 높음
		disabled,
	})

	for id := 1; id <= 4; id++ {
		d := crack(id, 1, t0, 5.0)
		if id == 4 {
			d = SensorData{SensorID: 4, Seq: 1, Time: t0, ReceivedAt: t0, Temperature: f64(5)}
		}
		e.Evaluate(d)
	}
	got := map[int]int64{}
	for _, c := range drain(e) {
		got[c.SensorID] = c.RuleID
	}
	if len(got) != 2 || got[1] != 10 || got[2] != 10 {
		t.Fatalf("fired = %v, want sensors 1,2 by rule 10 only", got)
	}
}

func TestMissingMetricIsSkipped(t *testing.T) {
	e := NewAlertEngine(10)
	e.SetRules([]AlertRule{thresholdRule(1, 7, "crack_width", "gt", 3.0)})
	e.Evaluate(SensorData{SensorID: 7, Seq: 1, Time: t0, ReceivedAt: t0, TiltX: f64(99)}) // 다른 채널
	if n := len(drain(e)); n != 0 {
		t.Fatalf("fired %d on a different channel", n)
	}
}

func TestReloadKeepsStateUnlessRuleChanged(t *testing.T) {
	e := NewAlertEngine(10)
	r := thresholdRule(1, 7, "crack_width", "gt", 3.0)
	e.SetRules([]AlertRule{r})
	e.Evaluate(crack(7, 1, t0, 9))
	drain(e)

	e.SetRules([]AlertRule{r}) // 주기적 재로딩: 정의가 같으면 위반 중 상태를 유지한다
	e.Evaluate(crack(7, 2, t0, 9))
	if n := len(drain(e)); n != 0 {
		t.Fatalf("reload re-fired %d", n)
	}

	r.Threshold = 4.0 // 정의 변경: 새 규칙으로 취급한다
	e.SetRules([]AlertRule{r})
	e.Evaluate(crack(7, 3, t0, 9))
	if n := len(drain(e)); n != 1 {
		t.Fatalf("changed rule fired %d, want 1", n)
	}
}

func TestFullQueueDropsInsteadOfBlockingHandler(t *testing.T) {
	e := NewAlertEngine(1)
	e.SetRules([]AlertRule{thresholdRule(1, 7, "crack_width", "gt", 3.0)})

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 10; i++ { // 위반/해소를 반복해 발화를 여러 번 만든다
			e.Evaluate(crack(7, int64(2*i), t0, 9))
			e.Evaluate(crack(7, int64(2*i+1), t0, 1))
		}
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Evaluate blocked on a full alert queue")
	}
	if n := len(drain(e)); n != 1 {
		t.Fatalf("queue held %d, want 1", n)
	}
}

// ----- AlertWriter -----

type fakeAlertStore struct {
	mu        sync.Mutex
	seen      map[[3]int64]bool
	failFirst int32 // 처음 N번은 일시 오류
	calls     atomic.Int32
	badRule   int64 // 이 규칙의 행이 있으면 FK 위반
}

func (s *fakeAlertStore) InsertAlerts(ctx context.Context, rows []AlertEventRow) (int64, error) {
	s.calls.Add(1)
	if s.calls.Load() <= s.failFirst {
		return 0, errors.New("connection refused")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.seen == nil {
		s.seen = map[[3]int64]bool{}
	}
	for _, r := range rows {
		if s.badRule != 0 && r.RuleID == s.badRule {
			return 0, &pgconn.PgError{Code: "23503"}
		}
	}
	var n int64
	for _, r := range rows {
		k := [3]int64{r.RuleID, int64(r.SensorID), r.Seq}
		if !s.seen[k] {
			s.seen[k] = true
			n++
		}
	}
	return n, nil
}

func (s *fakeAlertStore) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.seen)
}

func startWriter(t *testing.T, store AlertStore, in chan AlertCandidate) (stop func()) {
	t.Helper()
	w := &AlertWriter{Store: store, In: in, RetryInitial: time.Millisecond, RetryMax: 5 * time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = w.Run(ctx); close(done) }()
	return func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("writer did not stop")
		}
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func cand(rule int64, sensor int, seq int64) AlertCandidate {
	return AlertCandidate{RuleID: rule, SensorID: sensor, Seq: seq, DataTime: t0, TriggeredAt: time.Now(), Value: 1}
}

func TestWriterStoresAndDedupsRedeliveredAlerts(t *testing.T) {
	store := &fakeAlertStore{}
	in := make(chan AlertCandidate, 10)
	stop := startWriter(t, store, in)
	defer stop()

	in <- cand(1, 7, 5)
	in <- cand(1, 7, 5) // 같은 이벤트가 재전달돼 다시 평가된 경우
	in <- cand(1, 7, 6)
	waitFor(t, "alerts", func() bool { return store.count() == 2 && len(in) == 0 })
}

func TestWriterRetriesTransientErrors(t *testing.T) {
	store := &fakeAlertStore{failFirst: 3}
	in := make(chan AlertCandidate, 10)
	stop := startWriter(t, store, in)
	defer stop()

	in <- cand(1, 7, 1)
	waitFor(t, "alert after retries", func() bool { return store.count() == 1 })
	if c := store.calls.Load(); c < 4 {
		t.Fatalf("calls = %d, want >= 4", c)
	}
}

func TestWriterIsolatesPermanentFailureRow(t *testing.T) {
	store := &fakeAlertStore{badRule: 99}
	in := make(chan AlertCandidate, 10)
	// 한 배치로 묶이도록 쓰기 전에 모두 채운다.
	in <- cand(1, 7, 1)
	in <- cand(99, 7, 2) // 삭제된 규칙 → FK 위반
	in <- cand(1, 7, 3)
	stop := startWriter(t, store, in)
	defer stop()

	waitFor(t, "good alerts", func() bool { return store.count() == 2 })
}

func TestWriterDrainsQueueOnShutdown(t *testing.T) {
	store := &fakeAlertStore{}
	in := make(chan AlertCandidate, 10)
	w := &AlertWriter{Store: store, In: in}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 이미 종료 신호가 온 상태에서 큐에 남은 것을 비워야 한다.
	for i := 0; i < 5; i++ {
		in <- cand(1, 7, int64(i))
	}
	if err := w.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if store.count() != 5 {
		t.Fatalf("stored %d, want 5", store.count())
	}
}

func TestTransitionRulesAreSequential(t *testing.T) {
	for _, c := range [][2]string{{AlertOpen, AlertResolved}, {AlertResolved, AlertOpen}, {AlertInProgress, AlertOpen}, {AlertOpen, AlertOpen}} {
		if _, err := TransitionAlert(context.Background(), nil, 1, c[0], c[1], "x"); !errors.Is(err, ErrInvalidAlertTransition) {
			t.Errorf("%s -> %s: err = %v, want ErrInvalidAlertTransition", c[0], c[1], err)
		}
	}
}
