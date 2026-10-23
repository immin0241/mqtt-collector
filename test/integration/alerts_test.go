package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/immin0241/webframework-project-mqtt-collector/internal/app"
	"github.com/immin0241/webframework-project-mqtt-collector/internal/testenv"
	"github.com/immin0241/webframework-project-mqtt-collector/workers"
)

// Phase 5 통합 테스트: 센서 상태(TC-12, TC-13)와 경보(TC-08).
// 센서 id%3 == 1 이면 crack, 0 이면 env, 2 이면 tilt (testenv.SeedSensors). 예상 주기는 기본 1초.

func readFile(rel string) (string, error) {
	b, err := os.ReadFile(filepath.Join(testenv.RepoRoot(), rel))
	return string(b), err
}

func fastHealth(cfg *app.Config) {
	cfg.HealthMinTimeout = 500 * time.Millisecond
	cfg.HealthTick = 200 * time.Millisecond
	cfg.HealthSeenInterval = time.Second
	cfg.AlertRefresh = 500 * time.Millisecond
	cfg.FlatlineCount = 10
	cfg.OutOfRangeCount = 3
}

func reading(sensorID int, seq int64, crack float64) []byte {
	b, _ := json.Marshal(map[string]any{
		"sensor_id": sensorID, "seq": seq, "time": time.Now().UTC().Format(time.RFC3339Nano),
		"tilt_x": 0.1 + float64(seq)*0.001, "tilt_y": 0.2, "crack_width": crack, "temperature": 20.0,
	})
	return b
}

func sensorStatus(t *testing.T, conn *pgx.Conn, id int) string {
	t.Helper()
	var s string
	err := conn.QueryRow(context.Background(), `SELECT status FROM sensor_status WHERE sensor_id = $1`, id).Scan(&s)
	if err != nil {
		return "(none)"
	}
	return s
}

func waitStatus(t *testing.T, h *harness, id int, want string, timeout time.Duration) {
	t.Helper()
	conn := h.pg.Conn(t)
	defer conn.Close(context.Background())
	deadline := time.Now().Add(timeout)
	for {
		got := sensorStatus(t, conn, id)
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("sensor %d status = %s, want %s after %s", id, got, want, timeout)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func countRows(t *testing.T, h *harness, query string, args ...any) int {
	t.Helper()
	conn := h.pg.Conn(t)
	defer conn.Close(context.Background())
	var n int
	if err := conn.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return n
}

// feed는 주기적으로 센서들에게 값을 보낸다. 값은 매번 달라 flatline 으로 보이지 않는다.
type feeder struct {
	pub  *testenv.Publisher
	stop chan struct{}
	wg   sync.WaitGroup
	seq  atomic.Int64
}

func startFeeder(t *testing.T, pub *testenv.Publisher, every time.Duration, ids ...int) *feeder {
	f := &feeder{pub: pub, stop: make(chan struct{})}
	f.wg.Add(1)
	go func() {
		defer f.wg.Done()
		tick := time.NewTicker(every)
		defer tick.Stop()
		for {
			select {
			case <-f.stop:
				return
			case <-tick.C:
				n := f.seq.Add(1)
				for _, id := range ids {
					_ = pub.PublishRaw(testenv.Topic(id), reading(id, n, 1.0))
				}
			}
		}
	}()
	t.Cleanup(f.halt)
	return f
}

func (f *feeder) halt() {
	select {
	case <-f.stop:
	default:
		close(f.stop)
	}
	f.wg.Wait()
}

// 마이그레이션 000007 은 down 후 다시 up 해도 적용되고, down 은 새 값을 쓰던 행을 정리한다.
func TestMigration000007UpDownRoundTrip(t *testing.T) {
	testenv.RequireDocker(t)
	pg := testenv.StartPostgres(t)
	pg.SeedSensors(t, 1)
	ctx := context.Background()
	conn := pg.Conn(t)
	defer conn.Close(ctx)

	if _, err := conn.Exec(ctx, `INSERT INTO sensor_status (sensor_id, status, status_changed_at) VALUES (1, 'faulty', now())`); err != nil {
		t.Fatalf("faulty status should be allowed after 000007: %v", err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO sensor_event (time, sensor_id, event_type, source, message) VALUES (now(), 1, 'fault', 'data', 'x')`); err != nil {
		t.Fatalf("fault event should be allowed after 000007: %v", err)
	}

	down, err := readFile("migrations/000007_alerts_and_faulty.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, down); err != nil {
		t.Fatalf("down: %v", err)
	}
	var status string
	if err := conn.QueryRow(ctx, `SELECT status FROM sensor_status WHERE sensor_id = 1`).Scan(&status); err != nil || status != "online" {
		t.Fatalf("after down: status=%q err=%v, want online", status, err)
	}
	if _, err := conn.Exec(ctx, `UPDATE sensor_status SET status = 'faulty'`); err == nil {
		t.Fatal("faulty must be rejected after down")
	}
	var exists bool
	_ = conn.QueryRow(ctx, `SELECT to_regclass('alert_rule') IS NOT NULL`).Scan(&exists)
	if exists {
		t.Fatal("alert_rule still exists after down")
	}

	up, err := readFile("migrations/000007_alerts_and_faulty.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, up); err != nil {
		t.Fatalf("re-up: %v", err)
	}
}

// TC-08: 임계치 경보가 정확히 발생하고, 지속 위반·재전달로 중복 생성되지 않으며, 지연이 p99 2초 이내다.
func TestTC08_ThresholdAlertCorrectnessAndLatency(t *testing.T) {
	const sensor = 1 // crack
	h := newHarness(t, sensor)
	conn := h.pg.Conn(t)
	defer conn.Close(context.Background())
	var ruleID int64
	if err := conn.QueryRow(context.Background(), `
		INSERT INTO alert_rule (tenant_id, name, sensor_id, metric, kind, operator, threshold, severity)
		SELECT t.id, 'crack>3', $1, 'crack_width', 'threshold', 'gt', 3.0, 'critical' FROM tenant t WHERE t.idp_org_id = 'test-org'
		RETURNING id`, sensor).Scan(&ruleID); err != nil {
		t.Fatalf("insert rule: %v", err)
	}

	h.startAppWith(t, "it-tc08", fastHealth)
	h.waitForSubscribe(t)
	pub := testenv.NewPublisher(t, h.mq.Addr(t), "it-tc08-pub")

	// seq: 0 정상, 1 위반(경보), 2·3 위반 지속(경보 없음), 4 정상, 5 위반(경보)
	values := []float64{1, 5, 6, 7, 2, 4}
	for seq, v := range values {
		if err := pub.PublishRaw(testenv.Topic(sensor), reading(sensor, int64(seq), v)); err != nil {
			t.Fatal(err)
		}
	}
	// 위반 이벤트 재전달/중복 전송: 경보가 늘어나면 안 된다.
	for i := 0; i < 3; i++ {
		_ = pub.PublishRaw(testenv.Topic(sensor), reading(sensor, 1, 5))
	}

	deadline := time.Now().Add(15 * time.Second)
	for countRows(t, h, `SELECT count(*) FROM alert_event WHERE rule_id = $1`, ruleID) < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("alerts not created: have %d", countRows(t, h, `SELECT count(*) FROM alert_event`))
		}
		time.Sleep(200 * time.Millisecond)
	}
	time.Sleep(2 * time.Second) // 추가 경보가 생기지 않는지 지켜본다

	rows, err := conn.Query(context.Background(),
		`SELECT seq, value, status, extract(epoch FROM created_at - triggered_at) FROM alert_event WHERE rule_id = $1 ORDER BY seq`, ruleID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []int64
	var maxLatency float64
	for rows.Next() {
		var seq int64
		var value, latency float64
		var status string
		if err := rows.Scan(&seq, &value, &status, &latency); err != nil {
			t.Fatal(err)
		}
		got = append(got, seq)
		maxLatency = max(maxLatency, latency)
		if status != "open" {
			t.Errorf("seq %d status = %s, want open", seq, status)
		}
	}
	if len(got) != 2 || got[0] != 1 || got[1] != 5 {
		t.Fatalf("alerts at seq %v, want [1 5]", got)
	}
	if maxLatency > 2.0 {
		t.Errorf("alert latency %.3fs exceeds the p99 target of 2s", maxLatency)
	}
	t.Logf("alert latency (receive handler → alert_event.created_at) max over %d alerts: %.3fs", len(got), maxLatency)
}

// 경보 상태 전이는 동시에 요청해도 정확히 한 번만 성공한다.
func TestAlertStateTransitionExactlyOnce(t *testing.T) {
	const sensor = 1
	testenv.RequireDocker(t)
	pg := testenv.StartPostgres(t)
	pg.SeedSensors(t, sensor)
	ctx := context.Background()
	conn := pg.Conn(t)
	defer conn.Close(ctx)

	var alertID int64
	if err := conn.QueryRow(ctx, `
		WITH r AS (
		  INSERT INTO alert_rule (tenant_id, name, sensor_id, metric, kind, operator, threshold, severity)
		  SELECT t.id, 'r', $1, 'crack_width', 'threshold', 'gt', 3, 'warning' FROM tenant t WHERE t.idp_org_id = 'test-org'
		  RETURNING id)
		INSERT INTO alert_event (rule_id, sensor_id, seq, data_time, triggered_at, value)
		SELECT r.id, $1, 1, now(), now(), 9 FROM r RETURNING id`, sensor).Scan(&alertID); err != nil {
		t.Fatal(err)
	}

	db, err := gorm.Open(postgres.Open(pg.DSN), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}

	const racers = 20
	var wins atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ok, err := workers.TransitionAlert(ctx, db, alertID, workers.AlertOpen, workers.AlertInProgress, fmt.Sprintf("operator-%d", i))
			if err != nil {
				t.Errorf("transition: %v", err)
			}
			if ok {
				wins.Add(1)
			}
		}(i)
	}
	wg.Wait()
	if wins.Load() != 1 {
		t.Fatalf("%d concurrent open→in_progress transitions succeeded, want exactly 1", wins.Load())
	}

	// 순서를 건너뛰거나 거꾸로 가는 전이는 거부된다.
	if _, err := workers.TransitionAlert(ctx, db, alertID, workers.AlertOpen, workers.AlertResolved, "x"); err == nil {
		t.Error("open→resolved must be rejected")
	}
	if ok, _ := workers.TransitionAlert(ctx, db, alertID, workers.AlertOpen, workers.AlertInProgress, "late"); ok {
		t.Error("stale open→in_progress must not apply twice")
	}
	if ok, err := workers.TransitionAlert(ctx, db, alertID, workers.AlertInProgress, workers.AlertResolved, "op"); err != nil || !ok {
		t.Fatalf("in_progress→resolved: ok=%v err=%v", ok, err)
	}
	var status, by string
	if err := conn.QueryRow(ctx, `SELECT status, changed_by FROM alert_event WHERE id = $1`, alertID).Scan(&status, &by); err != nil {
		t.Fatal(err)
	}
	if status != "resolved" || by != "op" {
		t.Fatalf("final status=%s changed_by=%s", status, by)
	}
}

// TC-12: 통신 두절. 한 센서만 조용해지면 그 센서만 offline, 다시 오면 online 으로 복구된다.
func TestTC12_SilentSensorGoesOfflineAndRecovers(t *testing.T) {
	h := newHarness(t, 1, 2, 3, 4)
	h.startAppWith(t, "it-tc12", fastHealth)
	h.waitForSubscribe(t)
	pub := testenv.NewPublisher(t, h.mq.Addr(t), "it-tc12-pub")

	startFeeder(t, pub, 300*time.Millisecond, 1, 2, 3)
	// 센서 4는 한 번 보고 침묵한다.
	_ = pub.PublishRaw(testenv.Topic(4), reading(4, 1, 1))

	for id := 1; id <= 4; id++ {
		waitStatus(t, h, id, "online", 15*time.Second)
	}
	waitStatus(t, h, 4, "offline", 15*time.Second)
	for id := 1; id <= 3; id++ {
		waitStatus(t, h, id, "online", time.Second)
	}
	if n := countRows(t, h, `SELECT count(*) FROM sensor_event WHERE sensor_id = 4 AND event_type = 'timeout'`); n != 1 {
		t.Fatalf("timeout events for sensor 4 = %d, want 1", n)
	}
	if n := countRows(t, h, `SELECT count(*) FROM sensor_event WHERE sensor_id <> 4 AND event_type IN ('timeout','disconnected')`); n != 0 {
		t.Fatalf("healthy sensors got %d offline events", n)
	}

	_ = pub.PublishRaw(testenv.Topic(4), reading(4, 2, 1.2))
	waitStatus(t, h, 4, "online", 10*time.Second)
	if n := countRows(t, h, `SELECT count(*) FROM sensor_event WHERE sensor_id = 4 AND event_type = 'recovered'`); n != 1 {
		t.Fatalf("recovered events for sensor 4 = %d, want 1", n)
	}
}

// 전체 수신이 멈추면(수집 경로 문제) 센서를 하나씩 offline 으로 만들지 않고 수집 장애 이벤트 한 건만 남긴다.
func TestTC12_WholeFleetSilenceIsIngestStall(t *testing.T) {
	h := newHarness(t, 1, 2, 3)
	h.startAppWith(t, "it-tc12-stall", fastHealth)
	h.waitForSubscribe(t)
	pub := testenv.NewPublisher(t, h.mq.Addr(t), "it-tc12-stall-pub")

	f := startFeeder(t, pub, 300*time.Millisecond, 1, 2, 3)
	for id := 1; id <= 3; id++ {
		waitStatus(t, h, id, "online", 15*time.Second)
	}
	f.halt() // 모든 센서가 동시에 조용해진다

	deadline := time.Now().Add(15 * time.Second)
	for countRows(t, h, `SELECT count(*) FROM ingest_event WHERE event_type = 'stalled'`) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("no ingest stalled event")
		}
		time.Sleep(200 * time.Millisecond)
	}
	time.Sleep(3 * time.Second)
	if n := countRows(t, h, `SELECT count(*) FROM ingest_event WHERE event_type = 'stalled'`); n != 1 {
		t.Fatalf("stalled events = %d, want exactly 1", n)
	}
	if n := countRows(t, h, `SELECT count(*) FROM sensor_event WHERE event_type = 'timeout'`); n != 0 {
		t.Fatalf("%d sensors were flagged individually during a whole-fleet stall", n)
	}
	for id := 1; id <= 3; id++ {
		waitStatus(t, h, id, "online", time.Second)
	}
}

// TC-13: 상태 전이. 같은 값 반복은 faulty, 값이 바뀌면 online 으로 돌아오고 이력이 남는다.
func TestTC13_FlatlineBecomesFaultyThenClears(t *testing.T) {
	const sensor = 1
	h := newHarness(t, sensor)
	h.startAppWith(t, "it-tc13", fastHealth)
	h.waitForSubscribe(t)
	pub := testenv.NewPublisher(t, h.mq.Addr(t), "it-tc13-pub")

	flat, _ := json.Marshal(map[string]any{"tilt_x": 0.5, "tilt_y": 0.5, "crack_width": 1.0, "temperature": 20.0})
	for seq := 0; seq < 12; seq++ {
		var m map[string]any
		_ = json.Unmarshal(flat, &m)
		m["sensor_id"], m["seq"], m["time"] = sensor, seq, time.Now().UTC().Format(time.RFC3339Nano)
		b, _ := json.Marshal(m)
		if err := pub.PublishRaw(testenv.Topic(sensor), b); err != nil {
			t.Fatal(err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	waitStatus(t, h, sensor, "faulty", 15*time.Second)

	_ = pub.PublishRaw(testenv.Topic(sensor), reading(sensor, 100, 1.7))
	waitStatus(t, h, sensor, "online", 15*time.Second)

	conn := h.pg.Conn(t)
	defer conn.Close(context.Background())
	rows, err := conn.Query(context.Background(), `SELECT event_type FROM sensor_event WHERE sensor_id = $1 ORDER BY time`, sensor)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var e string
		_ = rows.Scan(&e)
		got = append(got, e)
	}
	want := []string{"connected", "fault", "fault_cleared"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
}

// 재시작 직후에는 오경보가 없고, 재시작을 가로질러 침묵한 센서는 여전히 감지된다.
func TestRestartNoFalseAlarmAndSilentSensorStillDetected(t *testing.T) {
	h := newHarness(t, 1, 2, 3, 4)
	a := h.startAppWith(t, "it-restart", fastHealth)
	h.waitForSubscribe(t)
	pub := testenv.NewPublisher(t, h.mq.Addr(t), "it-restart-pub")

	f := startFeeder(t, pub, 300*time.Millisecond, 1, 2, 3, 4)
	for id := 1; id <= 4; id++ {
		waitStatus(t, h, id, "online", 15*time.Second)
	}
	f.halt()
	time.Sleep(1500 * time.Millisecond) // last_seen 스냅샷이 DB에 남을 시간
	a.stop(t)

	// 다운타임이 센서 주기 한도보다 길어도, 재시작 직후 센서를 곧바로 offline 으로 만들지 않는다.
	time.Sleep(2 * time.Second)
	restart := time.Now()
	h.startAppWith(t, "it-restart", fastHealth)
	h.waitForSubscribe(t)
	f2 := startFeeder(t, pub, 300*time.Millisecond, 1, 2, 3) // 센서 4는 계속 침묵
	_ = f2

	waitStatus(t, h, 4, "offline", 20*time.Second)
	for id := 1; id <= 3; id++ {
		waitStatus(t, h, id, "online", time.Second)
	}
	if n := countRows(t, h, `SELECT count(*) FROM sensor_event WHERE sensor_id IN (1,2,3) AND event_type IN ('timeout','disconnected') AND time >= $1`, restart); n != 0 {
		t.Fatalf("false alarms after restart: %d", n)
	}
}
