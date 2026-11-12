package integration

import (
	"context"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/immin0241/webframework-project-mqtt-collector/internal/testenv"
	"github.com/immin0241/webframework-project-mqtt-collector/workers"
)

// 카탈로그의 정상 범위(migration 000012)는 센서 상태 판정의 기본값(workers.DefaultRanges)과 같아야 한다.
// 둘이 갈라지면 같은 값을 한쪽은 격리하고 다른 쪽은 정상으로 본다. (ADR-0013)
func TestCatalogRangesMatchDefaultRanges(t *testing.T) {
	testenv.RequireDocker(t)
	pg := testenv.StartPostgres(t)
	got := openStore(t, pg).Ranges()
	for _, name := range workers.KnownMetrics {
		want, ok := workers.DefaultRanges[name]
		if !ok {
			t.Fatalf("DefaultRanges has no entry for %s", name)
		}
		if got[name] != want {
			t.Errorf("%s: catalog range %v, DefaultRanges %v", name, got[name], want)
		}
	}
}

// TC-07: 범위 밖 값과 비정상 시각은 그 메시지만 격리 테이블로 가고(원문 payload, SQLSTATE, sensor_id·seq 포함),
// 같은 배치의 정상 메시지는 저장되며, 격리된 메시지도 ack 된다. (불변식 1·3)
func TestOutOfRangeAndBadTimeAreQuarantined(t *testing.T) {
	testenv.RequireDocker(t)
	pg := testenv.StartPostgres(t)
	pg.SeedSensors(t, 1)
	store := openStore(t, pg)

	now := time.Now().UTC().Truncate(time.Microsecond)
	msgs := []workers.SensorData{
		{Time: now, SensorID: 1, Seq: 1, TiltX: ptr(1), ReceivedAt: now},                           // 정상
		{Time: now, SensorID: 1, Seq: 2, TiltX: ptr(45), ReceivedAt: now},                          // 범위 밖 (±30)
		{Time: time.Time{}, SensorID: 1, Seq: 3, TiltX: ptr(1), ReceivedAt: now},                   // time 0
		{Time: now.Add(72 * time.Hour), SensorID: 1, Seq: 4, TiltX: ptr(1), ReceivedAt: now},       // 미래 시각
		{Time: now, SensorID: 1, Seq: 5, TiltX: ptr(1), TiltY: ptr(math.Pi), ReceivedAt: now},      // 정상
		{Time: now.Add(-30 * 24 * time.Hour), SensorID: 1, Seq: 6, TiltX: ptr(2), ReceivedAt: now}, // 늦게 도착한 데이터는 정상
	}

	data := make(chan workers.Envelope, 16)
	var mu sync.Mutex
	acked := map[int64]bool{}
	for _, d := range msgs {
		d := d
		data <- workers.Envelope{
			Data: d, Topic: "sensors/1/crack/1", Payload: []byte(fmt.Sprintf(`{"sensor_id":1,"seq":%d}`, d.Seq)), ReceivedAt: now,
			Ack: func() { mu.Lock(); acked[d.Seq] = true; mu.Unlock() },
		}
	}
	w := &workers.DbPushWorker{Store: store, DataChannel: data, BatchSize: len(msgs), FlushInterval: 100 * time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = w.Run(ctx); close(done) }()

	deadline := time.Now().Add(20 * time.Second)
	for {
		mu.Lock()
		n := len(acked)
		mu.Unlock()
		if n == len(msgs) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("acked %d of %d messages: %v", n, len(msgs), acked)
		}
		time.Sleep(50 * time.Millisecond)
	}
	cancel()
	<-done

	for _, seq := range []int64{1, 5, 6} {
		if got := pgCount(t, pg, `SELECT count(DISTINCT time) FROM sensor_data WHERE sensor_id = 1 AND seq = $1`, seq); got != 1 {
			t.Errorf("good message seq=%d stored %d times, want 1", seq, got)
		}
	}
	want := map[int64]string{2: workers.OutOfRangeCode, 3: workers.BadTimeCode, 4: workers.BadTimeCode}
	for seq, code := range want {
		if got := pgCount(t, pg, `SELECT count(*) FROM sensor_data_rejected WHERE sensor_id = 1 AND seq = $1 AND error_code = $2`, seq, code); got != 1 {
			t.Errorf("seq=%d: quarantine rows with code %s = %d, want 1", seq, code, got)
		}
		if got := pgCount(t, pg, `SELECT count(*) FROM sensor_data WHERE sensor_id = 1 AND seq = $1`, seq); got != 0 {
			t.Errorf("seq=%d was quarantined but %d measurement rows exist", seq, got)
		}
	}
	if got := pgCount(t, pg, `SELECT count(*) FROM sensor_data_rejected`); got != 3 {
		t.Errorf("quarantine rows = %d, want 3", got)
	}
}

// 000012는 down 후 다시 up 해도 적용되고, down 은 범위 컬럼만 제거한다. 범위를 NULL 로 바꾸면 수집기는 그 지표를 검증하지 않는다.
func TestMigration000012RoundTripAndNullRange(t *testing.T) {
	testenv.RequireDocker(t)
	pg := testenv.StartPostgres(t)
	pg.SeedSensors(t, 1)
	ctx := context.Background()
	conn := pg.Conn(t)
	defer conn.Close(ctx)

	// 범위를 NULL(무제한)로 바꾸고 다시 읽으면 어떤 값도 통과한다.
	if _, err := conn.Exec(ctx, `UPDATE metric SET min_value = NULL, max_value = NULL WHERE name = 'tilt_x'`); err != nil {
		t.Fatal(err)
	}
	store := openStore(t, pg)
	now := time.Now().UTC().Truncate(time.Microsecond)
	if n, err := store.InsertRows(ctx, []workers.SensorData{{Time: now, SensorID: 1, Seq: 1, TiltX: ptr(1000), ReceivedAt: now}}); err != nil || n != 1 {
		t.Fatalf("unbounded metric rejected a value: n=%d err=%v", n, err)
	}
	// min > max 는 제약으로 막는다.
	if _, err := conn.Exec(ctx, `UPDATE metric SET min_value = 10, max_value = 1 WHERE name = 'tilt_y'`); err == nil {
		t.Fatal("min_value > max_value must be rejected by the CHECK constraint")
	}

	down, err := readFile("migrations/000012_metric_range.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	up, err := readFile("migrations/000012_metric_range.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, down); err != nil {
		t.Fatalf("down: %v", err)
	}
	var hasCol bool
	_ = conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'metric' AND column_name = 'min_value')`).Scan(&hasCol)
	if hasCol {
		t.Fatal("min_value still exists after down")
	}
	if got := pgCount(t, pg, `SELECT count(*) FROM metric`); got != len(workers.KnownMetrics) {
		t.Fatalf("down must keep the catalog rows: %d", got)
	}
	if _, err := conn.Exec(ctx, up); err != nil {
		t.Fatalf("re-up: %v", err)
	}
	var lo, hi float64
	if err := conn.QueryRow(ctx, `SELECT min_value, max_value FROM metric WHERE name = 'crack_width'`).Scan(&lo, &hi); err != nil || lo != 0 || hi != 50 {
		t.Fatalf("after re-up crack_width range = [%v, %v] err=%v, want [0, 50]", lo, hi, err)
	}
}
