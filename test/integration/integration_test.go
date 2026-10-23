// Package integration은 실제 TimescaleDB와 Mosquitto 컨테이너에 수집 앱(internal/app)을 붙여 검증한다.
// Docker를 쓸 수 없으면 건너뛴다. 판정은 internal/reconcile 을 재사용한다.
package integration

import (
	"context"
	"testing"
	"time"

	"github.com/immin0241/webframework-project-mqtt-collector/internal/testenv"
)

// TC-01: 정상 수집 · 완전성. 센서 10 x 1,000건, 누락·중복·예상 외 0건.
func TestTC01_NormalIngestIsComplete(t *testing.T) {
	ids := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	h := newHarness(t, ids...)
	h.startApp(t, "it-tc01")
	h.waitForSubscribe(t)

	since := time.Now().Add(-time.Minute)
	pub := testenv.NewPublisher(t, h.mq.Addr(t), "it-tc01-pub")
	ctx := context.Background()
	const perSensor = 1000
	for i := 0; i < perSensor; i++ {
		for _, id := range ids {
			if _, err := pub.Next(ctx, id); err != nil {
				t.Fatalf("publish: %v", err)
			}
		}
	}

	reports := h.waitReconciled(t, pub, since, 60*time.Second)
	if len(reports) != len(ids) {
		t.Fatalf("got %d sensor reports, want %d", len(reports), len(ids))
	}
	for _, r := range reports {
		if r.Published != perSensor || r.Stored != perSensor {
			t.Errorf("sensor %d: published=%d stored=%d, want %d", r.SensorID, r.Published, r.Stored, perSensor)
		}
		if r.Missing != 0 || r.Unexpected != 0 || r.DBDuplicates != 0 || r.Rejected != 0 {
			t.Errorf("sensor %d: %+v", r.SensorID, r)
		}
	}
}

// TC-05: 같은 이벤트를 3번 보내도 DB에는 1건이고, 중복 제거 지표가 2 증가한다.
func TestTC05_DuplicateEventStoredOnce(t *testing.T) {
	h := newHarness(t, 1)
	h.startApp(t, "it-tc05")
	h.waitForSubscribe(t)

	since := time.Now().Add(-time.Minute)
	dedupBefore := counter(t, "dedup_total")

	pub := testenv.NewPublisher(t, h.mq.Addr(t), "it-tc05-pub")
	payload := testenv.Payload(1, 42, time.Now())
	for i := 0; i < 3; i++ {
		if err := pub.PublishRaw(testenv.Topic(1), payload); err != nil {
			t.Fatal(err)
		}
		pub.Record(1, 42)
		time.Sleep(300 * time.Millisecond) // 서로 다른 배치에 들어가도 흡수되는지 본다
	}

	reports := h.waitReconciled(t, pub, since, 30*time.Second)
	r := reports[0]
	if r.Stored != 1 || r.DBDuplicates != 0 || r.PublishedDuplicates != 1 {
		t.Fatalf("report: %+v", r)
	}

	deadline := time.Now().Add(10 * time.Second)
	for counter(t, "dedup_total")-dedupBefore < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("dedup_total increased by %v, want 2", counter(t, "dedup_total")-dedupBefore)
		}
		time.Sleep(200 * time.Millisecond)
	}
	if got := counter(t, "dedup_total") - dedupBefore; got != 2 {
		t.Fatalf("dedup_total increased by %v, want 2", got)
	}
}

// TC-07: 형식 오류와 FK 위반은 격리하고 정상 행은 저장하며, 무한 재시도하지 않는다.
func TestTC07_BadRowsAreQuarantined(t *testing.T) {
	h := newHarness(t, 1)
	h.startApp(t, "it-tc07")
	h.waitForSubscribe(t)

	since := time.Now().Add(-time.Minute)
	pub := testenv.NewPublisher(t, h.mq.Addr(t), "it-tc07-pub")

	bad := []byte("{not json")
	fk := testenv.Payload(9999, 1, time.Now()) // 존재하지 않는 센서
	if err := pub.PublishRaw(testenv.Topic(1), bad); err != nil {
		t.Fatal(err)
	}
	if err := pub.PublishRaw(testenv.Topic(1), fk); err != nil {
		t.Fatal(err)
	}
	// 같은 배치에 정상 행을 섞는다.
	for i := 0; i < 5; i++ {
		if _, err := pub.Next(context.Background(), 1); err != nil {
			t.Fatal(err)
		}
	}

	deadline := time.Now().Add(30 * time.Second)
	var rejected int
	for {
		conn := h.pg.Conn(t)
		_ = conn.QueryRow(context.Background(), `SELECT count(*) FROM sensor_data_rejected`).Scan(&rejected)
		conn.Close(context.Background())
		reports, _ := h.reconcileNow(t, pub.Published(), since)
		if rejected == 2 && allOK(reports) && reports[0].Stored == 5 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("rejected=%d reports=%+v", rejected, reports)
		}
		time.Sleep(500 * time.Millisecond)
	}

	// 무한 재시도 여부: 시간이 지나도 격리 행이 늘지 않는다.
	time.Sleep(3 * time.Second)
	conn := h.pg.Conn(t)
	defer conn.Close(context.Background())
	rows, err := conn.Query(context.Background(),
		`SELECT error_code, sensor_id, convert_from(payload, 'UTF8') FROM sensor_data_rejected ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	type rec struct {
		code    string
		sensor  *int
		payload string
	}
	var got []rec
	for rows.Next() {
		var r rec
		if err := rows.Scan(&r.code, &r.sensor, &r.payload); err != nil {
			t.Fatal(err)
		}
		got = append(got, r)
	}
	if len(got) != 2 {
		t.Fatalf("rejected rows = %d, want 2 (retry loop?): %+v", len(got), got)
	}
	byCode := map[string]rec{}
	for _, r := range got {
		byCode[r.code] = r
	}
	if r, ok := byCode["PARSE_ERROR"]; !ok || r.payload != "{not json" {
		t.Errorf("parse error row = %+v", byCode["PARSE_ERROR"])
	}
	if r, ok := byCode["23503"]; !ok || r.sensor == nil || *r.sensor != 9999 {
		t.Errorf("fk violation row = %+v", byCode["23503"])
	}
}

// TC-16: SIGTERM(정상 종료). 종료 신호 후 제한 시간 안에 반환하고, 재시작하면 누락·중복 없이 이어진다.
func TestTC16_GracefulShutdownLosesNothing(t *testing.T) {
	ids := []int{1, 2, 3}
	h := newHarness(t, ids...)
	first := h.startApp(t, "it-tc16")
	h.waitForSubscribe(t)

	since := time.Now().Add(-time.Minute)
	pub := testenv.NewPublisher(t, h.mq.Addr(t), "it-tc16-pub")
	ctx := context.Background()

	publish := func(rounds int) {
		for i := 0; i < rounds; i++ {
			for _, id := range ids {
				if _, err := pub.Next(ctx, id); err != nil {
					t.Fatalf("publish: %v", err)
				}
			}
		}
	}

	publish(300)
	// 발행이 계속되는 도중에 종료한다.
	stopped := make(chan time.Duration, 1)
	go func() { stopped <- first.stop(t) }()
	publish(300)
	elapsed := <-stopped
	t.Logf("graceful shutdown took %s", elapsed)
	if elapsed > 20*time.Second {
		t.Fatalf("shutdown took %s", elapsed)
	}

	// 종료 직후에도 DB에 중복이 없다.
	reports, _ := h.reconcileNow(t, pub.Published(), since)
	for _, r := range reports {
		if r.DBDuplicates != 0 || r.Unexpected != 0 {
			t.Fatalf("after shutdown: %+v", r)
		}
	}

	// 같은 clientID로 재시작하면 브로커가 보관한 메시지를 받아 누락이 없어진다.
	h.startApp(t, "it-tc16")
	publish(100)
	reports = h.waitReconciled(t, pub, since, 60*time.Second)
	for _, r := range reports {
		if r.Published != 700 || r.Stored != 700 {
			t.Errorf("sensor %d: published=%d stored=%d, want 700", r.SensorID, r.Published, r.Stored)
		}
	}
}
