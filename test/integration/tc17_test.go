package integration

import (
	"context"
	"os"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/immin0241/webframework-project-mqtt-collector/internal/reconcile"
	"github.com/immin0241/webframework-project-mqtt-collector/internal/testenv"
)

// TC-17: 한 테넌트가 폭주해도 다른 테넌트의 데이터는 유실·중복 없이 저장되고, 저장 지연이 한계 안에 머문다.
//
// 수집 경로는 테넌트와 무관하게 채널 하나, 저장 워커 하나를 공유한다. (불변식 5)
// 그래서 공정성(fairness)은 보장되지 않고, 이 테스트는 폭주 중 다른 테넌트가 겪는 지연을 측정해 남긴다.
// 지연 상한 assert 는 느슨하다(회귀 감지용). 실측값은 t.Log 와 docs/experiments/phase6-tc17.md 에 있다.
func TestTC17_TenantSurge(t *testing.T) {
	floodIDs := []int{1, 2, 3, 4, 5}
	const calmID = 101
	h := newHarness(t, floodIDs...)

	// 테넌트 B 와 센서 101
	ctx := context.Background()
	conn := h.pg.Conn(t)
	if _, err := conn.Exec(ctx, `INSERT INTO tenant (idp_org_id, name) VALUES ('calm-org', 'calm')`); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO sensor (id, tenant_id, facility_id, sensor_type, vendor)
		OVERRIDING SYSTEM VALUE SELECT $1, t.id, 1, 'crack', 'test' FROM tenant t WHERE t.idp_org_id = 'calm-org'`, calmID); err != nil {
		t.Fatal(err)
	}
	conn.Close(ctx)

	h.startApp(t, "it-tc17")
	h.waitForSubscribe(t)

	since := time.Now().Add(-time.Minute)
	pub := testenv.NewPublisher(t, h.mq.Addr(t), "it-tc17-pub")
	floodPub := testenv.NewPublisher(t, h.mq.Addr(t), "it-tc17-flood")

	// 폭주: 테넌트 A 센서마다 고루틴 하나가 쉬지 않고 발행한다.
	// 유한한 양(센서당 floodPerSensor 건)을 PUBACK 이 허용하는 최대 속도로 쏟아낸다.
	floodPerSensor := 4000
	if v, err := strconv.Atoi(os.Getenv("TC17_FLOOD_PER_SENSOR")); err == nil && v > 0 {
		floodPerSensor = v // 브로커 큐 상한(max_queued_messages) 초과 실험용
	}
	floodCtx, stopFlood := context.WithCancel(ctx)
	defer stopFlood()
	var wg sync.WaitGroup
	for _, id := range floodIDs {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < floodPerSensor; i++ {
				if _, err := floodPub.Next(floodCtx, id); err != nil {
					return
				}
			}
		}(id)
	}
	floodDone := make(chan struct{})
	floodStart := time.Now()
	go func() {
		wg.Wait()
		close(floodDone)
		t.Logf("flood published in %s", time.Since(floodStart).Round(time.Millisecond))
	}()
	flooding := func() bool {
		select {
		case <-floodDone:
			return false
		default:
			return true
		}
	}

	// 테넌트 B: 100ms 마다 한 건 발행하고, 그 행이 DB 에 보이기까지의 시간을 잰다.
	poll := h.pg.Conn(t)
	defer poll.Close(ctx)
	var lags []time.Duration
	for flooding() {
		start := time.Now()
		seq, err := pub.Next(ctx, calmID)
		if err != nil {
			t.Fatalf("calm publish: %v", err)
		}
		deadline := start.Add(3 * time.Minute)
		for {
			var n int
			if err := poll.QueryRow(ctx, `SELECT count(*) FROM sensor_data WHERE sensor_id = $1 AND seq = $2`, calmID, seq).Scan(&n); err != nil {
				t.Fatal(err)
			}
			if n == 1 {
				lags = append(lags, time.Since(start))
				break
			}
			if time.Now().After(deadline) {
				var stored, rejected int
				_ = poll.QueryRow(ctx, `SELECT count(*) FROM sensor_data`).Scan(&stored)
				_ = poll.QueryRow(ctx, `SELECT count(*) FROM sensor_data_rejected`).Scan(&rejected)
				t.Logf("diag: stored=%d rejected=%d received=%v dedup=%v channel_depth=%v floodPublished=%d",
					stored, rejected, counter(t, "messages_received_total"), counter(t, "dedup_total"), counter(t, "channel_depth"), floodPub.Count())
				t.Fatalf("calm tenant row seq=%d not stored within 3m during flood", seq)
			}
			time.Sleep(10 * time.Millisecond)
		}
		time.Sleep(100 * time.Millisecond)
	}
	<-floodDone

	// 폭주 측 + 조용한 측 모두 대조: 백로그가 소화된 뒤 누락·중복·예상 외 0건
	merged := func() reconcile.SeqCounts {
		all := floodPub.Published()
		for id, m := range pub.Published() {
			for seq, n := range m {
				for i := 0; i < n; i++ {
					all.Add(id, seq)
				}
			}
		}
		return all
	}
	drainStart := time.Now()
	var snap reconcile.Snapshot
	for {
		var reports []reconcile.SensorReport
		reports, snap = h.reconcileNow(t, merged(), since)
		if allOK(reports) {
			break
		}
		if time.Since(drainStart) > 3*time.Minute {
			t.Fatalf("not reconciled 3m after the flood ended: %+v", reports)
		}
		time.Sleep(time.Second)
	}
	t.Logf("backlog drained %s after the flood ended", time.Since(drainStart).Round(time.Second))

	sort.Slice(lags, func(i, j int) bool { return lags[i] < lags[j] })
	q := func(p float64) time.Duration { return lags[int(float64(len(lags)-1)*p)] }
	t.Logf("flood published=%d unique rows, calm probes=%d, snapshot=%+v", floodPub.Count(), len(lags), snap)
	t.Logf("calm tenant publish->visible latency: p50=%s p95=%s max=%s", q(0.5), q(0.95), lags[len(lags)-1])
	if lags[len(lags)-1] > 30*time.Second {
		t.Errorf("calm tenant max latency %s exceeds 30s during another tenant's flood", lags[len(lags)-1])
	}
}
