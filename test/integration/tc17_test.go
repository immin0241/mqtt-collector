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
// 수집 경로는 테넌트마다 MQTT 세션(브로커 큐)과 작은 채널을 따로 두고, 저장 워커 하나가 공용 채널에서 읽는다. (ADR-0011, 불변식 5)
// 공정성은 forwarder 의 FIFO 송신 대기열이 만든다. 이 테스트는 폭주 중 다른 테넌트가 겪는 지연을 측정해 남긴다.
// 지연 상한 assert 는 느슨하다(회귀 감지용). 실측값은 t.Log 와 docs/experiments/ 에 있다.
// TC17_FLOOD_PER_SENSOR 로 폭주량을 키워 한 테넌트의 브로커 큐 상한(max_queued_messages) 초과를 시험한다.
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
	var calmTenant int
	if err := conn.QueryRow(ctx, `SELECT id FROM tenant WHERE idp_org_id = 'calm-org'`).Scan(&calmTenant); err != nil {
		t.Fatal(err)
	}
	conn.Close(ctx)

	h.startApp(t, "it-tc17")
	h.waitForSubscribe(t)

	since := time.Now().Add(-time.Minute)
	pub := testenv.NewPublisher(t, h.mq.Addr(t), "it-tc17-pub")
	pub.SetTenant(calmID, calmTenant) // 평온한 테넌트의 토픽: sensors/{calmTenant}/...
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
			if err := poll.QueryRow(ctx, `SELECT count(DISTINCT time) FROM sensor_data WHERE sensor_id = $1 AND seq = $2`, calmID, seq).Scan(&n); err != nil {
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
	// 폭주량을 키운 실험(overflow)에서는 폭주 테넌트 자신의 브로커 큐가 넘쳐 그 테넌트의 메시지가 유실되는 것이 허용된다.
	// (ADR-0011: 피해는 폭주 테넌트에 한정된다.) 이때는 저장 건수가 더 늘지 않을 때까지 기다린 뒤,
	// 평온한 테넌트의 유실·중복 0과 폭주 테넌트의 중복·예상 외 0을 판정하고 폭주 테넌트의 유실량을 기록한다.
	overflow := os.Getenv("TC17_FLOOD_PER_SENSOR") != ""
	drainStart := time.Now()
	var snap reconcile.Snapshot
	var reports []reconcile.SensorReport
	lastTotal, stableSince := -1, time.Now()
	for {
		reports, snap = h.reconcileNow(t, merged(), since)
		if allOK(reports) {
			break
		}
		if overflow {
			total := 0
			for _, r := range reports {
				total += r.Stored
			}
			if total != lastTotal {
				lastTotal, stableSince = total, time.Now()
			} else if time.Since(stableSince) > 30*time.Second {
				break // 저장 건수가 30초 동안 늘지 않았다: 더 올 메시지가 없다
			}
		}
		if time.Since(drainStart) > 6*time.Minute {
			t.Fatalf("not reconciled 6m after the flood ended: %+v", reports)
		}
		time.Sleep(time.Second)
	}
	t.Logf("backlog drained %s after the flood ended", time.Since(drainStart).Round(time.Second))
	if overflow {
		var floodPub, floodStored, floodMissing int
		for _, r := range reports {
			if r.SensorID == calmID {
				if r.Missing != 0 || r.DBDuplicates != 0 || r.Unexpected != 0 || r.Stored != r.Published {
					t.Errorf("calm tenant must lose nothing during another tenant's overflow: published=%d stored=%d missing=%d dup=%d unexpected=%d",
						r.Published, r.Stored, r.Missing, r.DBDuplicates, r.Unexpected)
				}
				continue
			}
			if r.DBDuplicates != 0 || r.Unexpected != 0 {
				t.Errorf("flood sensor %d: duplicates=%d unexpected=%d", r.SensorID, r.DBDuplicates, r.Unexpected)
			}
			floodPub += r.Published
			floodStored += r.Stored
			floodMissing += r.Missing
		}
		t.Logf("OVERFLOW RESULT flood tenant: published=%d stored=%d missing=%d (%.2f%%); calm tenant: probes=%d",
			floodPub, floodStored, floodMissing, 100*float64(floodMissing)/float64(floodPub), len(lags))
	}

	sort.Slice(lags, func(i, j int) bool { return lags[i] < lags[j] })
	q := func(p float64) time.Duration { return lags[int(float64(len(lags)-1)*p)] }
	t.Logf("flood published=%d unique rows, calm probes=%d, snapshot=%+v", floodPub.Count(), len(lags), snap)
	t.Logf("calm tenant publish->visible latency: p50=%s p95=%s max=%s", q(0.5), q(0.95), lags[len(lags)-1])
	if lags[len(lags)-1] > 30*time.Second {
		t.Errorf("calm tenant max latency %s exceeds 30s during another tenant's flood", lags[len(lags)-1])
	}
}
