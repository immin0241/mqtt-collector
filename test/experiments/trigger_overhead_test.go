//go:build experiment

package experiments

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/immin0241/webframework-project-mqtt-collector/internal/testenv"
)

// migration 000008 의 sensor_data_set_tenant 트리거가 쓰기 처리량에 주는 영향.
// 같은 테이블에서 트리거를 켠 채(tenant_id 는 트리거가 채움)와 끈 채(tenant_id 를 직접 넣음)를 번갈아 측정한다.
//
//	go test -tags experiment -run TriggerOverhead -v ./test/experiments
func TestTriggerOverhead(t *testing.T) {
	testenv.RequireDocker(t)
	tenants := envInt("EXP_TENANTS", 20)
	perTenant := envInt("EXP_SENSORS_PER_TENANT", 25)
	batches := envInt("EXP_INSERT_BATCHES", 200)
	reps := envInt("EXP_TRIGGER_REPS", 5)
	const batch = 300
	ctx := context.Background()

	pg := testenv.StartPostgres(t)
	conn := pg.Conn(t)
	defer conn.Close(ctx)
	for tn := 1; tn <= tenants; tn++ {
		if _, err := conn.Exec(ctx, `INSERT INTO tenant (idp_org_id, name) VALUES ($1, $1)`, fmt.Sprintf("exp-%d", tn)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := conn.Exec(ctx, `INSERT INTO sensor (id, tenant_id, facility_id, sensor_type, vendor) OVERRIDING SYSTEM VALUE
		SELECT (t.id - 1) * $1 + g, t.id, 1, 'crack', 'exp' FROM tenant t, generate_series(1, $1::int) g WHERE t.idp_org_id LIKE 'exp-%'`, perTenant); err != nil {
		t.Fatal(err)
	}
	sensors := tenants * perTenant
	var metricIDs []int16
	mr, err := conn.Query(ctx, `SELECT id FROM metric WHERE name IN ('tilt_x', 'tilt_y') ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	for mr.Next() {
		var id int16
		_ = mr.Scan(&id)
		metricIDs = append(metricIDs, id)
	}
	mr.Close()

	run := func(withTrigger bool, base int) time.Duration {
		sql := "ALTER TABLE sensor_data DISABLE TRIGGER sensor_data_set_tenant"
		if withTrigger {
			sql = "ALTER TABLE sensor_data ENABLE TRIGGER sensor_data_set_tenant"
		}
		if _, err := conn.Exec(ctx, sql); err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		for b := 0; b < batches; b++ {
			var sb strings.Builder
			args := make([]any, 0, batch*len(metricIDs)*7)
			sb.WriteString("INSERT INTO sensor_data (time, tenant_id, sensor_id, seq, metric_id, value, received_at) VALUES ")
			first := true
			for i := 0; i < batch; i++ {
				sid := (i*7+b)%sensors + 1
				tid := (sid-1)/perTenant + 1
				seq := int64(base + b*batch + i)
				ts := time.Now().Add(-time.Duration(seq) * time.Millisecond)
				for _, mid := range metricIDs { // 메시지당 측정 행 2개 (세로 구조, ADR-0012)
					if !first {
						sb.WriteByte(',')
					}
					first = false
					n := len(args)
					args = append(args, ts, tid, sid, seq, mid, 1.5, time.Now())
					fmt.Fprintf(&sb, "($%d,$%d,$%d,$%d,$%d,$%d,$%d)", n+1, n+2, n+3, n+4, n+5, n+6, n+7)
				}
			}
			sb.WriteString(" ON CONFLICT DO NOTHING")
			if _, err := conn.Exec(ctx, sb.String(), args...); err != nil {
				t.Fatalf("insert: %v", err)
			}
		}
		return time.Since(start)
	}

	var on, off []time.Duration
	for r := 0; r < reps; r++ {
		base := r * 2 * batches * batch
		if r%2 == 0 {
			on = append(on, run(true, base))
			off = append(off, run(false, base+batches*batch))
		} else {
			off = append(off, run(false, base))
			on = append(on, run(true, base+batches*batch))
		}
	}
	rows := float64(batches * batch * len(metricIDs))
	var out strings.Builder
	fmt.Fprintf(&out, "조건: 센서 %d(테넌트 %d), 회당 %d행 = %d배치 x %d메시지(메시지당 2행) multi-row INSERT ON CONFLICT DO NOTHING, 단일 연결(superuser), %d회 반복 중앙값, 트리거 on/off 번갈아\n", sensors, tenants, int(rows), batches, batch, reps)
	fmt.Fprintf(&out, "- 트리거 on : %.0f rows/s (중앙값 %s)\n", rows/median(on).Seconds(), median(on).Round(time.Millisecond))
	fmt.Fprintf(&out, "- 트리거 off: %.0f rows/s (중앙값 %s)\n", rows/median(off).Seconds(), median(off).Round(time.Millisecond))
	fmt.Fprintf(&out, "- 트리거 오버헤드: %.1f%%\n", (float64(median(on))/float64(median(off))-1)*100)
	t.Logf("\n%s", out.String())
	if p := os.Getenv("EXP_OUT"); p != "" {
		_ = os.WriteFile(p, []byte(out.String()), 0o644)
	}
}
