//go:build experiment

// Package experiments 는 CI 에서 돌리지 않는 측정 실험이다.
//
//	go test -tags experiment -run TenantID -v -timeout 30m ./test/experiments
//
// 조건은 환경 변수로 바꾼다. 결과를 파일로 받으려면 EXP_OUT=경로.
package experiments

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/immin0241/webframework-project-mqtt-collector/internal/testenv"
)

func envInt(key string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(key)); err == nil && v > 0 {
		return v
	}
	return def
}

// 변형 A: sensor_data.tenant_id 재도입 (복합 FK + (tenant_id, sensor_id, time DESC) 인덱스), 뷰에 조인 없음
// 변형 B: 현재 스키마 (tenant_id 없음, 단일 FK + (sensor_id, time DESC) 인덱스), 뷰가 sensor 와 조인
func createVariant(ctx context.Context, conn *pgx.Conn, name string, withTenant bool) error {
	var ddl string
	if withTenant {
		ddl = fmt.Sprintf(`
CREATE TABLE %[1]s (
  time TIMESTAMPTZ NOT NULL, tenant_id INT NOT NULL, sensor_id INT NOT NULL, seq BIGINT NOT NULL,
  tilt_x DOUBLE PRECISION, tilt_y DOUBLE PRECISION, crack_width DOUBLE PRECISION, temperature DOUBLE PRECISION,
  received_at TIMESTAMPTZ NOT NULL,
  FOREIGN KEY (sensor_id, tenant_id) REFERENCES sensor (id, tenant_id),
  UNIQUE (sensor_id, seq, time));
CREATE INDEX ON %[1]s (tenant_id, sensor_id, time DESC);
SELECT create_hypertable('%[1]s', by_range('time', INTERVAL '1 day'));
CREATE VIEW v_%[1]s WITH (security_barrier = true) AS
  SELECT time, sensor_id, seq, tilt_x, tilt_y, crack_width, temperature, received_at FROM %[1]s
  WHERE tenant_id = app_tenant_id();`, name)
	} else {
		ddl = fmt.Sprintf(`
CREATE TABLE %[1]s (
  time TIMESTAMPTZ NOT NULL, sensor_id INT NOT NULL REFERENCES sensor (id), seq BIGINT NOT NULL,
  tilt_x DOUBLE PRECISION, tilt_y DOUBLE PRECISION, crack_width DOUBLE PRECISION, temperature DOUBLE PRECISION,
  received_at TIMESTAMPTZ NOT NULL,
  UNIQUE (sensor_id, seq, time));
CREATE INDEX ON %[1]s (sensor_id, time DESC);
SELECT create_hypertable('%[1]s', by_range('time', INTERVAL '1 day'));
CREATE VIEW v_%[1]s WITH (security_barrier = true) AS
  SELECT d.time, d.sensor_id, d.seq, d.tilt_x, d.tilt_y, d.crack_width, d.temperature, d.received_at
  FROM %[1]s d JOIN sensor s ON s.id = d.sensor_id WHERE s.tenant_id = app_tenant_id();`, name)
	}
	_, err := conn.Exec(ctx, ddl)
	return err
}

func median(d []time.Duration) time.Duration {
	s := slices.Clone(d)
	slices.Sort(s)
	return s[len(s)/2]
}

func p95(d []time.Duration) time.Duration {
	s := slices.Clone(d)
	slices.Sort(s)
	return s[min(len(s)-1, len(s)*95/100)]
}

func TestTenantIDVariants(t *testing.T) {
	testenv.RequireDocker(t)
	tenants := envInt("EXP_TENANTS", 20)
	perTenant := envInt("EXP_SENSORS_PER_TENANT", 25)
	rowsPerSensor := envInt("EXP_ROWS_PER_SENSOR", 2016) // 5분 주기 7일
	insertBatches := envInt("EXP_INSERT_BATCHES", 200)   // x 300행
	reps := envInt("EXP_QUERY_REPS", 20)
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
	if _, err := conn.Exec(ctx, `
		INSERT INTO sensor (id, tenant_id, facility_id, sensor_type, vendor) OVERRIDING SYSTEM VALUE
		SELECT (t.id - 1) * $1 + g, t.id, 1, 'crack', 'exp' FROM tenant t, generate_series(1, $1::int) g WHERE t.idp_org_id LIKE 'exp-%'`, perTenant); err != nil {
		t.Fatal(err)
	}
	sensors := tenants * perTenant

	variants := []struct {
		name   string
		tenant bool
	}{{"sd_a", true}, {"sd_b", false}, {"ins_a", true}, {"ins_b", false}}
	for _, v := range variants {
		if err := createVariant(ctx, conn, v.name, v.tenant); err != nil {
			t.Fatalf("create %s: %v", v.name, err)
		}
	}

	// 조회용 데이터: 서버에서 한 번에 채운다 (최근 rowsPerSensor x 5분)
	for _, v := range variants[:2] {
		cols, sel := "time, sensor_id, seq, crack_width, temperature, received_at", "now() - g * interval '5 minutes', s.id, g, 1 + random(), 20 + random(), now()"
		if v.tenant {
			cols, sel = "time, tenant_id, sensor_id, seq, crack_width, temperature, received_at", "now() - g * interval '5 minutes', s.tenant_id, s.id, g, 1 + random(), 20 + random(), now()"
		}
		start := time.Now()
		if _, err := conn.Exec(ctx, fmt.Sprintf(
			`INSERT INTO %s (%s) SELECT %s FROM sensor s, generate_series(1, %d) g WHERE s.vendor = 'exp'`, v.name, cols, sel, rowsPerSensor)); err != nil {
			t.Fatalf("fill %s: %v", v.name, err)
		}
		t.Logf("filled %s with %d rows in %s", v.name, sensors*rowsPerSensor, time.Since(start).Round(time.Millisecond))
	}
	for _, n := range []string{"sd_a", "sd_b"} {
		if _, err := conn.Exec(ctx, "ANALYZE "+n); err != nil {
			t.Fatal(err)
		}
	}

	var out strings.Builder
	fmt.Fprintf(&out, "조건: 테넌트 %d x 센서 %d = %d, 센서당 %d행(5분 주기), 총 %d행, 1일 chunk, 압축 없음, 쿼리 %d회 반복(예열 2회 제외)\n",
		tenants, perTenant, sensors, rowsPerSensor, sensors*rowsPerSensor, reps)

	fmt.Fprintln(&out, "\n| 변형 | 데이터 | 인덱스 | 합계 |\n| --- | --- | --- | --- |")
	for _, v := range []struct{ label, name string }{{"A tenant_id 재도입", "sd_a"}, {"B 현재(조인)", "sd_b"}} {
		var tbl, idx, tot int64
		if err := conn.QueryRow(ctx,
			`SELECT coalesce(table_bytes,0), coalesce(index_bytes,0), coalesce(total_bytes,0) FROM hypertable_detailed_size($1)`, v.name).Scan(&tbl, &idx, &tot); err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(&out, "| %s | %.1f MiB | %.1f MiB | %.1f MiB |\n", v.label, float64(tbl)/1048576, float64(idx)/1048576, float64(tot)/1048576)
	}

	const tenantUnderTest = 5
	if _, err := conn.Exec(ctx, fmt.Sprintf(`SET app.tenant_id = '%d'`, tenantUnderTest)); err != nil {
		t.Fatal(err)
	}
	oneSensor := (tenantUnderTest-1)*perTenant + 1
	queries := []struct{ label, sql string }{
		{"Q1 센서 1개 최근 1시간 원본", `SELECT time, crack_width FROM {V} WHERE sensor_id = {S} AND time > now() - interval '1 hour' ORDER BY time`},
		{"Q2 테넌트 전체 최근 24시간 5분 평균", `SELECT time_bucket('5 minutes', time) b, sensor_id, avg(crack_width) FROM {V} WHERE time > now() - interval '24 hours' GROUP BY 1, 2`},
		{"Q3 테넌트 전체 7일 행 수", `SELECT count(*) FROM {V} WHERE time > now() - interval '7 days'`},
		{"Q4 센서 1개 7일 일평균", `SELECT time_bucket('1 day', time) b, avg(crack_width) FROM {V} WHERE sensor_id = {S} AND time > now() - interval '7 days' GROUP BY 1`},
	}
	fmt.Fprintln(&out, "\n| 쿼리 | A 중앙값 | A p95 | B 중앙값 | B p95 | B/A (중앙값) |\n| --- | --- | --- | --- | --- | --- |")
	for _, q := range queries {
		res := map[string][]time.Duration{}
		for r := 0; r < reps+2; r++ {
			for _, name := range []string{"sd_a", "sd_b"} {
				sql := strings.NewReplacer("{V}", "v_"+name, "{S}", strconv.Itoa(oneSensor)).Replace(q.sql)
				start := time.Now()
				rows, err := conn.Query(ctx, sql)
				if err != nil {
					t.Fatalf("%s: %v", q.label, err)
				}
				for rows.Next() {
				}
				rows.Close()
				if rows.Err() != nil {
					t.Fatal(rows.Err())
				}
				if r >= 2 {
					res[name] = append(res[name], time.Since(start))
				}
			}
		}
		a, b := res["sd_a"], res["sd_b"]
		fmt.Fprintf(&out, "| %s | %s | %s | %s | %s | %.2f |\n", q.label,
			median(a).Round(10*time.Microsecond), p95(a).Round(10*time.Microsecond),
			median(b).Round(10*time.Microsecond), p95(b).Round(10*time.Microsecond),
			float64(median(b))/float64(median(a)))
	}

	// 쓰기: 앱과 같은 방식(300행 multi-row INSERT ... ON CONFLICT DO NOTHING). 순서를 번갈아 편향을 줄인다.
	insert := func(name string, withTenant bool, base int) time.Duration {
		start := time.Now()
		for b := 0; b < insertBatches; b++ {
			var sb strings.Builder
			args := make([]any, 0, batch*7)
			cols := "(time, sensor_id, seq, crack_width, temperature, received_at)"
			if withTenant {
				cols = "(time, tenant_id, sensor_id, seq, crack_width, temperature, received_at)"
			}
			fmt.Fprintf(&sb, "INSERT INTO %s %s VALUES ", name, cols)
			for i := 0; i < batch; i++ {
				if i > 0 {
					sb.WriteByte(',')
				}
				sid := (i*7+b)%sensors + 1
				tid := (sid-1)/perTenant + 1
				seq := int64(base + b*batch + i)
				ts := time.Now().Add(-time.Duration(seq) * time.Second)
				n := len(args)
				if withTenant {
					args = append(args, ts, tid, sid, seq, 1.5, 20.0, time.Now())
					fmt.Fprintf(&sb, "($%d,$%d,$%d,$%d,$%d,$%d,$%d)", n+1, n+2, n+3, n+4, n+5, n+6, n+7)
				} else {
					args = append(args, ts, sid, seq, 1.5, 20.0, time.Now())
					fmt.Fprintf(&sb, "($%d,$%d,$%d,$%d,$%d,$%d)", n+1, n+2, n+3, n+4, n+5, n+6)
				}
			}
			sb.WriteString(" ON CONFLICT DO NOTHING")
			if _, err := conn.Exec(ctx, sb.String(), args...); err != nil {
				t.Fatalf("insert %s: %v", name, err)
			}
		}
		return time.Since(start)
	}
	var ta, tb []time.Duration
	for r := 0; r < 4; r++ {
		base := r * insertBatches * batch
		if r%2 == 0 {
			ta = append(ta, insert("ins_a", true, base))
			tb = append(tb, insert("ins_b", false, base))
		} else {
			tb = append(tb, insert("ins_b", false, base))
			ta = append(ta, insert("ins_a", true, base))
		}
	}
	rows := float64(insertBatches * batch)
	fmt.Fprintf(&out, "\n쓰기(회당 %d행 = %d배치 x %d행 multi-row INSERT ON CONFLICT DO NOTHING, 단일 연결, 빈 테이블에서 시작해 누적, 4회 반복 중앙값):\n", int(rows), insertBatches, batch)
	fmt.Fprintf(&out, "- A tenant_id 재도입: %.0f rows/s (중앙값 %s)\n", rows/median(ta).Seconds(), median(ta).Round(time.Millisecond))
	fmt.Fprintf(&out, "- B 현재: %.0f rows/s (중앙값 %s)\n", rows/median(tb).Seconds(), median(tb).Round(time.Millisecond))

	t.Logf("\n%s", out.String())
	if p := os.Getenv("EXP_OUT"); p != "" {
		if err := os.WriteFile(p, []byte(out.String()), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}
