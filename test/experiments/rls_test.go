//go:build experiment

package experiments

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/immin0241/webframework-project-mqtt-collector/internal/testenv"
)

// 결정 1(B): RLS 로 tenant 격리를 강제할 수 있는지(호환성)와 비용(오버헤드)을 확인한다.
//
//	go test -tags experiment -run RLS -v -timeout 30m ./test/experiments
//
// 변형 (모두 tenant 는 session_user -> tenant_role 매핑으로 결정, 세션에서 바꿀 수 없다):
//
//	V  : 현재 스키마(tenant_id 없음) + security_barrier 뷰 + sensor 조인           (결정 1의 A)
//	R1 : 현재 스키마(tenant_id 없음) + sensor_data 에 RLS (정책이 sensor 를 서브쿼리)  (결정 1의 B)
//	R2 : tenant_id 컬럼 재도입 + sensor_data 에 RLS (정책이 컬럼 비교)               (B + 결정 2의 재도입)

func openAs(t *testing.T, addr, role string) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(context.Background(), fmt.Sprintf("postgres://%s:pw@%s/go?sslmode=disable", role, addr))
	if err != nil {
		t.Fatalf("connect as %s: %v", role, err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

func try(ctx context.Context, conn *pgx.Conn, sql string, args ...any) string {
	if _, err := conn.Exec(ctx, sql, args...); err != nil {
		return "ERROR: " + strings.TrimSpace(err.Error())
	}
	return "ok"
}

func countAs(ctx context.Context, conn *pgx.Conn, sql string) string {
	var n int
	if err := conn.QueryRow(ctx, sql).Scan(&n); err != nil {
		return "ERROR: " + strings.TrimSpace(err.Error())
	}
	return fmt.Sprint(n)
}

func TestRLSFeasibilityAndOverhead(t *testing.T) {
	testenv.RequireDocker(t)
	tenants := envInt("EXP_TENANTS", 20)
	perTenant := envInt("EXP_SENSORS_PER_TENANT", 25)
	rowsPerSensor := envInt("EXP_ROWS_PER_SENSOR", 2016)
	reps := envInt("EXP_QUERY_REPS", 20)
	ctx := context.Background()

	pg := testenv.StartPostgres(t)
	admin := pg.Conn(t)
	defer admin.Close(ctx)
	var out strings.Builder
	say := func(f string, a ...any) { fmt.Fprintf(&out, f+"\n", a...) }

	mustExec := func(sql string, args ...any) {
		t.Helper()
		if _, err := admin.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("%.80s: %v", sql, err)
		}
	}

	// ---- 기본 구성: 테넌트·센서·매핑·역할 ----
	for tn := 1; tn <= tenants; tn++ {
		mustExec(`INSERT INTO tenant (idp_org_id, name) VALUES ($1, $1)`, fmt.Sprintf("exp-%d", tn))
	}
	mustExec(`INSERT INTO sensor (id, tenant_id, facility_id, sensor_type, vendor) OVERRIDING SYSTEM VALUE
		SELECT (t.id - 1) * $1 + g, t.id, 1, 'crack', 'exp' FROM tenant t, generate_series(1, $1::int) g WHERE t.idp_org_id LIKE 'exp-%'`, perTenant)
	sensors := tenants * perTenant

	mustExec(`CREATE TABLE tenant_role (role_name TEXT PRIMARY KEY, tenant_id INT NOT NULL REFERENCES tenant (id))`)
	// session_user 기준: 세션에서 SET 으로 바꿀 수 없다. SECURITY DEFINER 라 tenant role 에 매핑 테이블 권한이 필요 없다.
	mustExec(`CREATE FUNCTION app_tenant_by_role() RETURNS INT LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public AS
		$$ SELECT tenant_id FROM tenant_role WHERE role_name = session_user $$`)
	mustExec(`REVOKE ALL ON FUNCTION app_tenant_by_role() FROM PUBLIC`)
	for _, r := range []string{"rls_t5", "rls_t6"} {
		mustExec(fmt.Sprintf(`CREATE ROLE %s LOGIN PASSWORD 'pw'`, r))
		mustExec(fmt.Sprintf(`GRANT EXECUTE ON FUNCTION app_tenant_by_role() TO %s`, r))
	}
	mustExec(`INSERT INTO tenant_role VALUES ('rls_t5', 5), ('rls_t6', 6)`)
	mustExec(`CREATE ROLE rls_worker LOGIN PASSWORD 'pw'`) // 수집 워커 역할(소유자가 아님)

	// ---- 변형 테이블 (RLS 와 압축은 함께 쓸 수 없어 성능 비교는 압축 없이 한다. 2번 항목 참고) ----
	type variant struct {
		name   string
		tenant bool
	}
	variants := []variant{{"sd_v", false}, {"sd_r1", false}, {"sd_r2", true}}
	for _, v := range variants {
		if err := createVariant(ctx, admin, v.name, v.tenant); err != nil {
			t.Fatalf("create %s: %v", v.name, err)
		}
		mustExec(fmt.Sprintf(`DROP VIEW v_%s`, v.name)) // createVariant 의 GUC 기반 뷰는 쓰지 않는다
	}
	mustExec(`CREATE VIEW v_sd_v WITH (security_barrier = true) AS
		SELECT d.time, d.sensor_id, d.seq, d.tilt_x, d.tilt_y, d.crack_width, d.temperature, d.received_at
		FROM sd_v d JOIN sensor s ON s.id = d.sensor_id WHERE s.tenant_id = (SELECT app_tenant_by_role())`)

	// RLS 변형
	mustExec(`ALTER TABLE sensor ENABLE ROW LEVEL SECURITY`)
	mustExec(`CREATE POLICY tenant_sensor ON sensor FOR SELECT USING (tenant_id = (SELECT app_tenant_by_role()))`)
	mustExec(`CREATE POLICY worker_sensor ON sensor FOR SELECT TO rls_worker USING (true)`)
	mustExec(`ALTER TABLE sd_r1 ENABLE ROW LEVEL SECURITY`)
	// sensor 의 RLS 가 테넌트를 걸러 주므로 정책은 "내가 볼 수 있는 센서인가" 만 확인하면 된다.
	mustExec(`CREATE POLICY tenant_read ON sd_r1 FOR SELECT USING (EXISTS (SELECT 1 FROM sensor s WHERE s.id = sd_r1.sensor_id))`)
	mustExec(`CREATE POLICY worker_write ON sd_r1 FOR INSERT TO rls_worker WITH CHECK (true)`)
	mustExec(`ALTER TABLE sd_r2 ENABLE ROW LEVEL SECURITY`)
	mustExec(`CREATE POLICY tenant_read ON sd_r2 FOR SELECT USING (tenant_id = (SELECT app_tenant_by_role()))`)
	mustExec(`CREATE POLICY worker_write ON sd_r2 FOR INSERT TO rls_worker WITH CHECK (true)`)

	for _, tbl := range []string{"sensor", "v_sd_v", "sd_r1", "sd_r2"} {
		mustExec(fmt.Sprintf(`GRANT SELECT ON %s TO rls_t5, rls_t6`, tbl))
	}
	mustExec(`GRANT SELECT ON sensor TO rls_worker`)
	mustExec(`GRANT INSERT ON sd_v, sd_r1, sd_r2 TO rls_worker`)

	// ---- 데이터 ----
	for _, v := range variants {
		cols, sel := "time, sensor_id, seq, crack_width, temperature, received_at", "now() - g * interval '5 minutes', s.id, g, 1 + random(), 20 + random(), now()"
		if v.tenant {
			cols, sel = "time, tenant_id, sensor_id, seq, crack_width, temperature, received_at", "now() - g * interval '5 minutes', s.tenant_id, s.id, g, 1 + random(), 20 + random(), now()"
		}
		mustExec(fmt.Sprintf(`INSERT INTO %s (%s) SELECT %s FROM sensor s, generate_series(1, %d) g WHERE s.vendor = 'exp'`, v.name, cols, sel, rowsPerSensor))
		mustExec("ANALYZE " + v.name)
	}
	say("조건: 테넌트 %d x 센서 %d = %d, 센서당 %d행(5분 주기 7일), 총 %d행/변형, chunk 1일", tenants, perTenant, sensors, rowsPerSensor, sensors*rowsPerSensor)

	// ---- 1. 격리 정확성 (테넌트 5 계정) ----
	t5 := openAs(t, pg.Addr, "rls_t5")
	say("\n### 1. 격리 정확성 (rls_t5 = 테넌트 5, 기대: 센서 %d개, 행 %d)", perTenant, perTenant*rowsPerSensor)
	for _, c := range []struct{ label, sql string }{
		{"V  v_sd_v", `SELECT count(*) FROM v_sd_v`},
		{"R1 sd_r1", `SELECT count(*) FROM sd_r1`},
		{"R2 sd_r2", `SELECT count(*) FROM sd_r2`},
		{"R1 sd_r1 (타 테넌트 센서 id 지정)", fmt.Sprintf(`SELECT count(*) FROM sd_r1 WHERE sensor_id = %d`, 6*perTenant)},
		{"sensor 행 수 (기대 " + fmt.Sprint(perTenant) + ")", `SELECT count(*) FROM sensor`},
	} {
		say("- %s: %s", c.label, countAs(ctx, t5, c.sql))
	}
	say("- V 기본 테이블 직접 조회: %s", try(ctx, t5, `SELECT * FROM sd_v LIMIT 1`))
	_, _ = t5.Exec(ctx, `SET app.tenant_id = '6'`)
	say("- SET app.tenant_id 우회 시도 후 R1 행 수: %s", countAs(ctx, t5, `SELECT count(*) FROM sd_r1`))
	say("- SET ROLE rls_t6 시도: %s", try(ctx, t5, `SET ROLE rls_t6`))
	say("- tenant_role 직접 조회: %s", try(ctx, t5, `SELECT * FROM tenant_role`))
	say("- 쓰기 시도(R1): %s", try(ctx, t5, `INSERT INTO sd_r1 (time, sensor_id, seq, received_at) VALUES (now(), 101, 1, now())`))

	// ---- 2. RLS 와 압축(columnstore) 호환성: migration 000004 는 sensor_data 압축을 켠다 ----
	say("\n### 2. RLS 와 압축 호환성")
	for _, name := range []string{"probe_a", "probe_b"} {
		mustExec(fmt.Sprintf(`CREATE TABLE %s (time TIMESTAMPTZ NOT NULL, tenant_id INT NOT NULL, sensor_id INT NOT NULL, v DOUBLE PRECISION)`, name))
		mustExec(fmt.Sprintf(`SELECT create_hypertable('%s', by_range('time', INTERVAL '1 day'))`, name))
	}
	compressSQL := func(name string) string {
		return fmt.Sprintf(`ALTER TABLE %s SET (timescaledb.compress, timescaledb.compress_segmentby = 'sensor_id', timescaledb.compress_orderby = 'time DESC')`, name)
	}
	say("- 압축을 먼저 켠 뒤 ENABLE ROW LEVEL SECURITY: %s", func() string {
		mustExec(compressSQL("probe_a"))
		return try(ctx, admin, `ALTER TABLE probe_a ENABLE ROW LEVEL SECURITY`)
	}())
	say("- RLS 를 먼저 켠 뒤 압축 설정(SET timescaledb.compress): %s", func() string {
		mustExec(`ALTER TABLE probe_b ENABLE ROW LEVEL SECURITY`)
		return try(ctx, admin, compressSQL("probe_b"))
	}())
	say("- 이미 압축 설정이 있는 sensor_data 형태(migration 000004)에 RLS: 위 첫 줄과 같다")

	// ---- 3. 연속 집계 ----
	say("\n### 3. 연속 집계")
	say("- RLS 걸린 sd_r2 위에 연속 집계 생성: %s", try(ctx, admin, `CREATE MATERIALIZED VIEW cagg_r2 WITH (timescaledb.continuous) AS
		SELECT time_bucket('1 hour', time) b, sensor_id, avg(crack_width) a FROM sd_r2 GROUP BY 1, 2 WITH NO DATA`))
	say("- 집계 refresh: %s", try(ctx, admin, `CALL refresh_continuous_aggregate('cagg_r2', NULL, NULL)`))
	say("- 집계에 RLS 활성화: %s", try(ctx, admin, `ALTER MATERIALIZED VIEW cagg_r2 ENABLE ROW LEVEL SECURITY`))
	_ = try(ctx, admin, `GRANT SELECT ON cagg_r2 TO rls_t5`)
	say("- 집계에 SELECT 를 주고 rls_t5 가 읽는 서로 다른 센서 수 (격리되면 %d, 기본 테이블 RLS 가 집계에 적용되지 않으면 %d): %s",
		perTenant, sensors, countAs(ctx, t5, `SELECT count(DISTINCT sensor_id) FROM cagg_r2`))

	// ---- 4. 조회 오버헤드 ----
	say("\n### 4. 조회 오버헤드 (rls_t5 연결, 압축 없음, 쿼리당 %d회 중앙값)", reps)
	oneSensor := 4*perTenant + 1
	queries := []struct{ label, tmpl string }{
		{"Q1 센서 1개 최근 1시간", `SELECT time, crack_width FROM {T} WHERE sensor_id = {S} AND time > now() - interval '1 hour' ORDER BY time`},
		{"Q2 테넌트 24시간 5분 평균", `SELECT time_bucket('5 minutes', time) b, sensor_id, avg(crack_width) FROM {T} WHERE time > now() - interval '24 hours' GROUP BY 1, 2`},
		{"Q3 테넌트 7일 행 수", `SELECT count(*) FROM {T} WHERE time > now() - interval '7 days'`},
		{"Q4 센서 1개 7일 일평균", `SELECT time_bucket('1 day', time) b, avg(crack_width) FROM {T} WHERE sensor_id = {S} AND time > now() - interval '7 days' GROUP BY 1`},
	}
	targets := []struct{ label, rel string }{{"V 뷰+조인", "v_sd_v"}, {"R1 RLS(서브쿼리)", "sd_r1"}, {"R2 RLS(컬럼)", "sd_r2"}}
	say("\n| 쿼리 | %s | %s | %s |\n| --- | --- | --- | --- |", targets[0].label, targets[1].label, targets[2].label)
	for _, q := range queries {
		cells := make([]string, len(targets))
		for ti, tg := range targets {
			var ds []time.Duration
			for r := 0; r < reps+2; r++ {
				sql := strings.NewReplacer("{T}", tg.rel, "{S}", fmt.Sprint(oneSensor)).Replace(q.tmpl)
				start := time.Now()
				rs, err := t5.Query(ctx, sql)
				if err != nil {
					cells[ti] = "ERROR " + err.Error()
					break
				}
				for rs.Next() {
				}
				rs.Close()
				if r >= 2 {
					ds = append(ds, time.Since(start))
				}
			}
			if cells[ti] == "" {
				cells[ti] = median(ds).Round(10 * time.Microsecond).String()
			}
		}
		say("| %s | %s | %s | %s |", q.label, cells[0], cells[1], cells[2])
	}

	// ---- 5. 쓰기 오버헤드: 워커 역할(소유자 아님)로 insert ----
	say("\n### 5. 쓰기 오버헤드 (rls_worker 연결, 300행 multi-row INSERT ON CONFLICT DO NOTHING 200회 = 60,000행, 3회 중앙값, 순서 순환)")
	worker := openAs(t, pg.Addr, "rls_worker")
	insert := func(v variant, base int) time.Duration {
		start := time.Now()
		for b := 0; b < 200; b++ {
			var sb strings.Builder
			args := make([]any, 0, 300*7)
			cols := "(time, sensor_id, seq, crack_width, temperature, received_at)"
			if v.tenant {
				cols = "(time, tenant_id, sensor_id, seq, crack_width, temperature, received_at)"
			}
			fmt.Fprintf(&sb, "INSERT INTO %s %s VALUES ", v.name, cols)
			for i := 0; i < 300; i++ {
				if i > 0 {
					sb.WriteByte(',')
				}
				sid := (i*7+b)%sensors + 1
				tid := (sid-1)/perTenant + 1
				seq := int64(rowsPerSensor + 10 + base + b*300 + i)
				ts := time.Now().Add(-time.Duration(seq) * time.Millisecond)
				n := len(args)
				if v.tenant {
					args = append(args, ts, tid, sid, seq, 1.5, 20.0, time.Now())
					fmt.Fprintf(&sb, "($%d,$%d,$%d,$%d,$%d,$%d,$%d)", n+1, n+2, n+3, n+4, n+5, n+6, n+7)
				} else {
					args = append(args, ts, sid, seq, 1.5, 20.0, time.Now())
					fmt.Fprintf(&sb, "($%d,$%d,$%d,$%d,$%d,$%d)", n+1, n+2, n+3, n+4, n+5, n+6)
				}
			}
			sb.WriteString(" ON CONFLICT DO NOTHING")
			if _, err := worker.Exec(ctx, sb.String(), args...); err != nil {
				t.Fatalf("insert %s: %v", v.name, err)
			}
		}
		return time.Since(start)
	}
	res := map[string][]time.Duration{}
	for r := 0; r < 3; r++ {
		for i := range variants {
			v := variants[(i+r)%len(variants)]
			res[v.name] = append(res[v.name], insert(v, r*60000))
		}
	}
	for _, v := range variants {
		m := median(res[v.name])
		say("- %s: %.0f rows/s (중앙값 %s)", v.name, 60000/m.Seconds(), m.Round(time.Millisecond))
	}

	t.Logf("\n%s", out.String())
	if p := os.Getenv("EXP_OUT"); p != "" {
		_ = os.WriteFile(p, []byte(out.String()), 0o644)
	}
}
