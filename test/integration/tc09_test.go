package integration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/immin0241/webframework-project-mqtt-collector/internal/testenv"
)

// TC-09 (DB 계층): tenant A 의 Grafana 조회 role 로 접속했을 때 tenant B 데이터가 어떤 경로로도 보이지 않는다.
// 공격자 모델: Grafana 데이터소스의 쿼리 편집기에서 임의 SQL 을 칠 수 있는 tenant 사용자.
// role 은 scripts/provision_tenant.sh 를 그대로 실행해 만든다.

type tenantFixture struct {
	pg       *testenv.Postgres
	tenantA  int
	tenantB  int
	sensorsA []int
	sensorsB []int
}

func provisionRole(t *testing.T, pg *testenv.Postgres, tenantID int) string {
	t.Helper()
	cid := pg.Container.GetContainerID()
	cmd := exec.Command("bash", filepath.Join(testenv.RepoRoot(), "scripts/provision_tenant.sh"), fmt.Sprint(tenantID))
	cmd.Env = append(os.Environ(),
		"PSQL=docker exec -i "+cid+" psql -U go -d go -v ON_ERROR_STOP=1",
		"GRAFANA_DB_PASSWORD=grafana")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("provision_tenant.sh %d: %v\n%s", tenantID, err, out)
	}
	return fmt.Sprintf("grafana_tenant_%d", tenantID)
}

func newTenantFixture(t *testing.T) *tenantFixture {
	t.Helper()
	testenv.RequireDocker(t)
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI not found")
	}
	pg := testenv.StartPostgres(t)
	ctx := context.Background()
	conn := pg.Conn(t)
	defer conn.Close(ctx)

	f := &tenantFixture{pg: pg, sensorsA: []int{1, 2}, sensorsB: []int{3, 4}}
	for _, x := range []struct {
		org  string
		name string
		dst  *int
		ids  []int
	}{{"org-a", "A", &f.tenantA, f.sensorsA}, {"org-b", "B", &f.tenantB, f.sensorsB}} {
		if err := conn.QueryRow(ctx, `INSERT INTO tenant (idp_org_id, name) VALUES ($1, $2) RETURNING id`, x.org, x.name).Scan(x.dst); err != nil {
			t.Fatalf("tenant %s: %v", x.name, err)
		}
		for _, id := range x.ids {
			if _, err := conn.Exec(ctx, `INSERT INTO sensor (id, tenant_id, facility_id, sensor_type, vendor) OVERRIDING SYSTEM VALUE VALUES ($1, $2, 1, 'crack', 'test')`, id, *x.dst); err != nil {
				t.Fatalf("sensor %d: %v", id, err)
			}
			if _, err := conn.Exec(ctx, `INSERT INTO sensor_status (sensor_id, status, status_changed_at) VALUES ($1, 'online', now())`, id); err != nil {
				t.Fatal(err)
			}
			if _, err := conn.Exec(ctx, `INSERT INTO sensor_event (time, sensor_id, event_type, source, message) VALUES (now(), $1, 'connected', 'data', 'x')`, id); err != nil {
				t.Fatal(err)
			}
			if _, err := conn.Exec(ctx, `
				INSERT INTO sensor_data (time, sensor_id, seq, crack_width, received_at)
				SELECT now() - make_interval(secs => g), $1, g, 1.0 + g, now() FROM generate_series(1, 50) g`, id); err != nil {
				t.Fatalf("data %d: %v", id, err)
			}
		}
	}
	// 경보 쪽 데이터. 이 테이블들은 tenant role 에 열려 있으면 안 된다.
	if _, err := conn.Exec(ctx, `
		INSERT INTO alert_rule (tenant_id, name, sensor_id, metric, kind, operator, threshold, severity)
		VALUES ($1, 'b-rule', 3, 'crack_width', 'threshold', 'gt', 3, 'critical')`, f.tenantB); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `
		INSERT INTO alert_event (rule_id, sensor_id, seq, data_time, triggered_at, value)
		SELECT id, 3, 1, now(), now(), 9 FROM alert_rule WHERE name = 'b-rule'`); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO sensor_data_rejected (received_at, topic, payload, sensor_id, error_code, error_message) VALUES (now(), 'sensors/crack/3', 'raw-b-secret', 3, '23503', 'x')`); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `CALL refresh_continuous_aggregate('sensor_data_1m', NULL, NULL)`); err != nil {
		t.Fatalf("refresh aggregate: %v", err)
	}
	return f
}

// asRole 은 tenant role 로 새 연결을 연다 (비밀번호는 provision_tenant.sh 기본값).
func (f *tenantFixture) asRole(t *testing.T, role string) *pgx.Conn {
	t.Helper()
	dsn := fmt.Sprintf("postgres://%s:grafana@%s/go?sslmode=disable", role, f.pg.Addr)
	conn, err := pgx.Connect(context.Background(), dsn)
	if err != nil {
		t.Fatalf("connect as %s: %v", role, err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

func intList(conn *pgx.Conn, query string) ([]int, error) {
	rows, err := conn.Query(context.Background(), query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []int
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

func TestTC09_TenantIsolationAtDB(t *testing.T) {
	f := newTenantFixture(t)
	roleA := provisionRole(t, f.pg, f.tenantA)
	provisionRole(t, f.pg, f.tenantB)

	isB := func(id int) bool { return id == 3 || id == 4 }

	t.Run("views return only own tenant", func(t *testing.T) {
		conn := f.asRole(t, roleA)
		for _, q := range []string{
			`SELECT DISTINCT id FROM v_sensor`,
			`SELECT DISTINCT sensor_id FROM v_sensor_status`,
			`SELECT DISTINCT sensor_id FROM v_sensor_event`,
			`SELECT DISTINCT sensor_id FROM v_sensor_data`,
			`SELECT DISTINCT sensor_id FROM v_sensor_data_1m`,
		} {
			ids, err := intList(conn, q)
			if err != nil {
				t.Errorf("%s: %v", q, err)
				continue
			}
			if len(ids) != 2 {
				t.Errorf("%s: got sensors %v, want exactly tenant A's 2", q, ids)
			}
			for _, id := range ids {
				if isB(id) {
					t.Errorf("%s leaked tenant B sensor %d", q, id)
				}
			}
		}
	})

	t.Run("base tables and alert tables are not readable", func(t *testing.T) {
		conn := f.asRole(t, roleA)
		for _, tbl := range []string{"tenant", "sensor", "sensor_data", "sensor_status", "sensor_event", "sensor_data_rejected", "alert_rule", "alert_event", "ingest_event", "sensor_data_1m"} {
			if _, err := conn.Exec(context.Background(), "SELECT * FROM "+tbl); err == nil {
				t.Errorf("tenant role can read base table %s", tbl)
			}
		}
	})

	t.Run("tenant role cannot write", func(t *testing.T) {
		conn := f.asRole(t, roleA)
		for _, q := range []string{
			`INSERT INTO sensor_data (time, sensor_id, seq, received_at) VALUES (now(), 1, 999, now())`,
			`UPDATE sensor SET tenant_id = 1`,
			`DELETE FROM sensor_data`,
		} {
			if _, err := conn.Exec(context.Background(), q); err == nil {
				t.Errorf("tenant role could run: %s", q)
			}
		}
	})

	t.Run("session cannot switch tenant via SET", func(t *testing.T) {
		conn := f.asRole(t, roleA)
		_, _ = conn.Exec(context.Background(), fmt.Sprintf(`SET app.tenant_id = '%d'`, f.tenantB))
		ids, err := intList(conn, `SELECT DISTINCT sensor_id FROM v_sensor_data`)
		if err != nil {
			t.Fatal(err)
		}
		for _, id := range ids {
			if isB(id) {
				t.Fatalf("SET app.tenant_id exposed tenant B sensor %d to tenant A's role", id)
			}
		}
	})

	t.Run("session cannot switch tenant via set_config", func(t *testing.T) {
		conn := f.asRole(t, roleA)
		_, _ = conn.Exec(context.Background(), fmt.Sprintf(`SELECT set_config('app.tenant_id', '%d', false)`, f.tenantB))
		ids, err := intList(conn, `SELECT DISTINCT id FROM v_sensor`)
		if err != nil {
			t.Fatal(err)
		}
		for _, id := range ids {
			if isB(id) {
				t.Fatalf("set_config exposed tenant B sensor %d", id)
			}
		}
	})

	t.Run("role without tenant mapping sees nothing (fail closed)", func(t *testing.T) {
		ctx := context.Background()
		admin := f.pg.Conn(t)
		defer admin.Close(ctx)
		if _, err := admin.Exec(ctx, `CREATE ROLE grafana_orphan LOGIN PASSWORD 'grafana'`); err != nil {
			t.Fatal(err)
		}
		// 매핑(tenant_role)이 없는 role
		if _, err := admin.Exec(ctx, `GRANT SELECT ON v_sensor, v_sensor_data TO grafana_orphan`); err != nil {
			t.Fatal(err)
		}
		conn := f.asRole(t, "grafana_orphan")
		for _, q := range []string{`SELECT id FROM v_sensor`, `SELECT sensor_id FROM v_sensor_data`} {
			ids, err := intList(conn, q)
			if err != nil || len(ids) != 0 {
				t.Errorf("%s: ids=%v err=%v, want empty", q, ids, err)
			}
		}
	})

	t.Run("hypertable chunks cannot be read directly", func(t *testing.T) {
		conn := f.asRole(t, roleA)
		rows, err := conn.Query(context.Background(),
			`SELECT format('%I.%I', chunk_schema, chunk_name) FROM timescaledb_information.chunks WHERE hypertable_name = 'sensor_data' LIMIT 3`)
		if err != nil {
			return // 카탈로그 접근 자체가 막혀 있으면 충분하다
		}
		var names []string
		for rows.Next() {
			var n string
			_ = rows.Scan(&n)
			names = append(names, n)
		}
		rows.Close()
		for _, n := range names {
			if _, err := conn.Exec(context.Background(), "SELECT * FROM "+n); err == nil {
				t.Errorf("tenant role can read chunk %s directly", n)
			}
		}
	})
}

// migration 000008 은 압축된 chunk 가 있는 sensor_data 에도 tenant_id 를 채우고(백필), 되돌릴 수 있다.
func TestMigration000008BackfillOnCompressedChunks(t *testing.T) {
	testenv.RequireDocker(t)
	pg := testenv.StartPostgres(t)
	pg.SeedSensors(t, 1, 2)
	ctx := context.Background()
	conn := pg.Conn(t)
	defer conn.Close(ctx)

	down, err := readFile("migrations/000008_tenant_isolation.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	up, err := readFile("migrations/000008_tenant_isolation.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, down); err != nil {
		t.Fatalf("down: %v", err)
	}
	var hasCol bool
	_ = conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'sensor_data' AND column_name = 'tenant_id')`).Scan(&hasCol)
	if hasCol {
		t.Fatal("tenant_id still present after down")
	}

	// 000008 이전 스키마에서 며칠에 걸친 데이터를 넣고 오래된 chunk 를 압축한다.
	if _, err := conn.Exec(ctx, `
		INSERT INTO sensor_data (time, sensor_id, seq, crack_width, received_at)
		SELECT now() - make_interval(days => d, secs => g), s, d * 1000 + g, 1.0, now()
		FROM generate_series(0, 6) d, generate_series(1, 100) g, unnest(ARRAY[1, 2]) s`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	var total int
	_ = conn.QueryRow(ctx, `SELECT count(*) FROM sensor_data`).Scan(&total)
	rows, err := conn.Query(ctx, `SELECT format('%I.%I', chunk_schema, chunk_name) FROM timescaledb_information.chunks WHERE hypertable_name = 'sensor_data' AND range_end < now() - interval '2 days'`)
	if err != nil {
		t.Fatal(err)
	}
	var chunks []string
	for rows.Next() {
		var c string
		_ = rows.Scan(&c)
		chunks = append(chunks, c)
	}
	rows.Close()
	for _, c := range chunks {
		if _, err := conn.Exec(ctx, fmt.Sprintf(`SELECT compress_chunk('%s')`, c)); err != nil {
			t.Fatalf("compress %s: %v", c, err)
		}
	}
	if len(chunks) == 0 {
		t.Fatal("no chunk to compress")
	}
	t.Logf("compressed %d chunks, %d rows", len(chunks), total)

	if _, err := conn.Exec(ctx, up); err != nil {
		t.Fatalf("up on compressed data: %v", err)
	}
	var nulls, withTenant int
	_ = conn.QueryRow(ctx, `SELECT count(*) FILTER (WHERE tenant_id IS NULL), count(*) FILTER (WHERE tenant_id = (SELECT tenant_id FROM sensor WHERE id = sensor_id)) FROM sensor_data`).Scan(&nulls, &withTenant)
	if nulls != 0 || withTenant != total {
		t.Fatalf("backfill: null=%d correct=%d of %d", nulls, withTenant, total)
	}

	// 트리거: tenant_id 를 주지 않아도 채우고, 거짓 값은 무시하며, 없는 센서는 23503
	if _, err := conn.Exec(ctx, `INSERT INTO sensor_data (time, sensor_id, seq, received_at) VALUES (now(), 1, 999999, now())`); err != nil {
		t.Fatalf("insert without tenant_id: %v", err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO sensor_data (time, tenant_id, sensor_id, seq, received_at) VALUES (now(), 12345, 2, 999998, now())`); err != nil {
		t.Fatalf("insert with bogus tenant_id: %v", err)
	}
	var bad int
	_ = conn.QueryRow(ctx, `SELECT count(*) FROM sensor_data d JOIN sensor s ON s.id = d.sensor_id WHERE d.seq >= 999998 AND d.tenant_id <> s.tenant_id`).Scan(&bad)
	if bad != 0 {
		t.Fatalf("%d rows kept a caller-supplied tenant_id", bad)
	}
	_, err = conn.Exec(ctx, `INSERT INTO sensor_data (time, sensor_id, seq, received_at) VALUES (now(), 9999, 1, now())`)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23503" {
		t.Fatalf("unknown sensor: err=%v, want SQLSTATE 23503", err)
	}

	// 다시 되돌려도 데이터가 남는다.
	if _, err := conn.Exec(ctx, down); err != nil {
		t.Fatalf("second down: %v", err)
	}
	var after int
	_ = conn.QueryRow(ctx, `SELECT count(*) FROM sensor_data`).Scan(&after)
	if after != total+2 {
		t.Fatalf("rows after round trip = %d, want %d", after, total+2)
	}
}

// provision_tenant.sh 는 중간에 실패하면 role 을 반쯤 만든 채 남기지 않는다. (존재하지 않는 tenant -> 매핑 FK 위반)
func TestProvisionTenantIsAtomic(t *testing.T) {
	testenv.RequireDocker(t)
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI not found")
	}
	pg := testenv.StartPostgres(t)
	cmd := exec.Command("bash", filepath.Join(testenv.RepoRoot(), "scripts/provision_tenant.sh"), "999")
	cmd.Env = append(os.Environ(), "PSQL=docker exec -i "+pg.Container.GetContainerID()+" psql -U go -d go -v ON_ERROR_STOP=1")
	if out, err := cmd.CombinedOutput(); err == nil {
		t.Fatalf("expected failure for a nonexistent tenant, got success:\n%s", out)
	}
	conn := pg.Conn(t)
	defer conn.Close(context.Background())
	var exists bool
	if err := conn.QueryRow(context.Background(), `SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'grafana_tenant_999')`).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("role grafana_tenant_999 was left behind after a failed provisioning")
	}
}

// migration 000009: 1시간보다 늦게 도착한 행도 1분 집계에 반영된다. (이전 정책에서는 반영되지 않는다)
func TestLateDataReachesAggregate(t *testing.T) {
	testenv.RequireDocker(t)
	pg := testenv.StartPostgres(t)
	pg.SeedSensors(t, 1)
	ctx := context.Background()
	conn := pg.Conn(t)
	defer conn.Close(ctx)

	runRefreshJob := func() {
		t.Helper()
		var jobID int
		if err := conn.QueryRow(ctx, `SELECT job_id FROM timescaledb_information.jobs WHERE proc_name = 'policy_refresh_continuous_aggregate' AND hypertable_name = 'sensor_data_1m'`).Scan(&jobID); err != nil {
			t.Fatalf("find refresh job: %v", err)
		}
		if _, err := conn.Exec(ctx, fmt.Sprintf(`CALL run_job(%d)`, jobID)); err != nil {
			t.Fatalf("run_job: %v", err)
		}
	}
	// 하루 이상 지난 버킷이 집계에 있는 개수 (늦게 도착한 행만이 만들 수 있다)
	oldBuckets := func() int {
		var n int
		_ = conn.QueryRow(ctx, `SELECT count(*) FROM sensor_data_1m WHERE sensor_id = 1 AND bucket < now() - interval '1 day'`).Scan(&n)
		return n
	}
	insertLate := func(seq int) {
		if _, err := conn.Exec(ctx, `INSERT INTO sensor_data (time, sensor_id, seq, crack_width, received_at) VALUES (now() - interval '2 days', 1, $1, 1.0, now())`, seq); err != nil {
			t.Fatal(err)
		}
	}

	down, _ := readFile("migrations/000009_cagg_late_data.down.sql")
	up, _ := readFile("migrations/000009_cagg_late_data.up.sql")

	// 이전 정책(1시간): 이틀 전 행은 집계에 반영되지 않는다.
	if _, err := conn.Exec(ctx, down); err != nil {
		t.Fatalf("down: %v", err)
	}
	insertLate(1001)
	runRefreshJob()
	if n := oldBuckets(); n != 0 {
		t.Fatalf("old policy: late row reached the aggregate (%d buckets); the test no longer demonstrates the problem", n)
	}

	// 새 정책(3일): 반영된다.
	if _, err := conn.Exec(ctx, up); err != nil {
		t.Fatalf("up: %v", err)
	}
	insertLate(1002)
	runRefreshJob()
	if n := oldBuckets(); n < 1 {
		t.Fatalf("new policy: late row did not reach the aggregate (old buckets = %d)", n)
	}
}
