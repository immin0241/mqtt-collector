//go:build experiment

package experiments

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/immin0241/webframework-project-mqtt-collector/internal/testenv"
)

// TC-19: 연속 집계(1분)와 압축이 조회 시간·저장 용량에 주는 영향.
// 같은 데이터로 원본 조회와 1분 집계 조회를 압축 전후에 각각 잰다.
//
//	go test -tags experiment -run AggregatesCompression -v -timeout 40m ./test/experiments
//
// 데이터: 센서 EXP_AGG_SENSORS × 지표 4 × 10초 간격 × EXP_AGG_DAYS 일(자정 정렬, UTC).
// 백그라운드 정책(압축·집계 갱신)은 멈추고 직접 실행해 시점을 통제한다.
func TestAggregatesCompression(t *testing.T) {
	testenv.RequireDocker(t)
	sensors := envInt("EXP_AGG_SENSORS", 30)
	days := envInt("EXP_AGG_DAYS", 14)
	const reps = 5
	ctx := context.Background()

	pg := testenv.StartPostgres(t)
	ids := make([]int, sensors)
	for i := range ids {
		ids[i] = i + 1
	}
	pg.SeedSensors(t, ids...)
	conn := pg.Conn(t)
	defer conn.Close(ctx)

	mustExec := func(sql string, args ...any) {
		t.Helper()
		if _, err := conn.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	// 정책 job 은 멈춘다. alter_job 은 행을 돌려주므로 Query 로 소비한다.
	jr, err := conn.Query(ctx, `SELECT alter_job(job_id, scheduled => false) FROM timescaledb_information.jobs
		WHERE proc_name IN ('policy_compression', 'policy_refresh_continuous_aggregate')`)
	if err != nil {
		t.Fatal(err)
	}
	for jr.Next() {
	}
	jr.Close()

	var end time.Time
	if err := conn.QueryRow(ctx, `SELECT date_trunc('day', now())`).Scan(&end); err != nil {
		t.Fatal(err)
	}
	start := end.AddDate(0, 0, -days)

	t0 := time.Now()
	for d := 0; d < days; d++ {
		day := start.AddDate(0, 0, d)
		mustExec(`INSERT INTO sensor_data (time, tenant_id, sensor_id, seq, metric_id, value, received_at)
			SELECT g.t, s.tenant_id, s.id, (extract(epoch FROM g.t)::bigint / 10), m.id,
			       1 + 0.1 * sin(extract(epoch FROM g.t) / 3600.0 + s.id) + random() * 0.05, g.t
			FROM generate_series($1::timestamptz, $1::timestamptz + interval '1 day' - interval '10 seconds', interval '10 seconds') g(t),
			     sensor s, metric m
			WHERE s.id <= $2`, day, sensors)
	}
	loadDur := time.Since(t0)
	var rows int64
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM sensor_data`).Scan(&rows); err != nil {
		t.Fatal(err)
	}

	t0 = time.Now()
	mustExec(`CALL refresh_continuous_aggregate('sensor_data_1m', NULL::timestamptz, $1::timestamptz)`, end)
	refreshDur := time.Since(t0)
	mustExec(`ANALYZE`)

	t.Logf("conditions: TimescaleDB container, same-machine, %d sensors x 4 metrics, 10s interval, %d days, rows=%d, migration 000011~000012", sensors, days, rows)
	t.Logf("load: %s (%.0f rows/s, server-side generate_series), cagg full refresh: %s", loadDur.Round(time.Second), float64(rows)/loadDur.Seconds(), refreshDur.Round(time.Millisecond))

	size := func(label string) {
		var tbl, idx, total int64
		if err := conn.QueryRow(ctx, `SELECT coalesce(table_bytes,0), coalesce(index_bytes,0), coalesce(total_bytes,0) FROM hypertable_detailed_size('sensor_data')`).Scan(&tbl, &idx, &total); err != nil {
			t.Fatal(err)
		}
		var agg int64
		if err := conn.QueryRow(ctx, `SELECT hypertable_size(format('%I.%I', materialization_hypertable_schema, materialization_hypertable_name)::regclass)
			FROM timescaledb_information.continuous_aggregates WHERE view_name = 'sensor_data_1m'`).Scan(&agg); err != nil {
			t.Fatal(err)
		}
		t.Logf("size[%s]: sensor_data table=%s index=%s total=%s | sensor_data_1m=%s", label, mib(tbl), mib(idx), mib(total), mib(agg))
	}
	size("before compression")

	rawMin := `SELECT time_bucket('1 minute', time) AS b, avg(value), min(value), max(value) FROM sensor_data
		WHERE sensor_id = 1 AND metric_id = 3 AND time >= $1 AND time < $2 GROUP BY b ORDER BY b`
	aggMin := `SELECT bucket, value_avg, value_min, value_max FROM sensor_data_1m
		WHERE sensor_id = 1 AND metric_id = 3 AND bucket >= $1 AND bucket < $2 ORDER BY bucket`
	rawHour := `SELECT time_bucket('1 hour', time) AS b, avg(value), min(value), max(value) FROM sensor_data
		WHERE sensor_id = 1 AND metric_id = 3 AND time >= $1 AND time < $2 GROUP BY b ORDER BY b`
	aggHour := `SELECT time_bucket('1 hour', bucket) AS b, sum(value_avg * sample_count) / sum(sample_count), min(value_min), max(value_max) FROM sensor_data_1m
		WHERE sensor_id = 1 AND metric_id = 3 AND bucket >= $1 AND bucket < $2 GROUP BY b ORDER BY b`
	// 전 센서, 한 지표의 구간 통계: 요약 리포트·대시보드 통계 패널 유형
	rawAll := `SELECT sensor_id, avg(value), min(value), max(value) FROM sensor_data
		WHERE metric_id = 3 AND time >= $1 AND time < $2 GROUP BY sensor_id ORDER BY sensor_id`
	aggAll := `SELECT sensor_id, sum(value_avg * sample_count) / sum(sample_count), min(value_min), max(value_max) FROM sensor_data_1m
		WHERE metric_id = 3 AND bucket >= $1 AND bucket < $2 GROUP BY sensor_id ORDER BY sensor_id`

	type window struct {
		name     string
		from, to time.Time
	}
	cutoff := end.AddDate(0, 0, -3) // 압축 정책 3일
	windows := []window{
		{"1h (recent)", end.Add(-time.Hour), end},
		{"1d (recent)", end.AddDate(0, 0, -1), end},
		{"1d (old)", start, start.AddDate(0, 0, 1)},
		{"7d (mixed)", end.AddDate(0, 0, -7), end},
		{"all days", start, end},
	}
	pairs := []struct {
		label    string
		raw, agg string
	}{
		{"1 sensor, 1m buckets", rawMin, aggMin},
		{"1 sensor, 1h buckets", rawHour, aggHour},
		{"all sensors, 1 stat", rawAll, aggAll},
	}

	run := func(phase string) {
		t.Logf("%-12s %-22s %-12s | %12s %12s %8s", phase, "query", "window", "raw p50", "1m-agg p50", "speedup")
		for _, p := range pairs {
			for _, w := range windows {
				rawT, rawN := medianQuery(t, ctx, conn, p.raw, w.from, w.to, reps)
				aggT, aggN := medianQuery(t, ctx, conn, p.agg, w.from, w.to, reps)
				t.Logf("%-12s %-22s %-12s | %12s %12s %7.1fx  (rows raw=%d agg=%d)", phase, p.label, w.name,
					rawT.Round(time.Microsecond), aggT.Round(time.Microsecond), float64(rawT)/float64(aggT), rawN, aggN)
			}
		}
	}
	run("uncompressed")

	t0 = time.Now()
	var compressed int
	cr, err := conn.Query(ctx, `SELECT compress_chunk(c) FROM show_chunks('sensor_data', older_than => $1::timestamptz) c`, cutoff)
	if err != nil {
		t.Fatal(err)
	}
	for cr.Next() {
		compressed++
	}
	cr.Close()
	if cr.Err() != nil {
		t.Fatal(cr.Err())
	}
	t.Logf("compressed %d chunks older than %s in %s", compressed, cutoff.Format("2006-01-02"), time.Since(t0).Round(time.Second))
	mustExec(`ANALYZE`)
	size("after compression")
	run("compressed")
}

func medianQuery(t *testing.T, ctx context.Context, conn *pgx.Conn, sql string, from, to time.Time, reps int) (time.Duration, int) {
	t.Helper()
	var n int
	one := func() time.Duration {
		s := time.Now()
		rows, err := conn.Query(ctx, sql, from, to)
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		n = 0
		for rows.Next() {
			n++
		}
		rows.Close()
		if rows.Err() != nil {
			t.Fatal(rows.Err())
		}
		return time.Since(s)
	}
	one() // warm
	d := make([]time.Duration, reps)
	for i := range d {
		d[i] = one()
	}
	sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
	return d[len(d)/2], n
}

func mib(b int64) string { return fmt.Sprintf("%.0f MiB", float64(b)/(1<<20)) }
