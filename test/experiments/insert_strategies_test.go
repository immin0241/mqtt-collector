//go:build experiment

package experiments

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/immin0241/webframework-project-mqtt-collector/internal/testenv"
)

// INSERT 방식 × 배치 크기 × DB 왕복 지연 비교. 같은 스키마(migration 000011~)에 같은 행을 넣는다.
// 메시지당 측정 행은 2개(tilt_x, tilt_y)라 한 배치의 행 수는 배치 크기 × 2 다.
//
//	go test -tags experiment -run InsertStrategies -v -timeout 30m ./test/experiments
//
// 방식:
//
//	multirow-tx   현재 구현: BEGIN + 다중 행 VALUES INSERT ... ON CONFLICT DO NOTHING RETURNING + COMMIT (왕복 3회)
//	multirow      트랜잭션 없이 같은 문장 하나 (왕복 1회, 문장 자체가 원자적이다)
//	unnest        트랜잭션 없이 배열 파라미터 6개 + unnest INSERT ... ON CONFLICT DO NOTHING (왕복 1회)
//	copy          BEGIN + 임시 테이블 + COPY + INSERT ... SELECT ON CONFLICT DO NOTHING + COMMIT (왕복 여러 회)
type batchData struct {
	times    []time.Time
	sensors  []int32
	seqs     []int64
	metrics  []int16
	values   []float64
	received []time.Time
}

func genBatch(n int, seq *int64, metricIDs []int16) batchData {
	var b batchData
	now := time.Now().UTC()
	for i := 0; i < n; i++ {
		*seq++
		ts := now.Add(time.Duration(*seq) * time.Microsecond)
		for j, m := range metricIDs {
			b.times = append(b.times, ts)
			b.sensors = append(b.sensors, int32(1+*seq%5))
			b.seqs = append(b.seqs, *seq)
			b.metrics = append(b.metrics, m)
			b.values = append(b.values, 0.1*float64(j+1))
			b.received = append(b.received, now)
		}
	}
	return b
}

func (b batchData) rows() int { return len(b.times) }

type strategy struct {
	name string
	run  func(ctx context.Context, c *pgx.Conn, b batchData) error
}

func strategies() []strategy {
	multiSQL := func(b batchData) (string, []any) {
		var sb strings.Builder
		sb.WriteString("INSERT INTO sensor_data (time, sensor_id, seq, metric_id, value, received_at) VALUES ")
		args := make([]any, 0, b.rows()*6)
		for i := 0; i < b.rows(); i++ {
			if i > 0 {
				sb.WriteByte(',')
			}
			k := i * 6
			fmt.Fprintf(&sb, "($%d,$%d,$%d,$%d,$%d,$%d)", k+1, k+2, k+3, k+4, k+5, k+6)
			args = append(args, b.times[i], b.sensors[i], b.seqs[i], b.metrics[i], b.values[i], b.received[i])
		}
		sb.WriteString(" ON CONFLICT DO NOTHING RETURNING sensor_id, seq, time")
		return sb.String(), args
	}
	drain := func(rows pgx.Rows) error {
		for rows.Next() {
		}
		rows.Close()
		return rows.Err()
	}
	return []strategy{
		{"multirow-tx", func(ctx context.Context, c *pgx.Conn, b batchData) error {
			tx, err := c.Begin(ctx)
			if err != nil {
				return err
			}
			sql, args := multiSQL(b)
			rows, err := tx.Query(ctx, sql, args...)
			if err != nil {
				_ = tx.Rollback(ctx)
				return err
			}
			if err := drain(rows); err != nil {
				_ = tx.Rollback(ctx)
				return err
			}
			return tx.Commit(ctx)
		}},
		{"multirow", func(ctx context.Context, c *pgx.Conn, b batchData) error {
			sql, args := multiSQL(b)
			rows, err := c.Query(ctx, sql, args...)
			if err != nil {
				return err
			}
			return drain(rows)
		}},
		{"unnest", func(ctx context.Context, c *pgx.Conn, b batchData) error {
			_, err := c.Exec(ctx, `INSERT INTO sensor_data (time, sensor_id, seq, metric_id, value, received_at)
				SELECT * FROM unnest($1::timestamptz[], $2::int[], $3::bigint[], $4::smallint[], $5::float8[], $6::timestamptz[])
				ON CONFLICT DO NOTHING`, b.times, b.sensors, b.seqs, b.metrics, b.values, b.received)
			return err
		}},
		{"copy", func(ctx context.Context, c *pgx.Conn, b batchData) error {
			tx, err := c.Begin(ctx)
			if err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `CREATE TEMP TABLE stg (time timestamptz, sensor_id int, seq bigint, metric_id smallint, value float8, received_at timestamptz) ON COMMIT DROP`); err != nil {
				_ = tx.Rollback(ctx)
				return err
			}
			src := make([][]any, b.rows())
			for i := range src {
				src[i] = []any{b.times[i], b.sensors[i], b.seqs[i], b.metrics[i], b.values[i], b.received[i]}
			}
			if _, err := tx.CopyFrom(ctx, pgx.Identifier{"stg"}, []string{"time", "sensor_id", "seq", "metric_id", "value", "received_at"}, pgx.CopyFromRows(src)); err != nil {
				_ = tx.Rollback(ctx)
				return err
			}
			if _, err := tx.Exec(ctx, `INSERT INTO sensor_data (time, sensor_id, seq, metric_id, value, received_at)
				SELECT time, sensor_id, seq, metric_id, value, received_at FROM stg ON CONFLICT DO NOTHING`); err != nil {
				_ = tx.Rollback(ctx)
				return err
			}
			return tx.Commit(ctx)
		}},
	}
}

func TestInsertStrategies(t *testing.T) {
	testenv.RequireDocker(t)
	targetRows := envInt("EXP_TARGET_ROWS", 40000) // 조합마다 넣는 대략의 행 수
	ctx := context.Background()

	pg := testenv.StartPostgres(t)
	pg.SeedSensors(t, 1, 2, 3, 4, 5)
	admin := pg.Conn(t)
	defer admin.Close(ctx)
	var ids []int16
	rows, err := admin.Query(ctx, `SELECT id FROM metric WHERE name IN ('tilt_x', 'tilt_y') ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id int16
		_ = rows.Scan(&id)
		ids = append(ids, id)
	}
	rows.Close()
	if len(ids) != 2 {
		t.Fatalf("metric ids = %v", ids)
	}

	proxy := testenv.NewFaultProxy(t, pg.Addr)
	conn, err := pgx.Connect(ctx, pg.DSNFor(proxy.Addr()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)

	var seq int64
	t.Logf("conditions: TimescaleDB container, same-machine, rows/combination≈%d, 2 rows per message", targetRows)
	t.Logf("%-12s %8s %10s | %10s %10s %12s %10s", "strategy", "batch", "latency/dir", "rows/s", "msg/s", "batch p50", "batch p95")
	for _, latency := range []time.Duration{0, 20 * time.Millisecond} {
		proxy.SetLatency(latency)
		for _, batch := range []int{300, 1000, 3000} {
			n := targetRows / (batch * len(ids))
			if n < 5 {
				n = 5
			}
			for _, st := range strategies() {
				lat := make([]time.Duration, 0, n)
				start := time.Now()
				for i := 0; i < n; i++ {
					b := genBatch(batch, &seq, ids)
					s := time.Now()
					if err := st.run(ctx, conn, b); err != nil {
						t.Fatalf("%s batch=%d: %v", st.name, batch, err)
					}
					lat = append(lat, time.Since(s))
				}
				total := time.Since(start)
				sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
				totalRows := n * batch * len(ids)
				t.Logf("%-12s %8d %10s | %10.0f %10.0f %12s %10s", st.name, batch, latency,
					float64(totalRows)/total.Seconds(), float64(n*batch)/total.Seconds(),
					lat[len(lat)/2].Round(time.Microsecond), lat[len(lat)*95/100].Round(time.Microsecond))
			}
		}
	}
	proxy.SetLatency(0)
}
