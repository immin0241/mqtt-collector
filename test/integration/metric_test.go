package integration

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/immin0241/webframework-project-mqtt-collector/internal/testenv"
	"github.com/immin0241/webframework-project-mqtt-collector/workers"
)

func ptr(v float64) *float64 { return &v }

func openStore(t *testing.T, pg *testenv.Postgres) *workers.GormStore {
	t.Helper()
	db, err := gorm.Open(postgres.Open(pg.DSN), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatalf("open gorm: %v", err)
	}
	store, err := workers.NewGormStore(context.Background(), db)
	if err != nil {
		t.Fatalf("NewGormStore: %v", err)
	}
	return store
}

func pgCount(t *testing.T, pg *testenv.Postgres, sql string, args ...any) int {
	t.Helper()
	conn := pg.Conn(t)
	defer conn.Close(context.Background())
	var n int
	if err := conn.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return n
}

// 세로 구조(ADR-0012): 메시지 하나가 지표별 행으로 펼쳐지고, 중복은 (sensor_id, seq, metric_id, time)으로 걸러지며,
// InsertRows는 행이 아니라 새로 저장된 메시지 수를 돌려준다.
func TestStoreExpandsMessagesIntoMeasurementRows(t *testing.T) {
	testenv.RequireDocker(t)
	pg := testenv.StartPostgres(t)
	pg.SeedSensors(t, 1, 2)
	store := openStore(t, pg)
	ctx := context.Background()

	now := time.Now().UTC().Truncate(time.Microsecond)
	tilt := workers.SensorData{Time: now, SensorID: 1, Seq: 1, TiltX: ptr(0.1), TiltY: ptr(0.2), ReceivedAt: now}
	crack := workers.SensorData{Time: now, SensorID: 2, Seq: 1, CrackWidth: ptr(1.5), ReceivedAt: now}

	n, err := store.InsertRows(ctx, []workers.SensorData{tilt, crack})
	if err != nil || n != 2 {
		t.Fatalf("first insert: n=%d err=%v, want 2 messages", n, err)
	}
	if got := pgCount(t, pg, `SELECT count(*) FROM sensor_data`); got != 3 {
		t.Fatalf("measurement rows = %d, want 3 (tilt_x, tilt_y, crack_width)", got)
	}
	if got := pgCount(t, pg, `SELECT count(*) FROM (SELECT DISTINCT sensor_id, seq FROM sensor_data) s`); got != 2 {
		t.Fatalf("distinct messages = %d, want 2", got)
	}

	// 같은 메시지를 다시 보내면 새로 저장된 메시지는 0이고 행은 늘지 않는다. 배치 안의 중복도 마찬가지다.
	n, err = store.InsertRows(ctx, []workers.SensorData{tilt, crack, tilt})
	if err != nil || n != 0 {
		t.Fatalf("redelivery: n=%d err=%v, want 0", n, err)
	}
	if got := pgCount(t, pg, `SELECT count(*) FROM sensor_data`); got != 3 {
		t.Fatalf("rows after redelivery = %d, want 3", got)
	}

	// 일부만 새 메시지인 배치는 새 메시지만 센다.
	next := workers.SensorData{Time: now.Add(time.Second), SensorID: 1, Seq: 2, TiltX: ptr(0.3), ReceivedAt: now}
	n, err = store.InsertRows(ctx, []workers.SensorData{tilt, next})
	if err != nil || n != 1 {
		t.Fatalf("mixed batch: n=%d err=%v, want 1", n, err)
	}
}

// 측정값이 하나도 없는 메시지는 영구 오류(SQLSTATE 22)로 분류되어, 이분법으로 그 메시지만 격리 테이블로 가고
// 같은 배치의 정상 메시지는 저장된다. 둘 다 ack되어야 한다. (불변식 3)
func TestMessageWithoutMeasurementIsQuarantined(t *testing.T) {
	testenv.RequireDocker(t)
	pg := testenv.StartPostgres(t)
	pg.SeedSensors(t, 1)
	store := openStore(t, pg)

	now := time.Now().UTC().Truncate(time.Microsecond)
	empty := workers.SensorData{Time: now, SensorID: 1, Seq: 7, ReceivedAt: now}
	good := workers.SensorData{Time: now, SensorID: 1, Seq: 8, TiltX: ptr(0.5), ReceivedAt: now}

	_, err := store.InsertRows(context.Background(), []workers.SensorData{empty})
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != workers.NoMeasurementCode {
		t.Fatalf("empty message: err=%v, want SQLSTATE %s", err, workers.NoMeasurementCode)
	}
	if workers.ClassifyDBError(err) != workers.KindPermanent {
		t.Fatal("empty message must be a permanent error (otherwise the worker retries forever)")
	}

	data := make(chan workers.Envelope, 4)
	var mu sync.Mutex
	acked := map[int64]bool{}
	mk := func(d workers.SensorData) workers.Envelope {
		return workers.Envelope{
			Data: d, Topic: "sensors/crack/1", Payload: []byte(fmt.Sprintf(`{"sensor_id":1,"seq":%d}`, d.Seq)), ReceivedAt: now,
			Ack: func() { mu.Lock(); acked[d.Seq] = true; mu.Unlock() },
		}
	}
	w := &workers.DbPushWorker{Store: store, DataChannel: data, BatchSize: 2, FlushInterval: 100 * time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = w.Run(ctx); close(done) }()
	data <- mk(empty)
	data <- mk(good)

	deadline := time.Now().Add(15 * time.Second)
	for {
		mu.Lock()
		ok := acked[7] && acked[8]
		mu.Unlock()
		if ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("messages not acked in time: %v", acked)
		}
		time.Sleep(50 * time.Millisecond)
	}
	cancel()
	<-done

	if got := pgCount(t, pg, `SELECT count(*) FROM sensor_data_rejected WHERE sensor_id = 1 AND seq = 7 AND error_code = $1`, workers.NoMeasurementCode); got != 1 {
		t.Fatalf("quarantined empty message rows = %d, want 1", got)
	}
	if got := pgCount(t, pg, `SELECT count(*) FROM sensor_data WHERE seq = 8`); got != 1 {
		t.Fatalf("good message rows = %d, want 1", got)
	}
	if got := pgCount(t, pg, `SELECT count(*) FROM sensor_data WHERE seq = 7`); got != 0 {
		t.Fatalf("empty message left %d measurement rows", got)
	}
}

// 뷰는 단위를 카탈로그에서 붙여 보여 주고, tenant role은 metric 테이블을 직접 읽을 수 없다.
func TestViewsExposeUnitFromCatalog(t *testing.T) {
	testenv.RequireDocker(t)
	pg := testenv.StartPostgres(t)
	pg.SeedSensors(t, 1)
	store := openStore(t, pg)
	ctx := context.Background()

	now := time.Now().UTC().Truncate(time.Microsecond)
	msg := workers.SensorData{Time: now, SensorID: 1, Seq: 1, TiltX: ptr(1.5), Temperature: ptr(21.0), ReceivedAt: now}
	if _, err := store.InsertRows(ctx, []workers.SensorData{msg}); err != nil {
		t.Fatal(err)
	}
	admin := pg.Conn(t)
	defer admin.Close(ctx)
	if _, err := admin.Exec(ctx, `CALL refresh_continuous_aggregate('sensor_data_1m', NULL, NULL)`); err != nil {
		t.Fatal(err)
	}

	role := provisionRole(t, pg, 1)
	conn, err := pgx.Connect(ctx, fmt.Sprintf("postgres://%s:grafana@%s/go?sslmode=disable", role, pg.Addr))
	if err != nil {
		t.Fatalf("connect as %s: %v", role, err)
	}
	defer conn.Close(ctx)

	want := map[string]string{"tilt_x": "°", "temperature": "°C"}
	for _, q := range []string{
		`SELECT metric, unit FROM v_sensor_data`,
		`SELECT metric, unit FROM v_sensor_data_1m`,
	} {
		rows, err := conn.Query(ctx, q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		got := map[string]string{}
		for rows.Next() {
			var m, u string
			_ = rows.Scan(&m, &u)
			got[m] = u
		}
		rows.Close()
		if len(got) != len(want) {
			t.Fatalf("%s: got %v, want %v", q, got, want)
		}
		for m, u := range want {
			if got[m] != u {
				t.Fatalf("%s: unit of %s = %q, want %q", q, m, got[m], u)
			}
		}
	}
	if _, err := conn.Exec(ctx, `SELECT * FROM metric`); err == nil {
		t.Fatal("tenant role can read the metric table directly")
	}
}

// 000011은 down 후 다시 up 해도 적용된다. down은 가로 구조를 빈 테이블로 복원하고 측정 데이터는 복구되지 않는다.
func TestMigration000011RoundTrip(t *testing.T) {
	testenv.RequireDocker(t)
	pg := testenv.StartPostgres(t)
	pg.SeedSensors(t, 1)
	store := openStore(t, pg)
	ctx := context.Background()
	conn := pg.Conn(t)
	defer conn.Close(ctx)

	now := time.Now().UTC().Truncate(time.Microsecond)
	if _, err := store.InsertRows(ctx, []workers.SensorData{{Time: now, SensorID: 1, Seq: 1, TiltX: ptr(1), TiltY: ptr(2), ReceivedAt: now}}); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO sensor_data_rejected (received_at, topic, payload, error_code, error_message) VALUES (now(), 't', 'x', 'PARSE_ERROR', 'x')`); err != nil {
		t.Fatal(err)
	}

	restore := withWideSensorData(t, conn)
	var wide, hasMetric bool
	_ = conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'sensor_data' AND column_name = 'tilt_x')`).Scan(&wide)
	_ = conn.QueryRow(ctx, `SELECT to_regclass('metric') IS NOT NULL`).Scan(&hasMetric)
	if !wide || hasMetric {
		t.Fatalf("after down: wide=%v metricTable=%v, want wide=true metricTable=false", wide, hasMetric)
	}
	var n, rejected int
	_ = conn.QueryRow(ctx, `SELECT count(*) FROM sensor_data`).Scan(&n)
	_ = conn.QueryRow(ctx, `SELECT count(*) FROM sensor_data_rejected`).Scan(&rejected)
	if n != 0 || rejected != 0 {
		t.Fatalf("after down: sensor_data=%d rejected=%d, want both 0 (data is discarded)", n, rejected)
	}
	// 복원된 가로 구조에서도 트리거가 tenant_id를 채운다.
	if _, err := conn.Exec(ctx, `INSERT INTO sensor_data (time, sensor_id, seq, tilt_x, received_at) VALUES (now(), 1, 1, 1.0, now())`); err != nil {
		t.Fatalf("insert into restored wide table: %v", err)
	}

	restore()
	var metrics int
	_ = conn.QueryRow(ctx, `SELECT count(*) FROM metric`).Scan(&metrics)
	if metrics != len(workers.KnownMetrics) {
		t.Fatalf("metric rows after re-up = %d, want %d", metrics, len(workers.KnownMetrics))
	}
	store = openStore(t, pg)
	if n, err := store.InsertRows(ctx, []workers.SensorData{{Time: now, SensorID: 1, Seq: 2, TiltX: ptr(1), ReceivedAt: now}}); err != nil || n != 1 {
		t.Fatalf("insert after re-up: n=%d err=%v", n, err)
	}
}
