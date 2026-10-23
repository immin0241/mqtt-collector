package workers

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// fakeStore는 Store 대역이다. 삽입과 ack의 선후 관계를 같은 로그에 남긴다.
type fakeStore struct {
	mu       sync.Mutex
	log      []string // "insert:<id>/<seq>", "ack:<id>/<seq>"
	rows     []SensorData
	rejected []RejectedRow

	failTransient int // 앞의 N번 InsertRows 호출은 일시적 오류
	badSensor     int // 이 sensor_id를 포함한 InsertRows는 FK 위반(23503)
	calls         int
	seen          map[string]bool // 중복 키 (sensor_id, seq, time)
}

func (f *fakeStore) InsertRows(ctx context.Context, rows []SensorData) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.calls <= f.failTransient {
		return 0, errors.New("connection refused")
	}
	for _, r := range rows {
		if f.badSensor != 0 && r.SensorID == f.badSensor {
			return 0, &pgconn.PgError{Code: "23503", Message: "foreign key violation"}
		}
	}
	var inserted int64
	for _, r := range rows {
		key := fmt.Sprintf("%d/%d/%d", r.SensorID, r.Seq, r.Time.UnixNano())
		if f.seen == nil {
			f.seen = map[string]bool{}
		}
		if f.seen[key] {
			continue // ON CONFLICT DO NOTHING
		}
		f.seen[key] = true
		f.rows = append(f.rows, r)
		f.log = append(f.log, fmt.Sprintf("insert:%d/%d", r.SensorID, r.Seq))
		inserted++
	}
	return inserted, nil
}

func (f *fakeStore) InsertRejected(ctx context.Context, row RejectedRow) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rejected = append(f.rejected, row)
	return nil
}

func (f *fakeStore) ack(id int, seq int64) func() {
	return func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.log = append(f.log, fmt.Sprintf("ack:%d/%d", id, seq))
	}
}

func (f *fakeStore) snapshot() (log []string, rows []SensorData, rejected []RejectedRow) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.log...), append([]SensorData(nil), f.rows...), append([]RejectedRow(nil), f.rejected...)
}

func (f *fakeStore) countPrefix(prefix string) int {
	log, _, _ := f.snapshot()
	n := 0
	for _, l := range log {
		if strings.HasPrefix(l, prefix) {
			n++
		}
	}
	return n
}

func envelope(f *fakeStore, id int, seq int64) Envelope {
	return Envelope{
		Data:       SensorData{SensorID: id, Seq: seq, Time: time.Unix(seq, 0)},
		Topic:      fmt.Sprintf("sensors/tilt/%d", id),
		Payload:    []byte(fmt.Sprintf(`{"sensor_id":%d,"seq":%d}`, id, seq)),
		ReceivedAt: time.Now(),
		Ack:        f.ack(id, seq),
	}
}

func eventually(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met in time")
}

func startWorker(t *testing.T, w *DbPushWorker) (cancel func(), done <-chan error) {
	t.Helper()
	ctx, c := context.WithCancel(context.Background())
	d := make(chan error, 1)
	go func() { d <- w.Run(ctx) }()
	t.Cleanup(c)
	return c, d
}

// 배치 건수가 BatchSize에 도달하면 flush하고, 커밋 뒤에만 ack한다.
func TestFlushOnBatchSize_AckAfterCommit(t *testing.T) {
	fs := &fakeStore{}
	ch := make(chan Envelope, 10)
	w := &DbPushWorker{Store: fs, DataChannel: ch, BatchSize: 3, FlushInterval: time.Hour}
	cancel, done := startWorker(t, w)

	for seq := int64(1); seq <= 3; seq++ {
		ch <- envelope(fs, 1, seq)
	}
	eventually(t, func() bool { return fs.countPrefix("ack:") == 3 })

	log, rows, rejected := fs.snapshot()
	if len(rows) != 3 || len(rejected) != 0 {
		t.Fatalf("rows=%d rejected=%d", len(rows), len(rejected))
	}
	// 모든 insert가 첫 ack보다 앞서야 한다. (불변식 1)
	want := []string{"insert:1/1", "insert:1/2", "insert:1/3", "ack:1/1", "ack:1/2", "ack:1/3"}
	for i := range want {
		if log[i] != want[i] {
			t.Fatalf("log[%d]=%s want %s (log=%v)", i, log[i], want[i], log)
		}
	}
	cancel()
	<-done
}

// 건수가 모자라도 첫 메시지 이후 FlushInterval이 지나면 flush한다.
func TestFlushOnInterval(t *testing.T) {
	fs := &fakeStore{}
	ch := make(chan Envelope, 10)
	w := &DbPushWorker{Store: fs, DataChannel: ch, BatchSize: 100, FlushInterval: 20 * time.Millisecond}
	cancel, done := startWorker(t, w)

	ch <- envelope(fs, 1, 1)
	eventually(t, func() bool { return fs.countPrefix("ack:") == 1 })
	cancel()
	<-done
}

// 일시적 오류는 배치를 버리지 않고 재시도하며, 성공 전에는 ack하지 않는다. (TC-02)
func TestTransientErrorRetriedWithoutAckOrLoss(t *testing.T) {
	fs := &fakeStore{failTransient: 4}
	ch := make(chan Envelope, 10)
	w := &DbPushWorker{
		Store: fs, DataChannel: ch, BatchSize: 2, FlushInterval: time.Hour,
		RetryInitial: time.Millisecond, RetryMax: 5 * time.Millisecond,
	}
	cancel, done := startWorker(t, w)

	ch <- envelope(fs, 1, 1)
	ch <- envelope(fs, 1, 2)
	eventually(t, func() bool { return fs.countPrefix("ack:") == 2 })

	log, rows, rejected := fs.snapshot()
	if len(rows) != 2 || len(rejected) != 0 {
		t.Fatalf("rows=%d rejected=%d, want 2/0", len(rows), len(rejected))
	}
	if log[0] != "insert:1/1" {
		t.Fatalf("ack happened before commit: %v", log)
	}
	cancel()
	<-done
}

// 영구 오류(FK 위반) 행 하나만 격리하고 나머지는 저장·ack한다. (TC-07)
func TestPermanentErrorIsolatesOnlyBadRow(t *testing.T) {
	fs := &fakeStore{badSensor: 99}
	ch := make(chan Envelope, 10)
	w := &DbPushWorker{Store: fs, DataChannel: ch, BatchSize: 5, FlushInterval: time.Hour}
	cancel, done := startWorker(t, w)

	ids := []int{1, 2, 99, 3, 4}
	for i, id := range ids {
		ch <- envelope(fs, id, int64(i+1))
	}
	eventually(t, func() bool { return fs.countPrefix("ack:") == 5 })

	_, rows, rejected := fs.snapshot()
	if len(rows) != 4 {
		t.Fatalf("stored %d rows, want 4", len(rows))
	}
	if len(rejected) != 1 {
		t.Fatalf("rejected %d rows, want 1", len(rejected))
	}
	r := rejected[0]
	if r.ErrorCode != "23503" || r.SensorID == nil || *r.SensorID != 99 || len(r.Payload) == 0 {
		t.Fatalf("unexpected rejected row: %+v", r)
	}
	cancel()
	<-done
}

// JSON 파싱 실패 메시지는 원문 그대로 격리하고 ack한다.
func TestParseErrorRejectedWithRawPayloadAndAcked(t *testing.T) {
	fs := &fakeStore{}
	ch := make(chan Envelope, 10)
	w := &DbPushWorker{Store: fs, DataChannel: ch, BatchSize: 2, FlushInterval: time.Hour}
	cancel, done := startWorker(t, w)

	bad := Envelope{
		Topic:      "sensors/tilt/1",
		Payload:    []byte("{not json"),
		ReceivedAt: time.Now(),
		ParseErr:   errors.New("invalid character"),
		Ack:        fs.ack(0, 0),
	}
	ch <- bad
	ch <- envelope(fs, 1, 1)
	eventually(t, func() bool { return fs.countPrefix("ack:") == 2 })

	_, rows, rejected := fs.snapshot()
	if len(rows) != 1 || len(rejected) != 1 {
		t.Fatalf("rows=%d rejected=%d, want 1/1", len(rows), len(rejected))
	}
	if rejected[0].ErrorCode != ParseErrorCode || string(rejected[0].Payload) != "{not json" {
		t.Fatalf("unexpected rejected row: %+v", rejected[0])
	}
	cancel()
	<-done
}

// 종료 시 채널에 남은 메시지를 drain해 최종 flush하고 ack한 뒤 Flushed를 닫는다.
func TestShutdownDrainsChannelAndFlushes(t *testing.T) {
	fs := &fakeStore{}
	ch := make(chan Envelope, 10)
	unsub := make(chan struct{})
	flushed := make(chan struct{})
	w := &DbPushWorker{
		Store: fs, DataChannel: ch, BatchSize: 100, FlushInterval: time.Hour,
		Unsubscribed: unsub, Flushed: flushed,
	}
	cancel, done := startWorker(t, w)

	for seq := int64(1); seq <= 5; seq++ {
		ch <- envelope(fs, 1, seq)
	}
	cancel()
	close(unsub)
	<-done

	select {
	case <-flushed:
	default:
		t.Fatal("Flushed not closed")
	}
	if got := fs.countPrefix("ack:"); got != 5 {
		t.Fatalf("acked %d, want 5", got)
	}
	_, rows, _ := fs.snapshot()
	if len(rows) != 5 {
		t.Fatalf("stored %d, want 5", len(rows))
	}
}

// 종료 중 DB가 계속 실패하면 ack하지 않는다. 브로커가 재전달하므로 유실이 아니다.
func TestShutdownFlushFailureLeavesMessagesUnacked(t *testing.T) {
	fs := &fakeStore{failTransient: 1 << 30}
	ch := make(chan Envelope, 10)
	unsub := make(chan struct{})
	close(unsub)
	w := &DbPushWorker{
		Store: fs, DataChannel: ch, BatchSize: 100, FlushInterval: time.Hour,
		RetryInitial: time.Millisecond, RetryMax: 2 * time.Millisecond,
		ShutdownTimeout: 50 * time.Millisecond, Unsubscribed: unsub,
	}
	cancel, done := startWorker(t, w)

	ch <- envelope(fs, 1, 1)
	cancel()
	<-done

	if got := fs.countPrefix("ack:"); got != 0 {
		t.Fatalf("acked %d messages despite failed commit", got)
	}
}

// 같은 이벤트가 3번 와도 DB에는 1건이고, 중복 제거 지표가 2 증가하며, 3건 모두 ack한다. (TC-05)
func TestDuplicateEventsDedupedAndCounted(t *testing.T) {
	fs := &fakeStore{}
	ch := make(chan Envelope, 10)
	w := &DbPushWorker{Store: fs, DataChannel: ch, BatchSize: 3, FlushInterval: time.Hour}
	committedBefore := testutil.ToFloat64(rowsCommitted)
	dedupBefore := testutil.ToFloat64(dedupTotal)
	cancel, done := startWorker(t, w)

	for i := 0; i < 3; i++ {
		ch <- envelope(fs, 7, 1)
	}
	eventually(t, func() bool { return fs.countPrefix("ack:") == 3 })

	_, rows, _ := fs.snapshot()
	if len(rows) != 1 {
		t.Fatalf("stored %d rows, want 1", len(rows))
	}
	if got := testutil.ToFloat64(dedupTotal) - dedupBefore; got != 2 {
		t.Fatalf("dedup_total increased by %v, want 2", got)
	}
	if got := testutil.ToFloat64(rowsCommitted) - committedBefore; got != 1 {
		t.Fatalf("rows_committed_total increased by %v, want 1", got)
	}
	cancel()
	<-done
}
