package workers

import (
	"testing"
	"time"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func f64(v float64) *float64 { return &v }

func newTracker(t *testing.T, cfg HealthConfig, ids ...int) *HealthTracker {
	t.Helper()
	if cfg.StartedAt.IsZero() {
		cfg.StartedAt = t0.Add(-time.Hour) // 기본은 유예 없음
	}
	tr := NewHealthTrackerWithConfig(cfg)
	intervals := map[int]time.Duration{}
	for _, id := range ids {
		intervals[id] = time.Second
	}
	tr.SyncSensors(intervals, t0)
	return tr
}

func reading(id int, seq int64, at time.Time, tiltX float64) SensorData {
	return SensorData{SensorID: id, Seq: seq, ReceivedAt: at, TiltX: f64(tiltX)}
}

func statusOf(tr *HealthTracker, id int) SensorStatus {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return tr.sensors[id].status
}

func eventTypes(evs []SensorEvent) []string {
	var out []string
	for _, e := range evs {
		out = append(out, e.EventType)
	}
	return out
}

func TestTimeoutUsesFactorAndMinimum(t *testing.T) {
	tr := newTracker(t, HealthConfig{TimeoutFactor: 3, MinTimeout: 500 * time.Millisecond}, 1)
	tr.ObserveData(reading(1, 0, t0, 0.1))

	tr.CheckTimeouts(t0.Add(2900 * time.Millisecond)) // 한도 3초 이내
	if got := statusOf(tr, 1); got != StatusOnline {
		t.Fatalf("status = %s, want online", got)
	}
	tr.CheckTimeouts(t0.Add(3100 * time.Millisecond))
	if got := statusOf(tr, 1); got != StatusOffline {
		t.Fatalf("status = %s, want offline", got)
	}
	evs, _ := tr.Drain(false)
	if got := eventTypes(evs); len(got) != 2 || got[0] != EventConnected || got[1] != EventTimeout {
		t.Fatalf("events = %v", got)
	}
}

func TestTimeoutNeverBelowMinTimeout(t *testing.T) {
	tr := newTracker(t, HealthConfig{TimeoutFactor: 1, MinTimeout: 5 * time.Second}, 1)
	tr.ObserveData(reading(1, 0, t0, 0.1))
	tr.CheckTimeouts(t0.Add(4 * time.Second)) // 주기 1초×1 이지만 하한 5초
	if got := statusOf(tr, 1); got != StatusOnline {
		t.Fatalf("status = %s, want online (min timeout)", got)
	}
}

func TestGraceSuppressesTimeoutsAfterStart(t *testing.T) {
	tr := newTracker(t, HealthConfig{Grace: 30 * time.Second, StartedAt: t0}, 1)
	tr.ObserveData(reading(1, 0, t0, 0.1))

	tr.CheckTimeouts(t0.Add(20 * time.Second)) // 유예 중
	if got := statusOf(tr, 1); got != StatusOnline {
		t.Fatalf("status during grace = %s", got)
	}
	tr.CheckTimeouts(t0.Add(31 * time.Second))
	if got := statusOf(tr, 1); got != StatusOffline {
		t.Fatalf("status after grace = %s, want offline", got)
	}
}

func TestRestoreSnapshotGivesGraceAndNoFalseAlarm(t *testing.T) {
	tr := newTracker(t, HealthConfig{}, 1, 2, 3)
	seq := int64(41)
	seen := t0.Add(-time.Hour)
	n := tr.RestoreSnapshot([]SensorStatusRow{
		{SensorID: 1, Status: StatusOnline, LastSeenAt: &seen, LastSeq: &seq, StatusChangedAt: seen},
		{SensorID: 2, Status: StatusOffline, LastSeenAt: &seen, StatusChangedAt: seen},
		{SensorID: 3, Status: StatusUnknown, StatusChangedAt: seen},
	}, t0)
	if n != 2 {
		t.Fatalf("restored = %d, want 2", n)
	}
	if statusOf(tr, 1) != StatusOnline || statusOf(tr, 2) != StatusOffline || statusOf(tr, 3) != StatusUnknown {
		t.Fatalf("states = %s %s %s", statusOf(tr, 1), statusOf(tr, 2), statusOf(tr, 3))
	}

	// 재시작 직후: 한 시간 전에 본 센서를 곧바로 offline 으로 만들면 오경보다.
	tr.CheckTimeouts(t0.Add(500 * time.Millisecond))
	if statusOf(tr, 1) != StatusOnline {
		t.Fatal("restored online sensor timed out immediately after restart")
	}
	// 복원한 상태는 DB와 같으므로 다시 쓰지 않는다 (unknown 신규 센서 3만 dirty).
	evs, rows := tr.Drain(false)
	if len(evs) != 0 {
		t.Fatalf("restore produced events: %v", eventTypes(evs))
	}
	if len(rows) != 1 || rows[0].SensorID != 3 {
		t.Fatalf("rows = %+v, want only sensor 3", rows)
	}

	// 그대로 침묵하면 한도 뒤 정상 경로로 offline 이 된다 (재시작 때문에 영영 판정되지 않는 일이 없다).
	tr.CheckTimeouts(t0.Add(5 * time.Second))
	if statusOf(tr, 1) != StatusOffline {
		t.Fatal("silent restored sensor never went offline")
	}
}

func TestRestoreDoesNotOverrideLiveData(t *testing.T) {
	tr := newTracker(t, HealthConfig{}, 1)
	tr.ObserveData(reading(1, 5, t0, 0.1)) // 복원보다 데이터가 먼저 도착
	seen := t0.Add(-time.Hour)
	if n := tr.RestoreSnapshot([]SensorStatusRow{{SensorID: 1, Status: StatusOffline, LastSeenAt: &seen}}, t0); n != 0 {
		t.Fatalf("restored %d, want 0", n)
	}
	if statusOf(tr, 1) != StatusOnline {
		t.Fatal("snapshot overwrote live state")
	}
}

func TestFlatlineMakesSensorFaultyNotOffline(t *testing.T) {
	tr := newTracker(t, HealthConfig{FlatlineCount: 5}, 1)
	for i := 0; i < 4; i++ {
		tr.ObserveData(reading(1, int64(i), t0.Add(time.Duration(i)*time.Second), 0.5))
	}
	if statusOf(tr, 1) != StatusOnline {
		t.Fatalf("faulty too early: %s", statusOf(tr, 1))
	}
	tr.ObserveData(reading(1, 4, t0.Add(4*time.Second), 0.5))
	if statusOf(tr, 1) != StatusFaulty {
		t.Fatalf("status = %s, want faulty", statusOf(tr, 1))
	}

	// 값이 바뀌면 장애가 풀린다.
	tr.ObserveData(reading(1, 5, t0.Add(5*time.Second), 0.6))
	if statusOf(tr, 1) != StatusOnline {
		t.Fatalf("status = %s, want online after change", statusOf(tr, 1))
	}
	evs, _ := tr.Drain(false)
	got := eventTypes(evs)
	want := []string{EventConnected, EventFault, EventFaultCleared}
	if len(got) != len(want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("events = %v, want %v", got, want)
		}
	}
}

func TestOutOfRangeNeedsConsecutiveReadings(t *testing.T) {
	tr := newTracker(t, HealthConfig{OutOfRangeCount: 3, FlatlineCount: 100}, 1)
	at := func(i int) time.Time { return t0.Add(time.Duration(i) * time.Second) }

	tr.ObserveData(reading(1, 0, at(0), 99)) // 범위 밖 1
	tr.ObserveData(reading(1, 1, at(1), 98)) // 2
	tr.ObserveData(reading(1, 2, at(2), 0.1))
	if statusOf(tr, 1) != StatusOnline {
		t.Fatalf("one-off out-of-range triggered fault: %s", statusOf(tr, 1))
	}
	for i := 3; i < 6; i++ {
		tr.ObserveData(reading(1, int64(i), at(i), 90+float64(i)))
	}
	if statusOf(tr, 1) != StatusFaulty {
		t.Fatalf("status = %s, want faulty", statusOf(tr, 1))
	}
	tr.ObserveData(reading(1, 6, at(6), 0.2))
	if statusOf(tr, 1) != StatusOnline {
		t.Fatalf("status = %s, want online", statusOf(tr, 1))
	}
}

func TestLWTOnlineDoesNotClearFault(t *testing.T) {
	tr := newTracker(t, HealthConfig{FlatlineCount: 2}, 1)
	tr.ObserveData(reading(1, 0, t0, 0.5))
	tr.ObserveData(reading(1, 1, t0.Add(time.Second), 0.5))
	tr.SetStatus(1, StatusOnline, t0.Add(2*time.Second))
	if statusOf(tr, 1) != StatusFaulty {
		t.Fatalf("status = %s, want faulty to persist", statusOf(tr, 1))
	}
	tr.SetStatus(1, StatusOffline, t0.Add(3*time.Second)) // 연결 자체가 끊기면 통신 오류가 우선
	if statusOf(tr, 1) != StatusOffline {
		t.Fatalf("status = %s, want offline", statusOf(tr, 1))
	}
}

func TestFaultySensorThatGoesSilentBecomesOffline(t *testing.T) {
	tr := newTracker(t, HealthConfig{FlatlineCount: 2}, 1)
	tr.ObserveData(reading(1, 0, t0, 0.5))
	tr.ObserveData(reading(1, 1, t0.Add(time.Second), 0.5))
	tr.CheckTimeouts(t0.Add(10 * time.Second))
	if statusOf(tr, 1) != StatusOffline {
		t.Fatalf("status = %s, want offline", statusOf(tr, 1))
	}
}

func TestWholeFleetSilenceIsIngestStallNotSensorFailures(t *testing.T) {
	tr := newTracker(t, HealthConfig{SystemicMinOnline: 3}, 1, 2, 3)
	for id := 1; id <= 3; id++ {
		tr.ObserveData(reading(id, 0, t0, 0.1))
	}
	tr.Drain(true)

	tr.CheckTimeouts(t0.Add(10 * time.Second)) // 전부 조용하다
	for id := 1; id <= 3; id++ {
		if statusOf(tr, id) != StatusOnline {
			t.Fatalf("sensor %d = %s; per-sensor judgment must be suspended", id, statusOf(tr, id))
		}
	}
	if !tr.Stalled() {
		t.Fatal("not marked stalled")
	}
	tr.CheckTimeouts(t0.Add(11 * time.Second)) // 반복해도 경보는 한 건
	ing := tr.DrainIngest()
	if len(ing) != 1 || ing[0].EventType != IngestStalled {
		t.Fatalf("ingest events = %+v, want one stalled", ing)
	}

	// 데이터가 다시 오면 재개하고, 아직 못 들은 센서에는 유예를 준다.
	tr.ObserveData(reading(1, 1, t0.Add(12*time.Second), 0.2))
	if tr.Stalled() {
		t.Fatal("still stalled after data resumed")
	}
	tr.CheckTimeouts(t0.Add(12*time.Second + 500*time.Millisecond))
	if statusOf(tr, 2) != StatusOnline || statusOf(tr, 3) != StatusOnline {
		t.Fatal("other sensors flagged right after resume")
	}
	ing = tr.DrainIngest()
	if len(ing) != 1 || ing[0].EventType != IngestResumed {
		t.Fatalf("ingest events = %+v, want one resumed", ing)
	}
}

func TestPartialSilenceStillMarksIndividualSensors(t *testing.T) {
	tr := newTracker(t, HealthConfig{SystemicMinOnline: 3}, 1, 2, 3)
	for id := 1; id <= 3; id++ {
		tr.ObserveData(reading(id, 0, t0, 0.1))
	}
	now := t0.Add(10 * time.Second)
	tr.ObserveData(reading(1, 1, now, 0.2))
	tr.ObserveData(reading(2, 1, now, 0.2))
	tr.CheckTimeouts(now.Add(100 * time.Millisecond))

	if statusOf(tr, 3) != StatusOffline {
		t.Fatalf("sensor 3 = %s, want offline", statusOf(tr, 3))
	}
	if tr.Stalled() {
		t.Fatal("one dead sensor must not look like an ingest stall")
	}
}

func TestSmallFleetUsesPerSensorJudgment(t *testing.T) {
	tr := newTracker(t, HealthConfig{SystemicMinOnline: 3}, 1, 2)
	tr.ObserveData(reading(1, 0, t0, 0.1))
	tr.ObserveData(reading(2, 0, t0, 0.1))
	tr.CheckTimeouts(t0.Add(10 * time.Second))
	if statusOf(tr, 1) != StatusOffline || statusOf(tr, 2) != StatusOffline {
		t.Fatalf("states = %s %s", statusOf(tr, 1), statusOf(tr, 2))
	}
}

func TestBrokerOutagePausesJudgmentAndRecordsEvents(t *testing.T) {
	tr := newTracker(t, HealthConfig{}, 1)
	tr.ObserveData(reading(1, 0, t0, 0.1))

	tr.SetIngestHealthy(false, t0.Add(time.Second))
	tr.CheckTimeouts(t0.Add(30 * time.Second))
	if statusOf(tr, 1) != StatusOnline {
		t.Fatal("sensor judged while broker connection was down")
	}
	tr.SetIngestHealthy(true, t0.Add(31*time.Second))
	tr.CheckTimeouts(t0.Add(31*time.Second + 500*time.Millisecond)) // 복구 직후 유예
	if statusOf(tr, 1) != StatusOnline {
		t.Fatal("sensor flagged right after broker came back")
	}

	ing := tr.DrainIngest()
	if len(ing) != 2 || ing[0].EventType != IngestBrokerDown || ing[1].EventType != IngestBrokerUp {
		t.Fatalf("ingest events = %+v", ing)
	}
}

func TestCountByState(t *testing.T) {
	tr := newTracker(t, HealthConfig{FlatlineCount: 2, SystemicMinOnline: 10}, 1, 2, 3, 4)
	tr.ObserveData(reading(1, 0, t0, 0.1))
	tr.ObserveData(reading(2, 0, t0, 0.1))
	tr.ObserveData(reading(2, 1, t0.Add(time.Second), 0.1)) // faulty
	tr.ObserveData(reading(3, 0, t0, 0.1))
	tr.CheckTimeouts(t0.Add(10 * time.Second)) // 1, 3 offline, 2 faulty→offline
	tr.ObserveData(reading(1, 1, t0.Add(10*time.Second), 0.3))

	got := tr.CountByState()
	if got[StatusOnline] != 1 || got[StatusOffline] != 2 || got[StatusUnknown] != 1 || got[StatusFaulty] != 0 {
		t.Fatalf("counts = %v", got)
	}
}

func TestStateJudgedAtReceiveTimeIndependentOfDB(t *testing.T) {
	// 불변식 7: DB 반영(Drain/Requeue)이 실패해도 메모리 상태는 그대로다.
	tr := newTracker(t, HealthConfig{}, 1)
	tr.ObserveData(reading(1, 0, t0, 0.1))
	evs, rows := tr.Drain(true)
	tr.Requeue(evs, rows) // DB 쓰기 실패를 흉내
	tr.CheckTimeouts(t0.Add(500 * time.Millisecond))
	if statusOf(tr, 1) != StatusOnline {
		t.Fatalf("status = %s", statusOf(tr, 1))
	}
	evs2, _ := tr.Drain(false)
	if len(evs2) != len(evs) {
		t.Fatalf("requeued events lost: %d vs %d", len(evs2), len(evs))
	}
}
