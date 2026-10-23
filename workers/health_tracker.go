package workers

import (
	"fmt"
	"sync"
	"time"
)

type SensorStatus string

const (
	StatusUnknown SensorStatus = "unknown"
	StatusOnline  SensorStatus = "online"
	StatusOffline SensorStatus = "offline"
	// StatusFaulty는 데이터는 오지만 값이 비정상인 센서다(flatline, 범위 밖 반복). 통신 오류(offline)와 구분한다.
	StatusFaulty SensorStatus = "faulty"
)

// 전이 이벤트 타입 / 출처 (sensor_event의 CHECK 제약과 동일)
const (
	EventConnected    = "connected"
	EventDisconnected = "disconnected"
	EventTimeout      = "timeout"
	EventRecovered    = "recovered"
	EventFault        = "fault"
	EventFaultCleared = "fault_cleared"

	SourceLWT     = "lwt"
	SourceTimeout = "timeout"
	SourceData    = "data"
)

// 수집 경로 이벤트 종류 (ingest_event의 CHECK 제약과 동일)
const (
	IngestStalled    = "stalled"
	IngestResumed    = "resumed"
	IngestBrokerDown = "broker_down"
	IngestBrokerUp   = "broker_up"
)

// IngestEvent는 ingest_event 테이블 행이다. 센서가 아니라 수집 경로 전체의 상태 변화다.
type IngestEvent struct {
	Time      time.Time
	EventType string
	Message   string
}

func (IngestEvent) TableName() string { return "ingest_event" }

// PhysicalRange는 센서가 정상이라면 낼 수 있는 값의 범위다. 이 밖의 값이 반복되면 장애로 본다.
type PhysicalRange struct{ Min, Max float64 }

// HealthConfig는 상태 판정 정책이다. 0 값은 기본값을 뜻한다.
type HealthConfig struct {
	// MinTimeout은 통신 오류 판정의 하한이다.
	MinTimeout time.Duration
	// TimeoutFactor(k): 마지막 수신 후 예상 주기 × k 를 넘기면 통신 오류. 기본 2.
	TimeoutFactor float64
	// Grace: 시작 직후 이 시간 동안은 통신 오류 판정을 하지 않는다. 재시작 직후 오경보 방지.
	Grace time.Duration
	// StartedAt은 Grace의 기준 시각이다. 0이면 생성 시각.
	StartedAt time.Time
	// FlatlineCount: 같은 값이 연속 이만큼 오면 장애. 기본 10.
	FlatlineCount int
	// OutOfRangeCount: 범위 밖 값이 연속 이만큼 오면 장애. 기본 3.
	OutOfRangeCount int
	// Ranges는 지표별 정상 범위다. 비어 있으면 DefaultRanges.
	Ranges map[string]PhysicalRange
	// SystemicMinOnline: online 센서가 이 수 이상이고 전부 동시에 타임아웃이면 센서 고장이 아니라
	// 수집 경로 문제로 보고 센서별 판정을 멈춘 채 "수집 장애" 한 건만 기록한다. 기본 3.
	SystemicMinOnline int
}

// DefaultRanges는 계측기 일반 사양 기준의 정상 범위다.
var DefaultRanges = map[string]PhysicalRange{
	"tilt_x":      {-30, 30}, // °
	"tilt_y":      {-30, 30}, // °
	"crack_width": {0, 50},   // mm
	"temperature": {-40, 85}, // °C
}

func (c *HealthConfig) applyDefaults() {
	if c.TimeoutFactor <= 0 {
		c.TimeoutFactor = 2
	}
	if c.FlatlineCount <= 0 {
		c.FlatlineCount = 10
	}
	if c.OutOfRangeCount <= 0 {
		c.OutOfRangeCount = 3
	}
	if c.SystemicMinOnline <= 0 {
		c.SystemicMinOnline = 3
	}
	if c.Ranges == nil {
		c.Ranges = DefaultRanges
	}
	if c.StartedAt.IsZero() {
		c.StartedAt = time.Now()
	}
}

// SensorEvent는 sensor_event 테이블 행이다.
type SensorEvent struct {
	Time      time.Time
	SensorID  int
	EventType string
	Source    string
	Message   string
}

func (SensorEvent) TableName() string { return "sensor_event" }

// SensorStatusRow는 sensor_status 테이블 행이다.
type SensorStatusRow struct {
	SensorID        int `gorm:"primaryKey"`
	Status          SensorStatus
	LastSeenAt      *time.Time
	LastSeq         *int64
	StatusChangedAt time.Time
}

func (SensorStatusRow) TableName() string { return "sensor_status" }

type sensorEntry struct {
	interval        time.Duration
	status          SensorStatus
	lastSeen        time.Time
	lastSeq         int64
	hasSeq          bool
	statusChangedAt time.Time

	seenDirty   bool // last_seen/last_seq가 DB에 아직 반영되지 않음
	statusDirty bool // status 전이가 DB에 아직 반영되지 않음

	// 장애(faulty) 판정용: 직전 값과 연속 횟수
	prev      [4]*float64
	flatRun   int
	rangeRun  int
	faultNote string
}

// HealthTracker는 센서별 접속 상태를 메모리에서 판단한다.
// 구독 콜백에서 호출되므로 모든 메서드는 O(1)이고 DB/채널을 기다리지 않는다.
type HealthTracker struct {
	mu           sync.Mutex
	sensors      map[int]*sensorEntry
	events       []SensorEvent
	ingestEvents []IngestEvent
	cfg          HealthConfig
	ingestUp     bool
	stalled      bool // 전체 수신 두절로 센서별 판정을 멈춘 상태
}

// NewHealthTracker는 기본 정책으로 tracker를 만든다. minTimeout은 통신 오류 판정의 하한이다.
func NewHealthTracker(minTimeout time.Duration) *HealthTracker {
	return NewHealthTrackerWithConfig(HealthConfig{MinTimeout: minTimeout})
}

func NewHealthTrackerWithConfig(cfg HealthConfig) *HealthTracker {
	cfg.applyDefaults()
	return &HealthTracker{
		sensors:  make(map[int]*sensorEntry),
		cfg:      cfg,
		ingestUp: true,
	}
}

// SyncSensors는 DB의 센서 목록(id -> 기대 주기)과 맞춘다.
// 새 센서는 unknown으로 등록하고, 목록에서 사라진 센서는 제거한다.
// 새로 등록된 센서 ID를 반환한다.
func (t *HealthTracker) SyncSensors(intervals map[int]time.Duration, now time.Time) (added []int) {
	t.mu.Lock()
	defer t.mu.Unlock()

	for id, interval := range intervals {
		if e, ok := t.sensors[id]; ok {
			e.interval = interval
			continue
		}
		t.sensors[id] = &sensorEntry{interval: interval, status: StatusUnknown, statusChangedAt: now, statusDirty: true}
		added = append(added, id)
	}
	for id := range t.sensors {
		if _, ok := intervals[id]; !ok {
			delete(t.sensors, id)
		}
	}
	return added
}

// Touch는 값 없이 수신 사실만 알린다. 값을 알면 ObserveData를 쓴다.
func (t *HealthTracker) Touch(sensorID int, seq int64, at time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.touch(sensorID, seq, at)
}

// ObserveData는 센서 데이터 수신 시 호출한다. 수신 사실(last_seen)과 값 이상(flatline, 범위 밖)을 함께 판정한다.
// 수신 핸들러에서 호출되므로 O(1)이며 DB를 기다리지 않는다. 판정은 DB 저장 성공 여부와 무관하다. (불변식 7)
func (t *HealthTracker) ObserveData(d SensorData) {
	t.mu.Lock()
	defer t.mu.Unlock()

	e := t.touch(d.SensorID, d.Seq, d.ReceivedAt)
	if e == nil {
		return
	}
	t.observeValues(d.SensorID, e, d)
}

// touch는 mu를 잡은 상태에서 호출한다. 등록되지 않은 센서면 nil.
func (t *HealthTracker) touch(sensorID int, seq int64, at time.Time) *sensorEntry {
	e, ok := t.sensors[sensorID]
	if !ok {
		return nil
	}
	t.resumeIfStalled(at)

	e.lastSeen = at
	if !e.hasSeq || seq > e.lastSeq {
		e.lastSeq, e.hasSeq = seq, true
	}
	e.seenDirty = true

	switch e.status {
	case StatusUnknown:
		t.transition(sensorID, e, StatusOnline, EventConnected, SourceData, "first data received", at)
	case StatusOffline:
		t.transition(sensorID, e, StatusOnline, EventRecovered, SourceData, "data resumed", at)
	}
	return e
}

func channelValues(d SensorData) [4]*float64 {
	return [4]*float64{d.TiltX, d.TiltY, d.CrackWidth, d.Temperature}
}

var channelNames = [4]string{"tilt_x", "tilt_y", "crack_width", "temperature"}

func sameValues(a, b [4]*float64) bool {
	seen := false
	for i := range a {
		if (a[i] == nil) != (b[i] == nil) {
			return false
		}
		if a[i] != nil {
			seen = true
			if *a[i] != *b[i] {
				return false
			}
		}
	}
	return seen
}

// observeValues는 mu를 잡은 상태에서 호출한다.
func (t *HealthTracker) observeValues(id int, e *sensorEntry, d SensorData) {
	vals := channelValues(d)

	if sameValues(vals, e.prev) {
		e.flatRun++
	} else {
		e.flatRun = 1
	}
	e.prev = vals

	outName := ""
	for i, v := range vals {
		if v == nil {
			continue
		}
		if r, ok := t.cfg.Ranges[channelNames[i]]; ok && (*v < r.Min || *v > r.Max) {
			outName = fmt.Sprintf("%s=%g (range %g..%g)", channelNames[i], *v, r.Min, r.Max)
			break
		}
	}
	if outName != "" {
		e.rangeRun++
	} else {
		e.rangeRun = 0
	}

	flat := e.flatRun >= t.cfg.FlatlineCount
	out := e.rangeRun >= t.cfg.OutOfRangeCount
	switch {
	case e.status == StatusOnline && (flat || out):
		note := fmt.Sprintf("flatline: %d identical readings", e.flatRun)
		if out {
			note = fmt.Sprintf("out of range %d times in a row: %s", e.rangeRun, outName)
		}
		e.faultNote = note
		t.transition(id, e, StatusFaulty, EventFault, SourceData, note, d.ReceivedAt)
	case e.status == StatusFaulty && !flat && !out:
		t.transition(id, e, StatusOnline, EventFaultCleared, SourceData, "readings back to normal", d.ReceivedAt)
	}
}

// SetStatus는 센서 상태 토픽(online/offline, LWT 포함) 수신 시 호출한다.
func (t *HealthTracker) SetStatus(sensorID int, status SensorStatus, at time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()

	e, ok := t.sensors[sensorID]
	if !ok || e.status == status {
		return
	}
	switch status {
	case StatusOnline:
		e.lastSeen = at // 접속 직후에는 첫 데이터까지 타임아웃 유예
		e.seenDirty = true
		if e.status == StatusFaulty {
			return // 접속했다고 장애가 풀리는 것은 아니다. 값이 정상으로 돌아와야 한다.
		}
		t.transition(sensorID, e, StatusOnline, EventConnected, SourceLWT, "status topic: online", at)
	case StatusOffline:
		t.transition(sensorID, e, StatusOffline, EventDisconnected, SourceLWT, "status topic: offline (LWT or graceful)", at)
	}
}

// SetIngestHealthy는 MQTT 수집 연결 상태를 알린다.
// 끊긴 동안에는 타임아웃 전이를 보류하고, 복구 시 online 센서의 lastSeen을 현재로 당겨 오탐을 막는다.
func (t *HealthTracker) SetIngestHealthy(up bool, now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if up && !t.ingestUp {
		t.graceOnlineSensors(now)
		t.ingestEvents = append(t.ingestEvents, IngestEvent{Time: now, EventType: IngestBrokerUp, Message: "mqtt connection restored"})
	}
	if !up && t.ingestUp {
		t.ingestEvents = append(t.ingestEvents, IngestEvent{Time: now, EventType: IngestBrokerDown, Message: "mqtt connection lost"})
	}
	t.ingestUp = up
}

func (t *HealthTracker) graceOnlineSensors(now time.Time) {
	for _, e := range t.sensors {
		if e.status == StatusOnline || e.status == StatusFaulty {
			e.lastSeen = now
			e.seenDirty = true
		}
	}
}

// resumeIfStalled는 전체 두절 중 데이터가 다시 오면 판정을 재개한다. mu를 잡은 상태에서 호출한다.
func (t *HealthTracker) resumeIfStalled(now time.Time) {
	if !t.stalled {
		return
	}
	t.stalled = false
	t.graceOnlineSensors(now)
	t.ingestEvents = append(t.ingestEvents, IngestEvent{Time: now, EventType: IngestResumed, Message: "data reception resumed"})
}

// RestoreSnapshot은 이전 실행이 남긴 센서 상태를 되살린다. SyncSensors 뒤에 한 번 호출한다.
// online/faulty 였던 센서는 lastSeen을 now로 두어 유예를 주므로, 재시작 직후 오경보가 없고
// 그대로 침묵하는 센서는 유예 뒤 정상 경로(타임아웃)로 offline 이 된다.
// 이미 데이터가 도착해 상태가 정해진 센서는 건드리지 않는다.
func (t *HealthTracker) RestoreSnapshot(rows []SensorStatusRow, now time.Time) (restored int) {
	t.mu.Lock()
	defer t.mu.Unlock()

	for _, r := range rows {
		e, ok := t.sensors[r.SensorID]
		if !ok || e.status != StatusUnknown || e.hasSeq || r.Status == StatusUnknown {
			continue
		}
		e.status = r.Status
		e.statusChangedAt = r.StatusChangedAt
		e.statusDirty = false // DB와 이미 같다
		switch r.Status {
		case StatusOnline, StatusFaulty:
			e.lastSeen = now
		default:
			if r.LastSeenAt != nil {
				e.lastSeen = *r.LastSeenAt
			}
		}
		if r.LastSeq != nil {
			e.lastSeq, e.hasSeq = *r.LastSeq, true
		}
		restored++
	}
	return restored
}

// CheckTimeouts는 데이터 무수신 센서를 offline으로 전이시킨다.
func (t *HealthTracker) CheckTimeouts(now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// 수집 연결이 끊겼거나 시작 직후 유예 중이면 센서를 탓하지 않는다.
	if !t.ingestUp || now.Before(t.cfg.StartedAt.Add(t.cfg.Grace)) {
		return
	}

	type timedOut struct {
		id  int
		e   *sensorEntry
		msg string
	}
	var expired []timedOut
	online := 0
	for id, e := range t.sensors {
		if e.status != StatusOnline && e.status != StatusFaulty {
			continue
		}
		online++
		limit := max(time.Duration(float64(e.interval)*t.cfg.TimeoutFactor), t.cfg.MinTimeout)
		if silent := now.Sub(e.lastSeen); silent > limit {
			expired = append(expired, timedOut{id, e, fmt.Sprintf("no data for %s (limit %s)", silent.Round(time.Millisecond), limit)})
		}
	}
	if len(expired) == 0 {
		return
	}

	// 모든 online 센서가 한꺼번에 조용해졌다면 센서 N개가 동시에 고장 난 것이 아니라 수집 경로 문제다.
	if online >= t.cfg.SystemicMinOnline && len(expired) == online {
		if !t.stalled {
			t.stalled = true
			t.ingestEvents = append(t.ingestEvents, IngestEvent{
				Time: now, EventType: IngestStalled,
				Message: fmt.Sprintf("no data from any of %d online sensors; per-sensor timeouts suspended", online),
			})
		}
		return
	}
	if t.stalled {
		return
	}
	for _, x := range expired {
		t.transition(x.id, x.e, StatusOffline, EventTimeout, SourceTimeout, x.msg, now)
	}
}

// CountByState는 상태별 센서 수다. 지표(sensors_by_state)가 쓴다.
func (t *HealthTracker) CountByState() map[SensorStatus]int {
	t.mu.Lock()
	defer t.mu.Unlock()

	out := map[SensorStatus]int{StatusUnknown: 0, StatusOnline: 0, StatusOffline: 0, StatusFaulty: 0}
	for _, e := range t.sensors {
		out[e.status]++
	}
	return out
}

// Stalled는 전체 수신 두절로 센서별 판정을 멈춘 상태인지 알려준다.
func (t *HealthTracker) Stalled() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.stalled
}

// DrainIngest는 DB에 반영할 수집 경로 이벤트를 꺼낸다.
func (t *HealthTracker) DrainIngest() []IngestEvent {
	t.mu.Lock()
	defer t.mu.Unlock()
	ev := t.ingestEvents
	t.ingestEvents = nil
	return ev
}

// RequeueIngest는 DB 반영에 실패한 수집 경로 이벤트를 되돌린다.
func (t *HealthTracker) RequeueIngest(ev []IngestEvent) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.ingestEvents = append(ev, t.ingestEvents...)
}

// Drain은 DB에 반영할 전이 이벤트와 상태 행을 꺼낸다.
// includeSeen이 true이면 last_seen이 바뀐 센서도 상태 행에 포함한다.
func (t *HealthTracker) Drain(includeSeen bool) ([]SensorEvent, []SensorStatusRow) {
	t.mu.Lock()
	defer t.mu.Unlock()

	events := t.events
	t.events = nil

	var rows []SensorStatusRow
	for id, e := range t.sensors {
		if !e.statusDirty && !(includeSeen && e.seenDirty) {
			continue
		}
		row := SensorStatusRow{SensorID: id, Status: e.status, StatusChangedAt: e.statusChangedAt}
		if !e.lastSeen.IsZero() {
			seen := e.lastSeen
			row.LastSeenAt = &seen
		}
		if e.hasSeq {
			seq := e.lastSeq
			row.LastSeq = &seq
		}
		rows = append(rows, row)
		e.statusDirty = false
		if includeSeen {
			e.seenDirty = false
		}
	}
	return events, rows
}

// Requeue는 DB 반영에 실패한 항목을 되돌려 다음 주기에 재시도하게 한다.
func (t *HealthTracker) Requeue(events []SensorEvent, rows []SensorStatusRow) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.events = append(events, t.events...)
	for _, r := range rows {
		if e, ok := t.sensors[r.SensorID]; ok {
			e.statusDirty = true
			e.seenDirty = true
		}
	}
}

// transition은 mu를 잡은 상태에서 호출해야 한다.
func (t *HealthTracker) transition(id int, e *sensorEntry, to SensorStatus, eventType, source, message string, at time.Time) {
	e.status = to
	e.statusChangedAt = at
	e.statusDirty = true
	t.events = append(t.events, SensorEvent{Time: at, SensorID: id, EventType: eventType, Source: source, Message: message})
}
