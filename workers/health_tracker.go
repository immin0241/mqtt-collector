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
)

// 전이 이벤트 타입 / 출처 (sensor_event의 CHECK 제약과 동일)
const (
	EventConnected    = "connected"
	EventDisconnected = "disconnected"
	EventTimeout      = "timeout"
	EventRecovered    = "recovered"

	SourceLWT     = "lwt"
	SourceTimeout = "timeout"
	SourceData    = "data"
)

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
}

// HealthTracker는 센서별 접속 상태를 메모리에서 판단한다.
// 구독 콜백에서 호출되므로 모든 메서드는 O(1)이고 DB/채널을 기다리지 않는다.
type HealthTracker struct {
	mu         sync.Mutex
	sensors    map[int]*sensorEntry
	events     []SensorEvent
	minTimeout time.Duration
	ingestUp   bool
}

func NewHealthTracker(minTimeout time.Duration) *HealthTracker {
	return &HealthTracker{
		sensors:    make(map[int]*sensorEntry),
		minTimeout: minTimeout,
		ingestUp:   true,
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

// Touch는 센서 데이터 수신 시 호출한다.
func (t *HealthTracker) Touch(sensorID int, seq int64, at time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()

	e, ok := t.sensors[sensorID]
	if !ok {
		return
	}
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
		for _, e := range t.sensors {
			if e.status == StatusOnline {
				e.lastSeen = now
				e.seenDirty = true
			}
		}
	}
	t.ingestUp = up
}

// CheckTimeouts는 데이터 무수신 센서를 offline으로 전이시킨다.
func (t *HealthTracker) CheckTimeouts(now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.ingestUp {
		return
	}
	for id, e := range t.sensors {
		if e.status != StatusOnline {
			continue
		}
		limit := max(2*e.interval, t.minTimeout)
		if silent := now.Sub(e.lastSeen); silent > limit {
			msg := fmt.Sprintf("no data for %s (limit %s)", silent.Round(time.Millisecond), limit)
			t.transition(id, e, StatusOffline, EventTimeout, SourceTimeout, msg, now)
		}
	}
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
