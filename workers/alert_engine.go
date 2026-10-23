package workers

import (
	"math"
	"sync"
	"time"
)

// 규칙 종류
const (
	RuleThreshold    = "threshold"
	RuleRateOfChange = "rate_of_change"
)

// AlertRule은 alert_rule 테이블 행이다.
type AlertRule struct {
	ID            int64 `gorm:"primaryKey"`
	TenantID      int
	Name          string
	SensorID      *int
	SensorType    *string
	Metric        string
	Kind          string
	Operator      *string
	Threshold     float64
	WindowSeconds *int
	Severity      string
	Enabled       bool
}

func (AlertRule) TableName() string { return "alert_rule" }

// SensorInfo는 타입 단위 규칙을 센서에 연결하는 데 필요한 sensor 테이블 정보다.
type SensorInfo struct {
	TenantID int
	Type     string
}

// AlertCandidate는 규칙을 어긴 이벤트다. AlertWriter가 alert_event 로 기록한다.
type AlertCandidate struct {
	RuleID      int64
	SensorID    int
	Seq         int64
	DataTime    time.Time
	TriggeredAt time.Time
	Value       float64
}

// maxSamples는 변화율 규칙이 센서·규칙마다 들고 있는 표본 수의 상한이다. (불변식 4)
const maxSamples = 2048

type sample struct {
	t time.Time
	v float64
}

type ruleState struct {
	breaching bool
	samples   []sample
}

type stateKey struct {
	rule   int64
	sensor int
}

// AlertEngine은 수신 핸들러에서 호출되는 메모리 규칙 평가기다.
//
// 판정은 DB 저장과 분리되어 있다. 같은 이벤트가 재전달돼 다시 평가되면 같은 (rule, sensor, seq) 후보가 나오고
// alert_event 의 UNIQUE 키가 중복을 막는다. 규칙은 "정상 → 위반" 으로 바뀌는 순간에만 후보를 낸다(edge trigger).
// 위반이 계속되는 동안 메시지마다 경보를 만들지 않기 위해서다. 프로세스를 다시 시작하면 상태가 사라져,
// 위반이 이어지는 중이었다면 첫 메시지에서 새 경보가 한 건 더 생길 수 있다.
type AlertEngine struct {
	// Out은 평가 결과 후보 큐다. 가득 차면 후보를 버리고 지표로 센다(수신 핸들러를 막지 않는다).
	Out chan AlertCandidate

	mu       sync.Mutex
	defs     map[int64]AlertRule
	sensors  map[int]SensorInfo
	bySensor map[int][]AlertRule
	state    map[stateKey]*ruleState
}

// NewAlertEngine은 후보 큐 크기(buffer)를 가진 엔진을 만든다.
func NewAlertEngine(buffer int) *AlertEngine {
	return &AlertEngine{
		Out:      make(chan AlertCandidate, buffer),
		defs:     map[int64]AlertRule{},
		sensors:  map[int]SensorInfo{},
		bySensor: map[int][]AlertRule{},
		state:    map[stateKey]*ruleState{},
	}
}

// SetRules는 활성 규칙 전체를 교체한다. 정의가 바뀌었거나 사라진 규칙의 평가 상태는 버린다.
func (e *AlertEngine) SetRules(rules []AlertRule) {
	e.mu.Lock()
	defer e.mu.Unlock()

	next := make(map[int64]AlertRule, len(rules))
	for _, r := range rules {
		if r.Enabled {
			next[r.ID] = r
		}
	}
	for k := range e.state {
		old, hadOld := e.defs[k.rule]
		cur, has := next[k.rule]
		if !has || !hadOld || !sameRule(old, cur) {
			delete(e.state, k)
		}
	}
	e.defs = next
	e.rebuild()
}

// SetSensors는 센서 목록(타입 단위 규칙의 대상 확인용)을 교체한다.
func (e *AlertEngine) SetSensors(sensors map[int]SensorInfo) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.sensors = sensors
	e.rebuild()
}

func sameRule(a, b AlertRule) bool {
	return a.Metric == b.Metric && a.Kind == b.Kind && derefOr(a.Operator, "") == derefOr(b.Operator, "") &&
		a.Threshold == b.Threshold && derefOr(a.WindowSeconds, 0) == derefOr(b.WindowSeconds, 0)
}

// rebuild는 mu를 잡은 상태에서 센서별 규칙 색인을 다시 만든다.
func (e *AlertEngine) rebuild() {
	idx := make(map[int][]AlertRule)
	for _, r := range e.defs {
		if r.SensorID != nil {
			idx[*r.SensorID] = append(idx[*r.SensorID], r)
			continue
		}
		if r.SensorType == nil {
			continue
		}
		for id, info := range e.sensors {
			if info.TenantID == r.TenantID && info.Type == *r.SensorType {
				idx[id] = append(idx[id], r)
			}
		}
	}
	e.bySensor = idx
}

func metricValue(d SensorData, metric string) *float64 {
	switch metric {
	case "tilt_x":
		return d.TiltX
	case "tilt_y":
		return d.TiltY
	case "crack_width":
		return d.CrackWidth
	case "temperature":
		return d.Temperature
	}
	return nil
}

// Evaluate는 측정값 하나에 규칙을 적용한다. 수신 핸들러에서 호출되므로 DB를 기다리지 않는다.
// d.ReceivedAt 은 수신 핸들러가 찍은 값이다. (불변식 6)
func (e *AlertEngine) Evaluate(d SensorData) {
	e.mu.Lock()
	rules := e.bySensor[d.SensorID]
	var out []AlertCandidate
	for _, r := range rules {
		v := metricValue(d, r.Metric)
		if v == nil {
			continue
		}
		key := stateKey{r.ID, d.SensorID}
		st := e.state[key]
		if st == nil {
			st = &ruleState{}
			e.state[key] = st
		}

		at := d.Time
		if at.IsZero() {
			at = d.ReceivedAt
		}

		breach := false
		switch r.Kind {
		case RuleThreshold:
			if derefOr(r.Operator, "gt") == "lt" {
				breach = *v < r.Threshold
			} else {
				breach = *v > r.Threshold
			}
		case RuleRateOfChange:
			window := time.Duration(derefOr(r.WindowSeconds, 0)) * time.Second
			cutoff := at.Add(-window)
			drop := 0
			for drop < len(st.samples) && st.samples[drop].t.Before(cutoff) {
				drop++
			}
			st.samples = st.samples[drop:]
			if len(st.samples) > 0 {
				breach = math.Abs(*v-st.samples[0].v) > r.Threshold
			}
			st.samples = append(st.samples, sample{at, *v})
			if len(st.samples) > maxSamples {
				st.samples = st.samples[len(st.samples)-maxSamples:]
			}
		}

		if breach && !st.breaching {
			out = append(out, AlertCandidate{
				RuleID: r.ID, SensorID: d.SensorID, Seq: d.Seq,
				DataTime: at, TriggeredAt: d.ReceivedAt, Value: *v,
			})
		}
		st.breaching = breach
	}
	e.mu.Unlock()

	for _, c := range out {
		select {
		case e.Out <- c:
		default:
			alertsDropped.WithLabelValues("queue_full").Inc()
		}
	}
}
