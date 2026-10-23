package workers

import (
	"sync"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// 지표 라벨에 sensor_id를 쓰지 않는다. (카디널리티)
var (
	messagesReceived = promauto.NewCounter(prometheus.CounterOpts{
		Name: "messages_received_total",
		Help: "MQTT 수신 핸들러가 받은 데이터 메시지 수 (파싱 실패 포함)",
	})
	rowsCommitted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "rows_committed_total",
		Help: "sensor_data에 새로 커밋된 행 수 (중복 제외)",
	})
	dedupTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dedup_total",
		Help: "중복 키로 건너뛴 행 수 (ON CONFLICT DO NOTHING)",
	})
	rowsRejected = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "rows_rejected_total",
		Help: "격리 테이블로 보낸 메시지 수",
	}, []string{"reason"}) // parse | db
	dbErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "db_errors_total",
		Help: "DB 오류 횟수",
	}, []string{"kind"}) // transient | permanent
	batchSize = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "batch_size",
		Help:    "flush된 배치의 메시지 수",
		Buckets: []float64{1, 5, 10, 25, 50, 100, 200, 300, 500},
	})
	batchFlushSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "batch_flush_seconds",
		Help:    "배치 하나를 저장(재시도·격리 포함)하는 데 걸린 시간",
		Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60},
	})
	ingestLagSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "ingest_lag_seconds",
		Help:    "received_at부터 DB 커밋까지 걸린 시간",
		Buckets: []float64{.01, .05, .1, .25, .5, 1, 2, 3, 5, 10, 30, 60, 120},
	})
	lastFlushTimestamp = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "last_flush_timestamp",
		Help: "마지막으로 배치 저장에 성공한 시각 (Unix 초)",
	})
)

var (
	channelDepthOnce sync.Once
	channelDepthSrc  atomic.Pointer[chan Envelope]
)

// RegisterChannelDepth는 수신 큐 길이 지표(channel_depth)를 등록한다.
// 한 프로세스에서 여러 번 호출해도(통합 테스트) 지표는 한 번만 등록하고, 마지막으로 넘긴 채널을 읽는다.
func RegisterChannelDepth(ch chan Envelope) {
	channelDepthSrc.Store(&ch)
	channelDepthOnce.Do(func() {
		promauto.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "channel_depth",
			Help: "수신 핸들러와 DbPushWorker 사이 채널에 쌓인 메시지 수",
		}, func() float64 {
			if p := channelDepthSrc.Load(); p != nil {
				return float64(len(*p))
			}
			return 0
		})
	})
}

// 경보 지표
var (
	alertsCreated = promauto.NewCounter(prometheus.CounterOpts{
		Name: "alerts_created_total",
		Help: "alert_event에 새로 기록된 경보 수 (재전달로 인한 중복 제외)",
	})
	alertsDeduped = promauto.NewCounter(prometheus.CounterOpts{
		Name: "alerts_deduped_total",
		Help: "(rule_id, sensor_id, seq) 중복 키로 건너뛴 경보 수",
	})
	alertsDropped = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "alerts_dropped_total",
		Help: "기록하지 못한 경보 수",
	}, []string{"reason"}) // queue_full | rejected
	alertLatencySeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "alert_latency_seconds",
		Help:    "수신 핸들러의 규칙 평가부터 경보가 DB에 기록되기까지 걸린 시간",
		Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2, 5, 10},
	})
)

var (
	sensorStateOnce sync.Once
	sensorStateSrc  atomic.Pointer[HealthTracker]
)

// RegisterSensorStates는 상태별 센서 수 지표(sensors_by_state{state})를 등록한다.
// 한 프로세스에서 여러 번 호출해도 지표는 한 번만 등록하고 마지막으로 넘긴 tracker를 읽는다.
func RegisterSensorStates(t *HealthTracker) {
	sensorStateSrc.Store(t)
	sensorStateOnce.Do(func() {
		for _, st := range []SensorStatus{StatusUnknown, StatusOnline, StatusOffline, StatusFaulty} {
			promauto.NewGaugeFunc(prometheus.GaugeOpts{
				Name:        "sensors_by_state",
				Help:        "상태별 센서 수",
				ConstLabels: prometheus.Labels{"state": string(st)},
			}, func() float64 {
				if tr := sensorStateSrc.Load(); tr != nil {
					return float64(tr.CountByState()[st])
				}
				return 0
			})
		}
	})
}
