package workers

import (
	"context"
	"log/slog"
	"time"
)

const (
	defaultBatchSize       = 300
	defaultFlushInterval   = 2 * time.Second
	defaultRetryInitial    = 500 * time.Millisecond
	defaultRetryMax        = 30 * time.Second
	defaultShutdownTimeout = 10 * time.Second

	// ParseErrorCode는 JSON 파싱 실패로 격리된 행의 error_code다.
	ParseErrorCode = "PARSE_ERROR"
)

type SensorData struct {
	Time        time.Time `gorm:"column:time" json:"time"`
	SensorID    int       `gorm:"column:sensor_id" json:"sensor_id"`
	Seq         int64     `json:"seq"`
	TiltX       *float64  `json:"tilt_x"`
	TiltY       *float64  `json:"tilt_y"`
	CrackWidth  *float64  `json:"crack_width"`
	Temperature *float64  `json:"temperature"`
	ReceivedAt  time.Time `json:"-"`
}

func (SensorData) TableName() string { return "sensor_data" }

// Envelope는 수신한 MQTT 메시지 하나다. DB 커밋(또는 격리 기록)이 끝난 뒤에만 Ack를 호출한다.
type Envelope struct {
	Data       SensorData // ParseErr == nil 일 때만 유효
	Topic      string
	Payload    []byte // 원문. 격리 테이블에 그대로 저장한다.
	ReceivedAt time.Time
	ParseErr   error
	Ack        func()
}

// DbPushWorker는 채널의 메시지를 배치로 모아 저장한다. 반드시 단일 고루틴으로 실행한다. (ack 순서 = 수신 순서)
type DbPushWorker struct {
	Store       Store
	DataChannel chan Envelope

	// 0이면 기본값을 쓴다.
	BatchSize       int
	FlushInterval   time.Duration
	RetryInitial    time.Duration
	RetryMax        time.Duration
	ShutdownTimeout time.Duration

	// 종료 순서 조율 (nil 허용). Unsubscribed는 수신 측이 수신을 멈추면(구독은 유지, ADR-0007) 닫고,
	// Flushed는 이 워커가 최종 flush와 ack를 마치면 닫는다.
	Unsubscribed <-chan struct{}
	Flushed      chan struct{}

	// Clock이 nil이면 실제 시계를 쓴다. 테스트에서 flush 주기와 백오프를 제어할 때 주입한다.
	Clock Clock
}

func (w *DbPushWorker) applyDefaults() {
	if w.Clock == nil {
		w.Clock = realClock{}
	}
	if w.BatchSize <= 0 {
		w.BatchSize = defaultBatchSize
	}
	if w.FlushInterval <= 0 {
		w.FlushInterval = defaultFlushInterval
	}
	if w.RetryInitial <= 0 {
		w.RetryInitial = defaultRetryInitial
	}
	if w.RetryMax <= 0 {
		w.RetryMax = defaultRetryMax
	}
	if w.ShutdownTimeout <= 0 {
		w.ShutdownTimeout = defaultShutdownTimeout
	}
}

func (w *DbPushWorker) Run(ctx context.Context) error {
	w.applyDefaults()
	if w.Flushed != nil {
		defer close(w.Flushed)
	}

	batch := make([]Envelope, 0, w.BatchSize)

	// 배치의 첫 메시지가 들어온 시점부터 FlushInterval 경과 시 flush
	timer := w.Clock.NewTimer(w.FlushInterval)
	if !timer.Stop() {
		<-timer.C()
	}

	// flush는 배치 전체가 커밋(또는 격리 기록)된 뒤에만 ack한다. 실패하면 배치를 그대로 둔다.
	flush := func(flushCtx context.Context) error {
		if !timer.Stop() {
			select {
			case <-timer.C():
			default:
			}
		}
		if len(batch) == 0 {
			return nil
		}
		start := time.Now()
		if err := w.persist(flushCtx, batch); err != nil {
			return err
		}
		batchSize.Observe(float64(len(batch)))
		batchFlushSeconds.Observe(time.Since(start).Seconds())
		lastFlushTimestamp.SetToCurrentTime()
		for i := range batch {
			if batch[i].Ack != nil {
				batch[i].Ack()
			}
			batch[i] = Envelope{}
		}
		batch = batch[:0]
		return nil
	}

loop:
	for {
		select {
		case env := <-w.DataChannel:
			if len(batch) == 0 {
				timer.Reset(w.FlushInterval)
			}
			batch = append(batch, env)

			if len(batch) >= w.BatchSize {
				if err := flush(ctx); err != nil {
					break loop
				}
			}
		case <-timer.C():
			if err := flush(ctx); err != nil {
				break loop
			}
		case <-ctx.Done():
			break loop
		}
	}

	// 종료: 수신 중단 대기 -> 채널 drain -> 최종 flush -> ack. 실패한 배치는 ack하지 않아 브로커가 재전달한다.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), w.ShutdownTimeout)
	defer cancel()

	if w.Unsubscribed != nil {
		select {
		case <-w.Unsubscribed:
		case <-shutdownCtx.Done():
			slog.Warn("timed out waiting for unsubscribe; draining anyway")
		}
	}

	for draining := true; draining; {
		select {
		case env := <-w.DataChannel:
			batch = append(batch, env)
			if len(batch) >= w.BatchSize {
				if err := flush(shutdownCtx); err != nil {
					slog.Error("final flush failed; messages left unacked (broker will redeliver)", "unacked", len(batch), "error", err)
					return nil
				}
			}
		default:
			draining = false
		}
	}
	if err := flush(shutdownCtx); err != nil {
		slog.Error("final flush failed; messages left unacked (broker will redeliver)", "unacked", len(batch), "error", err)
	}
	return nil
}

// persist는 배치를 저장한다. 일시적 오류는 성공할 때까지 재시도하고(ctx 취소 시 중단),
// 영구 오류는 이분법으로 문제 행을 찾아 격리 테이블로 보낸다.
func (w *DbPushWorker) persist(ctx context.Context, batch []Envelope) error {
	valid := make([]Envelope, 0, len(batch))
	for _, env := range batch {
		if env.ParseErr != nil {
			if err := w.reject(ctx, env, ParseErrorCode, env.ParseErr.Error()); err != nil {
				return err
			}
			continue
		}
		valid = append(valid, env)
	}
	return w.insertBisect(ctx, valid)
}

func (w *DbPushWorker) insertBisect(ctx context.Context, envs []Envelope) error {
	if len(envs) == 0 {
		return nil
	}

	rows := make([]SensorData, len(envs))
	for i, env := range envs {
		rows[i] = env.Data
	}

	var inserted int64
	err := w.retry(ctx, true, func() (err error) {
		inserted, err = w.Store.InsertRows(ctx, rows)
		return err
	}, "rows", len(rows), "first_sensor_id", envs[0].Data.SensorID, "first_seq", envs[0].Data.Seq)
	if err == nil {
		// 커밋 직후: 적재량, 중복 제거량, 수신~커밋 지연을 기록한다.
		rowsCommitted.Add(float64(inserted))
		dedupTotal.Add(float64(int64(len(rows)) - inserted))
		now := time.Now()
		for _, env := range envs {
			ingestLagSeconds.Observe(now.Sub(env.ReceivedAt).Seconds())
		}
		return nil
	}
	if ctx.Err() != nil || ClassifyDBError(err) != KindPermanent {
		return err
	}

	if len(envs) == 1 {
		return w.reject(ctx, envs[0], sqlStateOf(err), err.Error())
	}
	mid := len(envs) / 2
	if err := w.insertBisect(ctx, envs[:mid]); err != nil {
		return err
	}
	return w.insertBisect(ctx, envs[mid:])
}

func (w *DbPushWorker) reject(ctx context.Context, env Envelope, code, msg string) error {
	row := RejectedRow{
		ReceivedAt:   env.ReceivedAt,
		Topic:        env.Topic,
		Payload:      env.Payload,
		ErrorCode:    code,
		ErrorMessage: msg,
	}
	if env.ParseErr == nil {
		id, seq := env.Data.SensorID, env.Data.Seq
		row.SensorID, row.Seq = &id, &seq
	}
	reason := "db"
	if env.ParseErr != nil {
		reason = "parse"
	}
	slog.Warn("message rejected",
		"topic", env.Topic, "sensor_id", derefOr(row.SensorID, -1), "seq", derefOr(row.Seq, -1),
		"code", code, "reason", reason, "error", msg)

	// 격리 기록은 오류 종류와 무관하게 성공할 때까지 재시도한다. (여기서 포기하면 메시지가 사라진다)
	err := w.retry(ctx, false, func() error { return w.Store.InsertRejected(ctx, row) },
		"topic", env.Topic, "sensor_id", derefOr(row.SensorID, -1), "seq", derefOr(row.Seq, -1))
	if err == nil {
		rowsRejected.WithLabelValues(reason).Inc()
	}
	return err
}

// retry는 op를 성공할 때까지 지수 백오프로 반복한다. 반환하는 경우: 성공, ctx 취소,
// 또는 classify가 true일 때의 영구 오류. 재시도 중에는 호출 측(Run 루프)이 막혀 채널이 차므로 수신이 백프레셔를 받는다.
// attrs는 실패 로그에 덧붙일 구조화 필드(sensor_id, seq 등)다.
func (w *DbPushWorker) retry(ctx context.Context, classify bool, op func() error, attrs ...any) error {
	backoff := w.RetryInitial
	for attempt := 1; ; attempt++ {
		err := op()
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		kind := ClassifyDBError(err)
		dbErrors.WithLabelValues(string(kind)).Inc()
		if classify && kind == KindPermanent {
			return err
		}

		slog.Warn("db error; retrying",
			append([]any{"attempt", attempt, "backoff", backoff, "kind", string(kind), "error", err}, attrs...)...)
		select {
		case <-w.Clock.After(backoff):
		case <-ctx.Done():
			return ctx.Err()
		}
		backoff = min(backoff*2, w.RetryMax)
	}
}
