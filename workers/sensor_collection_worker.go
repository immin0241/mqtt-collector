package workers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

const defaultFlushWait = 15 * time.Second

// SensorCollectionWorker는 테넌트별 MQTT 세션을 관리하고, 세션마다 받은 메시지를 공용 DataChannel로 모은다.
//
// 저장 워커(DbPushWorker)는 여전히 하나이고 DataChannel도 하나다. (불변식 5)
// 테넌트마다 작은 채널과 forwarder 고루틴을 두는데, 공용 채널이 가득 차면 forwarder들이 송신 대기열에 FIFO로 서므로
// 폭주 테넌트의 forwarder와 평온한 테넌트의 forwarder가 번갈아 들어간다. (공정성은 단위 테스트로 확인한다)
type SensorCollectionWorker struct {
	BrokerAddress string
	ClientID      string // 테넌트별 client id 는 "<ClientID>-t<tenant_id>"
	Username      string
	Password      string
	DataChannel   chan Envelope
	Tracker       *HealthTracker
	// Alerts가 nil이 아니면 파싱된 측정값마다 경보 규칙을 메모리에서 평가한다. (수신 시점 평가)
	Alerts *AlertEngine

	// 수집 대상 테넌트. 필수.
	Tenants           TenantSource
	TenantRefresh     time.Duration // 테넌트 목록 재조회 주기. 기본 10s
	TenantChannelSize int           // 테넌트별 채널 크기. 기본 100
	MaxTenants        int           // 동시에 유지하는 세션 상한. 기본 50. 메모리 상한(불변식 4)의 일부다.

	// 종료 순서 조율 (nil 허용). 수신을 멈추면(구독은 유지) Unsubscribed를 닫고,
	// DbPushWorker가 최종 flush/ack를 마칠 때까지(Flushed) 기다린 뒤 연결을 끊는다.
	Unsubscribed chan struct{}
	Flushed      <-chan struct{}
	FlushWait    time.Duration

	ctx       context.Context
	mu        sync.Mutex
	sessions  map[int]*tenantSession
	unsubOnce sync.Once
}

// dataHandler는 데이터 메시지를 파싱해 tracker에 알리고 테넌트 채널로 넘긴다.
// 자동 ack를 끄고 있으므로 ack는 DbPushWorker가 커밋 뒤에 호출한다.
// 파싱에 실패한 메시지도 원문 그대로 넘겨 격리 테이블에 남긴다.
func (s *SensorCollectionWorker) dataHandler(ctx context.Context, sess *tenantSession) mqtt.MessageHandler {
	return func(client mqtt.Client, msg mqtt.Message) {
		messagesReceived.Inc()
		env := Envelope{
			Topic:      msg.Topic(),
			Payload:    msg.Payload(),
			ReceivedAt: time.Now(),
			Ack:        msg.Ack,
		}

		var data SensorData
		if err := json.Unmarshal(env.Payload, &data); err != nil {
			slog.Warn("invalid payload; will be rejected", "topic", env.Topic, "error", err)
			env.ParseErr = err
		} else {
			data.ReceivedAt = env.ReceivedAt
			env.Data = data
			s.Tracker.ObserveData(data)
			if s.Alerts != nil {
				s.Alerts.Evaluate(data)
			}
		}

		// 테넌트 채널이 가득 차면 이 세션의 핸들러만 막힌다. 다른 테넌트의 세션은 영향을 받지 않는다.
		select {
		case sess.ch <- env:
		case <-ctx.Done():
			// ack하지 않으므로 브로커가 재전달한다.
		}
	}
}

// forward는 테넌트 채널의 메시지를 순서대로 공용 채널로 옮긴다. 테넌트 안의 순서(= ack 순서)가 유지된다.
func (s *SensorCollectionWorker) forward(ctx context.Context, sess *tenantSession) {
	defer close(sess.done)
	for {
		select {
		case env := <-sess.ch:
			select {
			case s.DataChannel <- env:
			case <-ctx.Done():
				return // ack하지 않았으므로 브로커가 재전달한다.
			}
		case <-ctx.Done():
			return
		}
	}
}

// statusHandler는 상태 토픽(online/offline)을 tracker에 전달한다. 토픽: sensors/{tenant}/{type}/{id}/status
func (s *SensorCollectionWorker) statusHandler() mqtt.MessageHandler {
	return func(client mqtt.Client, msg mqtt.Message) {
		// 자동 ack를 껐으므로 상태 메시지도 직접 ack해야 브로커의 inflight 창이 막히지 않는다.
		// 상태는 메모리에만 반영하고 DB와 무관하므로 바로 ack한다.
		defer msg.Ack()

		// retained 메시지는 과거 상태이므로 이벤트로 취급하지 않는다.
		if msg.Retained() {
			return
		}
		parts := strings.Split(msg.Topic(), "/")
		if len(parts) != 5 {
			return
		}
		id, err := strconv.Atoi(parts[3])
		if err != nil {
			slog.Warn("skipping status with invalid sensor id", "topic", msg.Topic())
			return
		}

		switch SensorStatus(strings.TrimSpace(string(msg.Payload()))) {
		case StatusOnline:
			s.Tracker.SetStatus(id, StatusOnline, time.Now())
		case StatusOffline:
			s.Tracker.SetStatus(id, StatusOffline, time.Now())
		default:
			slog.Warn("skipping unknown status payload", "topic", msg.Topic(), "payload", string(msg.Payload()))
		}
	}
}

func (s *SensorCollectionWorker) subscribe(ctx context.Context, client mqtt.Client, sess *tenantSession) {
	subs := map[string]mqtt.MessageHandler{
		tenantDataFilter(sess.tenantID):   s.dataHandler(ctx, sess),
		tenantStatusFilter(sess.tenantID): s.statusHandler(),
	}
	for topic, handler := range subs {
		token := client.Subscribe(topic, 1, handler)
		token.Wait()
		if err := token.Error(); err != nil {
			slog.Error("subscribe failed", "topic", topic, "error", err)
			continue
		}
		slog.Info("subscribed", "topic", topic, "tenant_id", sess.tenantID)
	}
}

// CreateClient는 실행 컨텍스트와 기본값을 정한다. 연결은 Run이 테넌트 목록을 읽은 뒤 맺는다.
func (s *SensorCollectionWorker) CreateClient(ctx context.Context) {
	s.ctx = ctx
	if s.TenantRefresh <= 0 {
		s.TenantRefresh = defaultTenantRefresh
	}
	if s.TenantChannelSize <= 0 {
		s.TenantChannelSize = defaultTenantChannelSize
	}
	if s.MaxTenants <= 0 {
		s.MaxTenants = defaultMaxTenants
	}
	s.sessions = map[int]*tenantSession{}
	slog.Info("collector initialized", "broker", s.BrokerAddress, "client_id_base", s.ClientID,
		"tenant_channel_size", s.TenantChannelSize, "max_tenants", s.MaxTenants)
}

// updateIngestHealth는 모든 테넌트 세션이 연결되어 있을 때만 수집 경로를 정상으로 본다.
// 한 세션이라도 끊기면 그 테넌트 센서의 침묵이 센서 고장으로 오판되지 않도록 센서별 판정을 멈춘다.
func (s *SensorCollectionWorker) updateIngestHealth() {
	s.mu.Lock()
	total := len(s.sessions)
	connected := 0
	for _, sess := range s.sessions {
		if sess.connected.Load() {
			connected++
		}
	}
	s.mu.Unlock()
	tenantSessions.Set(float64(connected))
	if total > 0 {
		s.Tracker.SetIngestHealthy(connected == total, time.Now())
	}
}

// openSession은 테넌트 하나의 세션을 만들고 연결한다.
func (s *SensorCollectionWorker) openSession(tenantID int) error {
	sctx, cancel := context.WithCancel(s.ctx)
	sess := &tenantSession{
		tenantID: tenantID,
		ch:       make(chan Envelope, s.TenantChannelSize),
		cancel:   cancel,
		done:     make(chan struct{}),
	}
	clientID := tenantClientID(s.ClientID, tenantID)
	slog.Info("connecting to broker", "broker", s.BrokerAddress, "client_id", clientID, "tenant_id", tenantID)

	opts := mqtt.NewClientOptions()
	opts.AddBroker(s.BrokerAddress)
	opts.SetClientID(clientID)
	if s.Username != "" {
		opts.SetUsername(s.Username)
		opts.SetPassword(s.Password)
	}

	// 지속 세션: 재시작/재연결 후에도 브로커가 미확인(ack 전) 메시지를 보관·재전달한다.
	// 고정 ClientID가 전제이며, 재전달된 메시지는 DB의 중복 키가 흡수한다.
	opts.SetCleanSession(false)
	// ack는 배치 커밋 뒤에 DbPushWorker가 msg.Ack()로 보낸다.
	opts.SetAutoAckDisabled(true)

	// 재연결 시 구독이 사라질 수 있으므로 OnConnect에서 매번 구독한다.
	opts.OnConnect = func(client mqtt.Client) {
		slog.Info("connected to broker", "tenant_id", tenantID)
		sess.connected.Store(true)
		s.updateIngestHealth()
		s.subscribe(sctx, client, sess)
	}
	opts.OnConnectionLost = func(client mqtt.Client, err error) {
		slog.Warn("connection lost", "tenant_id", tenantID, "error", err)
		sess.connected.Store(false)
		s.updateIngestHealth()
	}

	opts.SetKeepAlive(60 * time.Second)
	opts.SetPingTimeout(5 * time.Second)
	opts.SetAutoReconnect(true)

	sess.client = mqtt.NewClient(opts)

	// 핸들러는 연결 전에 등록한다. 지속 세션은 CONNACK 직후 미ack 메시지를 곧바로 재전달하는데,
	// 라우트가 Subscribe(OnConnect)에서만 만들어지면 그 사이에 도착한 메시지는 핸들러가 없어
	// "Message will NOT be acknowledged" 로 버려지고 브로커에는 inflight로 남아 다음 재연결까지 멈춘다.
	sess.client.AddRoute(tenantDataFilter(tenantID), s.dataHandler(sctx, sess))
	sess.client.AddRoute(tenantStatusFilter(tenantID), s.statusHandler())

	s.mu.Lock()
	s.sessions[tenantID] = sess
	s.mu.Unlock()
	go s.forward(sctx, sess)

	if token := sess.client.Connect(); token.Wait() && token.Error() != nil {
		cancel()
		s.mu.Lock()
		delete(s.sessions, tenantID)
		s.mu.Unlock()
		return fmt.Errorf("tenant %d: unable to connect to broker: %v", tenantID, token.Error())
	}
	return nil
}

// closeSession은 테넌트가 사라졌을 때 세션을 닫는다. 브로커의 세션과 큐는 남으므로 운영 절차로 정리한다. (ADR-0011)
func (s *SensorCollectionWorker) closeSession(sess *tenantSession) {
	slog.Warn("tenant removed; closing session (broker-side session and queue remain until cleaned up)",
		"tenant_id", sess.tenantID, "client_id", tenantClientID(s.ClientID, sess.tenantID))
	sess.cancel()
	sess.client.Disconnect(250)
	s.mu.Lock()
	delete(s.sessions, sess.tenantID)
	s.mu.Unlock()
	s.updateIngestHealth()
}

// syncTenants는 tenant 테이블과 세션 목록을 맞춘다. 처음 호출에서 연결에 실패하면 오류를 돌려준다.
func (s *SensorCollectionWorker) syncTenants(ctx context.Context, initial bool) error {
	ids, err := s.Tenants.TenantIDs(ctx)
	if err != nil {
		return err
	}
	want := make(map[int]bool, len(ids))
	for _, id := range ids {
		want[id] = true
	}

	for _, id := range ids {
		s.mu.Lock()
		_, exists := s.sessions[id]
		n := len(s.sessions)
		s.mu.Unlock()
		if exists {
			continue
		}
		if n >= s.MaxTenants {
			slog.Error("tenant session limit reached; tenant is not collected", "tenant_id", id, "max_tenants", s.MaxTenants)
			continue
		}
		if err := s.openSession(id); err != nil {
			if initial {
				return err
			}
			slog.Warn("tenant session not opened; will retry", "tenant_id", id, "error", err)
		}
	}

	s.mu.Lock()
	var gone []*tenantSession
	for id, sess := range s.sessions {
		if !want[id] {
			gone = append(gone, sess)
		}
	}
	s.mu.Unlock()
	for _, sess := range gone {
		s.closeSession(sess)
	}
	return nil
}

func (s *SensorCollectionWorker) markUnsubscribed() {
	s.unsubOnce.Do(func() {
		if s.Unsubscribed != nil {
			close(s.Unsubscribed)
		}
	})
}

func (s *SensorCollectionWorker) Run(ctx context.Context) error {
	if s.ctx == nil || s.sessions == nil {
		return fmt.Errorf("MQTT client has not been initialized")
	}
	if s.Tenants == nil {
		return errors.New("Tenants is required")
	}
	// Connect 실패 등으로 일찍 반환해도 DbPushWorker가 종료 대기에 걸리지 않게 한다.
	defer s.markUnsubscribed()

	if err := s.syncTenants(ctx, true); err != nil {
		return fmt.Errorf("unable to start tenant sessions: %w", err)
	}

	ticker := time.NewTicker(s.TenantRefresh)
	defer ticker.Stop()
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case <-ticker.C:
			if err := s.syncTenants(ctx, false); err != nil && ctx.Err() == nil {
				slog.Warn("tenant list refresh failed; keeping current sessions", "error", err)
			}
		}
	}

	// 종료 순서: 수신 중단 -> (DbPushWorker: drain -> 최종 flush -> ack) -> 연결 종료
	// 구독은 해제하지 않는다. 지속 세션에서 Unsubscribe는 브로커의 구독 정보를 지워, 앱이 내려가 있는 동안
	// 발행된 메시지가 큐에 쌓이지 않고 유실된다(ADR-0007, TC-16). ctx가 취소된 뒤 도착한 메시지는
	// 핸들러가 큐에 넣지 않고 ack도 하지 않으므로 재시작 후 브로커가 다시 전달한다.
	s.markUnsubscribed()

	if s.Flushed != nil {
		wait := s.FlushWait
		if wait <= 0 {
			wait = defaultFlushWait
		}
		select {
		case <-s.Flushed:
		case <-time.After(wait):
			slog.Warn("timed out waiting for final flush; disconnecting")
		}
	}

	s.mu.Lock()
	all := make([]*tenantSession, 0, len(s.sessions))
	for _, sess := range s.sessions {
		all = append(all, sess)
	}
	s.mu.Unlock()
	for _, sess := range all {
		sess.client.Disconnect(250)
	}

	return nil
}
