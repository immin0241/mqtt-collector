package workers

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

const (
	// sensors/{type}/{id}
	dataTopicFilter = "sensors/+/+"
	// sensors/{type}/{id}/status  (payload: online | offline, retained, LWT)
	statusTopicFilter = "sensors/+/+/status"

	defaultFlushWait = 15 * time.Second
)

type SensorCollectionWorker struct {
	broker        mqtt.Client
	BrokerAddress string
	ClientID      string
	Username      string
	Password      string
	DataChannel   chan Envelope
	Tracker       *HealthTracker

	// 종료 순서 조율 (nil 허용). 구독 해제를 마치면 Unsubscribed를 닫고,
	// DbPushWorker가 최종 flush/ack를 마칠 때까지(Flushed) 기다린 뒤 연결을 끊는다.
	Unsubscribed chan struct{}
	Flushed      <-chan struct{}
	FlushWait    time.Duration

	unsubOnce sync.Once
}

// dataHandler는 데이터 메시지를 파싱해 tracker에 알리고 DB 큐로 넘긴다.
// 자동 ack를 끄고 있으므로 ack는 DbPushWorker가 커밋 뒤에 호출한다.
// 파싱에 실패한 메시지도 원문 그대로 큐로 넘겨 격리 테이블에 남긴다.
func (s *SensorCollectionWorker) dataHandler(ctx context.Context) mqtt.MessageHandler {
	return func(client mqtt.Client, msg mqtt.Message) {
		env := Envelope{
			Topic:      msg.Topic(),
			Payload:    msg.Payload(),
			ReceivedAt: time.Now(),
			Ack:        msg.Ack,
		}

		var data SensorData
		if err := json.Unmarshal(env.Payload, &data); err != nil {
			log.Printf("Invalid payload on %s, will be rejected: %v", env.Topic, err)
			env.ParseErr = err
		} else {
			data.ReceivedAt = env.ReceivedAt
			env.Data = data
			s.Tracker.Touch(data.SensorID, data.Seq, data.ReceivedAt)
		}

		// 큐가 가득 차면 여기서 막혀 수신에 백프레셔가 걸린다.
		select {
		case s.DataChannel <- env:
		case <-ctx.Done():
			// ack하지 않으므로 브로커가 재전달한다.
		}
	}
}

// statusHandler는 상태 토픽(online/offline)을 tracker에 전달한다.
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
		if len(parts) != 4 {
			return
		}
		id, err := strconv.Atoi(parts[2])
		if err != nil {
			log.Printf("Skipping status with invalid sensor id on %s", msg.Topic())
			return
		}

		switch SensorStatus(strings.TrimSpace(string(msg.Payload()))) {
		case StatusOnline:
			s.Tracker.SetStatus(id, StatusOnline, time.Now())
		case StatusOffline:
			s.Tracker.SetStatus(id, StatusOffline, time.Now())
		default:
			log.Printf("Skipping unknown status payload %q on %s", msg.Payload(), msg.Topic())
		}
	}
}

func (s *SensorCollectionWorker) subscribe(ctx context.Context, client mqtt.Client) {
	subs := map[string]mqtt.MessageHandler{
		dataTopicFilter:   s.dataHandler(ctx),
		statusTopicFilter: s.statusHandler(),
	}
	for topic, handler := range subs {
		token := client.Subscribe(topic, 1, handler)
		token.Wait()
		if err := token.Error(); err != nil {
			log.Printf("Failed to subscribe to %s: %v", topic, err)
			continue
		}
		log.Printf("Subscribed to topic: %s", topic)
	}
}

func (s *SensorCollectionWorker) CreateClient(ctx context.Context) {
	fmt.Printf("Connecting to %s\n", s.BrokerAddress)

	opts := mqtt.NewClientOptions()
	opts.AddBroker(s.BrokerAddress)
	opts.SetClientID(s.ClientID)
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
		fmt.Println("Connected successfully to the broker!")
		s.Tracker.SetIngestHealthy(true, time.Now())
		s.subscribe(ctx, client)
	}
	opts.OnConnectionLost = func(client mqtt.Client, err error) {
		log.Printf("Connection lost: %v", err)
		s.Tracker.SetIngestHealthy(false, time.Now())
	}

	opts.SetKeepAlive(60 * time.Second)
	opts.SetPingTimeout(5 * time.Second)
	opts.SetAutoReconnect(true)

	s.broker = mqtt.NewClient(opts)
}

func (s *SensorCollectionWorker) markUnsubscribed() {
	s.unsubOnce.Do(func() {
		if s.Unsubscribed != nil {
			close(s.Unsubscribed)
		}
	})
}

func (s *SensorCollectionWorker) Run(ctx context.Context) error {
	if s.broker == nil {
		return fmt.Errorf("MQTT client has not been initialized")
	}
	// Connect 실패 등으로 일찍 반환해도 DbPushWorker가 종료 대기에 걸리지 않게 한다.
	defer s.markUnsubscribed()

	if token := s.broker.Connect(); token.Wait() && token.Error() != nil {
		return fmt.Errorf("unable to connect to broker: %v", token.Error())
	}

	<-ctx.Done()

	// 종료 순서: 구독 해제 -> (DbPushWorker: drain -> 최종 flush -> ack) -> 연결 종료
	for _, topic := range []string{dataTopicFilter, statusTopicFilter} {
		token := s.broker.Unsubscribe(topic)
		token.Wait()
		if token.Error() != nil {
			log.Printf("Failed to unsubscribe from %s: %v", topic, token.Error())
		}
	}
	s.markUnsubscribed()

	if s.Flushed != nil {
		wait := s.FlushWait
		if wait <= 0 {
			wait = defaultFlushWait
		}
		select {
		case <-s.Flushed:
		case <-time.After(wait):
			log.Printf("Timed out waiting for final flush; disconnecting")
		}
	}

	s.broker.Disconnect(250)

	return nil
}
