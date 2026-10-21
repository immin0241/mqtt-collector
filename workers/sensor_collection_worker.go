package workers

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

const (
	// sensors/{type}/{id}
	dataTopicFilter = "sensors/+/+"
	// sensors/{type}/{id}/status  (payload: online | offline, retained, LWT)
	statusTopicFilter = "sensors/+/+/status"
)

type SensorCollectionWorker struct {
	broker        mqtt.Client
	BrokerAddress string
	Username      string
	Password      string
	DataChannel   chan SensorData
	Tracker       *HealthTracker
}

// dataHandler는 데이터 메시지를 파싱해 tracker에 알리고 DB 큐로 넘긴다.
func (s *SensorCollectionWorker) dataHandler(ctx context.Context) mqtt.MessageHandler {
	return func(client mqtt.Client, msg mqtt.Message) {
		var data SensorData
		if err := json.Unmarshal(msg.Payload(), &data); err != nil {
			log.Printf("Skipping invalid payload on %s: %v", msg.Topic(), err)
			return
		}
		data.ReceivedAt = time.Now()

		s.Tracker.Touch(data.SensorID, data.Seq, data.ReceivedAt)

		select {
		case s.DataChannel <- data:
		case <-ctx.Done():
		}
	}
}

// statusHandler는 상태 토픽(online/offline)을 tracker에 전달한다.
func (s *SensorCollectionWorker) statusHandler() mqtt.MessageHandler {
	return func(client mqtt.Client, msg mqtt.Message) {
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
	opts.SetClientID("go_mqtt_subscriber_client")
	if s.Username != "" {
		opts.SetUsername(s.Username)
		opts.SetPassword(s.Password)
	}

	// 재연결 시 구독이 사라지므로 OnConnect에서 매번 구독한다.
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

func (s *SensorCollectionWorker) Run(ctx context.Context) error {
	if s.broker == nil {
		return fmt.Errorf("MQTT client has not been initialized")
	}

	if token := s.broker.Connect(); token.Wait() && token.Error() != nil {
		return fmt.Errorf("unable to connect to broker: %v", token.Error())
	}

	<-ctx.Done()

	for _, topic := range []string{dataTopicFilter, statusTopicFilter} {
		token := s.broker.Unsubscribe(topic)
		token.Wait()
		if token.Error() != nil {
			log.Printf("Failed to unsubscribe from %s: %v", topic, token.Error())
		}
	}

	s.broker.Disconnect(250)

	return nil
}
