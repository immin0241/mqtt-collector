package workers

import (
	"context"
	"fmt"
	"log"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type SensorCollectionWorker struct {
	broker         mqtt.Client
	BrokerAddress  string
	Username       string
	Password       string
	MessageChannel chan mqtt.Message
}

func (s *SensorCollectionWorker) MessageHandler(ctx context.Context) mqtt.MessageHandler {
	return func(client mqtt.Client, msg mqtt.Message) {
		select {
		case s.MessageChannel <- msg:
		case <-ctx.Done():
		}
	}
}

var connectHandler mqtt.OnConnectHandler = func(client mqtt.Client) {
	fmt.Println("Connected successfully to the broker!")
}

var connectLostHandler mqtt.ConnectionLostHandler = func(client mqtt.Client, err error) {
	log.Printf("Connection lost: %v", err)
}

func (s *SensorCollectionWorker) CreateClient() {
	fmt.Printf("Connecting to %s\n", s.BrokerAddress)

	opts := mqtt.NewClientOptions()
	opts.AddBroker(s.BrokerAddress)
	opts.SetClientID("go_mqtt_subscriber_client")
	if s.Username != "" {
		opts.SetUsername(s.Username)
		opts.SetPassword(s.Password)
	}

	opts.OnConnect = connectHandler
	opts.OnConnectionLost = connectLostHandler

	opts.SetKeepAlive(60 * time.Second)
	opts.SetPingTimeout(5 * time.Second)
	opts.SetAutoReconnect(true)

	s.broker = mqtt.NewClient(opts)
}

func (s *SensorCollectionWorker) Run(ctx context.Context, topics []string) error {
	if s.broker == nil {
		return fmt.Errorf("MQTT client has not been initialized")
	}

	if token := s.broker.Connect(); token.Wait() && token.Error() != nil {
		return fmt.Errorf("unable to connect to broker: %v", token.Error())
	}

	for _, v := range topics {
		token := s.broker.Subscribe(v, 1, s.MessageHandler(ctx))
		token.Wait()

		if token.Error() != nil {
			return fmt.Errorf("failed to subscribe to %s: %v", v, token.Error())
		}

		log.Printf("Subscribed to topic: %s", v)
	}

	<-ctx.Done()

	for _, topic := range topics {
		token := s.broker.Unsubscribe(topic)
		token.Wait()
		if token.Error() != nil {
			log.Printf("Failed to unsubscribe from %s: %v", topic, token.Error())
		}
	}

	s.broker.Disconnect(250)

	return nil
}
