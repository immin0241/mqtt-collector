package testenv

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/immin0241/webframework-project-mqtt-collector/internal/reconcile"
)

// Publisher는 테스트용 센서 발행기다. 브로커가 PUBACK한 메시지만 "발행됨"으로 기록한다.
// 실패한 발행은 같은 payload로 성공할 때까지 재시도하므로(발행 측 버퍼링), 이후 DB 대조에서 생기는 누락은 수집 측 유실이다.
type Publisher struct {
	t      testing.TB
	client mqtt.Client

	mu        sync.Mutex
	published reconcile.SeqCounts
	next      map[int]int64
	tenants   map[int]int // sensorID -> tenantID (없으면 기본 테넌트)
}

// NewPublisher는 브로커(host:port)에 연결한다.
func NewPublisher(t testing.TB, brokerAddr, clientID string) *Publisher {
	t.Helper()
	opts := mqtt.NewClientOptions().
		AddBroker("tcp://" + brokerAddr).
		SetClientID(clientID).
		SetCleanSession(true).
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetConnectRetryInterval(500 * time.Millisecond).
		SetKeepAlive(5 * time.Second).
		SetPingTimeout(2 * time.Second)
	c := mqtt.NewClient(opts)
	if tok := c.Connect(); !tok.WaitTimeout(15*time.Second) || tok.Error() != nil {
		t.Fatalf("publisher connect: %v", tok.Error())
	}
	t.Cleanup(func() { c.Disconnect(250) })
	return &Publisher{t: t, client: c, published: reconcile.SeqCounts{}, next: map[int]int64{}}
}

// DefaultTenantID는 SeedSensors가 만드는 테넌트의 id다. (첫 번째 테넌트)
const DefaultTenantID = 1

// TopicFor는 테넌트 센서의 데이터 토픽이다: sensors/{tenant_id}/{type}/{sensor_id}.
// 타입은 id % 3 으로 SeedSensors와 맞춘다.
func TopicFor(tenantID, sensorID int) string {
	types := []string{"tilt", "crack", "env"}
	return fmt.Sprintf("sensors/%d/%s/%d", tenantID, types[sensorID%3], sensorID)
}

// Topic은 기본 테넌트(DefaultTenantID) 센서의 데이터 토픽이다.
func Topic(sensorID int) string { return TopicFor(DefaultTenantID, sensorID) }

// SetTenant은 센서가 속한 테넌트를 지정한다. 지정하지 않은 센서는 기본 테넌트다.
func (p *Publisher) SetTenant(sensorID, tenantID int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.tenants == nil {
		p.tenants = map[int]int{}
	}
	p.tenants[sensorID] = tenantID
}

func (p *Publisher) topic(sensorID int) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	if t, ok := p.tenants[sensorID]; ok {
		return TopicFor(t, sensorID)
	}
	return Topic(sensorID)
}

// Payload는 에뮬레이터와 같은 필드를 가진 측정값 JSON이다.
func Payload(sensorID int, seq int64, at time.Time) []byte {
	b, _ := json.Marshal(map[string]any{
		"sensor_id": sensorID, "seq": seq, "time": at.UTC().Format(time.RFC3339Nano),
		"tilt_x": 0.1, "tilt_y": 0.2, "crack_width": nil, "temperature": nil,
	})
	return b
}

// PublishRaw는 payload를 그대로 발행하고 PUBACK을 기다린다. 기록하지 않는다.
func (p *Publisher) PublishRaw(topic string, payload []byte) error {
	tok := p.client.Publish(topic, 1, false, payload)
	if !tok.WaitTimeout(5 * time.Second) {
		return fmt.Errorf("publish timeout")
	}
	return tok.Error()
}

// Next는 센서의 다음 seq로 발행하고, PUBACK을 받으면 기록한다. 실패하면 같은 payload로 ctx가 끝날 때까지 재시도한다.
func (p *Publisher) Next(ctx context.Context, sensorID int) (int64, error) {
	p.mu.Lock()
	seq := p.next[sensorID]
	p.mu.Unlock()

	payload := Payload(sensorID, seq, time.Now())
	for {
		if err := p.PublishRaw(p.topic(sensorID), payload); err == nil {
			break
		}
		select {
		case <-ctx.Done():
			return seq, ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}

	p.mu.Lock()
	p.published.Add(sensorID, seq)
	p.next[sensorID] = seq + 1
	p.mu.Unlock()
	return seq, nil
}

// Record는 직접 발행한 메시지를 기대 목록에 추가한다. (같은 이벤트 반복 발행 등)
func (p *Publisher) Record(sensorID int, seq int64) {
	p.mu.Lock()
	p.published.Add(sensorID, seq)
	p.mu.Unlock()
}

// Published는 지금까지 기록된 발행 목록의 복사본이다.
func (p *Publisher) Published() reconcile.SeqCounts {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := reconcile.SeqCounts{}
	for id, m := range p.published {
		for seq, n := range m {
			for i := 0; i < n; i++ {
				out.Add(id, seq)
			}
		}
	}
	return out
}

// Count는 기록된 고유 (sensor, seq) 수다.
func (p *Publisher) Count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, m := range p.published {
		n += len(m)
	}
	return n
}
