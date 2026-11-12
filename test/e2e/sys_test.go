//go:build e2e

package e2e

import (
	"sync"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// brokerDropped는 브로커의 $SYS/broker/publish/messages/dropped (inflight·큐 한도 때문에 버려진 PUBLISH 수)의 최신 값을 돌려주는 함수를 만든다.
// $SYS 값은 sys_interval(기본 10초)마다 갱신되므로 마지막 읽기는 최대 그만큼 늦다.
// 값을 아직 받지 못했으면 "unknown"이다.
func brokerDropped(t *testing.T, addr string) func() string {
	t.Helper()
	var mu sync.Mutex
	last := "unknown"
	opts := mqtt.NewClientOptions().
		AddBroker("tcp://" + addr).
		SetClientID("e2e-sys-watch").
		SetCleanSession(true).
		SetAutoReconnect(true)
	c := mqtt.NewClient(opts)
	if tok := c.Connect(); !tok.WaitTimeout(10*time.Second) || tok.Error() != nil {
		t.Logf("sys watcher connect failed: %v", tok.Error())
		return func() string { return last }
	}
	t.Cleanup(func() { c.Disconnect(100) })
	c.Subscribe("$SYS/broker/publish/messages/dropped", 0, func(_ mqtt.Client, m mqtt.Message) {
		mu.Lock()
		last = string(m.Payload())
		mu.Unlock()
	})
	return func() string {
		mu.Lock()
		defer mu.Unlock()
		return last
	}
}
