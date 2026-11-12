package workers

import (
	"context"
	"testing"
	"time"
)

func newTestSession(id, size int) *tenantSession {
	return &tenantSession{tenantID: id, ch: make(chan Envelope, size), done: make(chan struct{})}
}

func TestTenantTopicAndClientID(t *testing.T) {
	if got := tenantDataFilter(7); got != "sensors/7/+/+" {
		t.Fatalf("data filter = %q", got)
	}
	if got := tenantStatusFilter(7); got != "sensors/7/+/+/status" {
		t.Fatalf("status filter = %q", got)
	}
	if got := tenantClientID("collector", 7); got != "collector-t7" {
		t.Fatalf("client id = %q", got)
	}
}

// 공용 채널이 가득 찬 동안 폭주 테넌트(A)의 forwarder와 평온한 테넌트(B)의 forwarder가 번갈아 들어가야 한다.
// 공정하지 않다면 B의 메시지는 A의 백로그 전체 뒤에 처리된다. (ADR-0011)
func TestForwardersInterleaveTenants(t *testing.T) {
	const backlog = 200
	s := &SensorCollectionWorker{DataChannel: make(chan Envelope, 10)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a := newTestSession(1, backlog)
	b := newTestSession(2, 10)
	for i := 0; i < backlog; i++ {
		a.ch <- Envelope{Topic: "a"}
	}
	go s.forward(ctx, a)
	time.Sleep(50 * time.Millisecond) // A가 공용 채널을 채우고 송신 대기열에 선다

	const calm = 5
	for i := 0; i < calm; i++ {
		b.ch <- Envelope{Topic: "b", Payload: []byte{byte(i)}}
	}
	go s.forward(ctx, b)
	time.Sleep(50 * time.Millisecond)

	var order []Envelope
	for len(order) < backlog+calm {
		select {
		case e := <-s.DataChannel:
			order = append(order, e)
			time.Sleep(200 * time.Microsecond) // 느린 저장 워커
		case <-time.After(3 * time.Second):
			t.Fatalf("only %d of %d messages forwarded", len(order), backlog+calm)
		}
	}

	last, seen := -1, 0
	for i, e := range order {
		if e.Topic == "b" {
			if int(e.Payload[0]) != seen {
				t.Fatalf("tenant B order broken: got payload %d at position %d, want %d", e.Payload[0], i, seen)
			}
			seen++
			last = i
		}
	}
	if seen != calm {
		t.Fatalf("tenant B messages forwarded = %d, want %d", seen, calm)
	}
	// 공용 채널 버퍼(10)가 비워진 뒤에는 번갈아 들어가므로 B의 마지막 메시지는 대략 10 + 2×5 번째 안에 있다.
	if last > 30 {
		t.Fatalf("tenant B's last message at position %d of %d; flood tenant starves the calm tenant", last, len(order))
	}
	t.Logf("calm tenant's %d messages done by position %d of %d", calm, last, len(order))
}

// ctx 가 취소되면 forwarder 는 종료한다. 넘기지 못한 메시지는 ack 되지 않았으므로 브로커가 재전달한다.
func TestForwarderStopsOnCancel(t *testing.T) {
	s := &SensorCollectionWorker{DataChannel: make(chan Envelope)} // 소비자 없음
	ctx, cancel := context.WithCancel(context.Background())
	a := newTestSession(1, 4)
	a.ch <- Envelope{Topic: "a"}
	go s.forward(ctx, a)
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case <-a.done:
	case <-time.After(time.Second):
		t.Fatal("forwarder did not stop after cancel")
	}
}
