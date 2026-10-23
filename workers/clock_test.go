package workers

import (
	"sync"
	"testing"
	"time"
)

// fakeClock은 Advance로만 시간이 흐르는 Clock이다.
type fakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
	waits  []*fakeWait
	durs   []time.Duration // After에 전달된 대기 시간 기록
}

type fakeTimer struct {
	c        *fakeClock
	ch       chan time.Time
	deadline time.Time
	active   bool
}

type fakeWait struct {
	ch       chan time.Time
	deadline time.Time
	fired    bool
}

func newFakeClock() *fakeClock { return &fakeClock{now: time.Unix(1_000_000, 0)} }

func (c *fakeClock) NewTimer(d time.Duration) Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &fakeTimer{c: c, ch: make(chan time.Time, 1), deadline: c.now.Add(d), active: true}
	c.timers = append(c.timers, t)
	return t
}

func (c *fakeClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	w := &fakeWait{ch: make(chan time.Time, 1), deadline: c.now.Add(d)}
	c.waits = append(c.waits, w)
	c.durs = append(c.durs, d)
	return w.ch
}

func (t *fakeTimer) C() <-chan time.Time { return t.ch }

func (t *fakeTimer) Stop() bool {
	t.c.mu.Lock()
	defer t.c.mu.Unlock()
	was := t.active
	t.active = false
	return was
}

func (t *fakeTimer) Reset(d time.Duration) bool {
	t.c.mu.Lock()
	defer t.c.mu.Unlock()
	was := t.active
	t.deadline = t.c.now.Add(d)
	t.active = true
	return was
}

// Advance는 시간을 d만큼 흐르게 하고, 기한이 지난 타이머와 대기를 발화시킨다.
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	for _, t := range c.timers {
		if t.active && !t.deadline.After(c.now) {
			t.active = false
			select {
			case t.ch <- c.now:
			default:
			}
		}
	}
	for _, w := range c.waits {
		if !w.fired && !w.deadline.After(c.now) {
			w.fired = true
			w.ch <- c.now
		}
	}
}

func (c *fakeClock) activeTimers() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, t := range c.timers {
		if t.active {
			n++
		}
	}
	return n
}

func (c *fakeClock) waitCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.durs)
}

func (c *fakeClock) waitDurations() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]time.Duration(nil), c.durs...)
}

// 첫 메시지 이후 정확히 FlushInterval이 지나야 flush한다. (실제 시간 대기 없음)
func TestFlushInterval_FakeClock(t *testing.T) {
	fs := &fakeStore{}
	ch := make(chan Envelope, 10)
	clk := newFakeClock()
	w := &DbPushWorker{Store: fs, DataChannel: ch, BatchSize: 100, FlushInterval: 2 * time.Second, Clock: clk}
	cancel, done := startWorker(t, w)

	ch <- envelope(fs, 1, 1)
	eventually(t, func() bool { return clk.activeTimers() == 1 })

	clk.Advance(1999 * time.Millisecond)
	time.Sleep(30 * time.Millisecond) // 워커가 잘못 flush할 시간을 준다
	if got := fs.countPrefix("ack:"); got != 0 {
		t.Fatalf("flushed %d messages before FlushInterval elapsed", got)
	}

	clk.Advance(time.Millisecond)
	eventually(t, func() bool { return fs.countPrefix("ack:") == 1 })
	cancel()
	<-done
}

// 타이머는 배치의 첫 메시지 시점부터 잰다. 두 번째 메시지가 기한을 연장하지 않는다.
func TestFlushInterval_MeasuredFromFirstMessage(t *testing.T) {
	fs := &fakeStore{}
	ch := make(chan Envelope, 10)
	clk := newFakeClock()
	w := &DbPushWorker{Store: fs, DataChannel: ch, BatchSize: 100, FlushInterval: 2 * time.Second, Clock: clk}
	cancel, done := startWorker(t, w)

	ch <- envelope(fs, 1, 1)
	eventually(t, func() bool { return clk.activeTimers() == 1 })
	clk.Advance(1500 * time.Millisecond)
	ch <- envelope(fs, 1, 2)
	time.Sleep(30 * time.Millisecond)

	clk.Advance(500 * time.Millisecond) // 첫 메시지로부터 2초
	eventually(t, func() bool { return fs.countPrefix("ack:") == 2 })
	cancel()
	<-done
}

// 재시도 백오프는 초기값에서 두 배씩 늘다가 상한에서 멈춘다.
func TestRetryBackoffSequence_FakeClock(t *testing.T) {
	fs := &fakeStore{failTransient: 5}
	ch := make(chan Envelope, 10)
	clk := newFakeClock()
	w := &DbPushWorker{
		Store: fs, DataChannel: ch, BatchSize: 1, FlushInterval: time.Hour,
		RetryInitial: 500 * time.Millisecond, RetryMax: 4 * time.Second, Clock: clk,
	}
	cancel, done := startWorker(t, w)

	ch <- envelope(fs, 1, 1)
	for i := 1; i <= 5; i++ {
		eventually(t, func() bool { return clk.waitCount() == i })
		d := clk.waitDurations()[i-1]
		clk.Advance(d)
	}
	eventually(t, func() bool { return fs.countPrefix("ack:") == 1 })

	want := []time.Duration{500 * time.Millisecond, time.Second, 2 * time.Second, 4 * time.Second, 4 * time.Second}
	got := clk.waitDurations()
	if len(got) != len(want) {
		t.Fatalf("backoffs = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("backoffs = %v, want %v", got, want)
		}
	}
	cancel()
	<-done
}
