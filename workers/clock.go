package workers

import "time"

// Clock은 시간에 의존하는 로직(flush 주기, 재시도 백오프)을 테스트에서 제어하기 위한 추상화다.
type Clock interface {
	NewTimer(d time.Duration) Timer
	After(d time.Duration) <-chan time.Time
}

// Timer는 time.Timer와 같은 의미를 갖는다.
type Timer interface {
	C() <-chan time.Time
	Stop() bool
	Reset(d time.Duration) bool
}

type realClock struct{}

func (realClock) NewTimer(d time.Duration) Timer         { return realTimer{time.NewTimer(d)} }
func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

type realTimer struct{ *time.Timer }

func (t realTimer) C() <-chan time.Time { return t.Timer.C }
