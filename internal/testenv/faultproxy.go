package testenv

import (
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// FaultProxy는 TCP 연결을 중계하면서 장애를 주입한다. (연결 차단, 지연, 업스트림 교체)
//
// Toxiproxy 이미지(ghcr.io)를 받을 수 없는 환경에서도 같은 시나리오를 돌리기 위해 프로세스 안에 둔 최소 구현이다.
// 컨테이너를 재시작하면 매핑 포트가 바뀌므로, 클라이언트는 항상 이 프록시의 고정 주소로 접속한다.
type FaultProxy struct {
	ln net.Listener

	mu       sync.Mutex
	upstream string
	conns    map[net.Conn]struct{}

	cut     atomic.Bool
	latency atomic.Int64 // ns, 한 번 전달할 때마다 지연
}

// NewFaultProxy는 127.0.0.1의 임의 포트에서 upstream으로 중계하는 프록시를 시작한다.
func NewFaultProxy(t testing.TB, upstream string) *FaultProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("faultproxy listen: %v", err)
	}
	p := &FaultProxy{ln: ln, upstream: upstream, conns: map[net.Conn]struct{}{}}
	go p.serve()
	t.Cleanup(p.Close)
	return p
}

// Addr는 클라이언트가 접속할 host:port 다.
func (p *FaultProxy) Addr() string { return p.ln.Addr().String() }

// SetUpstream은 새 연결부터 쓸 업스트림 주소를 바꾼다.
func (p *FaultProxy) SetUpstream(addr string) {
	p.mu.Lock()
	p.upstream = addr
	p.mu.Unlock()
}

// Cut은 기존 연결을 모두 끊고 새 연결을 즉시 닫는다. (서버 중단과 같은 효과)
func (p *FaultProxy) Cut() {
	p.cut.Store(true)
	p.mu.Lock()
	for c := range p.conns {
		_ = c.Close()
	}
	p.mu.Unlock()
}

// Restore는 Cut을 해제한다.
func (p *FaultProxy) Restore() { p.cut.Store(false) }

// SetLatency는 양방향으로 데이터를 한 번 전달할 때마다 d만큼 지연시킨다. 0이면 해제한다.
func (p *FaultProxy) SetLatency(d time.Duration) { p.latency.Store(int64(d)) }

func (p *FaultProxy) Close() {
	_ = p.ln.Close()
	p.Cut()
}

func (p *FaultProxy) serve() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		if p.cut.Load() {
			_ = c.Close()
			continue
		}
		go p.handle(c)
	}
}

func (p *FaultProxy) handle(client net.Conn) {
	p.mu.Lock()
	upstream := p.upstream
	p.mu.Unlock()

	server, err := net.DialTimeout("tcp", upstream, 3*time.Second)
	if err != nil {
		_ = client.Close()
		return
	}

	p.mu.Lock()
	p.conns[client] = struct{}{}
	p.conns[server] = struct{}{}
	p.mu.Unlock()
	defer func() {
		p.mu.Lock()
		delete(p.conns, client)
		delete(p.conns, server)
		p.mu.Unlock()
		_ = client.Close()
		_ = server.Close()
	}()

	done := make(chan struct{}, 2)
	go func() { p.pipe(server, client); done <- struct{}{} }()
	go func() { p.pipe(client, server); done <- struct{}{} }()
	<-done
}

func (p *FaultProxy) pipe(dst, src net.Conn) {
	buf := make([]byte, 32*1024)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			if d := time.Duration(p.latency.Load()); d > 0 {
				time.Sleep(d)
			}
			if p.cut.Load() {
				return
			}
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			if err != io.EOF {
				_ = dst.Close()
			}
			return
		}
	}
}
