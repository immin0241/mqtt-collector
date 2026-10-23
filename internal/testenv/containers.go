// Package testenv는 통합·e2e 테스트용 인프라(컨테이너, 장애 프록시, 발행기)를 제공한다.
package testenv

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	timescaleImage = "timescale/timescaledb:latest-pg18"
	mosquittoImage = "eclipse-mosquitto:2"

	// 운영 설정(ADR-0004)과 같은 inflight/queue 값을 쓰고, 재시작에도 세션이 남도록 persistence를 켠다.
	mosquittoConf = `listener 1883
allow_anonymous true
persistence true
persistence_location /mosquitto/data/
autosave_interval 1
max_inflight_messages 600
max_queued_messages 60000
log_type error
log_type warning
`
	// 원인 분석용: MOSQUITTO_DEBUG=1 이면 모든 로그를 남긴다.
	mosquittoDebugConf = "log_type notice\nlog_type information\nconnection_messages true\n"
)

// RepoRoot는 저장소 루트 경로를 돌려준다.
func RepoRoot() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(file), "..", "..")
}

// RequireDocker는 Docker를 쓸 수 없으면 테스트를 건너뛴다.
func RequireDocker(t *testing.T) {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)
}

// Postgres는 마이그레이션이 적용된 TimescaleDB 컨테이너다.
type Postgres struct {
	Container *tcpostgres.PostgresContainer
	DSN       string // 컨테이너에 직접 연결하는 접속 문자열
	Addr      string // host:port
}

// StartPostgres는 TimescaleDB를 띄우고 migrations/*.up.sql을 번호 순서대로 적용한다.
func StartPostgres(t testing.TB) *Postgres {
	t.Helper()
	ctx := context.Background()
	c, err := tcpostgres.Run(ctx, timescaleImage,
		tcpostgres.WithDatabase("go"),
		tcpostgres.WithUsername("go"),
		tcpostgres.WithPassword("go"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(c) })

	dsn, err := c.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("postgres dsn: %v", err)
	}
	host, err := c.Host(ctx)
	if err != nil {
		t.Fatalf("postgres host: %v", err)
	}
	port, err := c.MappedPort(ctx, "5432/tcp")
	if err != nil {
		t.Fatalf("postgres port: %v", err)
	}
	p := &Postgres{Container: c, DSN: dsn, Addr: fmt.Sprintf("%s:%s", host, port.Port())}
	p.applyMigrations(t)
	return p
}

func (p *Postgres) applyMigrations(t testing.TB) {
	t.Helper()
	ctx := context.Background()
	files, err := filepath.Glob(filepath.Join(RepoRoot(), "migrations", "*.up.sql"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no migrations found: %v", err)
	}
	sort.Strings(files)

	conn := p.Conn(t)
	defer conn.Close(ctx)
	for _, f := range files {
		sql, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Exec(ctx, string(sql)); err != nil {
			t.Fatalf("apply %s: %v", filepath.Base(f), err)
		}
	}
}

// DSNFor는 같은 DB를 다른 host:port(예: FaultProxy)로 접속하는 문자열이다.
func (p *Postgres) DSNFor(addr string) string {
	return strings.Replace(p.DSN, p.Addr, addr, 1)
}

// Conn은 컨테이너에 직접 연결한다. 호출한 쪽이 닫는다.
func (p *Postgres) Conn(t testing.TB) *pgx.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, p.DSN)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	return conn
}

// SeedSensors는 테스트용 테넌트와 센서를 만든다. 센서 타입은 id % 3 으로 정한다.
func (p *Postgres) SeedSensors(t testing.TB, ids ...int) {
	t.Helper()
	ctx := context.Background()
	conn := p.Conn(t)
	defer conn.Close(ctx)

	if _, err := conn.Exec(ctx,
		`INSERT INTO tenant (idp_org_id, name) VALUES ('test-org', 'test') ON CONFLICT (idp_org_id) DO NOTHING`); err != nil {
		t.Fatalf("seed tenant: %v", err)
	}
	types := []string{"tilt", "crack", "env"}
	for _, id := range ids {
		if _, err := conn.Exec(ctx,
			`INSERT INTO sensor (id, tenant_id, facility_id, sensor_type, vendor)
			 OVERRIDING SYSTEM VALUE
			 SELECT $1, t.id, 1, $2, 'test' FROM tenant t WHERE t.idp_org_id = 'test-org'
			 ON CONFLICT (id) DO NOTHING`, id, types[id%3]); err != nil {
			t.Fatalf("seed sensor %d: %v", id, err)
		}
	}
}

// Mosquitto는 테스트용 브로커 컨테이너다.
type Mosquitto struct {
	Container testcontainers.Container
}

// StartMosquitto는 persistence가 켜진 브로커를 띄운다.
func StartMosquitto(t testing.TB) *Mosquitto {
	t.Helper()
	ctx := context.Background()
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        mosquittoImage,
			ExposedPorts: []string{"1883/tcp"},
			Files: []testcontainers.ContainerFile{{
				Reader:            strings.NewReader(brokerConf()),
				ContainerFilePath: "/mosquitto/config/mosquitto.conf",
				FileMode:          0o644,
			}},
			WaitingFor: wait.ForListeningPort("1883/tcp").WithStartupTimeout(60 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start mosquitto: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(c) })
	return &Mosquitto{Container: c}
}

func brokerConf() string {
	if os.Getenv("MOSQUITTO_DEBUG") == "1" {
		return mosquittoConf + mosquittoDebugConf
	}
	return mosquittoConf
}

// Logs는 브로커 컨테이너 로그다. 실패 분석용이다.
func (m *Mosquitto) Logs(t testing.TB) string {
	t.Helper()
	r, err := m.Container.Logs(context.Background())
	if err != nil {
		return "logs unavailable: " + err.Error()
	}
	defer r.Close()
	b, _ := io.ReadAll(r)
	return string(b)
}

// Addr는 호스트에서 본 현재 host:port 다. 컨테이너를 다시 시작하면 바뀔 수 있다.
func (m *Mosquitto) Addr(t testing.TB) string {
	t.Helper()
	ctx := context.Background()
	host, err := m.Container.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := m.Container.MappedPort(ctx, "1883/tcp")
	if err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("%s:%s", host, port.Port())
}

// Stop은 브로커를 정상 종료(SIGTERM)해 세션과 큐를 디스크에 저장하게 한다.
func (m *Mosquitto) Stop(t testing.TB) {
	t.Helper()
	timeout := 10 * time.Second
	if err := m.Container.Stop(context.Background(), &timeout); err != nil {
		t.Fatalf("stop mosquitto: %v", err)
	}
}

// Start는 멈춘 브로커를 다시 시작한다.
func (m *Mosquitto) Start(t testing.TB) {
	t.Helper()
	if err := m.Container.Start(context.Background()); err != nil {
		t.Fatalf("start mosquitto: %v", err)
	}
}
