# Phase 3 테스트 인프라 메모

## Toxiproxy 대신 프로세스 내 장애 프록시를 쓴 이유

CLAUDE.md는 TC-02/06에 Toxiproxy를 제안했다. 개발 환경(WSL2, Docker 29.8.0)에서 `ghcr.io` 이미지 pull이
`failed to fetch oauth token: denied` 로 거부되어 Toxiproxy 공식 이미지를 받을 수 없었다.
그래서 `internal/testenv/faultproxy.go` 에 TCP 중계 프록시를 직접 두고 다음만 지원한다.

- `Cut()` / `Restore()`: 기존 연결을 끊고 새 연결을 즉시 닫는다 (서버 중단과 같은 효과)
- `SetLatency(d)`: 데이터를 한 번 전달할 때마다 d 만큼 지연 (양방향)
- `SetUpstream(addr)`: 컨테이너를 재시작해 매핑 포트가 바뀌어도 클라이언트 주소를 유지

Toxiproxy를 쓸 수 있는 환경이면 같은 인터페이스(`Cut/Restore/SetLatency`)로 교체할 수 있다.
한계: 대역폭 제한, 부분 패킷 손실 같은 독성은 구현하지 않았다.

## 테스트 구성

| 위치 | 태그 | 내용 |
| --- | --- | --- |
| `workers/*_test.go` | - | 가짜 시계·가짜 Store 단위 테스트 (flush 조건, 재시도 백오프, 격리, drain) |
| `test/integration` | - | testcontainers(TimescaleDB, Mosquitto) + `internal/app`. TC-01, 05, 07, 16 |
| `test/e2e` | `e2e` | 장애 주입. TC-02, 03, 04, 06 |

- 판정은 모두 `internal/reconcile`(CLI `tools/reconcile`과 동일 로직)을 쓴다.
- 발행기(`testenv.Publisher`)는 PUBACK을 받은 메시지만 "발행됨"으로 기록하고 실패하면 같은 payload로 재시도한다.
  따라서 대조에서 나오는 누락은 발행 측이 아니라 수집 측 유실이다. (Phase 1 TC-03의 "발행 측 유실 1건" 모호함 해소)
- Docker를 쓸 수 없으면 통합·e2e 테스트는 건너뛴다 (`testcontainers.SkipIfProviderIsNotHealthy`).

## 실행

```bash
go test -race ./...                 # 단위 + 통합
go test -tags e2e ./test/e2e -v     # 장애 주입 (약 3분)
E2E_TC06_DURATION=5m go test -tags e2e ./test/e2e -run TC06 -v   # RFP 조건
```

## 측정 결과 (1회 실행)

조건: WSL2, Docker 29.8.0, Go 1.27.0, TimescaleDB pg18, Mosquitto 2 (`max_inflight_messages 600`, `max_queued_messages 60000`),
앱은 테스트 프로세스 안에서 실행(TC-04만 빌드한 바이너리를 하위 프로세스로 실행), 배치 300 / flush 기본 2s(통합 테스트는 200ms),
채널 600, 센서 5~10개, 발행 QoS1.

| TC | 조건 | 결과 |
| --- | --- | --- |
| TC-01 | 10센서 × 1,000건 | 누락·중복·예상 외 0, 약 11초 |
| TC-02 | 500 msg/s, DB 10초 차단 | 누락·중복 0, 발행 9,999건, `channel_depth` 최대 317, 힙 4→7MiB |
| TC-03 | 500 msg/s, 브로커 컨테이너 10초 중지 | 누락·중복 0, 발행 5,314건(중단 중 PUBACK 못 받은 분은 기록 제외) |
| TC-04 | 500 msg/s, 수집기 SIGKILL 후 3초 뒤 재시작 | 누락·중복 0, 발행 8,000건 |
| TC-06 | 1,000 msg/s 30초, DB 지연 100ms/방향 | 누락·중복 0, 달성 999 msg/s, `channel_depth` 최대 407, 힙 4→9MiB, 지연 해제 후 약 4초에 해소 |

해석과 한계:

- `channel_depth` 최대가 600이 아니라 300~400대인 것은 `max_inflight_messages 600` 때문으로 추정한다. 배치가 보유한 미ack 300건이
  inflight를 차지해 채널로 들어오는 양이 약 300으로 제한된다. (inflight 값을 바꿔 확인하지는 않았다.)
- TC-06은 RFP의 5분이 아니라 30초로 실행했다. 5분 조건은 `E2E_TC06_DURATION=5m` 으로 별도 측정이 필요하며 미측정이다.
  적체가 `max_queued_messages(60000)`를 넘는 조건에서는 브로커가 메시지를 버리므로 DB 지연 크기와 함께 정해야 한다.
- 각 TC는 1회 실행이다. 반복·통계적 안정성은 확인하지 않았다.

## 발견: 정상 종료 시 Unsubscribe 유실

TC-16 통합 테스트가 Phase 1 종료 순서의 결함을 드러냈다. → [ADR-0007](../adr/0007-shutdown-keeps-subscription.md)
