# StructWatch MQTT Collector

[![CI](https://github.com/immin0241/webframework-project-mqtt-collector/actions/workflows/ci.yml/badge.svg)](https://github.com/immin0241/webframework-project-mqtt-collector/actions/workflows/ci.yml)

마포구 「AI 스마트 IoT 계측센서 관리시스템」 RFP를 기반으로 한 IoT 계측 데이터 수집 플랫폼이다.
센서(기울기·균열·환경) 데이터를 MQTT로 받아 TimescaleDB에 **유실·중복 없이** 저장하는 것이 핵심이며,
"동작한다"보다 **"장애에서도 정합성을 증명한다"** 가 목표다. AI 기능은 이후 단계에서 붙인다.

## 아키텍처

```
mqtt_emulator ──QoS1──▶ Mosquitto ──▶ SensorCollectionWorker ──chan(600)──▶ DbPushWorker ──batch(300/2s)──▶ TimescaleDB
 (Python)                             (구독·파싱·received_at)                (재시도·격리·ack)               sensor_data
                                                                                  │                          sensor_data_rejected
                                                                                  └──▶ /metrics ──▶ Prometheus ──▶ Grafana
```

- 배치 flush: 건수(300) 도달 또는 배치 첫 메시지 이후 2초 경과.
- 중복 판정: `UNIQUE (sensor_id, seq, time)` + `ON CONFLICT DO NOTHING`.
- **ack는 DB 커밋(또는 격리 기록) 뒤에만** 보낸다. 일시적 DB 오류는 재시도하며 그동안 수신은 백프레셔로 막힌다.
  데이터 오류 행은 이분법으로 찾아 `sensor_data_rejected`로 격리한다.

## 실행 방법

요구 사항: Docker Compose v2. (선택) Go 1.27, [golang-migrate](https://github.com/golang-migrate/migrate) CLI.

```bash
cp .env.example .env
docker compose up -d --build init broker1 postgres migrate seed app emulator prometheus
```

`migrate`가 `migrations/`를 적용하고 `seed`가 `seeds/`(워커 DB role, 개발 센서 101~103)를 적용한 뒤에 `app`이 뜬다.

에뮬레이터는 시작 직후 센서가 없으므로 설정을 한 번 넣는다:

```bash
curl -X PUT localhost:8000/config -H 'Content-Type: application/json' -d '{
  "sensors": [{"id":101,"sensor_type":"tilt"},{"id":102,"sensor_type":"crack"},{"id":103,"sensor_type":"env"}],
  "interval_seconds": 1.0, "jitter_fraction": 0.1, "topic_prefix": "sensors", "qos": 1 }'
```

| 확인할 것 | 주소 |
| --- | --- |
| 앱 지표 | 컨테이너 내부 `app:9090/metrics` (호스트에 공개하지 않음) |
| Prometheus | http://localhost:9091 |
| Grafana (수집 경로 대시보드 `ingest-path`) | http://localhost:3000 (기본 admin / admin) — 먼저 `docker compose up -d grafana` |
| 에뮬레이터 API | http://localhost:8000 (`/config`, `/stats`, `/sensors`) |

Grafana와 Logto(OIDC) 설정은 [docs/logto-grafana-setup.md](docs/logto-grafana-setup.md)를 따른다.

### 정합성 대조 (reconcile)

```bash
export POSTGRESQL_URL='postgres://go:go@localhost:5432/go?sslmode=disable'
go run ./tools/reconcile --since 2026-10-23T01:00:00Z                       # 저장된 seq의 min~max를 기대로 삼음
go run ./tools/reconcile --published run.jsonl --since 2026-10-23T01:00:00Z # 발행 로그와 정확히 대조
```

발행 로그는 JSONL이고 한 줄이 `{"sensor_id": 101, "seq": 42}` 이다. 시나리오 실행기(`mqtt_emulator/run_scenario.py`)가 PUBACK 받은 전송마다 남긴다.
로그 없이 실행하면 저장된 seq의 min~max를 기대로 삼는 모드(꼬리 쪽 유실은 찾지 못함)로 동작한다. 누락, 예상 외, DB 중복이 하나라도 있으면 종료 코드 1을 돌려준다.
격리된 행은 누락이 아니라 `rejected`로 센다.

### 테스트

```bash
go test -race ./...                 # 단위 + 통합(testcontainers: TimescaleDB, Mosquitto; Docker 필요, 없으면 건너뜀)
go test -tags e2e ./test/e2e -v     # 장애 주입: TC-02(DB 차단), 03(브로커 중지), 04(SIGKILL), 06(부하+DB 지연)
```

통합 테스트는 TC-01·05·07·08·09(DB 계층)·12·13·16과 재시작 시 센서 상태, e2e는 TC-02·03·04·06을 코드로 재현하며 판정은 `tools/reconcile`과 같은 `internal/reconcile`을 쓴다.
구성, 측정 조건·결과, Toxiproxy 대신 프로세스 내 프록시를 쓴 이유는 [docs/notes/phase3-test-infra.md](docs/notes/phase3-test-infra.md)에 있다.
CI는 푸시마다 vet과 위 첫 번째 명령을 실행하고, e2e는 GitHub Actions에서 수동으로 실행한다.

### 주요 환경 변수

| 변수 | 설명 |
| --- | --- |
| `MQTT_BROKER`, `MQTT_USERNAME`, `MQTT_PASSWORD` | 브로커 주소와 계정 |
| `MQTT_CLIENT_ID` | **필수.** 지속 세션 식별자. 인스턴스마다 고유하고 재시작해도 같아야 한다 |
| `DATABASE_URL` | 워커 DB 접속 (role `app_worker`) |
| `METRICS_ADDR` | 지표 서버 주소 (기본 `:9090`) |
| `FLUSH_INTERVAL` | 배치 flush 주기 (기본 `2s`) |

## 설계 결정 (ADR)

| ADR | 결정 |
| --- | --- |
| [0001](docs/adr/0001-ack-timing.md) | 수동 ack, 배치 커밋 뒤 일괄 ack |
| [0002](docs/adr/0002-retry-and-backpressure.md) | 일시적 오류는 무한 지수 백오프 재시도, 채널 포화 시 수신 정지 |
| [0003](docs/adr/0003-error-classification-and-quarantine.md) | SQLSTATE 22/23은 영구 오류, 이분법으로 문제 행만 격리 |
| [0004](docs/adr/0004-broker-inflight-and-queue.md) | `max_inflight_messages 600`, `max_queued_messages 200000` (TC-06 5분 조건 때문에 60000에서 상향, 세션당 최악 ~130 MiB) |
| [0005](docs/adr/0005-keep-gorm.md) | gorm 유지, pgx 오류 코드로 분류 |
| [0006](docs/adr/0006-sensor-data-tenant-id-removal.md) | `sensor_data.tenant_id` 제거 이력 |
| [0007](docs/adr/0007-shutdown-keeps-subscription.md) | 정상 종료 시 구독을 해제하지 않는다 (TC-16) |
| [0008](docs/adr/0008-sensor-state-and-alerts.md) | 센서 상태(online/offline/faulty)와 경보를 수신 시점 메모리에서 평가, edge trigger |
| [0009](docs/adr/0009-tenant-isolation.md) | 테넌트 격리: role→테넌트 매핑 뷰(세션 설정 우회 차단), `sensor_data.tenant_id` 재도입 (0006 대체) |
| [0010](docs/adr/0010-alert-api.md) | 경보 규칙·상태 변경 API: 별도 Go 서비스, 조직 토큰(JWKS) 인증, scope로 관리자/운영자 분리, 최소 권한 API 전용 DB role |
| [0011](docs/adr/0011-tenant-surge-isolation.md) | 테넌트 폭주 격리: 테넌트별 구독 세션·큐 분리, topic `sensors/{tenant_id}/...`, 단일 저장 워커 유지 (구현 완료) |
| [0012](docs/adr/0012-metric-catalog-narrow-sensor-data.md) | 측정값을 세로 구조(metric, value)로 바꾸고 단위는 metric 카탈로그에 둔다. 기존 측정 데이터 폐기 |
| [0013](docs/adr/0013-value-range-validation.md) | 범위 밖 측정값·비정상 시각은 그 메시지만 격리하고 ack, 범위는 metric 카탈로그 컬럼에 둔다 |
| [0014](docs/adr/0014-mcp-daily-report.md) | 테넌트 일일 리포트 MCP 서버: 읽기 전용 도구, Logto 조직 토큰, 테넌트별 DB role(격리는 DB가 보장) |

## 측정 결과

> 조건이 다른 두 묶음이다. **자동 테스트**(Phase 3)는 Docker 컨테이너(TimescaleDB pg18, Mosquitto 2)와 프로세스 내 장애 프록시로 재현 가능하게 실행한 1회 결과이고,
> **수동 실험**(Phase 1)은 센서 3개, 합계 약 3 msg/s의 저부하 결과다. 환경은 WSL2 단일 호스트(Docker 29.8.0), 배치 300건 / flush 2초(통합 테스트는 200ms)다.
> 상세 조건은 [docs/notes/phase3-test-infra.md](docs/notes/phase3-test-infra.md), [docs/experiments/phase1-tc02-03-04.md](docs/experiments/phase1-tc02-03-04.md),
> [docs/experiments/phase4-emulator-live.md](docs/experiments/phase4-emulator-live.md) 에 있다. 처리량 주장이 아니다.

| TC | 기준 | 실측 | 판정 |
| --- | --- | --- | --- |
| TC-01 정상 수집 | 센서 10 × 1,000건, 누락·중복 0 | 통합 테스트와 시나리오 실행기(`tc01.json`) 모두 10,000건 저장, 누락·중복·예상 외 0 | 통과 |
| TC-02 DB 10초 중단 | panic 0, 누락·중복 0, 메모리 상한 | e2e(500 msg/s, DB 10초 차단): 누락·중복 0, `channel_depth` 최대 317, 힙 4→7MiB | 통과 |
| TC-03 브로커 10초 중지 | 재연결·재구독, 누락 0, 중복 0 | e2e: 누락·중복 0. 발행기가 PUBACK 받은 것만 기록하므로 수집 측 유실 0을 입증(Phase 1의 1건 누락은 발행 측 유실이었음). 시나리오 실행기 6초 단절도 누락 0 | 통과 |
| TC-04 SIGKILL | 재시작 후 누락 0 | e2e(500 msg/s, 바이너리 SIGKILL 후 3초 뒤 재시작): 누락·중복 0 | 통과 |
| TC-05 같은 이벤트 3회 | DB 1건, 중복 제거 지표 증가 | 통합 테스트: DB 1건, `dedup_total` +2 | 통과 |
| TC-06 1,000 msg/s 5분 + DB 지연 | 메모리 상한, 누락·중복 0 | e2e **5분**(DB 지연 100ms/방향, 970 msg/s, 291,076건): **누락·중복 0, 브로커 드롭 0**, `channel_depth` 최대 439, 힙 4→60MiB, 해소 42.6s. `max_queued_messages 60000`에서는 누락 45.5%였고(브로커 드롭 수 134,883과 일치) 큐를 200,000으로 올려 해결 — 원인은 큐 한도. 부족분이 큐보다 길면 다시 넘친다. [phase7-tc06-5min](docs/experiments/phase7-tc06-5min.md) | 통과 (큐 크기 의존) |
| TC-07 형식 오류·FK 위반 | 격리, 정상 행 저장, 무한 재시도 없음 | 통합 테스트: 깨진 JSON, FK 위반 격리, 정상 행 저장, 재시도 루프 없음. **범위 밖 값은 제약이 없어 격리되지 않음** | 부분 통과 |
| TC-08 경보 정확성·지연 | 임계치 경보 정확, 지연 p99 2초 | 통합 테스트: 위반 지속·재전달 3회에도 경보는 위반 진입 시점 2건뿐, `created_at − triggered_at` 최대 0.003초(경보 2건, 같은 호스트 시계). **p99를 낼 표본이 아니다. 부하 상태의 지연은 미측정** | 부분 통과 |
| TC-12 통신 두절 | 조용한 센서만 offline, 복구 시 online | 통합 테스트: 센서 4개 중 1개 침묵 → 그 센서만 offline(timeout 이벤트 1건), 재수신 시 recovered. 4개 전부 조용하면 센서별 전이 없이 `stalled` 한 건 | 통과 |
| TC-13 상태 전이 | flatline·범위 밖 → faulty, 해소 시 복구 | 통합 테스트: flatline 12회 → faulty → 값 변화 → online. 이력 connected→fault→fault_cleared. 범위 밖 반복·경보 상태 전이 동시성(20 경쟁에서 1건 성공)은 단위·통합 테스트 | 통과 |
| TC-09 테넌트 격리 (DB 계층) | 모든 경로에서 타 테넌트 데이터 0건 | 통합 테스트(`scripts/provision_tenant.sh`로 만든 role): 뷰·기본 테이블·경보 테이블·쓰기·chunk 직접 조회·`SET`/`set_config`/`SET ROLE` 우회 모두 0건. **시험 중 세션 설정 우회 구멍을 발견해 수정(ADR-0009).** 경보 뷰와 대시보드 쿼리 13개도 tenant role로 검증. API 경로는 통합 테스트(타 조직 규칙·경보 404, 타 조직 센서 규칙 422). Grafana 로그인 경로는 Logto 로그인 후 사용자 2명이 각자 org에만 속함을 `scripts/grafana_audit.sh`로 확인(수동 1회, 매핑 변경 전에 로그인한 사용자는 이전 org에 남아 있었고 수정함). 실제 Logto 토큰으로 API 인증은 미시험 | 부분 통과 |
| TC-17 테넌트 폭주 | 다른 테넌트에 영향 없음 | 폭주 150,000건(한 테넌트): **평온한 테넌트 유실 0건, 저장 지연 p95 148 ms**. 큐 60,000에서는 폭주 테넌트 자신이 32.8% 유실, 200,000으로 올리면 폭주 테넌트도 0% 유실. 조건·A/B 대조는 [phase7-tc17-tenant-sessions](docs/experiments/phase7-tc17-tenant-sessions.md), 결정은 [ADR-0011](docs/adr/0011-tenant-surge-isolation.md). | 통과 |
| TC-16 정상 종료(SIGTERM) | 종료 후 재시작해도 누락·중복 0 | 통합 테스트: 센서당 700건 모두 저장. 이 테스트가 종료 시 Unsubscribe 유실과 paho 라우트 미등록 유실을 발견해 수정 | 통과 |

수집 지연: Phase 2 환경에서 `ingest_lag_seconds` p99가 약 2.9초였다(약 3 msg/s, flush 주기 2초 영향).

## 알려진 한계

- 발행 측 버퍼링과 발행 로그는 시나리오 실행기에만 있다. FastAPI 데모 서버는 브로커 단절 중 생성분을 버퍼링하지 않는다.
- TC-06 5분 조건은 `max_queued_messages 200000`에서만 통과한다. 부족분이 큐보다 길면 다시 유실된다(큐 상향은 유실 시점을 늦출 뿐, 쓰기 경로 개선은 [phase7-insert-strategies](docs/experiments/phase7-insert-strategies.md)에 후보만 있고 미적용). 큐가 찬 세션 하나의 브로커 메모리는 약 130 MiB.
- 범위 밖 값·비정상 시각 검증은 ADR-0013으로 해결했다(격리 테이블로 보내고 ack).
- MCP 리포트 서버(ADR-0014): 격리·인증 거부·읽기 전용은 자체 서명 토큰으로 통합 테스트했다. 실제 Logto 로그인(M2M 조직 토큰, Claude Code PKCE, 두 테넌트 사용자)은 Claude Code 에서 확인했다. **Claude Desktop/claude.ai 커넥터는 공개 HTTPS가 필요한데 스택은 평문 HTTP라 시험하지 못했다.** 리포트 품질은 평가하지 않았다. 스킬은 `skills/daily-report/SKILL.md`, 설치 방법은 [docs/claude-desktop-install.md](docs/claude-desktop-install.md).
- 센서 상태/경보(ADR-0008): 정상 범위·flatline 횟수는 센서 종류와 무관한 전역 기본값이다. 경보 큐가 차면 경보를 버린다(`alerts_dropped_total`).
  DB 중단 중 센서 상태가 바뀌지 않는지는 단위 테스트(Requeue)로만 확인했고 e2e로는 확인하지 않았다. 경보 규칙을 등록하는 API는 Phase 6이며 지금은 SQL로 넣는다.
- Grafana의 Prometheus 데이터소스는 기본 org(1)에 등록된다. 지표에는 sensor_id가 없지만 전 테넌트 합산이므로,
  Phase 6에서 테넌트 org에 노출되지 않도록 폴더·데이터소스 권한을 정리해야 한다.

## 디렉터리

| 경로 | 내용 |
| --- | --- |
| `main.go`, `workers/` | 수집 워커 (수신, 배치 저장, 센서 상태) |
| `cmd/api/`, `internal/api/` | 경보 규칙·상태 변경 API (Swagger UI `:8080/docs`, ADR-0010) |
| `migrations/`, `seeds/` | 스키마(up/down 쌍)와 개발 시드 |
| `tools/reconcile/` | 발행 로그 ↔ DB 대조 도구 |
| `mqtt_emulator/` | Python 센서 에뮬레이터 ([README](mqtt_emulator/README.md)) |
| `prometheus/`, `grafana/` | 지표 수집 설정과 대시보드 |
| `docker-compose.demo.yml`, `demo/`, `scripts/demo_setup.sh` | 시연(EC2) 오버레이: 센서 9개·멀티 테넌트·오류 주입 ([런북](docs/demo-runbook.md), [OCI 판](docs/demo-runbook-oci.md)) |
| `docs/` | ADR(`adr/`), 실험 기록(`experiments/`), 확인 노트(`notes/`) |
