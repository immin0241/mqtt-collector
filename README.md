# StructWatch MQTT Collector

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

발행 로그는 JSONL이고 한 줄이 `{"sensor_id": 101, "seq": 42}` 이다. 에뮬레이터가 이 로그를 남기는 것은 Phase 4 작업이며,
그 전까지는 로그 없이 실행하는 모드(꼬리 쪽 유실은 찾지 못함)를 쓴다. 누락, 예상 외, DB 중복이 하나라도 있으면 종료 코드 1을 돌려준다.
격리된 행은 누락이 아니라 `rejected`로 센다.

### 테스트

```bash
go test -race ./...   # 단위 테스트 (flush 조건, ack 순서, 재시도, 격리, 종료 drain, 오류 분류, 중복 제거, reconcile)
```

통합·장애 주입 테스트(testcontainers, e2e)는 Phase 3에서 추가한다.

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
| [0004](docs/adr/0004-broker-inflight-and-queue.md) | `max_inflight_messages 600`, `max_queued_messages 60000` |
| [0005](docs/adr/0005-keep-gorm.md) | gorm 유지, pgx 오류 코드로 분류 |
| [0006](docs/adr/0006-sensor-data-tenant-id-removal.md) | `sensor_data.tenant_id` 제거 이력 |

## 측정 결과

> 아래 수치는 모두 **저부하 수동 실험**이다. 센서 3개, 합계 약 3 msg/s, QoS 1, WSL2 단일 호스트(Docker 29.8.0, 호스트 메모리 15.42GiB),
> 배치 300건 / 2초. 처리량 주장이 아니며, 조건과 절차는 [docs/experiments/phase1-tc02-03-04.md](docs/experiments/phase1-tc02-03-04.md)에 있다.

| TC | 기준 | 실측 | 판정 |
| --- | --- | --- | --- |
| TC-01 정상 수집 | 센서 10 × 1,000건, 누락·중복 0 | 센서 3 × 약 15~180건 연속 확인. **규모 미달** | 미검증 |
| TC-02 DB 10초 중단 | panic 0, 누락·중복 0, 메모리 상한 | panic 0, 누락 0, 중복 0, 앱 메모리 6.3MiB | 통과 |
| TC-03 브로커 10초 중지 | 재연결·재구독, 누락 0, 중복 0 | 재연결·재구독 정상, 중복 0, **누락 1건**(에뮬레이터 `publish_errors`=1, 발행 측 유실로 추정, 발행 로그 없어 미확정) | **미달** |
| TC-04 SIGKILL | 재시작 후 누락 0 | 추가 누락 0, 중복 0. 커밋~ack 사이 kill 여부는 미관측 | 통과(제한적) |
| TC-05 같은 이벤트 3회 | DB 1건, 중복 제거 지표 증가 | DB 1건, `dedup_total` +2 (Phase 2 환경에서 확인) | 통과 |
| TC-06 1,000 msg/s 5분 + DB 지연 | 메모리 상한, 누락·중복 0 | 실행하지 않음 | 미검증 |
| TC-07 형식 오류·FK 위반 | 격리, 정상 행 저장, 무한 재시도 없음 | 깨진 JSON, FK 위반 격리, 정상 행 저장. **범위 밖 값은 제약이 없어 격리되지 않음** | 부분 통과 |

수집 지연: Phase 2 환경에서 `ingest_lag_seconds` p99가 약 2.9초였다(약 3 msg/s, flush 주기 2초 영향).

## 알려진 한계

- 에뮬레이터는 발행 로그와 발행 측 버퍼링이 없다(Phase 4). 브로커 단절 시 발행 측 유실과 수집 측 유실을 아직 구분하지 못한다.
- 범위 밖 값, 시간이 0인 payload 검증이 미결이다(ADR-0003).
- Grafana의 Prometheus 데이터소스는 기본 org(1)에 등록된다. 지표에는 sensor_id가 없지만 전 테넌트 합산이므로,
  Phase 6에서 테넌트 org에 노출되지 않도록 폴더·데이터소스 권한을 정리해야 한다.

## 디렉터리

| 경로 | 내용 |
| --- | --- |
| `main.go`, `workers/` | 수집 워커 (수신, 배치 저장, 센서 상태) |
| `migrations/`, `seeds/` | 스키마(up/down 쌍)와 개발 시드 |
| `tools/reconcile/` | 발행 로그 ↔ DB 대조 도구 |
| `mqtt_emulator/` | Python 센서 에뮬레이터 ([README](mqtt_emulator/README.md)) |
| `prometheus/`, `grafana/` | 지표 수집 설정과 대시보드 |
| `docs/` | ADR(`adr/`), 실험 기록(`experiments/`), 확인 노트(`notes/`) |
