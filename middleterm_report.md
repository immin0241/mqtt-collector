# StructWatch MQTT Collector — 중간 보고서

- 작성일: 2026-10-25
- 기준: 저장소 전체 브랜치와 커밋 기록(`931b3fe` ~ `f1910ac`, 2026-10-10 ~ 2026-10-24), `README.md`, `docs/adr/`, `docs/experiments/`
- 현재 브랜치: `phase-6-tenancy` (Phase 6 진행 중)

## 1. 요약

마포구 「AI 스마트 IoT 계측센서 관리시스템」 RFP를 기반으로, 센서(기울기·균열·환경) 데이터를 MQTT로 받아 TimescaleDB에 **유실·중복 없이** 저장하는 수집 플랫폼을 만들고 있다.
목표는 "동작한다"가 아니라 "장애에서도 정합성을 증명한다"이다.

| 구분 | 상태 |
| --- | --- |
| Phase 1 수집 경로 정합성 | 완료 (dev 병합, PR #1) |
| Phase 2 측정 가능성·재현성 | 완료 (PR #2) |
| Phase 3 자동 테스트 | 완료 (PR #3) |
| Phase 4 에뮬레이터 고도화 | 완료 (PR #4) |
| Phase 5 센서 상태 판정·경보 | 완료 (PR #5) |
| Phase 6 멀티테넌시·관제 화면 | **진행 중** (`phase-6-tenancy`, 커밋 3건, PR 미생성) |
| Phase 7 확장·성능 실험 | 미착수 |

핵심 성과는 다음과 같다.

- P1 장애 시나리오(TC-02/03/04/05/07/16)를 자동 테스트로 재현했고, 판정은 대조 도구(`tools/reconcile`)로 통일했다.
- 테스트가 실제 결함 두 건(종료 시 구독 해제로 인한 유실, 재연결 직후 재전달 메시지 유실)과 테넌트 격리 우회 구멍 한 건을 발견해 수정했다.
- 설계 결정 9건을 ADR로 남겼고, 수치에는 측정 조건을 함께 적었다.

## 2. 브랜치 구조와 진행 흐름

`master`에서 Phase별 브랜치를 따고, 완료 시 `dev`로 PR을 보내 병합하는 방식으로 진행했다. `phase-1` ~ `phase-5`는 모두 `dev`에 병합되었다.
`master`에는 아직 Phase 1 이후 작업이 반영되지 않았다(`master` = `6ed192c`).

| 브랜치 | 역할 | 상태 | 커밋 수 | 대표 커밋 |
| --- | --- | --- | --- | --- |
| `master` | 초기 구현 + 센서 health·Grafana·Logto 선행 구현 | 기준선 | 10 | `6ed192c` |
| `docs/claude-md-roadmap` | `CLAUDE.md` 로드맵 문서 | 원격에만 존재, 미병합 | 2 | `3910690` |
| `phase-1-ingest-integrity` | 수동 ack·재시도·격리 | dev 병합 (PR #1) | 5 | `7fbbbc1` |
| `phase-2-observability` | 지표·로그·reconcile·README | dev 병합 (PR #2) | 5 | `7fdcf60` |
| `phase-3-tests` | 단위·통합·e2e·CI | dev 병합 (PR #3) | 5 | `f2c4149` |
| `phase-4-emulator` | 물리 모델·시나리오·장애 주입 | dev 병합 (PR #4) | 1 | `efe2217` |
| `phase-5-health-alerts` | faulty 상태·복원·경보 엔진 | dev 병합 (PR #5) | 1 | `08c92bc` |
| `phase-6-tenancy` | 테넌트 격리·`tenant_id` 재도입 | 진행 중 | 3 | `f1910ac` |
| `dev` | 통합 브랜치 | PR #5까지 | — | `feb3fe8` |

참고: `docs/claude-md-roadmap`은 병합되지 않았고, 현재 `CLAUDE.md`는 `.gitignore`에 포함되어 있다(커밋 `1186571`). 원격 로드맵 브랜치와 로컬 `CLAUDE.md`의 동기화 방침을 정할 필요가 있다.

## 3. 시기별 커밋 기록

### 3.1 초기 구현 (10/10 ~ 10/22, master)

| 날짜 | 커밋 | 내용 |
| --- | --- | --- |
| 10/10 | `931b3fe` | 초기 커밋 |
| 10/11 | `db41ea9` | DB·MQTT 워커 정상화, migration 추가, pgdb → TimescaleDB 이미지 교체 |
| 10/11 | `1a226e1` | `sensor_data` bulk insert, AutoMigrate 제거, 300건 또는 2초 flush, `tenant_id` 제거(migration 000002) |
| 10/12 | `e8be944` | FastAPI + aiomqtt 에뮬레이터, compose 연동, 개발 시드 |
| 10/22 | `0244289` | 센서 health·집계 테이블(000003~000005), tenant 격리 뷰, 프로비저닝 스크립트 |
| 10/22 | `c08a311` | HealthTracker/HealthWorker, 센서별 LWT 에뮬레이터 |
| 10/22 | `1186571` | Grafana 대시보드, Logto(OIDC) 연동, 센서 컨테이너 profile |
| 10/22 | `addee04`, `3910690` | `CLAUDE.md` 로드맵 추가·진행 체크 (별도 브랜치) |

### 3.2 Phase 1 — 수집 경로 정합성 (10/23, PR #1)

- `d789ad3` 격리 테이블 `sensor_data_rejected` 마이그레이션
- `6a331dc` Mosquitto `max_inflight_messages 600`, `max_queued_messages 60000`, 로그 레벨 축소
- `eea337e` **수동 ack(배치 커밋 후 일괄)**, 지속 세션, 무한 지수 백오프 재시도 + 백프레셔, SQLSTATE 22/23 이분법 격리, 파싱 실패 원문 격리, 종료 순서 정리
- `1a764b4` ADR 6건 (ack 시점, 재시도·백프레셔, 오류 분류, inflight/queue, gorm 유지, `tenant_id` 제거 이력)
- `7fbbbc1` TC-02/03/04/07 수동 실험, 브로커 큐 기본값 실측

### 3.3 Phase 2 — 측정 가능성 (10/23, PR #2)

- `521d532` Prometheus 지표(`sensor_id` 라벨 미사용)와 slog JSON 로그, 모듈명 변경
- `2522776` `.idea/` 추적 제거
- `53d55e7` compose `migrate`/`seed`/`prometheus`, `000001` down
- `e026133` Grafana 수집 경로 대시보드
- `688d888` `tools/reconcile` (누락·예상 외·중복·격리를 센서별로 출력, 문제 시 종료 코드 1)
- `7fdcf60` README, 에뮬레이터 README, `.env.example`

### 3.4 Phase 3 — 자동 테스트 (10/23, PR #3)

- `a8f13be` `Clock` 주입 단위 테스트(flush 주기·백오프 수열을 실제 대기 없이 검증)
- `b79d459`, `0fd9fe2` 워커 조립(`internal/app`)과 대조 로직(`internal/reconcile`) 라이브러리화
- `e5bd550` testcontainers 통합 테스트(TC-01/05/07/16). **TC-16이 정상 종료 시 Unsubscribe가 지속 세션 구독을 지워 앱 중단 중 발행분이 유실되는 결함을 재현했다(센서당 700건 중 307건 누락).** 구독을 유지하도록 수정(ADR-0007)
- `f2c4149` e2e 장애 주입(TC-02/03/04/06)과 CI. **paho가 라우트 없는 메시지를 ack 없이 버려 CONNACK 직후 재전달분이 inflight로 멈추는 결함**을 `AddRoute` 선등록으로 수정, 전체 스위트 12회 연속 통과

### 3.5 Phase 4 — 에뮬레이터 (10/23, PR #4)

- `efe2217` 온도 사인파·민감도·추세 물리 모델, JSON 시나리오와 시드 결정론, seq 영속화, 발행 로그(JSONL)·정답 파일, 장애 주입(중복·깨진 JSON·범위 밖·침묵·flatline·포맷 B), 브로커 단절 시 발행 측 버퍼링, 제어 API. 단위 테스트 26개, 실스택 실험 기록

### 3.6 Phase 5 — 센서 상태와 경보 (10/23, PR #5)

- `08c92bc` faulty 상태(flatline·범위 밖 연속), 타임아웃 계수 k·시작 유예·`sensor_status` 스냅샷 복원, 전체 두절 시 `ingest_event(stalled)` 한 건, 수신 핸들러 메모리 경보 엔진(임계치·변화율, edge trigger), `(rule_id, sensor_id, seq)` 중복 제거, 조건부 UPDATE 상태 전이, migration 000007, ADR-0008, 통합 테스트(TC-08/12/13, 재시작)

### 3.7 Phase 6 — 멀티테넌시 (10/24, 진행 중)

- `da16568` `tenant_id` 재도입 비용 측정(100만 행, 센서 500)
- `11491cf` RLS 호환성·오버헤드 측정
- `f1910ac` role 매핑 기반 격리, `sensor_data.tenant_id` 재도입, migration 000008, `provision_tenant.sh`, TC-09(DB 계층) 통합 테스트, ADR-0009

## 4. 설계 결정 (ADR 9건)

| ADR | 결정 | 비고 |
| --- | --- | --- |
| 0001 | 수동 ack, 배치 커밋 뒤 일괄 ack | 불변식 1 |
| 0002 | 일시적 오류는 무한 지수 백오프, 채널 포화 시 수신 정지 | 불변식 2 |
| 0003 | SQLSTATE 22/23은 영구 오류, 이분법으로 문제 행만 격리 | 불변식 3. 범위 밖 값 검증은 미결 |
| 0004 | inflight 600, queue 60000 | 기본값 실측 포함 |
| 0005 | gorm 유지, pgx 오류 코드로 분류 | |
| 0006 | `sensor_data.tenant_id` 제거 이력 | 0009로 대체 |
| 0007 | 정상 종료 시 구독을 해제하지 않는다 | TC-16이 발견 |
| 0008 | 센서 상태·경보를 수신 시점 메모리에서 평가, edge trigger | 임계값은 구현자 기본값, 4~7항 검토 필요 |
| 0009 | role→테넌트 매핑 뷰 + `tenant_id` 재도입 | 트리거 채움은 구현자 선택, 검토 필요 |

## 5. 측정 결과 (README 기준)

환경은 WSL2 단일 호스트(Docker 29.8.0), TimescaleDB pg18, Mosquitto 2, 배치 300건·flush 2초(통합 테스트는 200ms)이다. 모두 1회 실행 결과다.

| TC | 판정 | 요약 |
| --- | --- | --- |
| TC-01 정상 수집 | 통과 | 10,000건 저장, 누락·중복·예상 외 0 |
| TC-02 DB 10초 중단 | 통과 | 500 msg/s, 누락·중복 0, `channel_depth` 최대 317, 힙 4→7MiB |
| TC-03 브로커 10초 중지 | 통과 | 누락·중복 0. 발행기가 PUBACK 받은 것만 기록해 수집 측 유실 0 입증 (Phase 1의 1건 누락은 발행 측 유실) |
| TC-04 SIGKILL | 통과 | 500 msg/s, 재시작 후 누락·중복 0 |
| TC-05 3회 중복 | 통과 | DB 1건, `dedup_total` +2 |
| TC-06 1,000 msg/s + DB 지연 | **부분 통과** | 30초만 측정(999 msg/s, 누락·중복 0, 힙 4→9MiB). 5분 조건 미측정 |
| TC-07 형식 오류·FK 위반 | **부분 통과** | 깨진 JSON·FK 격리 확인. 범위 밖 값은 제약이 없어 격리되지 않음 |
| TC-08 경보 정확성 | **부분 통과** | 중복 방지와 지연 0.003초 확인, 표본 2건이라 p99 불가, 부하 지연 미측정 |
| TC-09 테넌트 격리 | **부분 통과** | DB 계층만 시험. 우회 구멍 발견·수정. Grafana·API 경로 미시험 |
| TC-12 통신 두절 | 통과 | 침묵 센서만 offline, 전체 두절은 `stalled` 한 건 |
| TC-13 상태 전이 | 통과 | flatline → faulty → 복구, 경보 상태 전이 20 경쟁에서 1건 성공 |
| TC-16 정상 종료 | 통과 | 센서당 700건 모두 저장 |

수집 지연은 약 3 msg/s에서 `ingest_lag_seconds` p99 약 2.9초이며 flush 주기 2초의 영향이다. 이 수치는 처리량 주장이 아니다.

### Phase 6 실험 (2026-10-24, 100만 행, 센서 500, 1회 실행)

| 항목 | 결과 |
| --- | --- |
| `tenant_id` 재도입 저장 공간 | 합계 +약 4% (182.0 vs 175.4 MiB) |
| 쓰기 처리량(재도입 vs 조인 방식) | 22,661 vs 23,264 rows/s, 노이즈 범위 |
| 테넌트 전체 7일 행 수 조회 | 4.42 ms vs 26.48 ms (6배 빠름). 센서 1개 조회는 동급 |
| RLS + 압축/연속 집계 | **호환 불가** (세 가지 오류 메시지로 확인) |
| RLS 서브쿼리형 조회 | 테넌트 7일 행 수 118.7 ms (뷰 방식 4.8 ms 대비 24배) |
| `tenant_id` 채움 트리거 오버헤드 | 15,754 vs 24,435 rows/s, **약 55%** (목표 1,000 msg/s의 약 15배 여유) |

## 6. 주요 발견과 교훈

1. **테스트가 설계 결함을 찾았다.** 정상 종료 시 Unsubscribe 유실, paho 라우트 미등록으로 인한 재전달 유실은 수동 실험으로는 드러나지 않았고 통합 테스트(TC-16)에서만 재현됐다.
2. **"누락 0" 주장은 발행 측 로그가 있어야 성립한다.** Phase 1의 TC-03은 발행 측 유실 1건으로 입증이 불완전했고, Phase 4의 PUBACK 후 기록 방식 발행 로그로 해소했다.
3. **세션 설정 기반 테넌트 격리는 뚫린다.** `SET app.tenant_id`로 다른 테넌트가 보였다. `session_user` 기반 매핑으로 교체해 fail closed가 되도록 고쳤다.
4. **RLS는 이 스택에서 선택지가 아니다.** TimescaleDB의 압축·연속 집계와 함께 쓸 수 없다는 점을 실험으로 확인해 뷰 방식을 채택했다.
5. 프로세스 측면에서 CLAUDE.md의 "핵심 결정은 묻고, 보일러플레이트는 바로 쓴다" 원칙에 따라 ADR 단위로 결정을 기록했다.

## 7. 알려진 한계와 미결 사항

- **TC-06**: 5분 조건과 적체가 `max_queued_messages`를 넘는 조건 미측정. 백프레셔·keepalive 상호작용 미확인.
- **TC-07**: 범위 밖 값·시간 0 payload 검증 미결(ADR-0003).
- **TC-08**: 부하 상태의 경보 지연 미측정. p99를 낼 표본 부족.
- **TC-09**: Grafana·API 경로 미시험. Prometheus 데이터소스가 전 테넌트 합산 지표를 기본 org에 노출하는 문제를 정리해야 한다.
- **ADR-0008 / 0009 검토 요청**: 센서 상태 임계값(정상 범위·flatline 횟수)이 센서 종류 무관 전역 기본값이다. `tenant_id` 채움을 트리거로 한 것은 구현자 선택이며 오버헤드가 55%다.
- **스키마 제약**: FK가 `ON UPDATE`를 막아 데이터가 있는 센서의 테넌트 이전 절차가 없다. 연속 집계 `sensor_data_1m`에는 `tenant_id`가 없어 조인을 유지한다.
- DB 중단 중 센서 상태 불변은 단위 테스트만 있고 e2e는 없다.
- 시나리오는 JSON만 지원하며(YAML 제외), FastAPI 데모 경로는 발행 측 버퍼링이 없다.
- 백필 시간은 대용량 데이터로 측정하지 않았다. 모든 실험은 1회 실행이다.


## 8. 남은 작업

### Phase 6 잔여

- [ ] Logto OIDC → Grafana 로그인, 역할(관리자·운영자) 매핑 검증
- [ ] Grafana 테넌트 org별 DB 역할 격리 실제 경로 검증, Prometheus 데이터소스 노출 정리
- [ ] 경보 규칙 등록·상태 변경 최소 API(Swagger), 조회는 DB 기준
- [ ] 관제 대시보드(시설물별 상태, 시계열, 경보 목록, 센서 상태)
- [ ] TC-09 전 경로 완성, TC-17(테넌트 폭주)
- [ ] `phase-6-tenancy` → `dev` PR 생성

### Phase 7

- [ ] inflight × 배치 크기 × 커밋 시간, 배치·flush별 처리량·지연, multi-row INSERT vs COPY
- [ ] 연속 집계·압축·보존 정책 전후 비교(TC-19), 이때 트리거 오버헤드 재측정
- [ ] 수평 확장(TC-18, 공유 구독 1→2→4→8), TC-10(30분), TC-11(2시간 이상)
- [ ] (선택) Kafka v2 비교

### 권장 선행 정리

1. 미측정 항목(TC-06 5분, 부하 경보 지연)을 Phase 7 시작 전에 한 번 측정해 README 표의 "부분 통과"를 줄인다.
2. ADR-0008·0009의 구현자 선택 항목을 검토해 확정한다.


## 9. 코드 규모

- `master` 대비 `phase-6-tenancy`: 92개 파일 변경, +8,792 / −276 줄
- 테스트: `test/integration`(TC-01/05/07/16, 경보, TC-09), `test/e2e`(TC-02/03/04/06), `test/experiments`(tenant_id, RLS, 트리거 오버헤드), 에뮬레이터 단위 테스트 26개 외 Go 단위 테스트
- 문서: ADR 9건, 실험 기록 4건, 확인 노트 2건, README 2종
