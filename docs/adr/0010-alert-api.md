# ADR-0010: 경보 규칙·상태 변경 API — 별도 Go 서비스, JWT(조직 토큰) 인증, API 전용 DB role

- 상태: 채택 (2026-10-25). 결정 3·4는 사용자가 "추천대로"로 확정했다. 그 아래 구현 세부(scope 이름, 읽기 권한, 삭제 미제공)는 구현자 선택이며 검토 대상이다.
- 관련: SFR-007, SFR-010, TC-09, ADR-0008, ADR-0009, `internal/api`, `cmd/api`, `seeds/01_app_api_role.sql`

## 배경

CLAUDE.md Phase 6 은 "경보 규칙 등록·상태 변경용 최소 API(Swagger), 조회는 DB 기준"을 요구한다.
`TransitionAlert`(조건부 UPDATE)와 `alert_rule`/`alert_event` 스키마는 Phase 5 에서 이미 있다.

## 결정

### 1. 별도 Go 서비스 (`cmd/api`)

| 선택지 | 판단 |
| --- | --- |
| A. 수집 바이너리에 내장 | `TransitionAlert` 재사용은 쉽지만 HTTP 트래픽·장애가 수집 프로세스와 같은 경계에 놓인다. 불변식 4(메모리 상한)와도 부딪힌다 |
| **B. 별도 Go 서비스 (채택)** | 같은 모듈이라 `workers.TransitionAlert` 를 그대로 쓰고, 프로세스가 분리돼 API 장애가 수집에 닿지 않는다 |
| C. 별도 Python(FastAPI) | Swagger 가 공짜지만 상태 전이 규칙이 두 코드베이스로 갈라진다 |

Swagger 는 `internal/api/openapi.yaml` 을 직접 쓰고 `/openapi.yaml`, `/docs`(Swagger UI, CDN 에서 버전 고정으로 로드)로 제공한다.
스펙과 구현이 따로 놀 위험이 있다. 현재는 테스트가 응답 코드를 검증하지만 스펙 자체를 검증하지는 않는다.

### 2. 인증: IdP 조직 토큰을 JWKS 로 검증

- 서명은 JWKS(`API_JWKS_URL`)로 검증하고 `iss`(`API_ISSUER`)와 만료를 요구한다. 알고리즘은 비대칭 키로 제한한다(HS256, `none` 거부).
- 테넌트는 토큰의 `aud` 에서 `API_AUDIENCE_PREFIX`(기본 `urn:logto:organization:`) 뒤의 조직 id 로 정하고, `tenant.idp_org_id` 로 찾는다. 요청 본문이나 헤더로 테넌트를 지정하지 못한다(`tenant_id` 필드를 보내면 400).
- 등록되지 않은 조직은 403 이다.

### 3. 권한: scope 로 관리자/운영자 분리

| 동작 | 필요 scope (환경 변수로 변경 가능) |
| --- | --- |
| 규칙 등록·수정 | `alert-rules:write` (관리자) |
| 경보 상태 전이 | `alerts:transition` (운영자) |
| 규칙·경보 조회 | 인증만 (같은 조직) |

두 scope 는 서로를 포함하지 않는다. 관리자가 상태를 바꾸려면 운영자 scope 도 받아야 한다.

### 4. DB 접속: API 전용 role 하나 + 앱 코드의 tenant_id 필터

테넌트마다 role 을 두고 `tenant_role` 매핑을 쓰는 방안(ADR-0009 의 Grafana 방식)도 가능하지만,
API 는 쓰기를 하고 연결 풀을 테넌트별로 늘려야 한다. 그래서 `app_api` 하나로 접속하고 모든 쿼리가 `tenant_id` 로 거른다.

- 이 구조에서 격리는 **코드가 필터를 빼먹지 않는 것**에 달려 있다. 피해 범위를 줄이려고 role 권한을 최소로 줬다:
  `tenant` SELECT, `alert_rule` SELECT/INSERT/UPDATE, `alert_event` SELECT + 상태 컬럼 3개 UPDATE. 센서 데이터·센서 상태·삭제는 불가.
- 타 테넌트 센서로 규칙을 만드는 것은 `alert_rule` 의 `(sensor_id, tenant_id)` 복합 FK 가 막는다(422).
- 타 테넌트의 규칙·경보는 존재하지 않는 것처럼 404 를 돌려준다.

### 5. 상태 전이

요청은 `{"to": "in_progress" | "resolved"}` 만 받고 서버가 직전 상태를 정한다(open→in_progress, in_progress→resolved).
`workers.TransitionAlert` 의 조건부 UPDATE 를 그대로 쓰므로 동시 요청 중 하나만 200, 나머지는 409(`current_status` 포함)다.
`changed_by` 에는 토큰의 `sub` 가 들어간다.

## 구현자 선택 (검토 대상)

- 규칙 삭제는 제공하지 않는다. 경보 이력이 규칙을 FK 로 참조하므로 `enabled=false` 로 끈다.
- 규칙 수정은 이름·임계치·심각도·사용 여부만 허용한다. 의미(kind, metric, 대상)를 바꾸려면 새 규칙을 만든다.
- 새 규칙이 수집 쪽에 반영되기까지 최대 `AlertRefresh`(기본 10초)가 걸린다. 즉시 반영이 필요하면 알림 채널(LISTEN/NOTIFY 등)이 필요하다.

## 검증하지 못한 것

- **Logto 실물과의 연동을 시험하지 않았다.** 테스트는 직접 만든 ES256 키와 JWKS 대신 정적 키 함수를 쓴다.
  Logto 조직 토큰의 `aud`(`urn:logto:organization:<id>`)와 `scope` 클레임 형식, 발급자 URL(`<ENDPOINT>/oidc`), 조직 역할을 scope 로 내려 주는 설정은
  문서 기억에 의존한 가정이다. 실제 Logto 에서 토큰을 받아 확인하고 `docs/notes/` 에 남겨야 한다.
- TLS, 속도 제한, CORS 는 다루지 않았다. 앞단 프록시를 가정한다.
- 경보 조회의 부하(대량 경보)와 페이지네이션은 `limit` 만 있고 커서가 없다.
