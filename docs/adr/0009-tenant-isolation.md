# ADR-0009: 테넌트 격리 — role 매핑 뷰 + sensor_data.tenant_id 재도입

- 상태: 채택 (2026-10-24). 결정 1·2는 사용자 결정, "채우는 방식(트리거)"은 구현자 선택이며 검토 대상이다.
- 관련: SER-003, SFR-010, TC-09, TC-17, ADR-0006(대체), migration 000008
- 근거 실험: [phase6-tenant-id.md](../experiments/phase6-tenant-id.md), [phase6-rls.md](../experiments/phase6-rls.md)

## 배경

Phase 6 시작 시점에 TC-09(DB 계층)를 시험했더니 기존 구성에 구멍이 있었다.
tenant 조회 role(`grafana_tenant_<id>`)은 `ALTER ROLE ... SET app.tenant_id`로 테넌트를 고정했고 뷰가 이 세션 설정을 읽었다.
그런데 `app.*` 설정은 role 본인이 바꿀 수 있어서, Grafana 쿼리 편집기에서 `SET app.tenant_id = '<다른 테넌트>'` 한 줄이면 다른 테넌트 데이터가 보였다.

## 결정

1. **격리 방식(결정 1): 뷰 + role→테넌트 매핑 테이블 (A).**
   - `tenant_role(role_name, tenant_id)`을 만들고 `app_tenant_by_role()`(`SECURITY DEFINER`, `session_user` 기준)이 테넌트를 돌려준다.
   - `session_user`는 `SET`/`SET ROLE`로 바꿀 수 없다. 매핑이 없는 role은 NULL이라 아무것도 보이지 않는다(fail closed).
   - 뷰는 `WHERE tenant_id = (SELECT app_tenant_by_role())`. 함수를 감싼 서브쿼리는 쿼리당 한 번만 평가된다.
2. **`sensor_data.tenant_id` 재도입(결정 2).** 복합 FK `(sensor_id, tenant_id) → sensor(id, tenant_id)`와 인덱스 `(tenant_id, sensor_id, time DESC)`를 되살린다. `v_sensor_data`는 조인 없이 `tenant_id`로 거른다.
3. **`tenant_id`를 채우는 방식(구현자 선택): BEFORE INSERT 트리거.** 수집기 payload에는 테넌트가 없으므로 DB가 `sensor`에서 채운다.
   없는 센서는 예전 단일 FK와 같은 `23503`을 던져 영구 오류로 분류돼 격리 테이블로 간다(TC-07 유지). 호출자가 준 `tenant_id`는 무시한다.

## 선택하지 않은 안

| 안 | 이유 |
| --- | --- |
| RLS(결정 1의 B) | 압축(columnstore)과 연속 집계 둘 다와 호환되지 않는다(`ENABLE ROW LEVEL SECURITY` 거부, 압축 설정 거부, RLS 테이블 위 연속 집계 생성 거부). 같은 위협 모델에서 뷰+매핑이 같은 시험을 모두 통과해 추가 방어도 확인하지 못했다. 서브쿼리형 RLS는 테넌트 전체 스캔이 24배 느렸다. |
| 연결 계층 고정(프록시) | 구성 요소가 늘고 DB 자체는 우회에 열려 있다. |
| `tenant_id`를 앱이 채움 | 수집기가 센서→테넌트 캐시를 들고 있어야 하고, 갓 등록된 센서의 캐시 부재 처리가 필요하다. 아래 트리거 비용이 문제가 되면 재검토. |

## 측정한 결과 (1회 실행, 조건은 근거 실험 문서)

- 저장 공간 +약 4%(합계), 쓰기 처리량 차이는 노이즈 범위, 테넌트 전체를 훑는 조회가 조인 대비 빠르다(7일 행 수 기준 6배, 센서 하나를 짚는 조회는 같은 수준).
- 트리거 오버헤드: 센서 500, 회당 60,000행, 300행 multi-row INSERT, 단일 연결 superuser 5회 중앙값 기준 **트리거 on 15,754 rows/s vs off 24,435 rows/s (약 55%)**.
  목표 처리량(1,000 msg/s)의 약 15배 여유가 있어 당장 병목은 아니나 비용은 있다.
  Phase 7 처리량 실험에서 다시 측정하고, 병목이면 앱 측 채움(위 표 3번째 안)으로 바꾼다.

## 결과와 한계

- `app_tenant_id()`(GUC 기반)는 제거했다. `scripts/provision_tenant.sh`는 `tenant_role`을 upsert하고 예전 role의 `app.tenant_id` 설정을 해제한다. 기존 `grafana_tenant_<id>` role은 migration이 매핑으로 옮긴다.
- 연속 집계 `sensor_data_1m`에는 `tenant_id`가 없어 `v_sensor_data_1m`은 `sensor` 조인을 유지한다.
- FK가 `ON UPDATE`를 막으므로 데이터가 있는 센서의 테넌트는 바꿀 수 없다(이전이 필요하면 별도 절차가 필요하다).
- 이 격리는 DB role 경로(Grafana)에 대한 것이다. API 경로의 인가는 별도다(Phase 6 API 단계).
- 압축된 chunk가 있는 상태에서 백필이 되는지 통합 테스트로 확인했다(`TestMigration000008BackfillOnCompressedChunks`). 대용량 운영 데이터의 백필 시간은 측정하지 않았다.
