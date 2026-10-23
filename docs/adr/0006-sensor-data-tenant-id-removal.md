# ADR-0006: sensor_data.tenant_id 제거 (마이그레이션 000002)

- 상태: 사후 기록. **제거 이유는 마이그레이션에 기록이 없어 아래를 추정으로 표시했다. 작성자 확인이 필요하다.**
- 재결정 시점: Phase 6 (tenant_id 재도입 여부를 측정 근거와 함께 확정)

## 사실 (마이그레이션 000002가 하는 일)

- `sensor_data.tenant_id` 컬럼과 복합 FK `(sensor_id, tenant_id) → sensor(id, tenant_id)`, 인덱스 `(tenant_id, sensor_id, time DESC)`를 삭제한다.
- 단일 FK `sensor_id → sensor(id)`와 인덱스 `(sensor_id, time DESC)`를 추가한다.
- `sensor.tenant_id`는 남아 있으므로 테넌트는 `sensor` 조인으로 얻는다.

## 추정되는 이유 (확인 필요)

- 에뮬레이터/센서 payload에 `tenant_id`가 없어 수집 시점에 알 수 없다. 센서가 테넌트를 결정하므로 `sensor_data`에 두면 파생 값의 중복 저장이다.
- 행마다 복합 FK 검증과 더 넓은 인덱스를 유지하는 비용을 줄인다.

## 트레이드오프 (Phase 6에서 측정할 항목)

- 제거(현재): 저장 공간과 쓰기 비용이 작다. 테넌트 격리 쿼리/뷰/RLS는 `sensor` 조인이 필요하고, 조회 비용이 늘 수 있다.
- 재도입: 쿼리와 RLS 정책이 단순하고 파티션 단위 격리가 쉽다. 대신 중복 저장, FK 비용, 센서의 테넌트 이전 시 정합성 문제가 생긴다.
