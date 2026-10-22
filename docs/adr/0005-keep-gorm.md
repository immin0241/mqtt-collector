# ADR-0005: DB 접근 라이브러리로 gorm 유지

- 상태: 채택 (2026-10-23, 사용자 결정 F)

## 선택지

| | 장단점 |
| --- | --- |
| **gorm 유지 (채택)** | 기존 코드와 `HealthWorker`가 이미 gorm을 쓴다. 교체 비용이 없다. |
| pgx 직접 사용 | `CopyFrom` 등 pgx 고유 기능과 오류 접근이 직접적이다. 대신 전면 교체가 필요하다. |

## 결정과 구현상 주의

- gorm 하위의 pgx 드라이버가 반환하는 `*pgconn.PgError`를 `errors.As`로 꺼내 SQLSTATE를 분류한다. 이를 위해 `github.com/jackc/pgx/v5`를 직접 의존성으로 승격했다. (`go.mod`)
- 저장소 접근은 `workers.Store` 인터페이스 뒤에 두어 테스트에서 대체하고, 이후 pgx `COPY`로 바꾸는 Phase 7 실험(INSERT vs COPY)에도 교체 지점이 된다.
- `ON CONFLICT DO NOTHING`은 `clause.OnConflict{DoNothing: true}`로 유지한다. 중복 건수는 영향 행 수로 알 수 있으나 `dedup_total` 지표는 Phase 2에서 다룬다.
