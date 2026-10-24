# Phase 6 실험: RLS로 테넌트 격리를 강제할 수 있는가 (결정 1의 B 검토)

- 목적: tenant 격리를 뷰가 아니라 `sensor_data`의 RLS로 강제하는 방안(B)의 호환성·오버헤드 확인
- 코드: `test/experiments/rls_test.go` (`go test -tags experiment -run RLS -v ./test/experiments`)
- 실행일: 2026-10-24, **1회 실행**, 환경은 [phase6-tenant-id.md](phase6-tenant-id.md)와 같다 (WSL2, Docker 29.8.0, TimescaleDB `latest-pg18` 컨테이너)
- 데이터: 테넌트 20 × 센서 25 = 500, 센서당 2,016행, 총 1,008,000행/변형, chunk 1일

## 변형

모든 변형에서 테넌트는 `session_user → tenant_role` 매핑으로 정한다 (`SECURITY DEFINER`, `STABLE`). 세션 설정(`app.tenant_id`)을 읽지 않는다.

| 변형 | 구성 |
| --- | --- |
| V (결정 1의 A) | 현재 스키마 + `security_barrier` 뷰 + `sensor` 조인 |
| R1 (결정 1의 B) | 현재 스키마 + `sensor_data` RLS. 정책은 `sensor`를 서브쿼리하고 `sensor` 자체에도 RLS |
| R2 (B + `tenant_id` 재도입) | `tenant_id` 컬럼 + `sensor_data` RLS. 정책은 컬럼 비교 |

## 결과

### 1. 격리 정확성 (테넌트 5 계정)
V, R1, R2 모두 50,400행(25센서)만 보인다. 다른 테넌트의 센서 id를 지정해도 0행. `SET app.tenant_id` 우회는 효과가 없고(정책이 세션 설정을 읽지 않음), `SET ROLE`·`tenant_role` 직접 조회·쓰기는 권한 오류. **매핑 방식이면 뷰(V)도 RLS도 우회되지 않는다.**

### 2. RLS와 압축(columnstore) — 호환되지 않음
| 시도 | 결과 |
| --- | --- |
| 압축을 켠 하이퍼테이블에 `ENABLE ROW LEVEL SECURITY` | `operation not supported on hypertables that have columnstore enabled` |
| RLS를 켠 하이퍼테이블에 압축 설정 | `columnstore cannot be used on table with row security` |

현재 migration 000004는 `sensor_data` 압축(3일 지난 chunk)을 켠다. 순서와 무관하게 둘은 한 테이블에서 함께 쓸 수 없다.

### 3. 연속 집계 — 호환되지 않음
RLS가 걸린 하이퍼테이블 위에 연속 집계를 만들 수 없다: `cannot create continuous aggregate on hypertable with row security`. 현재 `sensor_data_1m`(migration 000004)과 `v_sensor_data_1m` 뷰가 의존하는 구조다.

### 4. 조회 오버헤드 (압축 없음, rls_t5 연결, 쿼리당 20회 중앙값)
| 쿼리 | V 뷰+조인 | R1 RLS(서브쿼리) | R2 RLS(컬럼) |
| --- | --- | --- | --- |
| Q1 센서 1개 최근 1시간 | 0.40 ms | 0.51 ms | 0.38 ms |
| Q2 테넌트 24시간 5분 평균 | 7.73 ms | 18.63 ms | 8.27 ms |
| Q3 테넌트 7일 행 수 | 4.81 ms | 118.71 ms | 11.55 ms |
| Q4 센서 1개 7일 일평균 | 2.39 ms | 2.07 ms | 1.21 ms |

### 5. 쓰기 오버헤드 (비소유자 워커 role, 300행 × 200배치 = 60,000행, 3회 중앙값)
V 20,965 rows/s, R1 21,667 rows/s, R2 19,186 rows/s. 정책이 `FOR INSERT ... WITH CHECK (true)`라 RLS의 쓰기 검사는 사실상 비용이 없고, 차이는 1회 실행의 노이즈 범위다(R2는 `tenant_id` 컬럼 추가분).

## 해석

- **이점(RLS가 주는 것):** 격리가 테이블에 붙어 있어 뷰를 빠뜨린 쿼리에서도 막힌다. 그러나 이 실험에서 매핑 방식의 뷰(V)도 같은 시험을 모두 통과했으므로, 현재 위협 모델(Grafana 쿼리 편집기)에서 RLS만의 추가 방어는 확인하지 못했다.
- **비용(호환성):** RLS는 압축과 연속 집계 둘 다와 호환되지 않는다. 채택하려면 압축 정책 폐기(저장 공간 증가)와 `sensor_data_1m`의 대체가 필요한데, 이는 TC-19(조회 성능)와 Phase 7 압축·집계 실험의 전제를 바꾼다. 연속 집계는 어차피 뷰 또는 별도 접근 계층으로 격리해야 하므로 RLS 하나로 일원화되지도 않는다.
- **성능:** 서브쿼리형 RLS(R1)는 테넌트 전체를 훑는 쿼리에서 크게 느리다(Q3 24배). 컬럼형(R2)은 V와 비슷하지만 `tenant_id` 재도입이 전제다.
- **부수 발견:** V의 매핑 함수를 `(SELECT app_tenant_by_role())`로 감싸자 Q3가 이전 실험의 `app_tenant_id()` 조인 뷰(26.5 ms)보다 빨랐다(4.8 ms). 함수 호출을 한 번만 평가하게 만든 영향으로 추정되며, 같은 조건에서 직접 비교한 측정은 아니다.
- **한계:** 100만 행 단일 규모, 1회 실행, 압축 chunk 위의 RLS 성능은 측정할 수 없었다(호환 불가). 다중 연결·동시 부하는 측정하지 않았다.
