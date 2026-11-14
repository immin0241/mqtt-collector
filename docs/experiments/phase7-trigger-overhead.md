# 트리거 쓰기 오버헤드 재측정 (세로 구조)

- 날짜: 2026-11-14
- 목적: `sensor_data_set_tenant` 트리거(행마다 `sensor`를 조회해 `tenant_id`를 채운다, ADR-0009)의 쓰기 비용을 세로 구조(migration 000011~)에서 다시 잰다. 이전 값 약 55%는 가로 구조에서 잰 것이다([phase6-tenant-id.md](phase6-tenant-id.md)).
- 한계: 5회 반복 중앙값, 실행 1회, 단일 연결(superuser). 처리량 상한 주장이 아니라 on/off 상대 비교다.

## 조건

| 항목 | 값 |
| --- | --- |
| 장비 | AMD Ryzen 5 7500F (6코어 12스레드), RAM 15.8 GB, WSL2 위의 Docker. 클라이언트와 DB가 같은 머신 |
| DB | TimescaleDB 컨테이너, migration 000011~000012 |
| 데이터 | 테넌트 20 × 센서 25 = 센서 500 |
| 쓰기 | 300메시지(메시지당 측정 2행 = 600행) multi-row `INSERT ... ON CONFLICT DO NOTHING` 200회 = 회당 120,000행 |
| 방식 | 트리거 on/off(`ALTER TABLE ... DISABLE/ENABLE TRIGGER`)를 번갈아 5회. off일 때는 `tenant_id`를 직접 넣는다 |
| 코드 | `test/experiments/trigger_overhead_test.go` |

## 결과

| | 처리량 | 중앙값 |
| --- | --- | --- |
| 트리거 on | 11,990 rows/s (약 5,995 msg/s) | 10.008 s |
| 트리거 off | 15,539 rows/s (약 7,770 msg/s) | 7.722 s |

**오버헤드 29.6%** (이전 가로 구조: 약 55%).

## 해석

1. 상대 오버헤드는 줄었다(55% → 약 30%). 다만 **원인은 분리해 측정하지 않았다.** 행당 기본 삽입 비용이 세로 구조에서 커져(off 처리량 24,435 → 15,539 rows/s) 같은 트리거 비용이 상대적으로 작아졌을 가능성이 있다. 행당 트리거 비용을 계산하면 `1/11,990 − 1/15,539` ≈ 19 µs로, 이전(`1/15,754 − 1/24,435` ≈ 23 µs)과 비슷하다(1회 측정이라 차이는 노이즈 범위일 수 있다).
2. **메시지당 비용은 2행분이다.** 세로 구조에서는 메시지 하나가 행 2개이므로 트리거는 메시지당 2번 실행된다. 메시지 기준 처리량은 on 약 5,995 msg/s, off 약 7,770 msg/s다.
3. 목표 1,000 msg/s 대비 여유는 크다(on 기준 약 6배, DB 왕복 지연이 없는 조건). 지연이 있으면 왕복 수가 지배한다([phase7-insert-strategies.md](phase7-insert-strategies.md)).
4. 트리거를 없애는 방법(워커가 `tenant_id`를 직접 넣기)은 스키마·쓰기 경로 변경이라 **결정이 필요하고 이 실험에서는 구현하지 않았다.** 이득은 DB 지연이 없는 조건에서 처리량 약 30%다. 지연이 지배하는 환경에서는 이득이 더 작을 것으로 보이나 측정하지 않았다.

## 재현

```bash
go test -tags experiment -run TriggerOverhead -v ./test/experiments
```
약 1.5분.
