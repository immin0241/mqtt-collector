# Phase 4 실험: 시나리오 실행기 실제 구동 검증

수집 경로(Go 앱 → Mosquitto → TimescaleDB)에 시나리오 실행기(`mqtt_emulator/run_scenario.py`)를 붙여
발행 로그와 DB를 `tools/reconcile` 로 대조했다. (1회 실행, 통계적 반복 아님)

## 조건

- 장비: WSL2, Docker 29.8.0, Go 1.27.0, Python 3.12 (uv)
- 스택: 격리된 compose 프로젝트(`broker1`, `postgres`, `migrate`, `seed`, `app`). Mosquitto `max_inflight_messages 600`, `max_queued_messages 60000`.
  앱 기본값(배치 300, flush 2s, 채널 600). 실행기는 호스트에서 `localhost:1883` 으로 접속.
- 각 실험 전에 `TRUNCATE sensor_data, sensor_data_rejected`. 판정은 `--since`(실행 직전 UTC) 이후 `received_at` 기준.

## 결과

| 실험 | 시나리오 | 결과 |
| --- | --- | --- |
| TC-01 시나리오 실행 | `tc01.json`: 센서 201~210 × 1,000건, rate 500 | 발행 10,000(20.0초), 저장 10,000, 센서별 누락·중복·예상 외 0 |
| 장애 주입 | `faults-demo.json`: 센서 4개 × 600슬롯, rate 200, 중복 5%, 깨진 JSON 1%, 범위 밖 1%, 침묵 1구간, flatline 1구간 | 발행 2,598건. 모든 센서 OK(중복 전송은 DB에서 흡수, pub_dup 22~36). 격리 테이블의 sensor_id·seq 불명 행 17건 = 정답 파일의 malformed 17건. 정답 파일: duplicate 129, flatline 80, malformed 17, out_of_range 23, silence 1 |
| 브로커 단절 | 센서 3 × 1,500, rate 300, 실행 5초 뒤 브로커 6초 중지 후 재시작 | 생성 4,500 = 확인 4,500, 재접속 8회, 최대 버퍼 2,403건, 발행 측 유실(`publisher_dropped`) 0, 저장 4,500, 누락·중복 0 |
| 실행기 SIGKILL 후 재시작 | 센서 3 × 900, rate 300, 4초 뒤 `kill -9`, 같은 `--state`·`--append` 로 재시작 | 상태 `{201:334, 202:333, 203:333}` 에서 이어서 발행. 최종 센서당 1,233~1,234건 저장, 누락·중복 0. 죽기 직전 구간은 상태 저장 전에 죽어 다시 발행됐고 DB 중복 키가 흡수(pub_dup 57~58) |

## 해석

- 브로커 단절 실험에서 대조의 "누락 0"은 수집 측 정합성을 뜻한다. 실행기는 PUBACK 을 받은 전송만 로그에 올리고, 못 받은 것은 버퍼에서 재전송한다.
  Phase 1 의 TC-03 에서 남았던 "발행 측 유실 1건" 모호함이 해소된다.
- 같은 슬롯을 다시 만들어도 시각·값이 `(seed, sensor_id, seq)` 로만 정해져 payload 가 같으므로 재발행은 중복 키로 안전하게 흡수된다.
- 한계: 실행기는 PUBACK 을 순차로 기다리므로 rate 500 이상은 측정하지 않았다. 대량 부하(TC-06)는 Go 테스트의 발행기를 쓴다.
