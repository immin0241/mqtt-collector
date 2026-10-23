# mqtt_emulator

센서(기울기·균열·환경)를 흉내 내 MQTT로 측정값을 발행하는 Python(FastAPI) 에뮬레이터다. 수집기 시험용이다.

> 현재 값은 발행마다 균등 난수다. 물리 모델, 시드 고정, 발행 로그, 장애 주입은 Phase 4 작업이다.

## 실행

루트의 `docker-compose.yml` 로 실행한다. (`emulator` 서비스, 포트 8000)

```bash
docker compose up -d --build emulator
curl -X PUT localhost:8000/config -H 'Content-Type: application/json' -d '{
  "sensors": [{"id":101,"sensor_type":"tilt"},{"id":102,"sensor_type":"crack"},{"id":103,"sensor_type":"env"}],
  "interval_seconds": 1.0, "jitter_fraction": 0.1, "topic_prefix": "sensors", "qos": 1 }'
curl localhost:8000/stats
```

센서 id는 DB의 `sensor` 테이블에 있어야 한다. (`seeds/dev_sensors.sql` 이 101~103을 만든다. 없으면 FK 위반으로 격리된다)

환경 변수: `MQTT_HOST`(기본 localhost), `MQTT_PORT`(기본 1883).

## 모드

`PUT /config` 의 `mode`:

| mode | 설명 |
| --- | --- |
| `shared` (기본) | MQTT 연결 1개로 모든 센서를 발행. 대량 부하용이며 status/LWT 없음 |
| `per_sensor` | 센서마다 연결과 LWT를 가짐. 접속·단절 테스트용 |

센서 1개 = 컨테이너 1개로 띄우려면 `docker compose --profile sensors up -d sensor-101 sensor-102 sensor-103`.
`docker kill` 은 LWT(offline)를, `docker stop` 은 정상 종료를 만든다. 컨테이너 센서는 `run_sensor.py` 를 쓰며
`SENSOR_ID`, `SENSOR_TYPE`, `INTERVAL_SECONDS`, `JITTER_FRACTION`, `QOS`, `TOPIC_PREFIX`, `KEEPALIVE_SECONDS` 를 읽는다.

## API

| 메서드 | 경로 | 설명 |
| --- | --- | --- |
| GET / PUT | `/config` | 발행 설정 조회 / 교체 (`sensors`, `interval_seconds`, `jitter_fraction`, `topic_prefix`, `qos`, `mode`) |
| GET | `/stats` | `publish_completed`, `publish_errors`, `dropped_queue_full`, `queue_depth`, 지연 통계 |
| GET | `/sensors` | 센서 목록과 상태 |
| POST | `/sensors/{id}/pause`, `/resume` | 센서 일시 중지 / 재개 |
| POST | `/sensors/{id}/kill`, `/start` | 호스팅 센서 강제 종료 / 재시작 |
| POST | `/sensors/{id}/interval?seconds=` | 발행 주기 변경 |

## 메시지

토픽 `sensors/{type}/{id}`, QoS 1, JSON:

```json
{"sensor_id": 101, "seq": 42, "time": "2026-10-23T01:02:03.456789+00:00",
 "tilt_x": 0.123456, "tilt_y": -0.5, "crack_width": null, "temperature": null}
```

- `seq` 는 센서별로 0부터 증가하며 DB의 중복 판정 키 `(sensor_id, seq, time)` 에 쓰인다. 에뮬레이터를 재시작하면 0부터 다시 시작한다.
- 센서 타입별로 채워지는 필드: `tilt` → `tilt_x`, `tilt_y` / `crack` → `crack_width`(0~5mm) / `env` → `temperature`(-10~40°C). 나머지는 `null`.
- 상태 토픽 `sensors/{type}/{id}/status` 는 `online` / `offline` (retained, LWT) 이다.

## 알려진 한계

- 브로커가 끊긴 동안 생성된 메시지는 발행되지 않고 `publish_errors` 로만 집계된다. (Phase 4: 발행 측 버퍼링)
- 발행한 seq를 기록하지 않아 `tools/reconcile --published` 에 쓸 입력이 아직 없다. (Phase 4: 발행 로그)
