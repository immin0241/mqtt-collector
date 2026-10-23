
import asyncio
import heapq
import json
import os
import random
import time

from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Literal

import aiomqtt
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

import model as physics
from seqstore import SeqStore
from sensor import Sensor, build_reading, command_topic


MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
SEED = int(os.getenv("SEED", "0"))
# 선택: seq 를 파일에 보존해 재시작 후에도 이어서 발행한다 / PUBACK 받은 전송을 대조용 로그로 남긴다.
SEQ_STATE = os.getenv("SEQ_STATE") or None
PUBLISH_LOG = os.getenv("PUBLISH_LOG") or None

WORKERS = 32
QUEUE_CAPACITY = 5000
MAX_PER_SENSOR = 2000


class SensorSpec(BaseModel):
    id: int = Field(gt=0)
    sensor_type: Literal["tilt", "crack", "env"]


class PublisherConfig(BaseModel):
    sensors: list[SensorSpec] = Field(
        default_factory=list,
        max_length=100_000,
    )
    interval_seconds: float = Field(
        default=1.0, gt=0, le=86400
    )
    jitter_fraction: float = Field(
        default=0.1, ge=0, le=0.5
    )
    topic_prefix: str = "sensors"
    qos: Literal[0, 1] = 1
    # shared: 하나의 MQTT 연결로 모든 센서를 발행 (대량 부하용, status/LWT 없음)
    # per_sensor: 센서마다 MQTT 연결과 LWT를 가짐 (접속/단절 테스트용)
    mode: Literal["shared", "per_sensor"] = "shared"


class Message:
    def __init__(self, topic: str, payload: bytes, due: float, sensor_id: int = 0, seq: int = 0):
        self.topic = topic
        self.payload = payload
        self.due = due
        self.sensor_id = sensor_id
        self.seq = seq


config = PublisherConfig()
config_version = 0
config_lock = asyncio.Lock()

queue: asyncio.Queue[Message] = asyncio.Queue(
    maxsize=QUEUE_CAPACITY
)

stop_event: asyncio.Event | None = None

seq_store = SeqStore(SEQ_STATE)
publish_log = open(PUBLISH_LOG, "a", buffering=1, encoding="utf-8") if PUBLISH_LOG else None

# 센서별 sequence는 설정 변경 이후에도 유지합니다.
sequences: dict[int, int] = seq_store.snapshot()

# shared 모드 데모 제어: 계수 덮어쓰기, 침묵 센서
model_overrides: dict[int, physics.ModelParams] = {}
silenced: set[int] = set()
run_epoch = time.time()  # 진행 추세의 기준 시각

metrics = {
    "publish_completed": 0,
    "publish_errors": 0,
    "last_error": "",
    "dropped_queue_full": 0,
    "schedule_lag_total_ms": 0.0,
    "publish_latency_total_ms": 0.0,
    "publish_latency_max_ms": 0.0,
}

started_at = 0.0

# per_sensor 모드에서 이 프로세스가 호스팅하는 센서들
hosted: dict[int, tuple[Sensor, asyncio.Task]] = {}


def model_params(sensor_id: int) -> physics.ModelParams:
    return model_overrides.get(sensor_id) or physics.default_params(SEED, sensor_id)


def generate_reading(sensor: SensorSpec) -> dict:
    return build_reading(
        sensor.id, sensor.sensor_type, sequences.get(sensor.id, 0), SEED, model_params(sensor.id), run_epoch
    )


async def scheduler():
    local_version = -1
    heap = []

    while not stop_event.is_set():
        async with config_lock:
            cfg = config.model_copy(deep=True)
            version = config_version

        if cfg.mode != "shared":
            cfg.sensors = []

        now = time.monotonic()

        if version != local_version:
            heap.clear()

            # 모든 센서의 첫 발행 시점을 분산합니다.
            for sensor in cfg.sensors:
                sequences.setdefault(sensor.id, 0)

                due = now + random.uniform(
                    0.0, cfg.interval_seconds
                )

                heapq.heappush(heap, (due, sensor.id))

            local_version = version

        if not heap:
            try:
                await asyncio.wait_for(
                    stop_event.wait(), timeout=0.2
                )
            except asyncio.TimeoutError:
                pass
            continue

        due, sensor_id = heap[0]
        delay = due - time.monotonic()

        if delay > 0:
            try:
                await asyncio.wait_for(
                    stop_event.wait(), timeout=delay
                )
                continue
            except asyncio.TimeoutError:
                pass

        if local_version != config_version:
            continue

        heapq.heappop(heap)

        sensor = next(
            (s for s in cfg.sensors if s.id == sensor_id),
            None,
        )

        if sensor is None:
            continue

        if sensor.id not in silenced:
            payload = generate_reading(sensor)

            msg = Message(
                topic=f"{cfg.topic_prefix}/{sensor.sensor_type}/{sensor.id}",
                payload=json.dumps(
                    payload, separators=(",", ":")
                ).encode("utf-8"),
                due=due,
                sensor_id=sensor.id,
                seq=payload["seq"],
            )

            try:
                queue.put_nowait(msg)
            except asyncio.QueueFull:
                metrics["dropped_queue_full"] += 1

            sequences[sensor.id] = payload["seq"] + 1

        jitter = random.uniform(
            -cfg.jitter_fraction, cfg.jitter_fraction
        )

        next_due = due + cfg.interval_seconds * (1 + jitter)

        # 과도한 backlog를 한꺼번에 발행하지 않습니다.
        current = time.monotonic()
        if next_due <= current:
            next_due = current + cfg.interval_seconds * (
                    1 + random.uniform(
                -cfg.jitter_fraction, cfg.jitter_fraction
            )
            )

        heapq.heappush(heap, (next_due, sensor_id))


async def publisher_worker(client: aiomqtt.Client):
    while True:
        msg = await queue.get()

        try:
            async with config_lock:
                qos = config.qos

            start = time.monotonic()

            await client.publish(
                msg.topic,
                msg.payload,
                qos=qos,
            )

            end = time.monotonic()

            # PUBACK 을 받은 전송만 발행 로그와 seq 상태에 올린다. (발행 측 유실과 수집 측 유실을 구분하는 근거)
            if publish_log:
                publish_log.write(json.dumps({"sensor_id": msg.sensor_id, "seq": msg.seq}, separators=(",", ":")) + "\n")
            seq_store.acked(msg.sensor_id, msg.seq)

            metrics["publish_completed"] += 1
            if metrics["publish_completed"] % 200 == 0:
                seq_store.save()
            metrics["schedule_lag_total_ms"] += max(
                0.0, (start - msg.due) * 1000
            )

            latency_ms = (end - start) * 1000
            metrics["publish_latency_total_ms"] += latency_ms
            metrics["publish_latency_max_ms"] = max(
                metrics["publish_latency_max_ms"],
                latency_ms,
            )

        except asyncio.CancelledError:
            raise
        except Exception as e:
            metrics["publish_errors"] += 1
            metrics["last_error"] = f"{type(e).__name__}: {e}"

            # 연결 자체가 끊긴 경우에는 에러를 삼키지 않고 올려 run_publisher 가 재접속하게 한다.
            if not client._client.is_connected():
                raise
        finally:
            queue.task_done()


async def run_publisher():
    global started_at

    while True:
        try:
            async with aiomqtt.Client(
                    MQTT_HOST, port=MQTT_PORT
            ) as client:
                started_at = time.monotonic()

                async with asyncio.TaskGroup() as tg:
                    tg.create_task(scheduler())

                    for _ in range(WORKERS):
                        tg.create_task(publisher_worker(client))

        except asyncio.CancelledError:
            raise
        except Exception:
            await asyncio.sleep(2.0)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global stop_event

    stop_event = asyncio.Event()
    task = asyncio.create_task(run_publisher())

    try:
        yield
    finally:
        stop_event.set()
        seq_store.save()
        for sensor, sensor_task in hosted.values():
            sensor.stop(graceful=True)
            sensor_task.cancel()
        task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            pass


app = FastAPI(lifespan=lifespan)


def reconcile_sensors():
    """mode=per_sensor 일 때 설정과 호스팅 중인 Sensor 태스크를 맞춘다. config_lock 안에서 호출."""
    desired = (
        {spec.id: spec for spec in config.sensors}
        if config.mode == "per_sensor"
        else {}
    )

    for sensor_id in list(hosted):
        if sensor_id not in desired:
            sensor, _ = hosted.pop(sensor_id)
            sensor.stop(graceful=True)

    for sensor_id, spec in desired.items():
        existing = hosted.get(sensor_id)
        if existing and not existing[1].done():
            sensor = existing[0]
            sensor.interval = config.interval_seconds
            sensor.jitter = config.jitter_fraction
            sensor.qos = config.qos
            continue

        sensor = Sensor(
            sensor_id=spec.id,
            sensor_type=spec.sensor_type,
            host=MQTT_HOST,
            port=MQTT_PORT,
            interval=config.interval_seconds,
            jitter=config.jitter_fraction,
            qos=config.qos,
            topic_prefix=config.topic_prefix,
        )
        hosted[sensor_id] = (sensor, asyncio.create_task(sensor.run()))


async def send_command(sensor_id: int, payload: str):
    """센서의 cmd 토픽으로 명령을 발행한다. 센서가 컨테이너든 이 프로세스의 태스크든 동일하다."""
    try:
        async with aiomqtt.Client(MQTT_HOST, port=MQTT_PORT) as client:
            await client.publish(command_topic(sensor_id), payload, qos=1)
    except aiomqtt.MqttError as e:
        raise HTTPException(status_code=503, detail=f"broker unavailable: {e}")


@app.get("/sensors")
async def get_sensors():
    """이 프로세스가 호스팅 중인 센서(per_sensor 모드)의 상태."""
    return [
        {
            "id": sensor.id,
            "sensor_type": sensor.type,
            "state": "dead" if task.done() else sensor.state,
            "paused": sensor.paused,
            "interval_seconds": sensor.interval,
            "seq": sensor.seq,
            "published": sensor.published,
        }
        for sensor, task in hosted.values()
    ]


def _is_shared_sensor(sensor_id: int) -> bool:
    return config.mode == "shared" and any(s.id == sensor_id for s in config.sensors)


@app.post("/sensors/{sensor_id}/pause")
async def pause_sensor(sensor_id: int):
    """센서를 침묵시킨다. shared 모드는 발행만 멈추고, per_sensor/컨테이너 센서는 명령 토픽으로 전달한다."""
    if _is_shared_sensor(sensor_id):
        silenced.add(sensor_id)
    else:
        await send_command(sensor_id, "pause")
    return {"sensor_id": sensor_id, "command": "pause"}


@app.post("/sensors/{sensor_id}/resume")
async def resume_sensor(sensor_id: int):
    if _is_shared_sensor(sensor_id):
        silenced.discard(sensor_id)
    else:
        await send_command(sensor_id, "resume")
    return {"sensor_id": sensor_id, "command": "resume"}


@app.get("/sensors/{sensor_id}/model")
async def get_sensor_model(sensor_id: int):
    """shared 모드 센서의 현재 물리 모델 계수."""
    if not _is_shared_sensor(sensor_id):
        raise HTTPException(status_code=404, detail="not a shared-mode sensor")
    return {"sensor_id": sensor_id, "seed": SEED, "params": model_params(sensor_id).to_dict()}


@app.put("/sensors/{sensor_id}/model")
async def set_sensor_model(sensor_id: int, changes: dict[str, float]):
    """계수 일부를 바꾼다. 예: {"crack_trend_per_day": 0.5} 로 균열 진행을 시연한다.
    shared 모드는 즉시 반영하고, per_sensor/컨테이너 센서는 명령 토픽(model:<json>)으로 전달한다."""
    if _is_shared_sensor(sensor_id):
        try:
            model_overrides[sensor_id] = model_params(sensor_id).updated(**changes)
        except ValueError as e:
            raise HTTPException(status_code=422, detail=str(e))
        return {"sensor_id": sensor_id, "params": model_overrides[sensor_id].to_dict()}

    try:
        physics.ModelParams().updated(**changes)  # 보내기 전에 검증한다
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    await send_command(sensor_id, "model:" + json.dumps(changes, separators=(",", ":")))
    return {"sensor_id": sensor_id, "command": "model", "changes": changes}


@app.post("/sensors/{sensor_id}/kill")
async def kill_sensor(
    sensor_id: int, mode: Literal["graceful", "abrupt"] = "graceful"
):
    await send_command(sensor_id, f"kill:{mode}")
    return {"sensor_id": sensor_id, "command": f"kill:{mode}"}


@app.post("/sensors/{sensor_id}/interval")
async def set_sensor_interval(sensor_id: int, seconds: float):
    if not 0 < seconds <= 86400:
        raise HTTPException(status_code=422, detail="seconds out of range")
    await send_command(sensor_id, f"interval:{seconds}")
    return {"sensor_id": sensor_id, "command": f"interval:{seconds}"}


@app.post("/sensors/{sensor_id}/start")
async def start_sensor(sensor_id: int):
    """kill 된 호스팅 센서를 다시 시작한다 (컨테이너 센서는 docker start)."""
    async with config_lock:
        if config.mode != "per_sensor" or sensor_id not in {s.id for s in config.sensors}:
            raise HTTPException(status_code=404, detail="not a hosted per_sensor sensor")
        reconcile_sensors()
    return {"sensor_id": sensor_id, "command": "start"}


@app.get("/config")
async def get_config():
    async with config_lock:
        return config.model_dump()


@app.put("/config")
async def update_config(new_config: PublisherConfig):
    global config, config_version

    ids = [sensor.id for sensor in new_config.sensors]

    if len(ids) != len(set(ids)):
        raise HTTPException(
            status_code=422,
            detail="Sensor IDs must be unique",
        )

    if new_config.mode == "per_sensor" and len(ids) > MAX_PER_SENSOR:
        raise HTTPException(
            status_code=422,
            detail=f"per_sensor mode supports at most {MAX_PER_SENSOR} sensors",
        )

    async with config_lock:
        config = new_config
        config_version += 1
        reconcile_sensors()

    return {
        "version": config_version,
        "config": new_config.model_dump(),
    }


@app.get("/stats")
async def get_stats():
    now = time.monotonic()
    uptime = max(0.001, now - started_at) if started_at else 0

    completed = metrics["publish_completed"]

    return {
        **metrics,
        "uptime_seconds": round(uptime, 2),
        "average_completed_per_second": round(
            completed / uptime, 2
        ) if uptime else 0,
        "queue_depth": queue.qsize(),
        "queue_capacity": QUEUE_CAPACITY,
        "average_publish_latency_ms": round(
            metrics["publish_latency_total_ms"] / completed,
            3,
            ) if completed else 0,
        "average_schedule_lag_ms": round(
            metrics["schedule_lag_total_ms"] / completed,
            3,
            ) if completed else 0,
    }
