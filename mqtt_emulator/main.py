
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


MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))

WORKERS = 32
QUEUE_CAPACITY = 5000


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


class Message:
    def __init__(self, topic: str, payload: bytes, due: float):
        self.topic = topic
        self.payload = payload
        self.due = due


config = PublisherConfig()
config_version = 0
config_lock = asyncio.Lock()

queue: asyncio.Queue[Message] = asyncio.Queue(
    maxsize=QUEUE_CAPACITY
)

stop_event: asyncio.Event | None = None

# 센서별 sequence는 설정 변경 이후에도 유지합니다.
sequences: dict[int, int] = {}

metrics = {
    "publish_completed": 0,
    "publish_errors": 0,
    "dropped_queue_full": 0,
    "schedule_lag_total_ms": 0.0,
    "publish_latency_total_ms": 0.0,
    "publish_latency_max_ms": 0.0,
}

started_at = 0.0


def generate_reading(sensor: SensorSpec) -> dict:
    """DB의 sensor_data 컬럼에 대응하는 측정값을 생성합니다."""
    result = {
        "sensor_id": sensor.id,
        "seq": sequences.get(sensor.id, 0),
        "time": datetime.now(timezone.utc).isoformat(),
        "tilt_x": None,
        "tilt_y": None,
        "crack_width": None,
        "temperature": None,
    }

    if sensor.sensor_type == "tilt":
        result["tilt_x"] = round(random.uniform(-1.0, 1.0), 6)
        result["tilt_y"] = round(random.uniform(-1.0, 1.0), 6)

    elif sensor.sensor_type == "crack":
        result["crack_width"] = round(
            random.uniform(0.0, 5.0), 4
        )

    elif sensor.sensor_type == "env":
        result["temperature"] = round(
            random.uniform(-10.0, 40.0), 2
        )

    return result


async def scheduler():
    local_version = -1
    heap = []

    while not stop_event.is_set():
        async with config_lock:
            cfg = config.model_copy(deep=True)
            version = config_version

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

        payload = generate_reading(sensor)

        msg = Message(
            topic=f"{cfg.topic_prefix}/{sensor.sensor_type}/{sensor.id}",
            payload=json.dumps(
                payload, separators=(",", ":")
            ).encode("utf-8"),
            due=due,
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

            metrics["publish_completed"] += 1
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
        except Exception:
            metrics["publish_errors"] += 1
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
        task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            pass


app = FastAPI(lifespan=lifespan)


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

    async with config_lock:
        config = new_config
        config_version += 1

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
