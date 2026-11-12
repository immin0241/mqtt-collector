"""시나리오 파일(JSON)과 결정론적 이벤트 생성.

같은 시나리오·시드·시작 seq 에서는 항상 같은 이벤트(순서, payload, 정답)가 나온다.
시각은 실제 시계가 아니라 시나리오의 start_time + seq × interval_seconds 로 정한 모의 시각이다.

seq 는 "시간 슬롯 번호"다. 침묵 구간에서도 seq 는 흘러가므로(보내지 않을 뿐) seq 에 빈 구간이 생긴다.
대조는 발행 로그 기준이므로 빈 구간은 누락이 아니다.
"""
import json
import random
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Iterator, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

import model as physics

SensorType = Literal["tilt", "crack", "env"]

# 범위 밖 값 (RFP 계측기 사양을 크게 벗어난 값)
OUT_OF_RANGE = {"tilt_x": 45.0, "tilt_y": -45.0, "crack_width": 99.0, "temperature": 120.0}


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class SensorDef(_Strict):
    id: int = Field(gt=0)
    type: SensorType
    tenant_id: int = Field(default=1, gt=0)  # 토픽 sensors/{tenant_id}/{type}/{id}
    model: dict[str, float] = Field(default_factory=dict)  # ModelParams 덮어쓰기


class Window(_Strict):
    sensor_id: int
    from_seq: int = Field(alias="from", ge=0)
    to_seq: int = Field(alias="to", gt=0)  # [from, to)

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    @model_validator(mode="after")
    def _ordered(self):
        if self.to_seq <= self.from_seq:
            raise ValueError("window 'to' must be greater than 'from'")
        return self

    def covers(self, sensor_id: int, seq: int) -> bool:
        return self.sensor_id == sensor_id and self.from_seq <= seq < self.to_seq


class Faults(_Strict):
    duplicate_ratio: float = Field(default=0.0, ge=0, le=1)  # 이벤트를 추가로 다시 보낼 확률
    duplicate_copies: int = Field(default=2, ge=1, le=10)  # 추가 전송 횟수
    malformed_ratio: float = Field(default=0.0, ge=0, le=1)  # 깨진 JSON
    out_of_range_ratio: float = Field(default=0.0, ge=0, le=1)  # 범위 밖 값
    silence: list[Window] = Field(default_factory=list)
    flatline: list[Window] = Field(default_factory=list)
    vendor_b_sensors: list[int] = Field(default_factory=list)  # 제조사 포맷 B


class Scenario(_Strict):
    name: str = "unnamed"
    seed: int = 0
    start_time: datetime = datetime(2026, 1, 1, tzinfo=timezone.utc)
    interval_seconds: float = Field(default=1.0, gt=0)
    count: int = Field(gt=0)  # 센서당 슬롯 수
    topic_prefix: str = "sensors"
    qos: Literal[0, 1] = 1
    rate: float = Field(default=0.0, ge=0)  # 초당 발행 수 (0 이면 제한 없음)
    sensors: list[SensorDef] = Field(min_length=1)
    faults: Faults = Field(default_factory=Faults)

    @model_validator(mode="after")
    def _unique_sensors(self):
        ids = [s.id for s in self.sensors]
        if len(ids) != len(set(ids)):
            raise ValueError("sensor ids must be unique")
        return self


def load_scenario(path: str) -> Scenario:
    with open(path, encoding="utf-8") as f:
        return Scenario.model_validate(json.load(f))


@dataclass
class Event:
    sensor_id: int
    seq: int
    topic: str
    payload: bytes
    # 발행 로그(대조 입력)에 올릴지. 저장되지 않는 것이 정상인 이벤트(깨진 JSON, 포맷 B)는 False.
    loggable: bool = True
    truth: list[dict] = field(default_factory=list)


def _iso(t: float) -> str:
    return datetime.fromtimestamp(t, tz=timezone.utc).isoformat()


def _format_a(sensor_id: int, seq: int, t: float, values: dict) -> dict:
    return {"sensor_id": sensor_id, "seq": seq, "time": _iso(t), **values}


def _format_b(sensor_id: int, seq: int, t: float, values: dict) -> dict:
    """제조사 B: 필드명·구조가 다르다. 수집기는 아직 이 포맷을 해석하지 않는다. (TC-14)"""
    return {
        "id": sensor_id,
        "n": seq,
        "ts": _iso(t),
        "v": {
            "tx": values["tilt_x"],
            "ty": values["tilt_y"],
            "cw": values["crack_width"],
            "temp": values["temperature"],
        },
    }


def _dump(obj: dict) -> bytes:
    return json.dumps(obj, separators=(",", ":")).encode("utf-8")


def _first_present(values: dict) -> str:
    return next(k for k, v in values.items() if v is not None)


def events(sc: Scenario, start_seqs: dict[int, int] | None = None) -> Iterator[Event]:
    """count 개의 슬롯을 센서 순서대로(슬롯 우선) 생성한다."""
    start_seqs = start_seqs or {}
    t0 = sc.start_time.timestamp()
    params = {
        s.id: physics.default_params(sc.seed, s.id).updated(**s.model) if s.model else physics.default_params(sc.seed, s.id)
        for s in sc.sensors
    }
    frozen: dict[tuple[int, int], dict] = {}  # flatline 윈도우의 고정값
    f = sc.faults

    for i in range(sc.count):
        for s in sc.sensors:
            seq = start_seqs.get(s.id, 0) + i
            t = t0 + seq * sc.interval_seconds
            topic = f"{sc.topic_prefix}/{s.tenant_id}/{s.type}/{s.id}"

            silence = next((w for w in f.silence if w.covers(s.id, seq)), None)
            if silence is not None:
                continue  # 침묵 구간의 정답은 truth_windows 가 한 번만 낸다

            values = physics.reading(sc.seed, s.id, s.type, params[s.id], t, t0, seq)
            truth: list[dict] = []

            flat = next((w for w in f.flatline if w.covers(s.id, seq)), None)
            if flat is not None:
                key = (s.id, flat.from_seq)
                if key not in frozen:
                    frozen[key] = physics.reading(
                        sc.seed, s.id, s.type, params[s.id], t0 + flat.from_seq * sc.interval_seconds, t0, flat.from_seq
                    )
                values = dict(frozen[key])
                truth.append({"kind": "flatline", "sensor_id": s.id, "seq": seq})

            rng = random.Random(f"{sc.seed}:{s.id}:{seq}:fault")
            r_range, r_bad, r_dup = rng.random(), rng.random(), rng.random()

            if r_range < f.out_of_range_ratio:
                field_name = _first_present(values)
                values[field_name] = OUT_OF_RANGE[field_name]
                truth.append({"kind": "out_of_range", "sensor_id": s.id, "seq": seq, "field": field_name})

            if s.id in f.vendor_b_sensors:
                truth.append({"kind": "vendor_b", "sensor_id": s.id, "seq": seq})
                yield Event(s.id, seq, topic, _dump(_format_b(s.id, seq, t, values)), loggable=False, truth=truth)
                continue

            payload = _dump(_format_a(s.id, seq, t, values))
            if r_bad < f.malformed_ratio:
                truth.append({"kind": "malformed", "sensor_id": s.id, "seq": seq})
                yield Event(s.id, seq, topic, payload[: len(payload) // 2], loggable=False, truth=truth)
                continue

            copies = 1
            if r_dup < f.duplicate_ratio:
                copies += f.duplicate_copies
                truth.append({"kind": "duplicate", "sensor_id": s.id, "seq": seq, "extra_copies": f.duplicate_copies})

            for c in range(copies):
                # 정답은 첫 전송에만 붙인다.
                yield Event(s.id, seq, topic, payload, loggable=True, truth=truth if c == 0 else [])


def truth_windows(sc: Scenario) -> list[dict]:
    """침묵 구간 정답. 이벤트가 없으므로 이벤트 스트림과 별도로 한 번만 기록한다."""
    return [
        {"kind": "silence", "sensor_id": w.sensor_id, "seq_from": w.from_seq, "seq_to": w.to_seq}
        for w in sc.faults.silence
    ]
