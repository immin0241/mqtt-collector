"""실시간(shared) 모드의 런타임 오류 주입. 시나리오 파일의 faults 와 같은 종류를 대화형으로 켠다.

주입은 "다음 N번 발행"에 적용되며, 적용될 때마다 정답 파일(TRUTH_LOG)에 한 줄 남긴다.
  out_of_range: 값 하나를 범위 밖 값으로 바꾼다 (scenario.OUT_OF_RANGE 와 같은 값)
  flatline:     주입 시작 시점의 측정값을 N번 그대로 반복한다 (seq·시각은 계속 진행)
  malformed:    payload 를 절반만 보낸다. 깨진 JSON 이므로 발행 로그에는 올리지 않는다
  duplicate:    같은 payload 를 추가로 2번 더 보낸다 (발행 로그에는 전송마다 올린다)
"""
import json
from dataclasses import dataclass

from scenario import OUT_OF_RANGE

KINDS = ("out_of_range", "flatline", "malformed", "duplicate")
DUPLICATE_EXTRA_COPIES = 2
MAX_COUNT = 1000
# 센서 상태 판정이 읽는 payload 필드. seq/time 은 건드리지 않는다.
_VALUE_FIELDS = tuple(OUT_OF_RANGE)


@dataclass
class Send:
    payload: bytes
    loggable: bool  # 발행 로그(대조 입력)에 올리는 전송인가


def _dump(payload: dict) -> bytes:
    return json.dumps(payload, separators=(",", ":")).encode("utf-8")


class Injector:
    def __init__(self) -> None:
        self.pending: dict[int, dict[str, int]] = {}
        self._frozen: dict[int, dict] = {}

    def add(self, sensor_id: int, kind: str, count: int) -> None:
        if kind not in KINDS:
            raise ValueError(f"unknown kind {kind!r}, expected one of {KINDS}")
        if not 1 <= count <= MAX_COUNT:
            raise ValueError(f"count must be 1..{MAX_COUNT}")
        self.pending.setdefault(sensor_id, {})
        self.pending[sensor_id][kind] = self.pending[sensor_id].get(kind, 0) + count

    def clear(self, sensor_id: int) -> None:
        self.pending.pop(sensor_id, None)
        self._frozen.pop(sensor_id, None)

    def snapshot(self, sensor_id: int) -> dict[str, int]:
        return dict(self.pending.get(sensor_id, {}))

    def _take(self, sensor_id: int, kind: str) -> bool:
        left = self.pending.get(sensor_id, {}).get(kind, 0)
        if left <= 0:
            return False
        if left == 1:
            del self.pending[sensor_id][kind]
        else:
            self.pending[sensor_id][kind] = left - 1
        return True

    def apply(self, sensor_id: int, reading: dict) -> tuple[list[Send], list[dict]]:
        """reading 하나에 대해 실제로 보낼 전송 목록과 정답 기록을 돌려준다. 주입이 없으면 원본 한 건이다."""
        payload = dict(reading)
        truth: list[dict] = []
        base = {"sensor_id": sensor_id, "seq": reading["seq"]}

        if self._take(sensor_id, "flatline"):
            frozen = self._frozen.setdefault(sensor_id, {k: payload[k] for k in _VALUE_FIELDS if payload.get(k) is not None})
            payload.update(frozen)
            truth.append({"kind": "flatline", **base})
        else:
            self._frozen.pop(sensor_id, None)

        if self._take(sensor_id, "out_of_range"):
            field = next((k for k in _VALUE_FIELDS if payload.get(k) is not None), None)
            if field is not None:
                payload[field] = OUT_OF_RANGE[field]
                truth.append({"kind": "out_of_range", "field": field, **base})

        data = _dump(payload)
        if self._take(sensor_id, "malformed"):
            truth.append({"kind": "malformed", **base})
            return [Send(data[: len(data) // 2], loggable=False)], truth

        sends = [Send(data, loggable=True)]
        if self._take(sensor_id, "duplicate"):
            sends += [Send(data, loggable=True) for _ in range(DUPLICATE_EXTRA_COPIES)]
            truth.append({"kind": "duplicate", "extra_copies": DUPLICATE_EXTRA_COPIES, **base})
        return sends, truth
