"""센서별 다음 seq 를 파일에 보존한다. 재시작 후에도 seq 가 이어져 발행 로그와 DB 를 대조할 수 있다.

기록 시점은 "PUBACK 을 받은 뒤"다. 저장 직전에 죽으면 이미 보낸 슬롯을 다시 만들지만,
시각과 값이 (seed, sensor_id, seq) 로만 정해지므로 같은 payload 가 다시 나가고 DB 의 중복 키가 흡수한다.
"""
import json
import os
import tempfile


class SeqStore:
    def __init__(self, path: str | None):
        self.path = path
        self._next: dict[int, int] = {}
        if path and os.path.exists(path):
            with open(path, encoding="utf-8") as f:
                self._next = {int(k): int(v) for k, v in json.load(f).items()}

    def snapshot(self) -> dict[int, int]:
        return dict(self._next)

    def acked(self, sensor_id: int, seq: int) -> None:
        if seq + 1 > self._next.get(sensor_id, 0):
            self._next[sensor_id] = seq + 1

    def save(self) -> None:
        if not self.path:
            return
        directory = os.path.dirname(os.path.abspath(self.path))
        os.makedirs(directory, exist_ok=True)
        # 임시 파일에 쓰고 교체해서 도중에 죽어도 이전 상태가 남게 한다.
        fd, tmp = tempfile.mkstemp(dir=directory, prefix=".seq-")
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump({str(k): v for k, v in sorted(self._next.items())}, f)
        os.replace(tmp, self.path)
