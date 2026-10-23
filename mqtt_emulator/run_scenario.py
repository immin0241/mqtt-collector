"""시나리오 파일을 실행해 발행하고 발행 로그·정답 파일·통계를 남긴다.

    python run_scenario.py scenarios/tc01.json --out-dir out --host localhost

출력 (--out-dir):
- publish.jsonl : PUBACK 을 받은 전송마다 {"sensor_id","seq"} 한 줄. tools/reconcile --published 의 입력.
                  깨진 JSON·제조사 B 포맷처럼 저장되지 않는 것이 정상인 이벤트는 올리지 않는다.
- truth.jsonl   : 주입한 이상 내역(정답). 경보 검증과 이후 AI 평가용.
- stats.json    : 생성·확인·재시도·재접속·버퍼 넘침 집계. 발행 측 유실과 수집 측 유실을 구분하는 근거.
- seq_state.json: 센서별 다음 seq (--state 로 위치 변경). 재시작하면 이어서 발행한다.

브로커가 끊기면 이벤트를 버리지 않고 버퍼에 쌓았다가 재접속 후 순서대로 보낸다.
버퍼가 가득 차는 경우만 --max-buffer 초과분을 버리며(rate>0, 실시간 장치 의미), 그 수는 stats 에 남는다.
rate=0 이면 버퍼가 차도 버리지 않고 생성을 멈춰 기다린다.
"""
import argparse
import asyncio
import collections
import json
import os
import signal
import time

try:
    import aiomqtt
except ImportError:  # dry-run 과 단위 테스트는 MQTT 클라이언트 없이도 돈다
    aiomqtt = None

from scenario import Event, Scenario, events, load_scenario, truth_windows
from seqstore import SeqStore


class Stats:
    def __init__(self):
        self.generated = 0
        self.acked = 0
        self.retried = 0
        self.reconnects = 0
        self.publisher_dropped = 0
        self.max_buffered = 0
        self.last_error = ""
        self.started = time.monotonic()
        self.finished = False

    def to_dict(self) -> dict:
        return {
            "generated": self.generated,
            "acked": self.acked,
            "retried": self.retried,
            "reconnects": self.reconnects,
            "publisher_dropped": self.publisher_dropped,
            "max_buffered": self.max_buffered,
            "last_error": self.last_error,
            "duration_seconds": round(time.monotonic() - self.started, 3),
            "completed": self.finished,
        }


class Runner:
    def __init__(self, sc: Scenario, out_dir: str, host: str, port: int, state: str | None,
                 append: bool, dry_run: bool, max_buffer: int):
        self.sc = sc
        self.host, self.port = host, port
        self.dry_run = dry_run
        self.max_buffer = max_buffer
        self.out_dir = out_dir
        os.makedirs(out_dir, exist_ok=True)

        mode = "a" if append else "w"
        self.publish_log = open(os.path.join(out_dir, "publish.jsonl"), mode, buffering=1, encoding="utf-8")
        self.truth_log = open(os.path.join(out_dir, "truth.jsonl"), mode, buffering=1, encoding="utf-8")
        self.store = SeqStore(state if state is not None else os.path.join(out_dir, "seq_state.json"))
        self.stats = Stats()

        self.buffer: collections.deque[Event] = collections.deque()
        self.produced_all = False
        self.stop = asyncio.Event()
        self._wake = asyncio.Event()

    # ----- 생성 -------------------------------------------------------
    async def produce(self):
        for w in truth_windows(self.sc):
            self.truth_log.write(json.dumps(w, separators=(",", ":")) + "\n")

        rate = 0 if self.dry_run else self.sc.rate  # dry-run 은 실시간 속도를 흉내내지 않는다
        begin = time.monotonic()
        for n, ev in enumerate(events(self.sc, self.store.snapshot())):
            if self.stop.is_set():
                return
            if rate > 0:
                delay = begin + n / rate - time.monotonic()
                if delay > 0:
                    await asyncio.sleep(delay)
            else:
                while len(self.buffer) >= self.max_buffer and not self.stop.is_set():
                    await asyncio.sleep(0.005)
                if n % 200 == 0:
                    await asyncio.sleep(0)

            self.stats.generated += 1
            for t in ev.truth:
                self.truth_log.write(json.dumps(t, separators=(",", ":")) + "\n")

            if len(self.buffer) >= self.max_buffer:
                self.stats.publisher_dropped += 1
                self.truth_log.write(json.dumps(
                    {"kind": "publisher_dropped", "sensor_id": ev.sensor_id, "seq": ev.seq}, separators=(",", ":")) + "\n")
                continue
            self.buffer.append(ev)
            self.stats.max_buffered = max(self.stats.max_buffered, len(self.buffer))
            self._wake.set()
        self.produced_all = True
        self._wake.set()

    # ----- 발행 -------------------------------------------------------
    def _record_ack(self, ev: Event):
        self.stats.acked += 1
        self.store.acked(ev.sensor_id, ev.seq)
        if ev.loggable:
            self.publish_log.write(json.dumps({"sensor_id": ev.sensor_id, "seq": ev.seq}, separators=(",", ":")) + "\n")
        if self.stats.acked % 200 == 0:
            self.store.save()

    async def _next_event(self) -> Event | None:
        if self.stop.is_set():
            return None  # 종료 요청 후에는 비우지 않는다. 확인 못 받은 슬롯은 재시작 때 다시 만들어진다.
        while not self.buffer:
            if self.produced_all or self.stop.is_set():
                return None
            self._wake.clear()
            if self.buffer:
                break
            await self._wake.wait()
        return self.buffer[0]  # 확인(ack)되기 전에는 꺼내지 않는다. 재접속 후 같은 이벤트부터 다시 보낸다.

    async def publish_dry(self):
        while (ev := await self._next_event()) is not None:
            self.buffer.popleft()
            self._record_ack(ev)

    async def publish_mqtt(self):
        while True:
            try:
                async with aiomqtt.Client(self.host, port=self.port, keepalive=10,
                                          identifier=f"emulator-{self.sc.name}") as client:
                    while (ev := await self._next_event()) is not None:
                        await client.publish(ev.topic, ev.payload, qos=self.sc.qos)
                        self.buffer.popleft()
                        self._record_ack(ev)
                    return
            except aiomqtt.MqttError as e:
                self.stats.reconnects += 1
                self.stats.retried += 1  # 확인받지 못한 이벤트를 재접속 후 다시 보낸다
                self.stats.last_error = f"{type(e).__name__}: {e}"
                if self.stop.is_set():
                    return
                await asyncio.sleep(1.0)

    # ----- 실행 -------------------------------------------------------
    async def run(self) -> Stats:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self.stop.set)

        consumer = self.publish_dry() if self.dry_run else self.publish_mqtt()
        producer_task = asyncio.create_task(self.produce())
        consumer_task = asyncio.create_task(consumer)
        try:
            await producer_task
            self._wake.set()
            await consumer_task
        finally:
            for t in (producer_task, consumer_task):
                t.cancel()
            self.stats.finished = self.produced_all and not self.buffer and not self.stop.is_set()
            self.store.save()
            self.publish_log.close()
            self.truth_log.close()
            with open(os.path.join(self.out_dir, "stats.json"), "w", encoding="utf-8") as f:
                json.dump(self.stats.to_dict(), f, indent=2)
        return self.stats


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("scenario")
    ap.add_argument("--out-dir", default="out")
    ap.add_argument("--host", default=os.getenv("MQTT_HOST", "localhost"))
    ap.add_argument("--port", type=int, default=int(os.getenv("MQTT_PORT", "1883")))
    ap.add_argument("--state", default=None, help="seq 상태 파일 (기본: <out-dir>/seq_state.json)")
    ap.add_argument("--append", action="store_true", help="발행 로그·정답 파일에 이어쓴다 (재시작 후 대조용)")
    ap.add_argument("--dry-run", action="store_true", help="발행하지 않고 모든 전송이 확인된 것으로 기록한다")
    ap.add_argument("--max-buffer", type=int, default=100_000)
    args = ap.parse_args()

    sc = load_scenario(args.scenario)
    runner = Runner(sc, args.out_dir, args.host, args.port, args.state, args.append, args.dry_run, args.max_buffer)
    stats = asyncio.run(runner.run())
    print(json.dumps(stats.to_dict()))


if __name__ == "__main__":
    main()
