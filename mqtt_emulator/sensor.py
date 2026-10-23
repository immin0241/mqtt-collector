"""센서 1개를 흉내내는 모듈.

센서마다 자기 MQTT 연결(client_id=sensor-{id})과 LWT를 가진다.
- 데이터 토픽 : {prefix}/{type}/{id}           (JSON)
- 상태 토픽   : {prefix}/{type}/{id}/status    (online | offline, retained, LWT)
- 명령 토픽   : cmd/sensor/{id}                (pause | resume | interval:<sec> | kill:graceful | kill:abrupt)
"""
import asyncio
import json
import random
import socket
import time
from datetime import datetime, timezone

import aiomqtt

import model as physics
from seqstore import SeqStore


def build_reading(
    sensor_id: int,
    sensor_type: str,
    seq: int,
    seed: int = 0,
    params: physics.ModelParams | None = None,
    t0: float | None = None,
) -> dict:
    """DB의 sensor_data 컬럼에 대응하는 측정값을 물리 모델로 생성한다. 시각은 실제 시계를 쓴다."""
    now = datetime.now(timezone.utc)
    params = params or physics.default_params(seed, sensor_id)
    values = physics.reading(seed, sensor_id, sensor_type, params, now.timestamp(), now.timestamp() if t0 is None else t0, seq)
    return {"sensor_id": sensor_id, "seq": seq, "time": now.isoformat(), **values}


def command_topic(sensor_id: int) -> str:
    return f"cmd/sensor/{sensor_id}"


class Sensor:
    def __init__(
        self,
        sensor_id: int,
        sensor_type: str,
        host: str,
        port: int = 1883,
        interval: float = 1.0,
        jitter: float = 0.1,
        qos: int = 1,
        topic_prefix: str = "sensors",
        keepalive: int = 5,
        seed: int = 0,
        seq_state: str | None = None,
        publish_log: str | None = None,
    ):
        self.id = sensor_id
        self.type = sensor_type
        self.host = host
        self.port = port
        self.interval = interval
        self.jitter = jitter
        self.qos = qos
        self.keepalive = keepalive
        self.data_topic = f"{topic_prefix}/{sensor_type}/{sensor_id}"
        self.status_topic = f"{self.data_topic}/status"

        self.seed = seed
        self.params = physics.default_params(seed, sensor_id)
        self.t0 = time.time()  # 진행 추세의 기준 시각
        self._seqs = SeqStore(seq_state)
        self._publish_log = open(publish_log, "a", buffering=1, encoding="utf-8") if publish_log else None

        self.seq = self._seqs.snapshot().get(sensor_id, 0)  # 재시작 후에도 이어서 발행한다
        self.state = "starting"  # starting | online | paused | reconnecting | dead
        self.paused = False
        self.published = 0

        self._exit = asyncio.Event()  # kill 요청 시 set
        self._graceful = True

    # ----- 제어 -------------------------------------------------------
    def stop(self, graceful: bool = True):
        self._graceful = graceful
        self._exit.set()

    def _apply_command(self, payload: str):
        cmd, _, arg = payload.partition(":")
        if cmd == "pause":
            self.paused = True
        elif cmd == "resume":
            self.paused = False
        elif cmd == "interval":
            self.interval = max(0.01, float(arg))
        elif cmd == "model":
            # model:{"crack_temp_sensitivity":0.05} - 계수 일부를 바꾼다. 잘못된 값이면 ValueError 로 무시된다.
            self.params = self.params.updated(**json.loads(arg))
        elif cmd == "kill":
            self.stop(graceful=(arg != "abrupt"))

    # ----- 실행 -------------------------------------------------------
    async def run(self):
        """kill 될 때까지 실행한다. 브로커 연결이 끊기면 재접속한다."""
        while not self._exit.is_set():
            try:
                await self._session()
            except asyncio.CancelledError:
                raise
            except (aiomqtt.MqttError, ExceptionGroup):
                # TaskGroup 안에서 연결 오류가 나면 ExceptionGroup으로 감싸져 올라온다.
                if self._exit.is_set():
                    break
                self.state = "reconnecting"
                await asyncio.sleep(2.0)
        self.state = "dead"
        self._seqs.save()

    async def _session(self):
        will = aiomqtt.Will(self.status_topic, payload="offline", qos=1, retain=True)
        async with aiomqtt.Client(
            self.host,
            port=self.port,
            identifier=f"sensor-{self.id}",
            keepalive=self.keepalive,
            will=will,
        ) as client:
            await client.publish(self.status_topic, "online", qos=1, retain=True)
            await client.subscribe(command_topic(self.id), qos=1)
            self.state = "paused" if self.paused else "online"

            async with asyncio.TaskGroup() as tg:
                listener = tg.create_task(self._listen(client))
                publisher = tg.create_task(self._publish_loop(client))
                await self._exit.wait()
                listener.cancel()
                publisher.cancel()

            if self._graceful:
                # 정상 종료: DISCONNECT 이므로 브로커가 LWT를 발행하지 않는다 -> offline 을 직접 알린다.
                await client.publish(self.status_topic, "offline", qos=1, retain=True)
            else:
                # 비정상 종료: DISCONNECT 없이 소켓만 끊어 브로커가 LWT를 발행하게 한다.
                self._abort_socket(client)

    @staticmethod
    def _abort_socket(client: aiomqtt.Client):
        sock = client._client.socket()
        if sock is not None:
            sock.shutdown(socket.SHUT_RDWR)

    async def _listen(self, client: aiomqtt.Client):
        async for message in client.messages:
            payload = message.payload.decode() if isinstance(message.payload, bytes) else str(message.payload)
            try:
                self._apply_command(payload)
            except (ValueError, TypeError):
                continue
            if not self._exit.is_set():
                self.state = "paused" if self.paused else "online"

    async def _publish_loop(self, client: aiomqtt.Client):
        # 센서들이 같은 순간에 발행하지 않도록 첫 발행을 분산한다.
        await asyncio.sleep(random.uniform(0.0, self.interval))
        while True:
            if not self.paused:
                reading = build_reading(self.id, self.type, self.seq, self.seed, self.params, self.t0)
                payload = json.dumps(reading, separators=(",", ":"))
                await client.publish(self.data_topic, payload.encode(), qos=self.qos)
                # PUBACK 을 받은 뒤에만 발행 로그와 seq 상태에 반영한다.
                if self._publish_log:
                    self._publish_log.write(json.dumps({"sensor_id": self.id, "seq": self.seq}, separators=(",", ":")) + "\n")
                self._seqs.acked(self.id, self.seq)
                if self.seq % 50 == 0:
                    self._seqs.save()
                self.seq += 1
                self.published += 1
            jitter = random.uniform(-self.jitter, self.jitter)
            await asyncio.sleep(self.interval * (1 + jitter))
