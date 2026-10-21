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
from datetime import datetime, timezone

import aiomqtt


def build_reading(sensor_id: int, sensor_type: str, seq: int) -> dict:
    """DB의 sensor_data 컬럼에 대응하는 측정값을 생성합니다."""
    result = {
        "sensor_id": sensor_id,
        "seq": seq,
        "time": datetime.now(timezone.utc).isoformat(),
        "tilt_x": None,
        "tilt_y": None,
        "crack_width": None,
        "temperature": None,
    }

    if sensor_type == "tilt":
        result["tilt_x"] = round(random.uniform(-1.0, 1.0), 6)
        result["tilt_y"] = round(random.uniform(-1.0, 1.0), 6)
    elif sensor_type == "crack":
        result["crack_width"] = round(random.uniform(0.0, 5.0), 4)
    elif sensor_type == "env":
        result["temperature"] = round(random.uniform(-10.0, 40.0), 2)

    return result


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

        self.seq = 0
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
            except ValueError:
                continue
            if not self._exit.is_set():
                self.state = "paused" if self.paused else "online"

    async def _publish_loop(self, client: aiomqtt.Client):
        # 센서들이 같은 순간에 발행하지 않도록 첫 발행을 분산한다.
        await asyncio.sleep(random.uniform(0.0, self.interval))
        while True:
            if not self.paused:
                payload = json.dumps(build_reading(self.id, self.type, self.seq), separators=(",", ":"))
                await client.publish(self.data_topic, payload.encode(), qos=self.qos)
                self.seq += 1
                self.published += 1
            jitter = random.uniform(-self.jitter, self.jitter)
            await asyncio.sleep(self.interval * (1 + jitter))
