"""센서 1개를 프로세스(컨테이너) 1개로 실행한다.

SENSOR_ID, SENSOR_TYPE 필수. docker kill(SIGKILL)은 LWT 경로, docker stop(SIGTERM)은 정상 종료 경로.
"""
import asyncio
import os
import signal

from sensor import Sensor


async def main():
    sensor = Sensor(
        sensor_id=int(os.environ["SENSOR_ID"]),
        sensor_type=os.environ["SENSOR_TYPE"],
        host=os.getenv("MQTT_HOST", "localhost"),
        port=int(os.getenv("MQTT_PORT", "1883")),
        interval=float(os.getenv("INTERVAL_SECONDS", "1.0")),
        jitter=float(os.getenv("JITTER_FRACTION", "0.1")),
        qos=int(os.getenv("QOS", "1")),
        topic_prefix=os.getenv("TOPIC_PREFIX", "sensors"),
        keepalive=int(os.getenv("KEEPALIVE_SECONDS", "5")),
    )

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, sensor.stop, True)

    await sensor.run()


if __name__ == "__main__":
    asyncio.run(main())
