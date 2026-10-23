"""센서 물리 모델. 입력이 같으면 출력이 항상 같은 순수 함수만 둔다.

- 온도      = 기준 온도 + 진폭 × sin(2π (t − 위상) / 1일) + 노이즈
- 균열 폭   = 기준값 + 온도 민감도 × (온도 − 기준 온도) + 진행 추세 × 경과 일수 + 노이즈
- 기울기    = 균열과 같은 구조 (축마다 기준값·노이즈)

노이즈는 (seed, sensor_id, k) 로만 정해지므로 발행 순서나 재시작과 무관하다.
"""
import math
import random
from dataclasses import asdict, dataclass, fields, replace

SECONDS_PER_DAY = 86400.0


@dataclass(frozen=True)
class ModelParams:
    # 온도 (°C). 피크는 위상 + 6시간.
    base_temp: float = 15.0
    temp_amplitude: float = 8.0
    temp_phase_hours: float = 9.0
    temp_noise: float = 0.3
    # 균열 폭 (mm)
    crack_base: float = 1.0
    crack_temp_sensitivity: float = 0.02  # mm / °C
    crack_ref_temp: float = 15.0
    crack_trend_per_day: float = 0.001  # mm / day
    crack_noise: float = 0.005
    # 기울기 (°)
    tilt_x_base: float = 0.1
    tilt_y_base: float = -0.05
    tilt_temp_sensitivity: float = 0.002  # ° / °C
    tilt_ref_temp: float = 15.0
    tilt_trend_per_day: float = 0.0005  # ° / day
    tilt_noise: float = 0.003

    def updated(self, **changes) -> "ModelParams":
        """알 수 없는 필드는 거부한다. (제어 API 입력 검증)"""
        known = {f.name for f in fields(self)}
        unknown = set(changes) - known
        if unknown:
            raise ValueError(f"unknown model params: {sorted(unknown)}")
        return replace(self, **{k: float(v) for k, v in changes.items()})

    def to_dict(self) -> dict:
        return asdict(self)


def default_params(seed: int, sensor_id: int) -> ModelParams:
    """센서마다 조금씩 다른 기본 계수. (seed, sensor_id) 로만 결정된다."""
    rng = random.Random(f"{seed}:{sensor_id}:params")
    return ModelParams(
        base_temp=round(rng.uniform(10.0, 20.0), 2),
        temp_amplitude=round(rng.uniform(5.0, 10.0), 2),
        crack_base=round(rng.uniform(0.5, 2.0), 3),
        crack_temp_sensitivity=round(rng.uniform(0.01, 0.04), 4),
        crack_trend_per_day=round(rng.uniform(0.0, 0.003), 5),
        tilt_x_base=round(rng.uniform(-0.3, 0.3), 3),
        tilt_y_base=round(rng.uniform(-0.3, 0.3), 3),
    )


def _event_rng(seed: int, sensor_id: int, k: int) -> random.Random:
    return random.Random(f"{seed}:{sensor_id}:{k}")


def temperature(p: ModelParams, t: float, noise: float) -> float:
    """t: epoch 초. noise: 표준정규 난수."""
    phase = 2 * math.pi * (t - p.temp_phase_hours * 3600.0) / SECONDS_PER_DAY
    return p.base_temp + p.temp_amplitude * math.sin(phase) + p.temp_noise * noise


def reading(
    seed: int,
    sensor_id: int,
    sensor_type: str,
    p: ModelParams,
    t: float,
    t0: float,
    k: int,
) -> dict:
    """k 번째 측정값. t: 측정 시각(epoch 초), t0: 실행 시작 시각(추세 기준)."""
    rng = _event_rng(seed, sensor_id, k)
    n_temp, n_a, n_b = rng.gauss(0, 1), rng.gauss(0, 1), rng.gauss(0, 1)
    temp = temperature(p, t, n_temp)
    days = max(0.0, (t - t0) / SECONDS_PER_DAY)

    out = {"tilt_x": None, "tilt_y": None, "crack_width": None, "temperature": None}
    if sensor_type == "tilt":
        drift = p.tilt_temp_sensitivity * (temp - p.tilt_ref_temp) + p.tilt_trend_per_day * days
        out["tilt_x"] = round(p.tilt_x_base + drift + p.tilt_noise * n_a, 6)
        out["tilt_y"] = round(p.tilt_y_base + drift + p.tilt_noise * n_b, 6)
    elif sensor_type == "crack":
        out["crack_width"] = round(
            p.crack_base
            + p.crack_temp_sensitivity * (temp - p.crack_ref_temp)
            + p.crack_trend_per_day * days
            + p.crack_noise * n_a,
            4,
        )
    elif sensor_type == "env":
        out["temperature"] = round(temp, 2)
    else:
        raise ValueError(f"unknown sensor_type: {sensor_type}")
    return out
