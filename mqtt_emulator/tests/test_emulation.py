"""결정론·물리 모델·결함 주입·seq 영속화 테스트. `python -m unittest discover -s tests -t .` 로 실행한다."""
import asyncio
import json
import os
import statistics
import tempfile
import unittest

import model as physics
from run_scenario import Runner
from scenario import Scenario, events, load_scenario, truth_windows
from seqstore import SeqStore

SCENARIO_DIR = os.path.join(os.path.dirname(__file__), "..", "scenarios")


def make(**over) -> Scenario:
    base = {
        "name": "t",
        "seed": 3,
        "count": 50,
        "sensors": [{"id": 1, "type": "tilt"}, {"id": 2, "type": "crack"}, {"id": 3, "type": "env"}],
    }
    base.update(over)
    return Scenario.model_validate(base)


def run_dry(sc: Scenario, out_dir: str, state: str | None = None, append: bool = False):
    runner = Runner(sc, out_dir, "unused", 0, state or os.path.join(out_dir, "state.json"), append, True, 10_000)
    return asyncio.run(runner.run())


def read_lines(path: str) -> list[str]:
    with open(path, encoding="utf-8") as f:
        return f.read().splitlines()


class ModelTest(unittest.TestCase):
    def test_reading_is_pure(self):
        p = physics.default_params(1, 5)
        a = physics.reading(1, 5, "crack", p, 1000.0, 0.0, 7)
        b = physics.reading(1, 5, "crack", p, 1000.0, 0.0, 7)
        self.assertEqual(a, b)

    def test_temperature_is_daily_sine(self):
        p = physics.ModelParams(base_temp=10, temp_amplitude=5, temp_phase_hours=0, temp_noise=0)
        peak = physics.temperature(p, 6 * 3600, 0)  # 위상 + 6시간이 피크
        trough = physics.temperature(p, 18 * 3600, 0)
        self.assertAlmostEqual(peak, 15.0, places=6)
        self.assertAlmostEqual(trough, 5.0, places=6)
        self.assertAlmostEqual(physics.temperature(p, 0, 0), physics.temperature(p, 86400, 0), places=6)

    def test_crack_follows_temperature_and_trend(self):
        p = physics.ModelParams(
            temp_noise=0, crack_noise=0, crack_base=1.0, crack_temp_sensitivity=0.1, crack_ref_temp=15.0,
            base_temp=15.0, temp_amplitude=10.0, temp_phase_hours=0, crack_trend_per_day=0.0,
        )
        t_hot, t_cold = 6 * 3600, 18 * 3600  # 온도 25도, 5도
        hot = physics.reading(0, 1, "crack", p, t_hot, 0, 0)["crack_width"]
        cold = physics.reading(0, 1, "crack", p, t_cold, 0, 0)["crack_width"]
        self.assertAlmostEqual(hot - cold, 0.1 * 20, places=3)

        trending = p.updated(crack_trend_per_day=0.5, temp_amplitude=0.0)
        day0 = physics.reading(0, 1, "crack", trending, 0, 0, 0)["crack_width"]
        day2 = physics.reading(0, 1, "crack", trending, 2 * 86400, 0, 0)["crack_width"]
        self.assertAlmostEqual(day2 - day0, 1.0, places=3)

    def test_noise_is_zero_mean_and_bounded(self):
        p = physics.ModelParams(crack_noise=0.01, temp_amplitude=0.0, temp_noise=0.0, crack_trend_per_day=0.0)
        vals = [physics.reading(9, 1, "crack", p, 0.0, 0.0, k)["crack_width"] for k in range(2000)]
        self.assertLess(abs(statistics.mean(vals) - p.crack_base), 0.002)
        self.assertLess(max(abs(v - p.crack_base) for v in vals), 0.06)

    def test_unknown_param_rejected(self):
        with self.assertRaises(ValueError):
            physics.ModelParams().updated(not_a_param=1)

    def test_only_relevant_channels_are_set(self):
        p = physics.default_params(1, 1)
        tilt = physics.reading(1, 1, "tilt", p, 0, 0, 0)
        self.assertIsNotNone(tilt["tilt_x"])
        self.assertIsNone(tilt["crack_width"])
        env = physics.reading(1, 1, "env", p, 0, 0, 0)
        self.assertIsNotNone(env["temperature"])
        self.assertIsNone(env["tilt_x"])


class DeterminismTest(unittest.TestCase):
    def test_same_seed_same_events(self):
        sc = make(faults={"duplicate_ratio": 0.2, "malformed_ratio": 0.1, "out_of_range_ratio": 0.1})
        a = [(e.sensor_id, e.seq, e.payload, e.loggable, e.truth) for e in events(sc)]
        b = [(e.sensor_id, e.seq, e.payload, e.loggable, e.truth) for e in events(sc)]
        self.assertEqual(a, b)

    def test_different_seed_changes_values(self):
        a = [e.payload for e in events(make(seed=1))]
        b = [e.payload for e in events(make(seed=2))]
        self.assertNotEqual(a, b)

    def test_two_runs_write_identical_files(self):
        sc = make(faults={"duplicate_ratio": 0.1, "silence": [{"sensor_id": 2, "from": 10, "to": 20}]})
        with tempfile.TemporaryDirectory() as d1, tempfile.TemporaryDirectory() as d2:
            run_dry(sc, d1)
            run_dry(sc, d2)
            for name in ("publish.jsonl", "truth.jsonl"):
                self.assertEqual(read_lines(os.path.join(d1, name)), read_lines(os.path.join(d2, name)), name)

    def test_shipped_scenarios_load(self):
        for name in os.listdir(SCENARIO_DIR):
            sc = load_scenario(os.path.join(SCENARIO_DIR, name))
            self.assertGreater(len(sc.sensors), 0, name)

    def test_tc01_scenario_shape(self):
        sc = load_scenario(os.path.join(SCENARIO_DIR, "tc01.json"))
        self.assertEqual(len(sc.sensors), 10)
        self.assertEqual(sc.count, 1000)
        self.assertEqual(sum(1 for _ in events(sc)), 10_000)


class FaultInjectionTest(unittest.TestCase):
    def test_silence_skips_events_and_is_in_truth(self):
        sc = make(count=30, faults={"silence": [{"sensor_id": 2, "from": 10, "to": 20}]})
        sent = {(e.sensor_id, e.seq) for e in events(sc)}
        for seq in range(30):
            self.assertEqual((2, seq) in sent, not 10 <= seq < 20)
            self.assertIn((1, seq), sent)
        self.assertEqual(truth_windows(sc), [{"kind": "silence", "sensor_id": 2, "seq_from": 10, "seq_to": 20}])

    def test_flatline_repeats_the_same_value(self):
        sc = make(count=40, faults={"flatline": [{"sensor_id": 2, "from": 10, "to": 30}]})
        values = {}
        for e in events(sc):
            if e.sensor_id == 2:
                values[e.seq] = json.loads(e.payload)["crack_width"]
        window = {values[s] for s in range(10, 30)}
        self.assertEqual(len(window), 1)
        self.assertGreater(len({values[s] for s in range(0, 10)}), 1)

    def test_duplicates_share_payload_and_are_logged_per_send(self):
        sc = make(count=200, faults={"duplicate_ratio": 0.3, "duplicate_copies": 2})
        by_key: dict[tuple, list[bytes]] = {}
        for e in events(sc):
            by_key.setdefault((e.sensor_id, e.seq), []).append(e.payload)
        dup = {k: v for k, v in by_key.items() if len(v) > 1}
        self.assertTrue(dup)
        for sends in dup.values():
            self.assertEqual(len(sends), 3)
            self.assertEqual(len(set(sends)), 1)
        with tempfile.TemporaryDirectory() as d:
            run_dry(sc, d)
            lines = read_lines(os.path.join(d, "publish.jsonl"))
            self.assertEqual(len(lines), sum(len(v) for v in by_key.values()))

    def test_malformed_is_invalid_json_and_not_logged(self):
        sc = make(count=200, faults={"malformed_ratio": 0.2})
        bad = [e for e in events(sc) if not e.loggable]
        self.assertTrue(bad)
        for e in bad:
            with self.assertRaises(ValueError):
                json.loads(e.payload)
            self.assertEqual(e.truth[-1]["kind"], "malformed")
        with tempfile.TemporaryDirectory() as d:
            run_dry(sc, d)
            logged = {(json.loads(l)["sensor_id"], json.loads(l)["seq"]) for l in read_lines(os.path.join(d, "publish.jsonl"))}
            for e in bad:
                self.assertNotIn((e.sensor_id, e.seq), logged)

    def test_out_of_range_is_marked_in_truth(self):
        sc = make(count=300, faults={"out_of_range_ratio": 0.1})
        marked = [e for e in events(sc) if any(t["kind"] == "out_of_range" for t in e.truth)]
        self.assertTrue(marked)
        for e in marked:
            body = json.loads(e.payload)
            field = next(t["field"] for t in e.truth if t["kind"] == "out_of_range")
            self.assertEqual(body[field], {"tilt_x": 45.0, "crack_width": 99.0, "temperature": 120.0}[field])

    def test_vendor_b_has_a_different_shape_and_is_not_logged(self):
        sc = make(count=5, faults={"vendor_b_sensors": [3]})
        b = [e for e in events(sc) if e.sensor_id == 3]
        for e in b:
            body = json.loads(e.payload)
            self.assertNotIn("sensor_id", body)
            self.assertEqual(body["id"], 3)
            self.assertFalse(e.loggable)

    def test_invalid_scenarios_are_rejected(self):
        with self.assertRaises(ValueError):
            make(sensors=[{"id": 1, "type": "tilt"}, {"id": 1, "type": "env"}])
        with self.assertRaises(ValueError):
            make(faults={"silence": [{"sensor_id": 1, "from": 5, "to": 5}]})
        with self.assertRaises(ValueError):
            make(unknown_field=1)


class SeqContinuityTest(unittest.TestCase):
    def test_restart_continues_seq_and_log_covers_both_runs(self):
        with tempfile.TemporaryDirectory() as d:
            sc = make(count=20)
            run_dry(sc, d)
            run_dry(sc, d, append=True)
            seqs: dict[int, list[int]] = {}
            for line in read_lines(os.path.join(d, "publish.jsonl")):
                row = json.loads(line)
                seqs.setdefault(row["sensor_id"], []).append(row["seq"])
            for sid, s in seqs.items():
                self.assertEqual(s, list(range(40)), f"sensor {sid}")

    def test_replayed_slot_is_byte_identical(self):
        # 저장 직전에 죽어 같은 슬롯을 다시 만들어도 payload 가 같아야 DB 중복 키가 흡수한다.
        sc = make(count=10)
        first = {(e.sensor_id, e.seq): e.payload for e in events(sc)}
        again = {(e.sensor_id, e.seq): e.payload for e in events(sc)}
        self.assertEqual(first, again)

    def test_store_roundtrip_and_monotonic(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "s.json")
            st = SeqStore(path)
            st.acked(1, 4)
            st.acked(1, 2)  # 되돌아가지 않는다
            st.save()
            self.assertEqual(SeqStore(path).snapshot(), {1: 5})

    def test_time_follows_seq_not_wall_clock(self):
        sc = make(count=3, interval_seconds=10)
        t = [json.loads(e.payload)["time"] for e in events(sc) if e.sensor_id == 1]
        self.assertEqual(t[0], "2026-01-01T00:00:00+00:00")
        self.assertEqual(t[1], "2026-01-01T00:00:10+00:00")


class ControlTest(unittest.TestCase):
    """데모용 제어: 계수 변경과 센서 침묵."""

    def setUp(self):
        import main

        self.main = main
        main.config = main.PublisherConfig(sensors=[{"id": 1, "sensor_type": "crack"}, {"id": 2, "sensor_type": "env"}])
        main.model_overrides.clear()
        main.silenced.clear()

    def test_model_change_affects_next_reading(self):
        from fastapi import HTTPException

        m = self.main
        before = m.generate_reading(m.config.sensors[0])["crack_width"]
        asyncio.run(m.set_sensor_model(1, {"crack_base": 50.0}))
        after = m.generate_reading(m.config.sensors[0])["crack_width"]
        self.assertGreater(after, before + 30)
        self.assertEqual(asyncio.run(m.get_sensor_model(1))["params"]["crack_base"], 50.0)
        with self.assertRaises(HTTPException) as ctx:
            asyncio.run(m.set_sensor_model(1, {"nope": 1.0}))
        self.assertEqual(ctx.exception.status_code, 422)

    def test_pause_and_resume_silence_a_shared_sensor(self):
        m = self.main
        asyncio.run(m.pause_sensor(2))
        self.assertEqual(m.silenced, {2})
        asyncio.run(m.resume_sensor(2))
        self.assertEqual(m.silenced, set())

    def test_sensor_model_command(self):
        from sensor import Sensor

        s = Sensor(1, "crack", "localhost")
        s._apply_command('model:{"crack_base":9.5}')
        self.assertEqual(s.params.crack_base, 9.5)
        with self.assertRaises(ValueError):
            s._apply_command('model:{"bogus":1}')
        with self.assertRaises(ValueError):
            s._apply_command("model:not-json")
        self.assertEqual(s.params.crack_base, 9.5)

    def test_sensor_resumes_seq_from_state_file(self):
        from sensor import Sensor

        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "seq.json")
            st = SeqStore(path)
            st.acked(7, 41)
            st.save()
            self.assertEqual(Sensor(7, "env", "localhost", seq_state=path).seq, 42)


if __name__ == "__main__":
    unittest.main()
