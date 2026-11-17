import io
import json
import os
import random
import re
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout

from loggen import evaluate, plant, scenarios
from loggen.__main__ import main
from loggen.simulator import Simulator
from loggen.sinks import KinesisSink

START = 1794902400.0  # 2026-11-17T08:00:00Z


def simulate(duration=120, anomalies=(), seed=3, **kwargs):
    stations = plant.build_stations()
    return list(Simulator(stations, list(anomalies), START, seed=seed, **kwargs).run(duration))


def readings(lines, pattern):
    return [float(m.group(1)) for m in (re.search(pattern, line) for line in lines) if m]


class SimulatorTest(unittest.TestCase):

    def test_same_seed_same_output(self):
        self.assertEqual(simulate(seed=5), simulate(seed=5))
        self.assertNotEqual(simulate(seed=5), simulate(seed=6))

    def test_every_station_speaks_its_own_dialect(self):
        by_source = {}
        for _, source, line in simulate(garbage_ratio=0):
            by_source.setdefault(source, []).append(line)
        dialects = {
            "MIXER-01": r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z (INFO|WARN|ERROR) +mixer-ctl MIXER-01 ",
            "EXTRUDER-02": r"^\[\d{4}-\d\d-\d\d \d\d:\d\d:\d\d,\d{3}\] EXTRUDER-02 \| (INFO|WARNING|ERROR) \| ",
            "KILN-01": r"^Nov 17 \d\d:\d\d:\d\d kiln-01 kilnd\[\d+\]: <(info|warn|err)> ",
            "PRESS-01": r"^\d{13}\|PRESS-01\|[IWE]\|",
        }
        for source, pattern in dialects.items():
            self.assertTrue(by_source[source], source)
            for line in by_source[source]:
                self.assertRegex(line, pattern)
        for line in by_source["PACKER-01"]:
            self.assertEqual("PACKER-01", json.loads(line)["unit"])
        self.assertIn("chatter", by_source)

    def test_lines_are_emitted_in_delivery_order_and_within_the_duration(self):
        times = [emit_time for emit_time, _, _ in simulate(duration=60, max_delay_s=3)]
        self.assertEqual(times, sorted(times))
        self.assertGreaterEqual(times[0], START)
        self.assertLess(times[-1], START + 60 + 3)

    def test_rate_multiplies_the_volume(self):
        base, double = len(simulate(chatter_per_s=0)), len(simulate(chatter_per_s=0, rate=2))
        self.assertAlmostEqual(2.0, double / base, delta=0.2)

    def test_garbage_ratio(self):
        lines = simulate(duration=600, garbage_ratio=0.05)
        share = sum(1 for _, source, _ in lines if source == "garbage") / len(lines)
        self.assertAlmostEqual(0.05, share, delta=0.02)

    def test_spike_moves_the_reading_and_only_during_the_anomaly(self):
        anomaly = scenarios.Anomaly("KILN-01", "spike", START + 100, START + 200, "zone temp", 1)
        lines = [line for _, source, line in simulate(300, [anomaly], max_delay_s=0, garbage_ratio=0) if source == "KILN-01"]
        temps = readings(lines, r"zone temp reading ([\d.]+)C")
        self.assertGreater(max(temps), 850)
        self.assertLess(max(temps[:40]), 850)   # first 80 seconds
        self.assertLess(max(temps[-40:]), 850)  # last 80 seconds
        self.assertTrue(any("<warn>" in line for line in lines))

    def test_drift_grows_gradually(self):
        anomaly = scenarios.Anomaly("MIXER-01", "drift", START, START + 300, "visc", -1)
        lines = [line for _, source, line in simulate(300, [anomaly], max_delay_s=0, garbage_ratio=0) if source == "MIXER-01"]
        visc = readings(lines, r"visc=([\d.]+)cP")
        self.assertLess(sum(visc[-20:]) / 20, sum(visc[:20]) / 20 - 80)

    def test_error_burst_stuck_and_silence(self):
        anomalies = [
            scenarios.Anomaly("PRESS-01", "error_burst", START, START + 60),
            scenarios.Anomaly("PACKER-01", "silence", START, START + 1000),
            scenarios.Anomaly("EXTRUDER-01", "stuck", START + 10, START + 1000, "barrel temp"),
        ]
        by_source = {}
        for _, source, line in simulate(120, anomalies, max_delay_s=0, garbage_ratio=0):
            by_source.setdefault(source, []).append(line)
        self.assertGreater(sum("|E|" in line for line in by_source["PRESS-01"]), 100)
        self.assertEqual(0, sum("|E|" in line for line in by_source["PRESS-02"]))
        self.assertNotIn("PACKER-01", by_source)
        frozen = readings(by_source["EXTRUDER-01"], r"barrel temp ([\d.]+) C")[15:]
        self.assertEqual(1, len(set(frozen)))


class ScenarioTest(unittest.TestCase):

    def test_random_schedule_never_overlaps_on_a_station(self):
        stations = plant.build_stations()
        schedule = scenarios.random_schedule(stations, START, START + 86400, 2.0, random.Random(1))
        self.assertGreater(len(schedule), 50)
        for station in stations:
            own = [a for a in schedule if a.station == station.id]
            for previous, following in zip(own, own[1:]):
                self.assertGreater(following.start, previous.end)
        self.assertEqual([], scenarios.random_schedule(stations, START, START + 86400, 0, random.Random(1)))

    def test_parse_injection(self):
        stations = plant.build_stations()
        anomaly = scenarios.parse_injection("kiln-01:spike:120:60:o2", stations, START, random.Random(1))
        self.assertEqual(("KILN-01", "spike", START + 120, START + 180, "o2"),
                         (anomaly.station, anomaly.kind, anomaly.start, anomaly.end, anomaly.metric))
        for bad in ("KILN-01:spike:120", "LATHE-01:spike:1:1", "KILN-01:meltdown:1:1"):
            with self.assertRaises(ValueError):
                scenarios.parse_injection(bad, stations, START, random.Random(1))


class FakeKinesis:
    """Rejects every record whose index in the call is in ``fail``, for the first ``failing_calls`` calls."""

    def __init__(self, fail=(), failing_calls=0):
        self.fail, self.failing_calls, self.calls = set(fail), failing_calls, []

    def put_records(self, StreamName, Records):
        self.calls.append(list(Records))
        failing = len(self.calls) <= self.failing_calls
        results = [{"ErrorCode": "ProvisionedThroughputExceededException"} if failing and i in self.fail
                   else {"SequenceNumber": "1"} for i in range(len(Records))]
        return {"FailedRecordCount": sum("ErrorCode" in r for r in results), "Records": results}


class KinesisSinkTest(unittest.TestCase):

    def sink(self, client, **kwargs):
        return KinesisSink(client, "logs", sleep=lambda s: None, clock=lambda: 0.0, **kwargs)

    def test_batches_respect_the_put_records_limit(self):
        client = FakeKinesis()
        sink = self.sink(client)
        for i in range(1201):
            sink.write("KILN-01", f"line {i}")
        sink.close()
        self.assertEqual([500, 500, 201], [len(call) for call in client.calls])
        self.assertEqual(1201, sink.records_sent)
        self.assertEqual({"KILN-01"}, {r["PartitionKey"] for call in client.calls for r in call})

    def test_packs_lines_of_one_source_into_one_record(self):
        client = FakeKinesis()
        sink = self.sink(client, lines_per_record=3)
        for i in range(4):
            sink.write("KILN-01", f"k{i}")
            sink.write("PRESS-01", f"p{i}")
        sink.close()
        data = sorted(r["Data"].decode() for call in client.calls for r in call)
        self.assertEqual(["k0\nk1\nk2", "k3", "p0\np1\np2", "p3"], data)

    def test_only_failed_records_are_retried(self):
        client = FakeKinesis(fail={1, 3}, failing_calls=1)
        sink = self.sink(client)
        for i in range(5):
            sink.write("s", f"line {i}")
        sink.close()
        self.assertEqual([5, 2], [len(call) for call in client.calls])
        self.assertEqual([b"line 1", b"line 3"], [r["Data"] for r in client.calls[1]])
        self.assertEqual(5, sink.records_sent)

    def test_gives_up_loudly(self):
        sink = self.sink(FakeKinesis(fail={0}, failing_calls=99), max_attempts=3)
        sink.write("s", "line")
        with self.assertRaises(RuntimeError):
            sink.close()


class CliTest(unittest.TestCase):

    def run_cli(self, *args):
        out = io.StringIO()
        with redirect_stdout(out), redirect_stderr(io.StringIO()):
            self.assertEqual(0, main(list(args)))
        return out.getvalue().splitlines()

    def test_backfill_then_live_with_labels(self):
        with tempfile.TemporaryDirectory() as directory:
            labels_path = os.path.join(directory, "labels.jsonl")
            lines = self.run_cli("--seed", "1", "--start", "2026-11-17T08:00:00Z", "--backfill", "60",
                                 "--duration", "600", "--speed", "0", "--anomaly-rate", "0",
                                 "--inject", "PRESS-02:error_burst:30:20", "--labels-file", labels_path)
            with open(labels_path) as labels:
                truth = [json.loads(label) for label in labels]
        self.assertEqual(1, len(truth))
        # Injection offsets count from the end of the backfill
        self.assertEqual(START + 60 + 30, truth[0]["start"])
        self.assertGreater(sum("|PRESS-02|E|" in line for line in lines), 20)
        self.assertGreater(len(lines), 4000)

    def test_rejects_bad_arguments(self):
        for args in (["--sink", "kinesis"], ["--sink", "file"], ["--kinds", "meltdown"], ["--inject", "nope"]):
            with self.assertRaises(SystemExit), redirect_stderr(io.StringIO()):
                main(args + ["--duration", "1", "--speed", "0"])


class EvaluateTest(unittest.TestCase):

    def test_detections_and_false_alerts(self):
        labels = [{"station": "KILN-01", "kind": "spike", "start": 100.0, "end": 200.0, "metric": "o2"},
                  {"station": "PRESS-01", "kind": "drift", "start": 100.0, "end": 400.0, "metric": None}]
        alerts = [{"stationId": "KILN-01", "windowEnd": 310_000, "summary": "late but within the margin"},
                  {"stationId": "KILN-01", "windowEnd": 90_000, "summary": "before the anomaly"},
                  {"stationId": "PACKER-01", "windowEnd": 150_000, "summary": "other station"}]
        result = evaluate.evaluate(labels, alerts, margin_s=120)
        self.assertEqual({"spike": [1, 1], "drift": [1, 0]}, result["per_kind"])
        self.assertEqual(2, len(result["false_alerts"]))
        self.assertAlmostEqual(0.5, result["recall"])
        self.assertAlmostEqual(1 / 3, result["precision"])


if __name__ == "__main__":
    unittest.main()
