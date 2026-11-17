"""Command line entry point: ``python -m loggen --help``."""

from __future__ import annotations

import argparse
import json
import random
import signal
import sys
import time
from datetime import datetime, timezone

from . import __version__, plant, scenarios
from .simulator import Simulator
from .sinks import KinesisSink, StreamSink, kinesis_client


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="loggen",
        description="Generates unstructured production logs for the fictional Zorblatt thermoplasmic "
                    "sprocket line, with labelled anomalies.")
    parser.add_argument("--version", action="version", version=__version__)

    out = parser.add_argument_group("output")
    out.add_argument("--sink", choices=("stdout", "file", "kinesis"), default="stdout")
    out.add_argument("--file", help="output path for --sink file")
    out.add_argument("--stream", help="stream name for --sink kinesis")
    out.add_argument("--region", help="AWS region for --sink kinesis (default: from the environment)")
    out.add_argument("--endpoint-url", help="custom Kinesis endpoint, e.g. http://localhost:4566 for LocalStack")
    out.add_argument("--lines-per-record", type=int, default=1,
                     help="pack up to N lines of one source into one Kinesis record (default: 1)")
    out.add_argument("--labels-file", help="write the injected anomalies (ground truth) to this JSON-lines file")

    sim = parser.add_argument_group("simulation")
    sim.add_argument("--duration", type=float, default=0,
                     help="seconds of plant time to simulate after the backfill; 0 runs until interrupted")
    sim.add_argument("--backfill", type=float, default=0,
                     help="first emit this many seconds of history as fast as possible, so the "
                          "detector's models are trained by the time live data starts")
    sim.add_argument("--speed", type=float, default=1.0,
                     help="plant seconds per wall-clock second; 0 emits as fast as possible (default: 1)")
    sim.add_argument("--rate", type=float, default=1.0, help="multiplier on the number of lines (default: 1)")
    sim.add_argument("--seed", type=int, default=None, help="random seed, for reproducible output")
    sim.add_argument("--start", help="ISO-8601 start of plant time (default: now, minus the backfill)")
    sim.add_argument("--max-delay", type=float, default=2.0,
                     help="lines are delivered up to this many seconds late and out of order (default: 2)")
    sim.add_argument("--garbage-ratio", type=float, default=0.002,
                     help="share of lines replaced by binary garbage (default: 0.002)")
    sim.add_argument("--chatter", type=float, default=0.5,
                     help="lines per second from sources that are not stations (default: 0.5)")

    anomalies = parser.add_argument_group("anomalies")
    anomalies.add_argument("--anomaly-rate", type=float, default=1.0,
                           help="random anomalies per station per hour of live time (default: 1)")
    anomalies.add_argument("--kinds", default=",".join(scenarios.KINDS),
                           help=f"anomaly kinds to draw from (default: {','.join(scenarios.KINDS)})")
    anomalies.add_argument("--inject", action="append", default=[], metavar="STATION:KIND:START_S:DURATION_S[:METRIC]",
                           help="schedule a specific anomaly, START_S seconds after live time begins; repeatable")
    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    seed = args.seed if args.seed is not None else random.SystemRandom().randint(0, 2 ** 31)
    rng = random.Random(seed)

    if args.start:
        start = datetime.fromisoformat(args.start.replace("Z", "+00:00")).astimezone(timezone.utc).timestamp()
    else:
        start = time.time() - args.backfill
    live_start = start + args.backfill
    # "Until interrupted" is simulated as ten years
    live_duration = args.duration if args.duration > 0 else 10 * 365 * 86400.0

    stations = plant.build_stations()
    kinds = [k.strip() for k in args.kinds.split(",") if k.strip()]
    unknown = sorted(set(kinds) - set(scenarios.KINDS))
    if unknown:
        raise SystemExit(f"unknown anomaly kinds {unknown}; choose from {sorted(scenarios.KINDS)}")

    # The backfill is clean on purpose: it is the "normal" the models learn. For an endless run,
    # random anomalies are planned for the first week.
    horizon = live_start + min(live_duration, 7 * 86400.0)
    planned = scenarios.random_schedule(stations, live_start + 60, horizon, args.anomaly_rate, rng, kinds)
    try:
        planned += [scenarios.parse_injection(spec, stations, live_start, rng) for spec in args.inject]
    except (ValueError, StopIteration) as error:
        raise SystemExit(str(error) or "invalid --inject: unknown metric for that station")

    if args.labels_file:
        with open(args.labels_file, "w", encoding="utf-8") as labels:
            for anomaly in sorted(planned, key=lambda a: a.start):
                labels.write(json.dumps(anomaly.to_label()) + "\n")

    if args.sink == "kinesis":
        if not args.stream:
            raise SystemExit("--sink kinesis requires --stream")
        sink = KinesisSink(kinesis_client(args.region, args.endpoint_url), args.stream, args.lines_per_record)
    elif args.sink == "file":
        if not args.file:
            raise SystemExit("--sink file requires --file")
        sink = StreamSink.to_file(args.file)
    else:
        sink = StreamSink()

    print(f"loggen {__version__}: seed={seed} stations={[s.id for s in stations]} "
          f"backfill={args.backfill:.0f}s anomalies={len(planned)}", file=sys.stderr)

    # Finish the batch in flight on SIGTERM (container stop) as on Ctrl-C
    signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(KeyboardInterrupt()))

    simulator = Simulator(stations, planned, start, seed=seed, rate=args.rate, chatter_per_s=args.chatter,
                          garbage_ratio=args.garbage_ratio, max_delay_s=args.max_delay)
    wall_origin = time.time()
    lines = 0
    try:
        for emit_time, source, line in simulator.run(args.backfill + live_duration):
            if emit_time >= live_start and args.speed > 0:
                # Live phase: pace plant time against the wall clock
                wait = wall_origin + (emit_time - live_start) / args.speed - time.time()
                if wait > 0:
                    sink.flush()
                    time.sleep(wait)
            elif emit_time < live_start:
                wall_origin = time.time()
            sink.write(source, line)
            lines += 1
    except (KeyboardInterrupt, BrokenPipeError):
        pass
    finally:
        try:
            sink.close()
        except BrokenPipeError:
            pass
    print(f"loggen: {lines} lines emitted", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
