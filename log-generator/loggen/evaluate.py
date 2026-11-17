"""Scores the detector's alerts against the ground truth written by ``--labels-file``.

    python -m loggen.evaluate --labels labels.jsonl --alerts alerts.jsonl [--hours 3]

An injected anomaly counts as detected when the detector alerted on the same station for a
window ending between the anomaly's start and its end plus a margin (the model sees a change
with up to one window and one shingle of delay). Any other alert is a false alert.
"""

from __future__ import annotations

import argparse
import collections
import json
import sys
from typing import Dict, List


def load(path: str) -> List[dict]:
    with open(path, encoding="utf-8") as lines:
        return [json.loads(line) for line in lines if line.strip()]


def evaluate(labels: List[dict], alerts: List[dict], margin_s: float = 120.0) -> dict:
    def matches(alert: dict, label: dict) -> bool:
        return (alert["stationId"] == label["station"]
                and label["start"] * 1000 <= alert["windowEnd"] <= (label["end"] + margin_s) * 1000)

    per_kind: Dict[str, List[int]] = collections.defaultdict(lambda: [0, 0])
    rows = []
    for label in sorted(labels, key=lambda l: l["start"]):
        hit = next((a for a in alerts if matches(a, label)), None)
        per_kind[label["kind"]][0] += 1
        per_kind[label["kind"]][1] += hit is not None
        rows.append((label, hit))
    false_alerts = [a for a in alerts if not any(matches(a, label) for label in labels)]
    detected = sum(hit is not None for _, hit in rows)
    return {
        "rows": rows,
        "per_kind": dict(per_kind),
        "false_alerts": false_alerts,
        "recall": detected / len(labels) if labels else None,
        "precision": (len(alerts) - len(false_alerts)) / len(alerts) if alerts else None,
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="loggen.evaluate", description=__doc__.splitlines()[0])
    parser.add_argument("--labels", required=True, help="ground truth written by loggen --labels-file")
    parser.add_argument("--alerts", required=True, help="alerts as JSON lines (the detector's alert stream)")
    parser.add_argument("--margin", type=float, default=120.0, help="seconds after an anomaly's end an alert still counts")
    parser.add_argument("--hours", type=float, help="monitored hours, to report false alerts per station-hour")
    parser.add_argument("--stations", type=int, default=7, help="number of stations (default: 7)")
    parser.add_argument("--quiet", action="store_true", help="only print the summary")
    args = parser.parse_args(argv)

    result = evaluate(load(args.labels), load(args.alerts), args.margin)
    if not args.quiet:
        for label, hit in result["rows"]:
            what = f"{label['station']:<12} {label['kind']:<12} {label.get('metric') or '':<14}"
            print(f"{what} {'detected: ' + hit['summary'][:90] if hit else 'MISSED'}")
        for alert in result["false_alerts"]:
            print(f"false alert  {alert['summary'][:110]}")
    print("kind          injected  detected")
    for kind, (total, found) in sorted(result["per_kind"].items()):
        print(f"{kind:<13} {total:>8}  {found:>8}")
    recall = result["recall"]
    print(f"recall {recall:.0%}" if recall is not None else "recall n/a", end="  ")
    precision = result["precision"]
    print(f"precision {precision:.0%}" if precision is not None else "precision n/a", end="  ")
    print(f"false alerts {len(result['false_alerts'])}", end="")
    if args.hours:
        print(f" ({len(result['false_alerts']) / (args.hours * args.stations):.2f} per station-hour)")
    else:
        print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
