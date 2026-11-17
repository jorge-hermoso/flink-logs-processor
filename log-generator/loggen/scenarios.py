"""Anomaly scenarios: what goes wrong on the line, when, and for how long."""

from __future__ import annotations

import random
from dataclasses import asdict, dataclass
from typing import List, Optional

from .plant import Station

#: Scenario kinds and their (min, max) duration in seconds.
KINDS = {
    "spike": (60, 180),         # a reading jumps well away from normal and stays there
    "drift": (300, 600),        # a reading creeps away slowly
    "error_burst": (30, 90),    # the controller floods the log with errors
    "stuck": (180, 420),        # a sensor freezes on its last value
    "silence": (120, 300),      # the station stops logging altogether
}

SPIKE_SIGMAS = 9.0
DRIFT_SIGMAS = 12.0


@dataclass
class Anomaly:
    station: str
    kind: str
    start: float
    end: float
    metric: Optional[str] = None
    direction: int = 1

    def to_label(self) -> dict:
        """Ground truth record, for scoring the detector afterwards."""
        return asdict(self)

    def active(self, now: float) -> bool:
        return self.start <= now < self.end

    def apply(self, station: Station, now: float) -> None:
        """Puts the station in the state this anomaly dictates at time ``now``."""
        if self.kind == "spike":
            station.signal(self.metric).offset_sigmas = self.direction * SPIKE_SIGMAS
        elif self.kind == "drift":
            progress = (now - self.start) / (self.end - self.start)
            station.signal(self.metric).offset_sigmas = self.direction * DRIFT_SIGMAS * progress
        elif self.kind == "stuck":
            station.signal(self.metric).stuck = True
        elif self.kind == "error_burst":
            station.error_burst = True
        elif self.kind == "silence":
            station.silent = True

    @staticmethod
    def clear(station: Station) -> None:
        station.silent = False
        station.error_burst = False
        for signal in station.signals:
            signal.offset_sigmas = 0.0
            signal.stuck = False


def parse_injection(spec: str, stations: List[Station], origin: float, rng: random.Random) -> Anomaly:
    """Parses ``STATION:KIND:START_OFFSET_S:DURATION_S[:METRIC]``, offsets relative to ``origin``."""
    parts = spec.split(":")
    if len(parts) not in (4, 5):
        raise ValueError(f"invalid injection '{spec}': expected STATION:KIND:START_OFFSET_S:DURATION_S[:METRIC]")
    station_id, kind = parts[0].upper(), parts[1]
    station = next((s for s in stations if s.id == station_id), None)
    if station is None:
        raise ValueError(f"invalid injection '{spec}': unknown station, choose from {[s.id for s in stations]}")
    if kind not in KINDS:
        raise ValueError(f"invalid injection '{spec}': unknown kind, choose from {sorted(KINDS)}")
    start = origin + float(parts[2])
    metric = parts[4] if len(parts) == 5 else rng.choice(station.signals).name
    if kind in ("spike", "drift", "stuck"):
        station.signal(metric)  # raises StopIteration for an unknown metric
    else:
        metric = None
    return Anomaly(station_id, kind, start, start + float(parts[3]), metric, rng.choice((-1, 1)))


def random_schedule(stations: List[Station], start: float, end: float, per_station_hour: float,
                    rng: random.Random, kinds=None) -> List[Anomaly]:
    """Poisson arrivals per station, never overlapping on the same station."""
    kinds = list(kinds or KINDS)
    anomalies: List[Anomaly] = []
    if per_station_hour <= 0:
        return anomalies
    for station in stations:
        now = start
        while True:
            now += rng.expovariate(per_station_hour / 3600.0)
            if now >= end:
                break
            kind = rng.choice(kinds)
            duration = rng.uniform(*KINDS[kind])
            metric = rng.choice(station.signals).name if kind in ("spike", "drift", "stuck") else None
            anomalies.append(Anomaly(station.id, kind, now, now + duration, metric, rng.choice((-1, 1))))
            # Leave the station a few quiet minutes to look normal again
            now += duration + 300
    return sorted(anomalies, key=lambda a: a.start)
