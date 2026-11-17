"""Drives the plant on a virtual clock and yields (emit_time, source, line) tuples."""

from __future__ import annotations

import heapq
import random
from typing import Iterator, List, Tuple

from . import plant
from .scenarios import Anomaly


class Simulator:
    """Deterministic for a given seed and start time.

    ``emit_time`` can be later than the timestamp printed in the line: real log shippers deliver
    late and out of order, and ``max_delay_s`` reproduces that.
    """

    def __init__(self, stations: List[plant.Station], anomalies: List[Anomaly], start: float, seed: int = 0,
                 rate: float = 1.0, chatter_per_s: float = 0.5, garbage_ratio: float = 0.002,
                 event_ratio: float = 0.04, max_delay_s: float = 2.0):
        self.stations = {s.id: s for s in stations}
        self.anomalies = anomalies
        self.start = start
        self.rng = random.Random(seed)
        self.rate = rate
        self.chatter_per_s = chatter_per_s
        self.garbage_ratio = garbage_ratio
        self.event_ratio = event_ratio
        self.max_delay_s = max_delay_s

    def _apply_anomalies(self, station: plant.Station, now: float) -> None:
        Anomaly.clear(station)
        for anomaly in self.anomalies:
            if anomaly.station == station.id and anomaly.active(now):
                anomaly.apply(station, now)

    def _station_lines(self, station: plant.Station, now: float) -> List[str]:
        self._apply_anomalies(station, now)
        if station.silent:
            return []
        lines = [station.telemetry_line(self.rng, now)]
        if self.rng.random() < self.event_ratio:
            lines.append(station.event_line(self.rng, now))
        if station.error_burst:
            lines.extend(station.error_line(self.rng, now) for _ in range(self.rng.randint(3, 8)))
        return lines

    def run(self, duration_s: float) -> Iterator[Tuple[float, str, str]]:
        end = self.start + duration_s
        # (time, tie-breaker, station id or None for chatter)
        schedule: List[Tuple[float, int, object]] = []
        for index, station in enumerate(self.stations.values()):
            heapq.heappush(schedule, (self.start + self.rng.random() * station.type.period_s, index, station.id))
        if self.chatter_per_s > 0:
            heapq.heappush(schedule, (self.start + self.rng.random(), len(schedule), None))

        # Lines waiting for their (delayed) emission time
        pending: List[Tuple[float, int, str, str]] = []
        sequence = 0

        while schedule:
            now, order, source = heapq.heappop(schedule)
            if now >= end:
                continue

            if source is None:
                lines, origin = [plant.chatter_line(self.rng, now)], "chatter"
                step = self.rng.expovariate(self.chatter_per_s * self.rate)
            else:
                station = self.stations[source]
                lines, origin = self._station_lines(station, now), source
                step = station.type.period_s / self.rate * self.rng.uniform(0.9, 1.1)
            heapq.heappush(schedule, (now + step, order, source))

            for line in lines:
                if self.rng.random() < self.garbage_ratio:
                    line, origin = plant.garbage_line(self.rng), "garbage"
                delay = self.rng.uniform(0, self.max_delay_s) if self.max_delay_s > 0 else 0.0
                sequence += 1
                heapq.heappush(pending, (now + delay, sequence, origin, line))

            # Everything due before the next scheduled event can go out
            horizon = schedule[0][0] if schedule else float("inf")
            while pending and pending[0][0] <= horizon:
                emit_time, _, origin_out, line_out = heapq.heappop(pending)
                yield emit_time, origin_out, line_out

        while pending:
            emit_time, _, origin_out, line_out = heapq.heappop(pending)
            yield emit_time, origin_out, line_out
