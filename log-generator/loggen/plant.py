"""The simulated plant: stations, their sensors, and the log dialect each one speaks.

Every station type comes from a different (imaginary) vendor, so every type has its own line
layout and timestamp notation. That is the point: the detector must cope with logs nobody
designed to be parsed together.
"""

from __future__ import annotations

import json
import math
import random
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional


def _utc(ts: float) -> datetime:
    return datetime.fromtimestamp(ts, tz=timezone.utc)


@dataclass
class Signal:
    """A sensor reading: base level + slow wave + autocorrelated noise, plus an injected offset."""

    name: str
    base: float
    sigma: float
    wave_amplitude: float = 0.0
    wave_period_s: float = 900.0
    decimals: int = 1
    _noise: float = field(default=0.0, repr=False)
    _frozen: Optional[float] = field(default=None, repr=False)
    #: Set by anomaly scenarios, in units of sigma.
    offset_sigmas: float = 0.0
    stuck: bool = False

    def read(self, rng: random.Random, now: float) -> float:
        if self.stuck and self._frozen is not None:
            return self._frozen
        # AR(1) noise: consecutive readings are correlated, like a real process variable
        self._noise = 0.8 * self._noise + rng.gauss(0.0, self.sigma * 0.6)
        wave = self.wave_amplitude * math.sin(2 * math.pi * now / self.wave_period_s)
        value = round(self.base + wave + self._noise + self.offset_sigmas * self.sigma, self.decimals)
        self._frozen = value
        return value


#: Renders one line. Arguments: station, timestamp, level (info|warn|error), message.
Formatter = Callable[["Station", float, str, str], str]


def _fmt_mixer(station: "Station", ts: float, level: str, message: str) -> str:
    # ISO-8601 with millis, logfmt-like body
    stamp = _utc(ts).strftime("%Y-%m-%dT%H:%M:%S.") + f"{int(ts * 1000) % 1000:03d}Z"
    lvl = {"info": "INFO", "warn": "WARN", "error": "ERROR"}[level]
    return f"{stamp} {lvl:<5} mixer-ctl {station.id} {message}"


def _fmt_extruder(station: "Station", ts: float, level: str, message: str) -> str:
    # Bracketed local-looking time with a comma before the millis and no zone
    stamp = _utc(ts).strftime("%Y-%m-%d %H:%M:%S,") + f"{int(ts * 1000) % 1000:03d}"
    lvl = {"info": "INFO", "warn": "WARNING", "error": "ERROR"}[level]
    return f"[{stamp}] {station.id} | {lvl} | {message}"


def _fmt_kiln(station: "Station", ts: float, level: str, message: str) -> str:
    # Classic syslog: no year, no millis, lower-case host
    when = _utc(ts)
    stamp = f"{when.strftime('%b')} {when.day:>2} {when.strftime('%H:%M:%S')}"
    lvl = {"info": "<info>", "warn": "<warn>", "error": "<err>"}[level]
    return f"{stamp} {station.id.lower()} kilnd[{station.pid}]: {lvl} {message}"


def _fmt_press(station: "Station", ts: float, level: str, message: str) -> str:
    # Epoch millis, pipe separated, one-letter level
    lvl = {"info": "I", "warn": "W", "error": "E"}[level]
    return f"{int(ts * 1000)}|{station.id}|{lvl}|{message}"


def _fmt_packer(station: "Station", ts: float, level: str, message: str) -> str:
    # JSON envelope around a free-text message
    stamp = _utc(ts).strftime("%Y-%m-%dT%H:%M:%S.") + f"{int(ts * 1000) % 1000:03d}Z"
    return json.dumps({"ts": stamp, "unit": station.id, "lvl": level, "msg": message}, separators=(",", ":"))


@dataclass
class StationType:
    name: str
    formatter: Formatter
    #: Seconds between telemetry lines.
    period_s: float
    make_signals: Callable[[], List[Signal]]
    #: Renders the telemetry message from the readings, keyed by signal name.
    telemetry: Callable[["Station", Dict[str, float]], str]
    #: (level, message) pairs logged now and then during normal operation.
    events: List[tuple]
    #: Messages logged in bulk during an error burst.
    errors: List[str]


@dataclass
class Station:
    id: str
    type: StationType
    signals: List[Signal]
    pid: int
    counter: int = 0
    silent: bool = False
    error_burst: bool = False
    next_telemetry: float = 0.0

    def signal(self, name: str) -> Signal:
        return next(s for s in self.signals if s.name == name)

    def telemetry_line(self, rng: random.Random, now: float) -> str:
        self.counter += 1
        readings = {s.name: s.read(rng, now) for s in self.signals}
        # A reading well off its base makes the controller itself complain
        level = "warn" if any(abs(s.offset_sigmas) >= 4 for s in self.signals) else "info"
        return self.type.formatter(self, now, level, self.type.telemetry(self, readings))

    def event_line(self, rng: random.Random, now: float) -> str:
        level, message = rng.choice(self.type.events)
        return self.type.formatter(self, now, level, message.format(n=rng.randint(1, 9), c=self.counter))

    def error_line(self, rng: random.Random, now: float) -> str:
        return self.type.formatter(self, now, "error", rng.choice(self.type.errors).format(n=rng.randint(1, 9)))


STATION_TYPES: Dict[str, StationType] = {
    "MIXER": StationType(
        "MIXER", _fmt_mixer, 1.0,
        lambda: [Signal("visc", 1520.0, 12.0, 25.0, 1200.0), Signal("temp", 64.0, 0.4, 1.0, 1800.0),
                 Signal("agitator_rpm", 118.0, 1.0, 0.0, 900.0, 0)],
        lambda st, r: (f"batch=B{st.counter // 600:05d} visc={r['visc']}cP temp={r['temp']}C "
                       f"agitator_rpm={int(r['agitator_rpm'])}"),
        [("info", "batch=B{c} additive dose complete hopper={n}"), ("info", "lid interlock closed"),
         ("warn", "slurry level low in hopper {n}, refill requested")],
        ["agitator overcurrent trip on phase {n}", "viscometer probe fault code E{n}7", "slurry feed pump stalled"],
    ),
    "EXTRUDER": StationType(
        "EXTRUDER", _fmt_extruder, 1.0,
        lambda: [Signal("melt pressure", 142.0, 1.5, 3.0, 600.0), Signal("screw torque", 81.0, 0.9, 1.5, 600.0),
                 Signal("barrel temp", 221.0, 0.6, 1.0, 1500.0)],
        lambda st, r: (f"melt pressure {r['melt pressure']} bar, screw torque {r['screw torque']} Nm, "
                       f"barrel temp {r['barrel temp']} C"),
        [("info", "die head purge cycle finished"), ("info", "pellet feeder speed adjusted by operator"),
         ("warn", "heater band {n} duty cycle high")],
        ["screw jam detected, drive disabled", "melt filter differential pressure alarm", "heater band {n} open circuit"],
    ),
    "KILN": StationType(
        "KILN", _fmt_kiln, 2.0,
        lambda: [Signal("zone temp", 842.0, 1.2, 3.0, 2400.0), Signal("o2", 3.1, 0.05, 0.1, 1200.0, 2),
                 Signal("belt", 1.20, 0.01, 0.0, 900.0, 2)],
        lambda st, r: (f"zone temp reading {r['zone temp']}C (setpoint 840.0C) o2={r['o2']}% "
                       f"belt {r['belt']} m/min"),
        [("info", "damper position adjusted"), ("info", "tray {c} entered curing zone"),
         ("warn", "burner {n} flame signal weak")],
        ["burner {n} flame-out, purge started", "thermocouple TC-{n} open circuit", "exhaust fan vibration trip"],
    ),
    "PRESS": StationType(
        "PRESS", _fmt_press, 1.5,
        lambda: [Signal("peak_force", 312.0, 2.0, 0.0, 900.0), Signal("cycle_time", 4.20, 0.03, 0.05, 700.0, 2),
                 Signal("hyd_press", 182.0, 0.8, 1.0, 1100.0)],
        lambda st, r: (f"cycle {st.counter} done; peak_force={r['peak_force']}kN "
                       f"cycle_time={r['cycle_time']}s hyd_press={r['hyd_press']}bar"),
        [("info", "die lubrication pulse"), ("info", "light curtain test passed"),
         ("warn", "ram return slow on cycle {c}")],
        ["hydraulic oil overtemperature", "die misalignment detected, cycle aborted", "light curtain breach, e-stop"],
    ),
    "PACKER": StationType(
        "PACKER", _fmt_packer, 2.0,
        lambda: [Signal("rate", 42.0, 0.5, 1.0, 800.0), Signal("rejects", 0.8, 0.08, 0.1, 1300.0, 2),
                 Signal("vacuum", -61.0, 0.4, 0.5, 1000.0)],
        lambda st, r: f"rate {r['rate']} units/min, rejects {r['rejects']} %, vacuum {r['vacuum']} kPa",
        [("info", "carton magazine refilled"), ("info", "label roll change acknowledged"),
         ("warn", "gripper {n} pick retry")],
        ["vacuum loss on gripper {n}", "carton jam at infeed", "label printer offline"],
    ),
}

#: The default line-up: (station type, number of stations).
DEFAULT_LAYOUT = [("MIXER", 1), ("EXTRUDER", 2), ("KILN", 1), ("PRESS", 2), ("PACKER", 1)]


def build_stations(layout=None) -> List[Station]:
    stations = []
    for type_name, count in layout or DEFAULT_LAYOUT:
        station_type = STATION_TYPES[type_name]
        for index in range(1, count + 1):
            stations.append(Station(f"{type_name}-{index:02d}", station_type, station_type.make_signals(),
                                    pid=400 + 7 * len(stations)))
    return stations


# Lines that do not belong to any station: the detector has to ignore them.
_CHATTER = [
    lambda rng, ts: f"{_utc(ts).strftime('%Y-%m-%dT%H:%M:%SZ')} INFO scada-gw heartbeat ok seq={int(ts) % 100000}",
    lambda rng, ts: f"{_utc(ts).strftime('%Y-%m-%d %H:%M:%S')} mes-bridge: synced {rng.randint(1, 40)} work orders in {rng.randint(20, 900)}ms",
    lambda rng, ts: f"{_utc(ts).strftime('%H:%M:%S')} hmi operator={rng.choice(['mkowalski', 'tnguyen', 'afolabi'])} action=ack_banner screen=overview",
    lambda rng, ts: (f"{_utc(ts).strftime('%Y-%m-%dT%H:%M:%SZ')} WARN historian write queue depth {rng.randint(100, 5000)}\n"
                     "    at com.zorblatt.historian.Writer.flush(Writer.java:214)\n"
                     "    at com.zorblatt.historian.Writer.run(Writer.java:98)"),
    lambda rng, ts: "---- shift handover: line running, no open deviations ----",
]

# Broken input: the detector must survive it and route it to the dead-letter stream.
_GARBAGE = [
    lambda rng: "".join(chr(rng.randint(0, 31)) for _ in range(rng.randint(8, 40))),
    lambda rng: "�" * rng.randint(5, 30),
    lambda rng: "\x00\x00\x1b[2J\x07\x07" + "\x01" * rng.randint(3, 12),
]


def chatter_line(rng: random.Random, now: float) -> str:
    return rng.choice(_CHATTER)(rng, now)


def garbage_line(rng: random.Random) -> str:
    return rng.choice(_GARBAGE)(rng)
