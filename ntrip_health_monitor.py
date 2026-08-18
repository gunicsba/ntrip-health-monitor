#!/usr/bin/env python3
"""
NTRIP RTCM Health Monitor
=========================

Standalone monitoring/audit tool for an NTRIP caster.

Main outputs (one timestamped directory per run):
  - sourcetable.csv
  - streams.csv
  - observations.csv       (satellite/signal-level CNR samples)
  - summary.csv            (base x constellation summary)
  - report_data.json
  - report.html            (interactive map + charts)

Python dependency:
    python -m pip install --upgrade pyrtcm

Example:
    python ntrip_health_monitor.py ^
        --host crtk.net ^
        --port 2101 ^
        --username Gunics ^
        --password "YOUR_PASSWORD" ^
        --workers 5 ^
        --min-seconds 5 ^
        --max-seconds 20 ^
        --min-epochs 3

Filters:
    --country "HU,SK,!FR"
    --stream "BUD*,PEST*,!TEST*"

Notes:
- "!" means exclusion.
- Wildcards (*, ?) are supported.
- The output directory is always unique, so open CSV files never collide
  with a later run.
"""

from __future__ import annotations

import argparse
import base64
import csv
import fnmatch
import html
import json
import math
import os
import re
import socket
import statistics
import sys
import threading
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from pyrtcm import RTCMReader, ERR_IGNORE


VERSION = "0.5.0"

MSM_CONSTELLATIONS = {
    "107": "GPS",
    "108": "GLONASS",
    "109": "GALILEO",
    "110": "SBAS",
    "111": "QZSS",
    "112": "BEIDOU",
    "113": "NAVIC",
}

CONSTELLATION_ORDER = [
    "GPS", "GLONASS", "GALILEO", "BEIDOU", "QZSS", "SBAS", "NAVIC"
]

EPOCH_FIELDS = {
    "GPS": "DF004",
    "GLONASS": "DF034",
    "GALILEO": "DF248",
    "SBAS": "DF004",
    "QZSS": "DF428",
    "BEIDOU": "DF427",
    "NAVIC": "DF546",
}


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class SourceTableEntry:
    mountpoint: str
    identifier: str = ""
    format: str = ""
    format_details: str = ""
    carrier: str = ""
    nav_system: str = ""
    network: str = ""
    country: str = ""
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    nmea: str = ""
    solution: str = ""
    generator: str = ""
    compression: str = ""
    authentication: str = ""
    fee: str = ""
    bitrate: Optional[int] = None
    misc: str = ""
    receiver_type: str = ""
    antenna_type: str = ""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def timestamp_for_path() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]


def safe_float(v: Any) -> Optional[float]:
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def safe_int(v: Any) -> Optional[int]:
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def finite_or_none(v: Any) -> Optional[float]:
    try:
        x = float(v)
        return x if math.isfinite(x) else None
    except (TypeError, ValueError):
        return None


def median_or_none(values: list[float]) -> Optional[float]:
    return statistics.median(values) if values else None


def mean_or_none(values: list[float]) -> Optional[float]:
    return statistics.fmean(values) if values else None


def percentile(values: list[float], p: float) -> Optional[float]:
    """Simple linear interpolated percentile; p in [0, 1]."""
    if not values:
        return None
    xs = sorted(values)
    if len(xs) == 1:
        return xs[0]
    k = (len(xs) - 1) * p
    lo = math.floor(k)
    hi = math.ceil(k)
    if lo == hi:
        return xs[lo]
    return xs[lo] * (hi - k) + xs[hi] * (k - lo)


def json_safe(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {str(k): json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [json_safe(v) for v in obj]
    if isinstance(obj, tuple):
        return [json_safe(v) for v in obj]
    if isinstance(obj, set):
        return sorted(json_safe(v) for v in obj)
    if isinstance(obj, float) and not math.isfinite(obj):
        return None
    return obj


# ---------------------------------------------------------------------------
# Filters
# ---------------------------------------------------------------------------

def parse_filter(expression: Optional[str]) -> tuple[list[str], list[str]]:
    includes: list[str] = []
    excludes: list[str] = []
    if not expression:
        return includes, excludes

    for raw in expression.split(","):
        item = raw.strip()
        if not item:
            continue
        if item.startswith("!"):
            if len(item) > 1:
                excludes.append(item[1:])
        else:
            includes.append(item)
    return includes, excludes


def match_filter(value: str, expression: Optional[str]) -> bool:
    if not expression:
        return True

    includes, excludes = parse_filter(expression)
    v = (value or "").upper()

    def matches(pattern: str) -> bool:
        return fnmatch.fnmatchcase(v, pattern.upper())

    if any(matches(p) for p in excludes):
        return False

    return True if not includes else any(matches(p) for p in includes)


# ---------------------------------------------------------------------------
# NTRIP / sourcetable
# ---------------------------------------------------------------------------

def auth_header(username: str, password: str) -> str:
    raw = f"{username}:{password}".encode("utf-8")
    return "Basic " + base64.b64encode(raw).decode("ascii")


def recv_until_header_end(sock: socket.socket, max_bytes: int = 131072) -> tuple[bytes, bytes]:
    """Return (header, already-read body)."""
    buf = bytearray()
    marker = b"\r\n\r\n"

    while marker not in buf:
        chunk = sock.recv(4096)
        if not chunk:
            break
        buf.extend(chunk)
        if len(buf) > max_bytes:
            raise RuntimeError("NTRIP/HTTP response headers exceeded limit.")

    pos = buf.find(marker)
    if pos >= 0:
        return bytes(buf[:pos]), bytes(buf[pos + len(marker):])

    # Some old NTRIP responses can be non-standard.
    return bytes(buf), b""


def response_ok(header: bytes) -> bool:
    text = header.decode("latin-1", errors="replace")
    line = text.splitlines()[0] if text else ""
    upper = line.upper()
    return (
        "200" in upper
        or upper.startswith("ICY 200")
        or upper.startswith("SOURCETABLE 200")
    )


def parse_sourcetable(text: str) -> list[SourceTableEntry]:
    result: list[SourceTableEntry] = []

    for line in text.splitlines():
        line = line.strip()
        if not line.startswith("STR;"):
            continue

        f = line.split(";")
        if len(f) < 2:
            continue

        def at(i: int, default: str = "") -> str:
            return f[i].strip() if i < len(f) else default

        result.append(
            SourceTableEntry(
                mountpoint=at(1),
                identifier=at(2),
                format=at(3),
                format_details=at(4),
                carrier=at(5),
                nav_system=at(6),
                network=at(7),
                country=at(8).upper(),
                latitude=safe_float(at(9)),
                longitude=safe_float(at(10)),
                nmea=at(11),
                solution=at(12),
                generator=at(13),
                compression=at(14),
                authentication=at(15),
                fee=at(16),
                bitrate=safe_int(at(17)),
                misc=at(18),
            )
        )
    return result


def infer_receiver_antenna(entry: SourceTableEntry) -> tuple[str, str]:
    """
    Best-effort extraction from sourcetable metadata.

    NTRIP sourcetables do not have standardized dedicated receiver/antenna
    columns. Some casters expose this information in generator/misc fields.
    We keep both values as metadata and never use them in the health score.
    """
    text = " | ".join(
        x for x in [
            entry.generator or "",
            entry.misc or "",
            entry.identifier or "",
            entry.format_details or "",
        ] if x
    )

    receiver = ""
    antenna = ""

    # Common free-text patterns.
    rx_patterns = [
        r"(?:receiver|recv|rx)\s*[:=]\s*([^|;,]+)",
        r"\b(Trimble\s+[A-Za-z0-9._+-]+)",
        r"\b(Septentrio\s+[A-Za-z0-9._+-]+)",
        r"\b(Leica\s+[A-Za-z0-9._+-]+)",
        r"\b(Topcon\s+[A-Za-z0-9._+-]+)",
        r"\b(u-blox\s+[A-Za-z0-9._+-]+)",
    ]
    ant_patterns = [
        r"(?:antenna|ant)\s*[:=]\s*([^|;,]+)",
    ]

    for pat in rx_patterns:
        m = re.search(pat, text, flags=re.I)
        if m:
            receiver = m.group(1).strip()
            break

    for pat in ant_patterns:
        m = re.search(pat, text, flags=re.I)
        if m:
            antenna = m.group(1).strip()
            break

    return receiver, antenna


def fetch_sourcetable(
    host: str,
    port: int,
    username: str,
    password: str,
    timeout: float,
) -> list[SourceTableEntry]:

    request = (
        "GET / HTTP/1.0\r\n"
        f"Host: {host}:{port}\r\n"
        f"User-Agent: NTRIP ntrip-health-monitor/{VERSION}\r\n"
        "Ntrip-Version: Ntrip/2.0\r\n"
        f"Authorization: {auth_header(username, password)}\r\n"
        "Connection: close\r\n"
        "\r\n"
    ).encode("ascii")

    with socket.create_connection((host, port), timeout=timeout) as sock:
        sock.settimeout(timeout)
        sock.sendall(request)

        header, initial = recv_until_header_end(sock)

        # If response is a standard HTTP/NTRIP response, body follows headers.
        # If the server returns sourcetable without normal headers, include header
        # bytes in the candidate text as well.
        chunks = [initial]
        while True:
            try:
                chunk = sock.recv(65536)
            except socket.timeout:
                break
            if not chunk:
                break
            chunks.append(chunk)

    body = b"".join(chunks)
    header_text = header.decode("latin-1", errors="replace")

    if response_ok(header):
        text = body.decode("latin-1", errors="replace")
    elif "STR;" in header_text:
        text = (header + b"\r\n\r\n" + body).decode("latin-1", errors="replace")
    else:
        first = header_text.splitlines()[0] if header_text else "(empty response)"
        raise RuntimeError(f"Could not retrieve sourcetable: {first}")

    entries = parse_sourcetable(text)
    if not entries:
        raise RuntimeError("Caster response contained no STR sourcetable entries.")

    for entry in entries:
        entry.receiver_type, entry.antenna_type = infer_receiver_antenna(entry)

    return entries


class BufferedSocketStream:
    """read(n)->bytes adapter accepted by RTCMReader."""

    def __init__(self, sock: socket.socket, initial: bytes = b""):
        self.sock = sock
        self.buffer = bytearray(initial)

    def read(self, size: int = 1) -> bytes:
        if size <= 0:
            return b""

        while len(self.buffer) < size:
            chunk = self.sock.recv(max(4096, size - len(self.buffer)))
            if not chunk:
                break
            self.buffer.extend(chunk)

        out = bytes(self.buffer[:size])
        del self.buffer[:size]
        return out


def open_mountpoint(
    host: str,
    port: int,
    mountpoint: str,
    username: str,
    password: str,
    timeout: float,
) -> tuple[socket.socket, BufferedSocketStream]:

    sock = socket.create_connection((host, port), timeout=timeout)
    sock.settimeout(timeout)

    mp = mountpoint.lstrip("/")
    request = (
        f"GET /{mp} HTTP/1.0\r\n"
        f"Host: {host}:{port}\r\n"
        f"User-Agent: NTRIP ntrip-health-monitor/{VERSION}\r\n"
        "Ntrip-Version: Ntrip/2.0\r\n"
        f"Authorization: {auth_header(username, password)}\r\n"
        "Connection: close\r\n"
        "\r\n"
    ).encode("ascii")

    sock.sendall(request)
    header, initial = recv_until_header_end(sock)

    if not response_ok(header):
        first = header.decode("latin-1", errors="replace").splitlines()
        first = first[0] if first else "(empty response)"
        sock.close()
        raise RuntimeError(f"NTRIP connection failed: {first}")

    return sock, BufferedSocketStream(sock, initial)


# ---------------------------------------------------------------------------
# RTCM / MSM extraction
# ---------------------------------------------------------------------------

def constellation_from_identity(identity: str) -> Optional[str]:
    return MSM_CONSTELLATIONS.get(identity[:3]) if len(identity) >= 3 else None


def is_msm_identity(identity: str) -> bool:
    if len(identity) != 4 or not identity.isdigit():
        return False
    return identity[:3] in MSM_CONSTELLATIONS and identity[-1] in "1234567"


def extract_epoch(msg: Any, constellation: str) -> Optional[str]:
    attr = EPOCH_FIELDS.get(constellation)
    if attr and hasattr(msg, attr):
        return str(getattr(msg, attr))
    return None


def extract_msm_cells(msg: Any, constellation: str, identity: str) -> list[dict[str, Any]]:
    """
    Return one row per MSM cell (satellite x signal).
    CNR is available in MSM4/5 (DF403) and MSM6/7 (DF408).
    """
    rows: list[dict[str, Any]] = []
    ncell = safe_int(getattr(msg, "NCell", 0)) or 0
    epoch = extract_epoch(msg, constellation)

    for i in range(1, ncell + 1):
        suffix = f"_{i:02d}"

        prn = getattr(msg, f"CELLPRN{suffix}", None)
        signal = getattr(msg, f"CELLSIG{suffix}", None)

        cnr = None
        cnr_field = None

        # MSM6/7 high-resolution CNR
        if hasattr(msg, f"DF408{suffix}"):
            cnr = finite_or_none(getattr(msg, f"DF408{suffix}"))
            cnr_field = "DF408"
        # MSM4/5 standard-resolution CNR
        elif hasattr(msg, f"DF403{suffix}"):
            cnr = finite_or_none(getattr(msg, f"DF403{suffix}"))
            cnr_field = "DF403"

        rows.append({
            "constellation": constellation,
            "message_type": identity,
            "epoch": epoch,
            "prn": str(prn) if prn is not None else "",
            "signal": str(signal) if signal is not None else "",
            "cnr_dbhz": cnr,
            "cnr_field": cnr_field or "",
        })

    return rows


# ---------------------------------------------------------------------------
# Sampling
# ---------------------------------------------------------------------------

def sample_mountpoint(
    entry: SourceTableEntry,
    host: str,
    port: int,
    username: str,
    password: str,
    min_duration: float,
    max_duration: float,
    min_epochs: int,
    timeout: float,
) -> dict[str, Any]:

    started_iso = utc_iso()
    t0 = time.monotonic()

    sock: Optional[socket.socket] = None
    message_types: Counter[str] = Counter()
    epoch_sets: dict[str, set[str]] = defaultdict(set)
    sat_sets_latest: dict[str, set[str]] = defaultdict(set)
    sat_sets_union: dict[str, set[str]] = defaultdict(set)
    observations: list[dict[str, Any]] = []

    messages_received = 0
    bytes_received = 0
    error: Optional[str] = None

    try:
        sock, stream = open_mountpoint(
            host, port, entry.mountpoint, username, password, timeout
        )

        reader = RTCMReader(
            stream,
            quitonerror=ERR_IGNORE,
            labelmsm=1,  # RINEX signal labels, e.g. 1C, 2W, 5Q
        )

        while True:
            elapsed = time.monotonic() - t0

            if elapsed >= max_duration:
                break

            if elapsed >= min_duration and epoch_sets:
                # "Complete enough": each constellation we've actually observed
                # has at least min_epochs unique MSM epochs.
                if min(len(v) for v in epoch_sets.values()) >= min_epochs:
                    break

            try:
                raw, msg = reader.read()
            except socket.timeout:
                continue
            except Exception as exc:
                error = f"{type(exc).__name__}: {exc}"
                break

            if raw is None:
                break

            bytes_received += len(raw)

            if msg is None:
                continue

            messages_received += 1
            identity = str(getattr(msg, "identity", ""))
            if identity:
                message_types[identity] += 1

            if not is_msm_identity(identity):
                continue

            constellation = constellation_from_identity(identity)
            if not constellation:
                continue

            epoch = extract_epoch(msg, constellation)
            if epoch is not None:
                epoch_sets[constellation].add(epoch)

            cells = extract_msm_cells(msg, constellation, identity)
            if cells:
                sat_this_message = {
                    r["prn"] for r in cells if r["prn"]
                }
                sat_sets_latest[constellation] = sat_this_message
                sat_sets_union[constellation].update(sat_this_message)

                received_at = utc_iso()
                elapsed_now = round(time.monotonic() - t0, 3)

                for r in cells:
                    r.update({
                        "timestamp": received_at,
                        "elapsed_seconds": elapsed_now,
                        "mountpoint": entry.mountpoint,
                        "country": entry.country,
                        "latitude": entry.latitude,
                        "longitude": entry.longitude,
                    })
                    observations.append(r)

    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"

    finally:
        if sock is not None:
            try:
                sock.close()
            except Exception:
                pass

    duration = round(time.monotonic() - t0, 3)

    constellations = sorted(
        set(epoch_sets) | set(sat_sets_union),
        key=lambda x: CONSTELLATION_ORDER.index(x)
        if x in CONSTELLATION_ORDER else 999
    )

    complete = bool(epoch_sets) and all(
        len(epoch_sets[c]) >= min_epochs for c in epoch_sets
    )

    stream_summary: dict[str, Any] = {
        "timestamp": started_iso,
        "mountpoint": entry.mountpoint,
        "identifier": entry.identifier,
        "country": entry.country,
        "latitude": entry.latitude,
        "longitude": entry.longitude,
        "format": entry.format,
        "format_details": entry.format_details,
        "nav_system": entry.nav_system,
        "network": entry.network,
        "generator": entry.generator,
        "receiver_type": entry.receiver_type,
        "antenna_type": entry.antenna_type,
        "bitrate": entry.bitrate,
        "duration_seconds": duration,
        "bytes_received": bytes_received,
        "messages_received": messages_received,
        "message_types": dict(sorted(message_types.items())),
        "constellations_observed": constellations,
        "complete_sample": complete,
        "error": error,
        "observations": observations,
    }

    return stream_summary


# ---------------------------------------------------------------------------
# Summary calculations
# ---------------------------------------------------------------------------

def make_base_constellation_summary(
    stream_results: list[dict[str, Any]],
) -> list[dict[str, Any]]:

    rows: list[dict[str, Any]] = []

    for stream in stream_results:
        by_const: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for obs in stream.get("observations", []):
            by_const[obs["constellation"]].append(obs)

        if not by_const:
            rows.append({
                "mountpoint": stream["mountpoint"],
                "country": stream["country"],
                "latitude": stream["latitude"],
                "longitude": stream["longitude"],
                "receiver_type": stream.get("receiver_type", ""),
                "antenna_type": stream.get("antenna_type", ""),
                "constellation": "",
                "satellite_count": 0,
                "signal_cell_count": 0,
                "cnr_sample_count": 0,
                "cnr_mean": None,
                "cnr_median": None,
                "cnr_p10": None,
                "cnr_p90": None,
                "cnr_min": None,
                "cnr_max": None,
                "epochs_seen": 0,
                "complete_sample": stream["complete_sample"],
                "error": stream["error"],
            })
            continue

        for constellation, obs_rows in by_const.items():
            # For satellite count, use the maximum unique PRNs seen in a single
            # epoch, not union across the whole sample (union can overcount when
            # sats rise/set during longer sampling).
            sats_by_epoch: dict[str, set[str]] = defaultdict(set)
            all_signals: set[tuple[str, str]] = set()
            cnr: list[float] = []

            for o in obs_rows:
                epoch = o.get("epoch") or ""
                prn = o.get("prn") or ""
                sig = o.get("signal") or ""
                if prn:
                    sats_by_epoch[epoch].add(prn)
                if prn or sig:
                    all_signals.add((prn, sig))
                if o.get("cnr_dbhz") is not None:
                    cnr.append(float(o["cnr_dbhz"]))

            satellite_count = max(
                (len(sats) for sats in sats_by_epoch.values()),
                default=len({o["prn"] for o in obs_rows if o.get("prn")})
            )

            rows.append({
                "mountpoint": stream["mountpoint"],
                "country": stream["country"],
                "latitude": stream["latitude"],
                "longitude": stream["longitude"],
                "receiver_type": stream.get("receiver_type", ""),
                "antenna_type": stream.get("antenna_type", ""),
                "constellation": constellation,
                "satellite_count": satellite_count,
                "signal_cell_count": len(all_signals),
                "cnr_sample_count": len(cnr),
                "cnr_mean": mean_or_none(cnr),
                "cnr_median": median_or_none(cnr),
                "cnr_p10": percentile(cnr, 0.10),
                "cnr_p90": percentile(cnr, 0.90),
                "cnr_min": min(cnr) if cnr else None,
                "cnr_max": max(cnr) if cnr else None,
                "epochs_seen": len({o.get("epoch") for o in obs_rows if o.get("epoch")}),
                "complete_sample": stream["complete_sample"],
                "error": stream["error"],
            })

    return rows


def add_country_reference(summary_rows: list[dict[str, Any]]) -> None:
    grouped_sat: dict[tuple[str, str], list[float]] = defaultdict(list)
    grouped_cnr: dict[tuple[str, str], list[float]] = defaultdict(list)

    for r in summary_rows:
        country = r.get("country") or ""
        const = r.get("constellation") or ""
        if not const or r.get("error"):
            continue

        grouped_sat[(country, const)].append(float(r["satellite_count"]))

        if r.get("cnr_median") is not None:
            grouped_cnr[(country, const)].append(float(r["cnr_median"]))

    for r in summary_rows:
        key = (r.get("country") or "", r.get("constellation") or "")
        sat_ref = median_or_none(grouped_sat.get(key, []))
        cnr_ref = median_or_none(grouped_cnr.get(key, []))

        r["country_satellite_median"] = sat_ref
        r["satellite_delta_vs_country"] = (
            r["satellite_count"] - sat_ref
            if sat_ref is not None else None
        )

        r["country_cnr_median"] = cnr_ref
        r["cnr_delta_vs_country"] = (
            r["cnr_median"] - cnr_ref
            if r.get("cnr_median") is not None and cnr_ref is not None
            else None
        )



def compute_health_scores(summary_rows: list[dict[str, Any]]) -> None:
    """
    Relative diagnostic score, 0..100, normalized inside each
    country x constellation peer group from the same run.

    Design:
      - A station exactly at the peer median starts at 80 ("normal/good").
      - Being somewhat above the peer group can lift the score toward 100.
      - Falling below the peer group is penalized more strongly.
      - Satellite count: 55%
      - Median CNR:      40%
      - Sample quality:   5%

    This is deliberately a health/ranking indicator, NOT an absolute RTK
    usability verdict. Thresholds should be tuned after observing the network.
    """

    def sat_component(delta: Optional[float]) -> float:
        if delta is None:
            return 65.0
        d = float(delta)
        # Median = 80. Below median costs 12 points/satellite.
        # Above median earns only 5 points/satellite.
        score = 80.0 + (d * (12.0 if d < 0 else 5.0))
        return max(0.0, min(100.0, score))

    def cnr_component(delta: Optional[float]) -> float:
        if delta is None:
            return 65.0
        d = float(delta)
        # Median = 80. Below median costs 5 points/dB-Hz.
        # Above median earns 2.5 points/dB-Hz.
        score = 80.0 + (d * (5.0 if d < 0 else 2.5))
        return max(0.0, min(100.0, score))

    for r in summary_rows:
        if not r.get("constellation"):
            r["health_score"] = None
            r["health_label"] = "NO DATA"
            continue

        if r.get("error"):
            r["health_score"] = 0.0
            r["health_label"] = "ERROR"
            continue

        sat = sat_component(r.get("satellite_delta_vs_country"))
        cnr = cnr_component(r.get("cnr_delta_vs_country"))
        sample = 100.0 if r.get("complete_sample") else 35.0

        score = round(
            max(0.0, min(100.0, 0.55 * sat + 0.40 * cnr + 0.05 * sample)),
            1,
        )

        if score < 20:
            label = "CRITICAL"
        elif score < 40:
            label = "POOR"
        elif score < 60:
            label = "SUSPECT"
        elif score < 75:
            label = "FAIR"
        elif score < 90:
            label = "GOOD"
        else:
            label = "EXCELLENT"

        r["health_score"] = score
        r["health_label"] = label


def compute_base_health(summary_rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """
    Aggregate constellation scores into one base-level health score.

    We use a conservative blend:
      60% mean constellation health
      40% weakest constellation health

    Therefore one clearly bad constellation matters, but does not by itself
    completely dominate an otherwise healthy multi-GNSS station.
    """
    grouped: dict[str, list[float]] = defaultdict(list)

    for r in summary_rows:
        score = r.get("health_score")
        if r.get("constellation") and score is not None:
            grouped[r["mountpoint"]].append(float(score))

    out: dict[str, dict[str, Any]] = {}

    for mountpoint, scores in grouped.items():
        avg = statistics.fmean(scores)
        weakest = min(scores)
        score = round(0.60 * avg + 0.40 * weakest, 1)

        if score < 20:
            label = "CRITICAL"
        elif score < 40:
            label = "POOR"
        elif score < 60:
            label = "SUSPECT"
        elif score < 75:
            label = "FAIR"
        elif score < 90:
            label = "GOOD"
        else:
            label = "EXCELLENT"

        out[mountpoint] = {
            "base_health_score": score,
            "base_health_label": label,
            "mean_constellation_health": round(avg, 1),
            "weakest_constellation_health": round(weakest, 1),
            "constellations_scored": len(scores),
        }

    return out


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0088
    a1, a2 = math.radians(lat1), math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)

    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(a1) * math.cos(a2) * math.sin(dlon / 2) ** 2
    )
    return 2 * r * math.asin(min(1.0, math.sqrt(a)))


def nearest_neighbors(
    bases: list[dict[str, Any]],
    count: int,
) -> dict[str, list[dict[str, Any]]]:

    valid = [
        b for b in bases
        if b.get("latitude") is not None and b.get("longitude") is not None
    ]

    result: dict[str, list[dict[str, Any]]] = {}

    for base in valid:
        distances = []
        for other in valid:
            if other["mountpoint"] == base["mountpoint"]:
                continue
            d = haversine_km(
                float(base["latitude"]), float(base["longitude"]),
                float(other["latitude"]), float(other["longitude"])
            )
            distances.append({
                "mountpoint": other["mountpoint"],
                "country": other.get("country", ""),
                "distance_km": round(d, 3),
            })

        distances.sort(key=lambda x: x["distance_km"])
        result[base["mountpoint"]] = distances[:count]

    return result


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: Optional[list[str]] = None) -> None:
    if fieldnames is None:
        seen = []
        seen_set = set()
        for row in rows:
            for key in row.keys():
                if key not in seen_set:
                    seen.append(key)
                    seen_set.add(key)
        fieldnames = seen

    with path.open("w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, obj: Any) -> None:
    path.write_text(
        json.dumps(json_safe(obj), indent=2, ensure_ascii=False),
        encoding="utf-8"
    )


def flatten_stream_for_csv(stream: dict[str, Any]) -> dict[str, Any]:
    return {
        "timestamp": stream["timestamp"],
        "mountpoint": stream["mountpoint"],
        "identifier": stream["identifier"],
        "country": stream["country"],
        "latitude": stream["latitude"],
        "longitude": stream["longitude"],
        "format": stream["format"],
        "format_details": stream["format_details"],
        "nav_system": stream["nav_system"],
        "network": stream["network"],
        "generator": stream["generator"],
        "receiver_type": stream.get("receiver_type", ""),
        "antenna_type": stream.get("antenna_type", ""),
        "bitrate": stream["bitrate"],
        "duration_seconds": stream["duration_seconds"],
        "bytes_received": stream["bytes_received"],
        "messages_received": stream["messages_received"],
        "message_types": json.dumps(stream["message_types"], ensure_ascii=False),
        "constellations_observed": ",".join(stream["constellations_observed"]),
        "complete_sample": stream["complete_sample"],
        "error": stream["error"] or "",
    }


def build_html_report(report_data: dict[str, Any]) -> str:
    # JSON is embedded into the HTML. Escape </script> defensively.
    data_json = json.dumps(json_safe(report_data), ensure_ascii=False).replace("</", "<\\/")

    title = f"NTRIP RTCM Health Monitor — {html.escape(report_data['run']['timestamp'])}"

    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{title}</title>

<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>

<style>
  :root {{
    --bg:#f4f6f8; --card:#fff; --text:#1d2733; --muted:#667085;
    --border:#dfe4ea; --accent:#2457d6; --ok:#2f855a; --bad:#c53030;
  }}
  * {{ box-sizing:border-box; }}
  body {{ margin:0; font-family:Segoe UI,Arial,sans-serif; background:var(--bg); color:var(--text); }}
  header {{ padding:18px 24px; background:#182230; color:#fff; }}
  header h1 {{ margin:0 0 5px; font-size:22px; }}
  header .sub {{ opacity:.75; font-size:13px; }}
  .wrap {{ padding:18px; max-width:1700px; margin:auto; }}
  .cards {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(180px,1fr)); gap:12px; margin-bottom:14px; }}
  .card {{ background:var(--card); border:1px solid var(--border); border-radius:10px; padding:14px; }}
  .metric {{ font-size:26px; font-weight:700; }}
  .label {{ color:var(--muted); font-size:12px; margin-top:3px; }}
  .grid {{ display:grid; grid-template-columns:minmax(420px,1fr) minmax(500px,1.3fr); gap:14px; }}
  #map {{ height:610px; border-radius:8px; }}
  .panel {{ background:var(--card); border:1px solid var(--border); border-radius:10px; padding:12px; overflow:hidden; }}
  .toolbar {{ display:flex; gap:10px; align-items:center; flex-wrap:wrap; margin-bottom:8px; }}
  select {{ padding:6px 8px; }}
  .radio-group {{ display:flex; gap:8px; flex-wrap:wrap; align-items:center; }}
  .radio-pill {{ display:inline-flex; align-items:center; gap:5px; border:1px solid var(--border); padding:5px 9px; border-radius:999px; background:#fff; cursor:pointer; }}
  .health-legend {{ display:flex; align-items:center; gap:8px; margin:8px 0 12px; }}
  .health-gradient {{ width:220px; height:12px; border-radius:6px; background:linear-gradient(90deg,#8b0000 0%,#d73027 25%,#f46d43 45%,#fee08b 60%,#d9ef8b 72%,#66bd63 85%,#006400 100%); border:1px solid #bbb; }}
  #cnrChart, #satChart {{ height:330px; }}
  table {{ width:100%; border-collapse:collapse; font-size:12px; }}
  th,td {{ border-bottom:1px solid var(--border); padding:7px; text-align:left; }}
  th {{ background:#f8fafc; position:sticky; top:0; }}
  .tablebox {{ max-height:560px; overflow:auto; }}
  .small {{ font-size:12px; color:var(--muted); }}
  .filters {{ display:flex; gap:8px; flex-wrap:wrap; margin:10px 0; align-items:center; }}
  .filters input,.filters select {{ padding:6px 8px; border:1px solid var(--border); border-radius:6px; background:#fff; }}
  #summaryTable th.sortable {{ cursor:pointer; user-select:none; }}
  #summaryTable th.sortable:hover {{ background:#edf2f7; }}
  #summaryTable th .sortmark {{ opacity:.55; margin-left:4px; }}
  .error {{ color:var(--bad); }}
  @media (max-width:1000px) {{
    .grid {{ grid-template-columns:1fr; }}
    #map {{ height:500px; }}
  }}
</style>
</head>
<body>
<header>
  <h1>NTRIP RTCM Health Monitor</h1>
  <div class="sub">Run: {html.escape(report_data['run']['timestamp'])} · Caster: {html.escape(report_data['run']['host'])}:{report_data['run']['port']}</div>
</header>

<div class="wrap">
  <div class="cards">
    <div class="card"><div class="metric" id="mStreams"></div><div class="label">Streams sampled</div></div>
    <div class="card"><div class="metric" id="mComplete"></div><div class="label">Complete samples</div></div>
    <div class="card"><div class="metric" id="mErrors"></div><div class="label">Streams with errors</div></div>
    <div class="card"><div class="metric" id="mObs"></div><div class="label">Satellite/signal CNR rows</div></div>
  </div>

  <div class="grid">
    <div class="panel">
      <div class="toolbar">
        <strong>Base map</strong>
        <span class="small">Click a marker to select a base.</span>
      </div>
      <div id="map"></div>
    </div>

    <div class="panel">
      <div class="toolbar">
        <strong>Selected base:</strong>
        <select id="baseSelect"></select>
        <strong>Constellation:</strong>
        <div id="constRadios" class="radio-group"></div>
      </div>

      <div id="selectedInfo" class="small"></div>
      <div class="health-legend">
        <span class="small">Health</span>
        <div class="health-gradient"></div>
        <span class="small">0 → 100</span>
      </div>
      <div id="cnrChart"></div>
      <div id="satChart"></div>
    </div>
  </div>

  <div class="panel" style="margin-top:14px;">
    <h3 style="margin-top:0;">Base × constellation summary</h3>
    <div class="small">Country medians are calculated from this same run. Health 75–90 is normal/good; 90+ is unusually strong relative to peers. Click any column heading to sort.</div>
    <div class="filters">
      <input id="tableSearch" type="search" placeholder="Search base / receiver / antenna...">
      <select id="filterCountry"><option value="">All countries</option></select>
      <select id="filterConst"><option value="">All constellations</option></select>
      <select id="filterHealth">
        <option value="">All health</option>
        <option value="0-20">0–20 Critical</option>
        <option value="20-40">20–40 Poor</option>
        <option value="40-60">40–60 Suspect</option>
        <option value="60-75">60–75 Fair</option>
        <option value="75-90">75–90 Good</option>
        <option value="90-101">90–100 Excellent</option>
      </select>
      <select id="filterError">
        <option value="">Errors + healthy</option>
        <option value="no">No error</option>
        <option value="yes">Errors only</option>
      </select>
      <span id="tableCount" class="small"></span>
    </div>
    <div class="tablebox">
      <table id="summaryTable">
        <thead><tr>
          <th data-key="mountpoint">Base</th><th data-key="country">Country</th><th data-key="receiver_type">Receiver</th><th data-key="antenna_type">Antenna</th><th data-key="constellation">Constellation</th>
          <th data-key="base_health_score">Base health</th><th data-key="health_score">Const. health</th><th data-key="satellite_count">Sats</th><th data-key="country_satellite_median">Country sat median</th><th data-key="satellite_delta_vs_country">Δ sats</th>
          <th data-key="cnr_median">CNR median</th><th data-key="country_cnr_median">Country CNR median</th><th data-key="cnr_delta_vs_country">Δ CNR</th>
          <th data-key="cnr_p10">CNR p10</th><th data-key="cnr_p90">CNR p90</th><th data-key="epochs_seen">Epochs</th><th data-key="error">Error</th>
        </tr></thead>
        <tbody></tbody>
      </table>
    </div>
  </div>
</div>

<script>
const DATA = {data_json};

const streams = DATA.streams;
const summary = DATA.summary;
const observations = DATA.observations;
const neighbors = DATA.neighbors;
const baseHealthData = DATA.base_health || {{}};

document.getElementById('mStreams').textContent = streams.length;
document.getElementById('mComplete').textContent = streams.filter(x=>x.complete_sample).length;
document.getElementById('mErrors').textContent = streams.filter(x=>x.error).length;
document.getElementById('mObs').textContent = observations.filter(x=>x.cnr_dbhz!==null).length;

const streamByName = Object.fromEntries(streams.map(x=>[x.mountpoint,x]));
const summaryKey = new Map(summary.map(x=>[[x.mountpoint,x.constellation].join('|'),x]));

const baseSelect = document.getElementById('baseSelect');
const constRadios = document.getElementById('constRadios');

[...streams].sort((a,b)=>a.mountpoint.localeCompare(b.mountpoint)).forEach(s=>{{
  const o=document.createElement('option'); o.value=s.mountpoint; o.textContent=s.mountpoint; baseSelect.appendChild(o);
}});

const allConsts = [...new Set(summary.map(x=>x.constellation).filter(Boolean))];
const constOrder = ['GPS','GLONASS','GALILEO','BEIDOU','QZSS','SBAS','NAVIC'];
allConsts.sort((a,b)=>(constOrder.indexOf(a)<0?99:constOrder.indexOf(a))-(constOrder.indexOf(b)<0?99:constOrder.indexOf(b)));

allConsts.forEach((c,idx)=>{{
  const label=document.createElement('label');
  label.className='radio-pill';
  label.innerHTML=`<input type="radio" name="constellation" value="${{c}}" ${{(c==='GPS'||(idx===0&&!allConsts.includes('GPS')))?'checked':''}}> ${{c}}`;
  const input=label.querySelector('input');
  input.addEventListener('change', refresh);
  constRadios.appendChild(label);
}});

function selectedConstellation() {{
  return document.querySelector('input[name="constellation"]:checked')?.value || allConsts[0];
}}

function fmt(v,d=1) {{
  if(v===null || v===undefined || Number.isNaN(Number(v))) return '';
  return Number(v).toFixed(d);
}}

function healthColor(score) {{
  if(score===null || score===undefined || Number.isNaN(Number(score))) return '#9aa3ad';
  const s=Math.max(0,Math.min(100,Number(score)));
  // Red -> orange -> yellow -> light green -> strong green
  if(s < 20) return '#8b0000';
  if(s < 40) return '#d73027';
  if(s < 60) return '#f46d43';
  if(s < 75) return '#fee08b';
  if(s < 90) return '#91cf60';
  return '#006400';
}}

const baseHealth = {{}};
streams.forEach(s=>{{
  const h=baseHealthData[s.mountpoint];
  baseHealth[s.mountpoint]=h ? Number(h.base_health_score) : null;
}});

function markerColor(s) {{
  if(s.error) return '#8b0000';
  return healthColor(baseHealth[s.mountpoint]);
}}

const validLoc = streams.filter(s=>s.latitude!==null && s.longitude!==null);
let center = [47.2,19.5];
if(validLoc.length) {{
  center = [
    validLoc.reduce((a,b)=>a+Number(b.latitude),0)/validLoc.length,
    validLoc.reduce((a,b)=>a+Number(b.longitude),0)/validLoc.length
  ];
}}

const map = L.map('map').setView(center, validLoc.length ? 6 : 4);
L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
  maxZoom:19, attribution:'&copy; OpenStreetMap contributors'
}}).addTo(map);

const markerByBase = {{}};
validLoc.forEach(s=>{{
  const marker=L.circleMarker([s.latitude,s.longitude],{{
    radius:5, color:markerColor(s), fillColor:markerColor(s), fillOpacity:.8, weight:1
  }}).addTo(map);
  marker.bindTooltip(`${{s.mountpoint}} (${{s.country||''}})`);
  marker.on('click',()=>{{ baseSelect.value=s.mountpoint; refresh(); }});
  markerByBase[s.mountpoint]=marker;
}});

if(validLoc.length>1) {{
  const bounds=L.latLngBounds(validLoc.map(s=>[s.latitude,s.longitude]));
  map.fitBounds(bounds.pad(.08));
}}

function relevantBases(base) {{
  const n=(neighbors[base]||[]).slice(0,4).map(x=>x.mountpoint);
  return [base,...n];
}}

function aggregatePrnCnr(base,constellation) {{
  const rows=observations.filter(o=>o.mountpoint===base && o.constellation===constellation && o.cnr_dbhz!==null);
  const by={{}};
  rows.forEach(r=>{{
    // Multiple signals / epochs may exist for one PRN.
    // Median of all available CNR cells is intentionally used for robust visual comparison.
    (by[r.prn]??=[]).push(Number(r.cnr_dbhz));
  }});
  const out={{}};
  Object.entries(by).forEach(([prn,vals])=>{{
    vals.sort((a,b)=>a-b);
    const m=Math.floor(vals.length/2);
    out[prn]=vals.length%2?vals[m]:(vals[m-1]+vals[m])/2;
  }});
  return out;
}}

function refresh() {{
  const base=baseSelect.value;
  const constellation=selectedConstellation();
  if(!base || !constellation) return;

  const s=streamByName[base];
  const ns=neighbors[base]||[];
  document.getElementById('selectedInfo').innerHTML =
    `<b>${{base}}</b> · ${{s.country||''}} · ` +
    `health ${{fmt(baseHealth[base],0)}}/100 · ` +
    `${{s.receiver_type||'receiver n/a'}} · ${{s.antenna_type||'antenna n/a'}} · ` +
    `${{s.latitude??''}}, ${{s.longitude??''}} · ` +
    `nearest: ${{ns.slice(0,4).map(x=>`${{x.mountpoint}} (${{fmt(x.distance_km,1)}} km)`).join(', ')||'n/a'}}` +
    (s.error ? `<br><span class="error">${{s.error}}</span>` : '');

  const bases=relevantBases(base);

  // CNR chart: x = PRN, grouped bars = selected base + four nearest bases.
  const prns=new Set();
  const series=bases.map(b=>{{
    const vals=aggregatePrnCnr(b,constellation);
    Object.keys(vals).forEach(p=>prns.add(p));
    return [b,vals];
  }});
  const x=[...prns].sort((a,b)=>Number(a)-Number(b) || a.localeCompare(b));

  const cnrTraces=series.map(([b,vals])=>({{
    type:'bar',
    name:b,
    x:x,
    y:x.map(p=>vals[p]??null),
    width: b===base ? 0.18 : 0.11,
    marker: {{
      line: {{width: b===base ? 2.5 : 0.5}}
    }},
    opacity: b===base ? 1.0 : 0.72
  }}));

  const selectedSummary=summaryKey.get(`${{base}}|${{constellation}}`);
  const countryMedian=selectedSummary?.country_cnr_median;

  const layout={{
    title:`${{constellation}} CNR by satellite (median over sampled cells)`,
    barmode:'group',
    xaxis:{{title:'Satellite PRN', type:'category', categoryorder:'array', categoryarray:x}},
    yaxis:{{title:'CNR / C/N0 (dB-Hz)', rangemode:'tozero'}},
    margin:{{t:50,l:55,r:20,b:45}},
    shapes: countryMedian===null || countryMedian===undefined ? [] : [{{
      type:'line', xref:'paper', x0:0, x1:1,
      y0:countryMedian, y1:countryMedian,
      line:{{dash:'dash',width:2}}
    }}],
    annotations: countryMedian===null || countryMedian===undefined ? [] : [{{
      xref:'paper',x:1,y:countryMedian,
      text:`Country median ${{fmt(countryMedian)}} dB-Hz`,
      showarrow:false,xanchor:'right',yanchor:'bottom'
    }}]
  }};
  Plotly.react('cnrChart',cnrTraces,layout,{{responsive:true,displaylogo:false}});

  // Satellite-count comparison
  const satY=bases.map(b=>summaryKey.get(`${{b}}|${{constellation}}`)?.satellite_count ?? null);
  const satCountryMedian=selectedSummary?.country_satellite_median;
  Plotly.react('satChart',[{{
    type:'bar', x:bases, y:satY, name:'Visible satellites'
  }}],{{
    title:`${{constellation}} satellite count`,
    yaxis:{{title:'Satellites',rangemode:'tozero'}},
    margin:{{t:50,l:55,r:20,b:70}},
    shapes: satCountryMedian===null || satCountryMedian===undefined ? [] : [{{
      type:'line',xref:'paper',x0:0,x1:1,y0:satCountryMedian,y1:satCountryMedian,
      line:{{dash:'dash',width:2}}
    }}],
    annotations: satCountryMedian===null || satCountryMedian===undefined ? [] : [{{
      xref:'paper',x:1,y:satCountryMedian,
      text:`Country median ${{fmt(satCountryMedian)}}`,
      showarrow:false,xanchor:'right',yanchor:'bottom'
    }}]
  }},{{responsive:true,displaylogo:false}});
}}

baseSelect.addEventListener('change',refresh);

const tbody=document.querySelector('#summaryTable tbody');
const tableSearch=document.getElementById('tableSearch');
const filterCountry=document.getElementById('filterCountry');
const filterConst=document.getElementById('filterConst');
const filterHealth=document.getElementById('filterHealth');
const filterError=document.getElementById('filterError');
const tableCount=document.getElementById('tableCount');

[...new Set(summary.map(r=>r.country).filter(Boolean))].sort().forEach(v=>{{
  const o=document.createElement('option');o.value=v;o.textContent=v;filterCountry.appendChild(o);
}});
[...new Set(summary.map(r=>r.constellation).filter(Boolean))].sort().forEach(v=>{{
  const o=document.createElement('option');o.value=v;o.textContent=v;filterConst.appendChild(o);
}});

let tableSortKey='base_health_score';
let tableSortAsc=true;

document.querySelectorAll('#summaryTable thead th[data-key]').forEach(th=>{{
  th.classList.add('sortable');
  th.innerHTML += '<span class="sortmark"></span>';
  th.addEventListener('click',()=>{{
    const key=th.dataset.key;
    if(tableSortKey===key) tableSortAsc=!tableSortAsc;
    else {{ tableSortKey=key; tableSortAsc=true; }}
    renderSummaryTable();
  }});
}});

function cmpValue(v) {{
  if(v===null || v===undefined || v==='') return null;
  const n=Number(v);
  return Number.isNaN(n) ? String(v).toLowerCase() : n;
}}

function renderSummaryTable() {{
  const q=tableSearch.value.trim().toLowerCase();
  const country=filterCountry.value;
  const constellation=filterConst.value;
  const err=filterError.value;
  const hr=filterHealth.value ? filterHealth.value.split('-').map(Number) : null;

  let rows=summary.filter(r=>r.constellation);

  rows=rows.filter(r=>{{
    if(country && r.country!==country) return false;
    if(constellation && r.constellation!==constellation) return false;
    if(err==='yes' && !r.error) return false;
    if(err==='no' && r.error) return false;
    if(hr) {{
      const h=Number(r.health_score);
      if(Number.isNaN(h) || h<hr[0] || h>=hr[1]) return false;
    }}
    if(q) {{
      const hay=[r.mountpoint,r.country,r.receiver_type,r.antenna_type,r.constellation,r.health_label,r.base_health_label,r.error]
        .map(x=>String(x||'').toLowerCase()).join(' ');
      if(!hay.includes(q)) return false;
    }}
    return true;
  }});

  rows.sort((a,b)=>{{
    const av=cmpValue(a[tableSortKey]), bv=cmpValue(b[tableSortKey]);
    if(av===null && bv===null) return 0;
    if(av===null) return 1;
    if(bv===null) return -1;
    let c;
    if(typeof av==='number' && typeof bv==='number') c=av-bv;
    else c=String(av).localeCompare(String(bv));
    return tableSortAsc ? c : -c;
  }});

  document.querySelectorAll('#summaryTable thead th[data-key] .sortmark').forEach(x=>x.textContent='');
  const active=document.querySelector(`#summaryTable thead th[data-key="${{tableSortKey}}"] .sortmark`);
  if(active) active.textContent=tableSortAsc?'▲':'▼';

  tbody.innerHTML='';
  rows.forEach(r=>{{
    const tr=document.createElement('tr');
    const hc=healthColor(r.health_score);
    const bhc=healthColor(r.base_health_score);
    tr.innerHTML=`
      <td>${{r.mountpoint}}</td><td>${{r.country||''}}</td><td>${{r.receiver_type||''}}</td><td>${{r.antenna_type||''}}</td><td>${{r.constellation}}</td>
      <td style="background:${{bhc}};font-weight:700;text-align:center">${{fmt(r.base_health_score,0)}} ${{r.base_health_label||''}}</td>
      <td style="background:${{hc}};font-weight:700;text-align:center">${{fmt(r.health_score,0)}} ${{r.health_label||''}}</td>
      <td>${{r.satellite_count}}</td><td>${{fmt(r.country_satellite_median)}}</td><td>${{fmt(r.satellite_delta_vs_country)}}</td>
      <td>${{fmt(r.cnr_median)}}</td><td>${{fmt(r.country_cnr_median)}}</td><td>${{fmt(r.cnr_delta_vs_country)}}</td>
      <td>${{fmt(r.cnr_p10)}}</td><td>${{fmt(r.cnr_p90)}}</td><td>${{r.epochs_seen}}</td>
      <td class="${{r.error?'error':''}}">${{r.error||''}}</td>`;
    tr.style.cursor='pointer';
    tr.onclick=()=>{{
      baseSelect.value=r.mountpoint;
      const radio=document.querySelector(`input[name="constellation"][value="${{r.constellation}}"]`);
      if(radio) radio.checked=true;
      refresh();
      window.scrollTo({{top:0,behavior:'smooth'}});
    }};
    tbody.appendChild(tr);
  }});

  tableCount.textContent=`${{rows.length}} rows`;
}}

[tableSearch,filterCountry,filterConst,filterHealth,filterError].forEach(el=>{{
  el.addEventListener(el===tableSearch?'input':'change',renderSummaryTable);
}});
renderSummaryTable();

if(baseSelect.options.length) {{
  baseSelect.selectedIndex=0;
  refresh();
}}
</script>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# CLI / main
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Parallel NTRIP RTCM base-station health monitor"
    )

    p.add_argument("--host", default="crtk.net")
    p.add_argument("--port", type=int, default=2101)
    p.add_argument("--username", default=os.getenv("NTRIP_USERNAME", ""))
    p.add_argument("--password", default=os.getenv("NTRIP_PASSWORD", ""))

    p.add_argument("--country", help='Filter, e.g. "HU,SK,!FR"')
    p.add_argument("--stream", help='Filter, e.g. "BUD*,!TEST*"')

    p.add_argument("--workers", type=int, default=5)
    p.add_argument("--min-seconds", type=float, default=5.0)
    p.add_argument("--max-seconds", type=float, default=20.0)
    p.add_argument("--min-epochs", type=int, default=3)
    p.add_argument("--timeout", type=float, default=5.0)
    p.add_argument("--limit", type=int, default=None)

    p.add_argument(
        "--neighbors",
        type=int,
        default=4,
        help="Number of nearest bases stored for visual comparison (default: 4)",
    )

    p.add_argument(
        "--output-root",
        default="runs",
        help="Parent directory for timestamped run directories",
    )

    return p


def main() -> int:
    args = build_parser().parse_args()

    if not args.username:
        print("ERROR: --username is required (or set NTRIP_USERNAME).", file=sys.stderr)
        return 2
    if not args.password:
        print("ERROR: --password is required (or set NTRIP_PASSWORD).", file=sys.stderr)
        return 2
    if args.workers < 1:
        print("ERROR: --workers must be >= 1.", file=sys.stderr)
        return 2
    if args.min_seconds < 0 or args.max_seconds <= 0 or args.min_seconds > args.max_seconds:
        print("ERROR: require 0 <= --min-seconds <= --max-seconds.", file=sys.stderr)
        return 2
    if args.min_epochs < 1:
        print("ERROR: --min-epochs must be >= 1.", file=sys.stderr)
        return 2

    run_stamp = timestamp_for_path()
    run_dir = Path(args.output_root) / f"run_{run_stamp}"
    run_dir.mkdir(parents=True, exist_ok=False)

    print(f"NTRIP RTCM Health Monitor v{VERSION}")
    print(f"Run directory: {run_dir.resolve()}")
    print(f"Fetching sourcetable from {args.host}:{args.port} ...")

    try:
        all_entries = fetch_sourcetable(
            args.host, args.port, args.username, args.password, args.timeout
        )
    except Exception as exc:
        print(f"ERROR fetching sourcetable: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1

    write_csv(
        run_dir / "sourcetable.csv",
        [asdict(e) for e in all_entries],
    )

    entries = [
        e for e in all_entries
        if match_filter(e.country, args.country)
        and match_filter(e.mountpoint, args.stream)
    ]

    entries.sort(key=lambda e: (e.country, e.mountpoint))

    if args.limit is not None:
        entries = entries[: max(0, args.limit)]

    print(f"Sourcetable STR entries: {len(all_entries)}")
    print(f"Selected streams:         {len(entries)}")
    print(
        f"Sampling: {args.workers} workers, "
        f"{args.min_seconds:g}-{args.max_seconds:g}s, "
        f"minimum {args.min_epochs} MSM epochs"
    )

    if not entries:
        print("No streams matched the filters.")
        return 0

    results: list[dict[str, Any]] = []
    print_lock = threading.Lock()
    done = 0

    def worker(entry: SourceTableEntry) -> dict[str, Any]:
        return sample_mountpoint(
            entry=entry,
            host=args.host,
            port=args.port,
            username=args.username,
            password=args.password,
            min_duration=args.min_seconds,
            max_duration=args.max_seconds,
            min_epochs=args.min_epochs,
            timeout=args.timeout,
        )

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        future_map = {
            executor.submit(worker, e): e for e in entries
        }

        for future in as_completed(future_map):
            entry = future_map[future]
            done += 1

            try:
                result = future.result()
            except Exception as exc:
                result = {
                    "timestamp": utc_iso(),
                    "mountpoint": entry.mountpoint,
                    "identifier": entry.identifier,
                    "country": entry.country,
                    "latitude": entry.latitude,
                    "longitude": entry.longitude,
                    "format": entry.format,
                    "format_details": entry.format_details,
                    "nav_system": entry.nav_system,
                    "network": entry.network,
                    "generator": entry.generator,
                    "receiver_type": entry.receiver_type,
                    "antenna_type": entry.antenna_type,
                    "bitrate": entry.bitrate,
                    "duration_seconds": 0,
                    "bytes_received": 0,
                    "messages_received": 0,
                    "message_types": {},
                    "constellations_observed": [],
                    "complete_sample": False,
                    "error": f"{type(exc).__name__}: {exc}",
                    "observations": [],
                }

            results.append(result)

            status = (
                "ERROR" if result.get("error")
                else ("OK" if result.get("complete_sample") else "PARTIAL")
            )

            with print_lock:
                print(
                    f"[{done:4}/{len(entries):4}] "
                    f"[{status:7}] "
                    f"{entry.mountpoint:30} "
                    f"{result.get('duration_seconds', 0):6.2f}s "
                    f"{result.get('messages_received', 0):6} msgs "
                    f"{len(result.get('observations', [])):6} cells"
                )

    results.sort(key=lambda x: x["mountpoint"])

    # Flatten observations into one raw table.
    observations = [
        obs
        for stream in results
        for obs in stream.get("observations", [])
    ]

    summary = make_base_constellation_summary(results)
    add_country_reference(summary)
    compute_health_scores(summary)
    base_health = compute_base_health(summary)

    for row in summary:
        bh = base_health.get(row["mountpoint"], {})
        row.update(bh)

    base_meta = [
        {
            "mountpoint": s["mountpoint"],
            "country": s["country"],
            "latitude": s["latitude"],
            "longitude": s["longitude"],
        }
        for s in results
    ]
    neighbors = nearest_neighbors(base_meta, args.neighbors)

    # CSV outputs
    write_csv(
        run_dir / "streams.csv",
        [flatten_stream_for_csv(s) for s in results],
    )

    write_csv(
        run_dir / "observations.csv",
        observations,
        fieldnames=[
            "timestamp", "elapsed_seconds", "mountpoint", "country",
            "latitude", "longitude", "constellation", "message_type",
            "epoch", "prn", "signal", "cnr_dbhz", "cnr_field"
        ],
    )

    write_csv(
        run_dir / "summary.csv",
        summary,
        fieldnames=[
            "mountpoint", "country", "latitude", "longitude",
            "receiver_type", "antenna_type", "constellation",
            "health_score", "health_label",
            "base_health_score", "base_health_label",
            "mean_constellation_health", "weakest_constellation_health",
            "constellations_scored",
            "satellite_count", "country_satellite_median",
            "satellite_delta_vs_country",
            "signal_cell_count", "epochs_seen",
            "cnr_sample_count", "cnr_mean", "cnr_median",
            "country_cnr_median", "cnr_delta_vs_country",
            "cnr_p10", "cnr_p90", "cnr_min", "cnr_max",
            "complete_sample", "error",
        ],
    )

    # Strip observations from stream objects for report duplication avoidance.
    report_streams = []
    for s in results:
        s2 = dict(s)
        s2.pop("observations", None)
        report_streams.append(s2)

    report_data = {
        "run": {
            "timestamp": utc_iso(),
            "run_id": run_stamp,
            "host": args.host,
            "port": args.port,
            "workers": args.workers,
            "min_seconds": args.min_seconds,
            "max_seconds": args.max_seconds,
            "min_epochs": args.min_epochs,
            "country_filter": args.country,
            "stream_filter": args.stream,
            "selected_streams": len(entries),
        },
        "streams": report_streams,
        "summary": summary,
        "base_health": base_health,
        "observations": observations,
        "neighbors": neighbors,
    }

    write_json(run_dir / "report_data.json", report_data)

    html_report = build_html_report(report_data)
    (run_dir / "report.html").write_text(html_report, encoding="utf-8")

    errors = sum(1 for s in results if s.get("error"))
    complete = sum(1 for s in results if s.get("complete_sample"))

    print()
    print("Finished.")
    print(f"  Streams:      {len(results)}")
    print(f"  Complete:     {complete}")
    print(f"  Errors:       {errors}")
    print(f"  Observations: {len(observations)}")
    print()
    print(f"Open this report:")
    print(f"  {(run_dir / 'report.html').resolve()}")
    print()
    print("Raw data:")
    print(f"  {(run_dir / 'observations.csv').resolve()}")
    print(f"  {(run_dir / 'summary.csv').resolve()}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
