"""Recorder health from the bucket's point of view, per feed:
- stale: no new capture for longer than the feed's limit (the recorder or R2 writes stopped);
- low coverage: well under the scheduled captures in the last hour;
- frozen: captures keep arriving but the feed's own header timestamp stopped moving (the
  city's feed is stuck, not the recorder).

Also the daily coverage report for a closed day. Prints counts only, so it is safe in public logs.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory

from google.protobuf.message import DecodeError
from google.transit import gtfs_realtime_pb2 as rt

from . import keys as K
from .cities import City
from .coverage import HOUR_MS, coverage, markdown_table
from .pack import DAY_MS, day_start_ms, load_manifests
from .store import Store

MIN_STALE_MS = 10 * 60_000
FROZEN_S = 15 * 60
FAIL_RATIO = 0.5  # last hour
DAILY_FAIL_RATIO = 0.9


def _iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, UTC).strftime("%Y-%m-%d/%H")


@dataclass
class Problem:
    feed: str
    text: str


def check(store: Store, city: City, now_ms: int | None = None) -> tuple[str, list[Problem]]:
    now_ms = now_ms if now_ms is not None else int(time.time() * 1000)
    # The last 60 whole minutes, leaving the current minute's fetches time to land.
    end = now_ms - now_ms % 60_000 - 60_000
    start = end - HOUR_MS
    hours = sorted({_iso(t) for t in (start, end - 1, now_ms)})
    days = sorted({h[:10] for h in hours})

    found: list[str] = []
    for feed in city.feed_ids:
        for hour in hours:
            found += [o.key for o in store.list(K.raw_prefix(city.id, feed, hour))]
    for day in days:
        found += [o.key for o in store.list(K.errors_prefix(city.id, day))]
    cov = coverage(city, found, start, end)

    newest_any: dict[str, str] = {}
    for key in found:
        if m := K.RAW_RE.match(key):
            if key > newest_any.get(m["feed"], ""):
                newest_any[m["feed"]] = key

    problems: list[Problem] = []
    rows = ["| Feed | Newest capture | Feed header age | Last hour |", "| --- | ---: | ---: | ---: |"]
    for f in city.feeds:
        c = cov[f.feed]
        limit = max(MIN_STALE_MS, 3 * f.every_minutes * 60_000)
        key = newest_any.get(f.feed)
        newest = int(K.RAW_RE.match(key)["ms"]) if key else 0
        age_s = (now_ms - newest) // 1000 if newest else None
        header_age = _header_age_s(store, key, newest) if key else None
        rows.append(
            f"| {f.feed} | {f'{age_s} s ago' if age_s is not None else 'none'} | "
            f"{f'{header_age} s' if header_age is not None else '-'} | {c.captured}/{c.expected} |"
        )
        if not newest or now_ms - newest > limit:
            problems.append(Problem(f.feed, f"no capture for over {limit // 60_000} min (recorder or R2 writes stopped)"))
        elif header_age is not None and header_age > FROZEN_S:
            problems.append(Problem(f.feed, f"captures arrive but the feed's own timestamp is {header_age // 60} min old (upstream feed frozen)"))
        elif header_age is None:
            problems.append(Problem(f.feed, "newest capture is not a readable GTFS-Realtime message"))
        if c.expected and c.ratio < FAIL_RATIO:
            problems.append(Problem(f.feed, f"only {c.captured} of {c.expected} scheduled captures in the last hour"))

    report = [
        f"## Recorder check, {datetime.fromtimestamp(now_ms / 1000, UTC):%Y-%m-%d %H:%M} UTC",
        "",
        *rows,
        "",
        "Last hour in detail:",
        "",
        markdown_table(cov),
        "",
    ]
    report += [f"**{p.feed}:** {p.text}" for p in problems] or ["All feeds healthy."]
    return "\n".join(report), problems


def _header_age_s(store: Store, key: str, fetched_ms: int) -> int | None:
    """Seconds between the capture and the feed's own header timestamp; None if unreadable."""
    with TemporaryDirectory() as d:
        path = Path(d) / "snap.pb"
        store.download(key, path)
        msg = rt.FeedMessage()
        try:
            msg.ParseFromString(path.read_bytes())
        except DecodeError:
            return None
    if not msg.header.HasField("timestamp"):
        return None
    return max(0, fetched_ms // 1000 - msg.header.timestamp)


def daily_coverage(store: Store, city: City, date: str) -> tuple[str, bool]:
    """Coverage of a closed UTC day: from its manifests once packed, else from landing objects.

    Returns (report, ok) where ok means every feed captured at least DAILY_FAIL_RATIO of its slots.
    """
    start = day_start_ms(date)
    found = {o["key"] for _, m in load_manifests(store, city, date) for o in m["objects"]}
    for feed in city.feed_ids:
        found |= {o.key for o in store.list(K.raw_prefix(city.id, feed, date))}
    found |= {o.key for o in store.list(K.errors_prefix(city.id, date))}
    cov = coverage(city, sorted(found), start, start + DAY_MS)
    low = [c for c in cov.values() if c.ratio < DAILY_FAIL_RATIO]
    lines = [f"## Coverage for {date} (UTC)", "", markdown_table(cov), ""]
    # The hours each feed lost most, to tell a short outage from a slow leak.
    for c in cov.values():
        worst = sorted((v[1] - v[0], h) for h, v in c.by_hour.items() if v[1] < v[0])[:3]
        if worst:
            hrs = ", ".join(f"{datetime.fromtimestamp(h / 1000, UTC):%H}:00 ({-d} short)" for d, h in worst)
            lines.append(f"- {c.feed}: worst hours {hrs}")
    lines.append("")
    if low:
        lines.append(f"**Below {DAILY_FAIL_RATIO:.0%}:** {', '.join(c.feed for c in low)}")
    else:
        lines.append(f"All feeds at or above {DAILY_FAIL_RATIO:.0%}.")
    return "\n".join(lines), not low
