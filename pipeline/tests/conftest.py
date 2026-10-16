"""Synthetic sample data shaped like the recorder's output. No real feed data is used."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from google.transit import gtfs_realtime_pb2 as rt

from transit_pulse.cities import City, Feed
from transit_pulse.pack import DAY_MS, day_start_ms
from transit_pulse.store import LocalStore

CITY = City("testcity", (
    Feed("vehicle-positions", 1, (0, 15, 30, 45)),
    Feed("trip-updates", 1, (5,)),
    Feed("alerts", 5, (10,)),
))
DAY = "2026-10-15"


def positions(ts: int, n: int = 3) -> bytes:
    m = rt.FeedMessage()
    m.header.gtfs_realtime_version = "2.0"
    m.header.incrementality = rt.FeedHeader.FULL_DATASET
    m.header.timestamp = ts
    for i in range(n):
        e = m.entity.add(id=f"v{i}")
        v = e.vehicle
        v.vehicle.id = f"bus-{i}"
        v.trip.trip_id = f"trip-{i}"
        v.trip.route_id = "94"
        v.position.latitude = 42.69 + i / 1000
        v.position.longitude = 23.32 + (ts % 600) / 100000
        v.timestamp = ts - i
        if i == 0:
            v.position.speed = 0.0  # present-but-zero must survive as 0, not null
    return m.SerializeToString()


def trip_updates(ts: int) -> bytes:
    m = rt.FeedMessage()
    m.header.gtfs_realtime_version = "2.0"
    m.header.timestamp = ts
    e = m.entity.add(id="t1")
    tu = e.trip_update
    tu.trip.trip_id = "trip-1"
    tu.trip.schedule_relationship = rt.TripDescriptor.SCHEDULED
    # Changes every 5 minutes, so versions repeat between snapshots.
    for seq in range(3):
        s = tu.stop_time_update.add(stop_sequence=seq + 1, stop_id=f"S{seq}")
        s.arrival.delay = 60 * ((ts // 300) % 4)
    c = m.entity.add(id="t2")
    c.trip_update.trip.trip_id = "trip-cancelled"
    c.trip_update.trip.schedule_relationship = rt.TripDescriptor.CANCELED
    return m.SerializeToString()


def alerts(ts: int) -> bytes:
    m = rt.FeedMessage()
    m.header.gtfs_realtime_version = "2.0"
    m.header.timestamp = ts
    a = m.entity.add(id="a1").alert
    a.informed_entity.add(route_id="94")
    a.header_text.translation.add(text="Ремонт", language="bg")
    return m.SerializeToString()


BUILDERS = {"vehicle-positions": positions, "trip-updates": trip_updates, "alerts": alerts}


def put_capture(store: LocalStore, feed: str, ms: int, body: bytes | None = None) -> str:
    from datetime import UTC, datetime

    iso = datetime.fromtimestamp(ms / 1000, UTC).strftime("%Y-%m-%d/%H")
    key = f"raw/{CITY.id}/{feed}/{iso}/{ms}.pb"
    store.put(
        key, BUILDERS[feed](ms // 1000) if body is None else body,
        content_type="application/x-protobuf",
        etag="e" + str(ms), last_modified="2026-10-15T00:00:00+00:00",
        metadata={"slot": str(ms), "status": "200", "fetch_ms": "40"},
    )
    return key


def put_error(store: LocalStore, feed: str, slot: int) -> str:
    from datetime import UTC, datetime

    key = f"errors/{CITY.id}/{datetime.fromtimestamp(slot / 1000, UTC):%Y-%m-%d}/{slot}-{feed}.json"
    store.put(key, json.dumps({"feed": feed, "slot": slot, "status": 503, "error": "HTTP 503"}).encode(),
              content_type="application/json")
    return key


def fill_hours(store: LocalStore, date: str, hours: range) -> list[str]:
    """Every scheduled capture for `hours` of `date`, with one positions slot failing each hour."""
    out = []
    start = day_start_ms(date)
    for h in hours:
        for minute in range(60):
            t = start + h * 3_600_000 + minute * 60_000
            for f in CITY.feeds:
                if (t // 60_000) % f.every_minutes:
                    continue
                for off in f.offsets_s:
                    at = t + off * 1000 + 300
                    if f.feed == "vehicle-positions" and minute == 7 and off == 30:
                        out.append(put_error(store, f.feed, at))
                    else:
                        out.append(put_capture(store, f.feed, at))
    return out


@pytest.fixture
def store(tmp_path: Path) -> LocalStore:
    return LocalStore(tmp_path / "bucket")


@pytest.fixture
def after_day() -> int:
    """`now` two hours after DAY closed."""
    return day_start_ms(DAY) + DAY_MS + 2 * 3_600_000
