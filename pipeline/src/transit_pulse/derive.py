"""Decode preserved raw bundles into Parquet tables, one UTC day at a time.

Runs after packing and independently of it: it reads only verified bundles listed in raw
manifests, so a bug here never blocks preservation or cleanup, and any day can be rebuilt from
the bundles with a newer schema version.

Tables (schema v1), all one row per thing as published, nothing forward-filled:
- snapshots: every capture, including ones that failed to parse.
- fetch_errors: the recorder's error records (HTTP errors, timeouts, HTML pages).
- vehicle_positions: every vehicle entity in every snapshot.
- trip_updates / stop_time_updates: each distinct trip-update entity version once, its stop
  updates in published order.
- alerts: each distinct alert version once, as JSON so selectors, periods and translations
  are complete.
- entity_membership: which trip-update and alert versions (and deletions) each snapshot held.
  Deduplicating versions keeps the volume down without losing snapshot membership.

Absent protobuf fields are null, never a default 0. Times the feed publishes stay as epoch
seconds; capture times are UTC timestamps.
"""

from __future__ import annotations

import hashlib
import json
import shutil
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Callable

import pyarrow as pa
import pyarrow.parquet as pq
from google.protobuf import json_format
from google.protobuf.message import DecodeError
from google.transit import gtfs_realtime_pb2 as rt

from . import keys as K
from .cities import City
from .pack import load_manifests, read_bundle
from .store import Store

SCHEMA_VERSION = "v1"
MANIFEST_SCHEMA = "transit-pulse/derived-manifest/v1"

TS = pa.timestamp("ms", tz="UTC")
S, I32, I64, F32, F64, B = pa.string(), pa.int32(), pa.int64(), pa.float32(), pa.float64(), pa.bool_()

TRIP = [("trip_id", S), ("route_id", S), ("direction_id", I32), ("start_time", S), ("start_date", S),
        ("trip_schedule_relationship", S)]

SCHEMAS = {
    "snapshots": [
        ("snapshot_key", S), ("feed", S), ("fetched_at", TS), ("slot_at", TS), ("http_status", I32),
        ("fetch_ms", I32), ("bytes", I64), ("sha256", S), ("parse_ok", B), ("parse_error", S),
        ("gtfs_realtime_version", S), ("incrementality", S), ("header_timestamp", I64),
        ("entity_count", I32), ("vehicle_count", I32), ("trip_update_count", I32), ("alert_count", I32),
    ],
    "fetch_errors": [
        ("error_key", S), ("feed", S), ("slot_at", TS), ("started_at", TS), ("ended_at", TS),
        ("http_status", I32), ("error", S), ("body_prefix", S),
    ],
    "vehicle_positions": [
        ("snapshot_key", S), ("fetched_at", TS), ("entity_id", S), ("is_deleted", B), *TRIP,
        ("vehicle_id", S), ("vehicle_label", S), ("license_plate", S),
        ("latitude", F32), ("longitude", F32), ("bearing", F32), ("odometer", F64), ("speed", F32),
        ("current_stop_sequence", I64), ("stop_id", S), ("current_status", S), ("timestamp", I64),
        ("congestion_level", S), ("occupancy_status", S), ("occupancy_percentage", I32),
    ],
    "trip_updates": [
        ("version_id", S), ("entity_id", S), *TRIP, ("vehicle_id", S), ("vehicle_label", S),
        ("timestamp", I64), ("delay", I32), ("stop_time_update_count", I32),
    ],
    "stop_time_updates": [
        ("version_id", S), ("position", I32), ("stop_sequence", I64), ("stop_id", S),
        ("arrival_delay", I32), ("arrival_time", I64), ("arrival_uncertainty", I32),
        ("departure_delay", I32), ("departure_time", I64), ("departure_uncertainty", I32),
        ("schedule_relationship", S),
    ],
    "alerts": [("version_id", S), ("entity_id", S), ("alert_json", S)],
    "entity_membership": [
        ("snapshot_key", S), ("fetched_at", TS), ("feed", S), ("entity_id", S), ("is_deleted", B),
        ("kind", S), ("version_id", S),
    ],
}


class TableWriter:
    """Buffers rows and writes them as zstd Parquet in batches, so a day never sits in memory."""

    def __init__(self, path: Path, fields: list[tuple[str, pa.DataType]], batch_rows: int = 200_000):
        self.schema = pa.schema(fields)
        self.path, self.batch_rows, self.rows = path, batch_rows, 0
        self.buf: list[dict] = []
        self.writer = pq.ParquetWriter(path, self.schema, compression="zstd")

    def add(self, row: dict) -> None:
        self.buf.append(row)
        if len(self.buf) >= self.batch_rows:
            self.flush()

    def flush(self) -> None:
        if self.buf:
            self.writer.write_table(pa.Table.from_pylist(self.buf, schema=self.schema))
            self.rows += len(self.buf)
            self.buf = []

    def close(self) -> int:
        self.flush()
        self.writer.close()
        if pq.read_metadata(self.path).num_rows != self.rows:
            raise ValueError(f"{self.path.name}: wrote {self.rows} rows, file says otherwise")
        return self.rows


def opt(msg, name: str):
    """The field's value if the feed set it, else None (proto2 keeps presence).

    An enum number newer than our bindings is parsed into unknown fields, so it also reads as
    None here; the raw bundles still hold it."""
    if not msg.HasField(name):
        return None
    value = getattr(msg, name)
    enum = msg.DESCRIPTOR.fields_by_name[name].enum_type
    if enum is not None:
        v = enum.values_by_number.get(value)
        return v.name if v else str(value)
    return value


def ms_ts(ms) -> datetime | None:
    return datetime.fromtimestamp(int(ms) / 1000, UTC) if ms not in (None, "") else None


def trip_cols(msg, field: str = "trip") -> dict:
    t = getattr(msg, field) if msg.HasField(field) else rt.TripDescriptor()
    return {
        "trip_id": opt(t, "trip_id"), "route_id": opt(t, "route_id"), "direction_id": opt(t, "direction_id"),
        "start_time": opt(t, "start_time"), "start_date": opt(t, "start_date"),
        "trip_schedule_relationship": opt(t, "schedule_relationship"),
    }


def version_id(entity: rt.FeedEntity) -> str:
    return hashlib.sha256(entity.SerializeToString(deterministic=True)).hexdigest()[:32]


class DayDecoder:
    def __init__(self, outdir: Path):
        self.w = {name: TableWriter(outdir / f"{name}.parquet", f) for name, f in SCHEMAS.items()}
        self.seen_versions: set[str] = set()
        self.parse_failures = 0
        self.unknown_keys = 0

    def error_record(self, key: str, data: bytes) -> None:
        r = json.loads(data)
        self.w["fetch_errors"].add({
            "error_key": key, "feed": r.get("feed"), "slot_at": ms_ts(r.get("slot")),
            "started_at": ms_ts(r.get("started")), "ended_at": ms_ts(r.get("ended")),
            "http_status": r.get("status") or None, "error": r.get("error"), "body_prefix": r.get("body") or None,
        })

    def snapshot(self, key: str, entry: dict, data: bytes) -> None:
        m = K.RAW_RE.match(key)
        if m is None:  # not a key the recorder writes; keep the day derivable
            self.unknown_keys += 1
            return
        feed, fetched = m["feed"], ms_ts(m["ms"])
        meta = entry.get("metadata", {})
        snap = {
            "snapshot_key": key, "feed": feed, "fetched_at": fetched, "slot_at": ms_ts(meta.get("slot")),
            "http_status": int(meta["status"]) if meta.get("status") else None,
            "fetch_ms": int(meta["fetch_ms"]) if meta.get("fetch_ms") else None,
            "bytes": len(data), "sha256": entry["sha256"], "parse_ok": True, "parse_error": None,
        }
        msg = rt.FeedMessage()
        try:
            msg.ParseFromString(data)
        except DecodeError as e:
            self.parse_failures += 1
            self.w["snapshots"].add({**snap, "parse_ok": False, "parse_error": str(e)[:500]})
            return
        counts = {"vehicle_count": 0, "trip_update_count": 0, "alert_count": 0}
        for e in msg.entity:
            deleted = e.is_deleted if e.HasField("is_deleted") else None
            base = {"snapshot_key": key, "fetched_at": fetched, "feed": feed, "entity_id": e.id, "is_deleted": deleted}
            if e.HasField("vehicle"):
                counts["vehicle_count"] += 1
                self.vehicle(base, e.vehicle)
            if e.HasField("trip_update"):
                counts["trip_update_count"] += 1
                self.versioned(base, e, "trip_update")
            if e.HasField("alert"):
                counts["alert_count"] += 1
                self.versioned(base, e, "alert")
            if not (e.HasField("vehicle") or e.HasField("trip_update") or e.HasField("alert")):
                self.w["entity_membership"].add({**base, "kind": "empty", "version_id": None})
        h = msg.header
        self.w["snapshots"].add({
            **snap, "gtfs_realtime_version": opt(h, "gtfs_realtime_version"),
            "incrementality": opt(h, "incrementality"), "header_timestamp": opt(h, "timestamp"),
            "entity_count": len(msg.entity), **counts,
        })

    def vehicle(self, base: dict, v: rt.VehiclePosition) -> None:
        d = v.vehicle if v.HasField("vehicle") else rt.VehicleDescriptor()
        p = v.position if v.HasField("position") else rt.Position()
        self.w["vehicle_positions"].add({
            "snapshot_key": base["snapshot_key"], "fetched_at": base["fetched_at"],
            "entity_id": base["entity_id"], "is_deleted": base["is_deleted"], **trip_cols(v),
            "vehicle_id": opt(d, "id"), "vehicle_label": opt(d, "label"), "license_plate": opt(d, "license_plate"),
            "latitude": opt(p, "latitude"), "longitude": opt(p, "longitude"), "bearing": opt(p, "bearing"),
            "odometer": opt(p, "odometer"), "speed": opt(p, "speed"),
            "current_stop_sequence": opt(v, "current_stop_sequence"), "stop_id": opt(v, "stop_id"),
            "current_status": opt(v, "current_status"), "timestamp": opt(v, "timestamp"),
            "congestion_level": opt(v, "congestion_level"), "occupancy_status": opt(v, "occupancy_status"),
            "occupancy_percentage": opt(v, "occupancy_percentage"),
        })

    def versioned(self, base: dict, e: rt.FeedEntity, kind: str) -> None:
        # Hash only this payload plus the entity id, so a combined vehicle+trip_update entity
        # does not get a new trip-update version every time the position moves.
        part = rt.FeedEntity(id=e.id)
        getattr(part, kind).CopyFrom(getattr(e, kind))
        vid = version_id(part)
        self.w["entity_membership"].add({**base, "kind": kind, "version_id": vid})
        if vid in self.seen_versions:
            return
        self.seen_versions.add(vid)
        if kind == "alert":
            self.w["alerts"].add({"version_id": vid, "entity_id": e.id,
                                  "alert_json": json_format.MessageToJson(e.alert, indent=None,
                                                                          preserving_proto_field_name=True,
                                                                          ensure_ascii=False)})
            return
        tu = e.trip_update
        d = tu.vehicle if tu.HasField("vehicle") else rt.VehicleDescriptor()
        self.w["trip_updates"].add({
            "version_id": vid, "entity_id": e.id, **trip_cols(tu), "vehicle_id": opt(d, "id"),
            "vehicle_label": opt(d, "label"), "timestamp": opt(tu, "timestamp"), "delay": opt(tu, "delay"),
            "stop_time_update_count": len(tu.stop_time_update),
        })
        for i, s in enumerate(tu.stop_time_update):
            a = s.arrival if s.HasField("arrival") else rt.TripUpdate.StopTimeEvent()
            dp = s.departure if s.HasField("departure") else rt.TripUpdate.StopTimeEvent()
            self.w["stop_time_updates"].add({
                "version_id": vid, "position": i, "stop_sequence": opt(s, "stop_sequence"),
                "stop_id": opt(s, "stop_id"), "arrival_delay": opt(a, "delay"), "arrival_time": opt(a, "time"),
                "arrival_uncertainty": opt(a, "uncertainty"), "departure_delay": opt(dp, "delay"),
                "departure_time": opt(dp, "time"), "departure_uncertainty": opt(dp, "uncertainty"),
                "schedule_relationship": opt(s, "schedule_relationship"),
            })

    def close(self) -> dict[str, int]:
        return {name: w.close() for name, w in self.w.items()}


def pending_days(store: Store, city: City) -> list[tuple[str, list[str]]]:
    """Days whose raw manifests changed since they were last derived, oldest first."""
    out = []
    for date in store.list_dirs(K.raw_manifests_prefix(city.id)):
        sources = sorted(o.key for o in store.list(K.raw_manifests_prefix(city.id, date)) if o.key.endswith(".json"))
        mkey = K.derived_manifest_key(city.id, SCHEMA_VERSION, date)
        if sources and (not store.exists(mkey) or json.loads(store.read(mkey))["source_manifests"] != sources):
            out.append((date, sources))
    return out


def derive_day(store: Store, city: City, date: str, workdir: Path, *, now_ms: int, code_version: str) -> dict:
    manifests = load_manifests(store, city, date)
    outdir = workdir / "out"
    outdir.mkdir(parents=True, exist_ok=True)
    dec = DayDecoder(outdir)
    for _, m in manifests:
        for b in m["bundles"]:
            local = workdir / "bundle.tar.zst"
            store.download(b["key"], local)
            for entry, data in read_bundle(local):
                if entry["key"].startswith("errors/"):
                    dec.error_record(entry["key"], data)
                else:
                    dec.snapshot(entry["key"], entry, data)
            local.unlink()
    rows = dec.close()
    tables = {}
    for name, n in rows.items():
        key = K.derived_key(city.id, SCHEMA_VERSION, name, date)
        store.upload(key, outdir / f"{name}.parquet", "application/vnd.apache.parquet")
        tables[name] = {"key": key, "rows": n, "bytes": (outdir / f"{name}.parquet").stat().st_size}
    manifest = {
        "schema": MANIFEST_SCHEMA, "schema_version": SCHEMA_VERSION, "city": city.id, "date": date,
        "created_at": datetime.fromtimestamp(now_ms / 1000, UTC).isoformat(), "code_version": code_version,
        "source_manifests": [k for k, _ in manifests], "tables": tables, "parse_failures": dec.parse_failures,
        "skipped_unknown_keys": dec.unknown_keys,
    }
    store.write(K.derived_manifest_key(city.id, SCHEMA_VERSION, date),
                json.dumps(manifest, indent=1, sort_keys=True).encode(), "application/json")
    return manifest


def run(
    store: Store, city: City, workdir: Path, *, max_days: int, code_version: str,
    log: Callable[[str], None] = print, now_ms: int | None = None,
) -> int:
    """Derive every day with new raw manifests. Returns the number of days that failed."""
    now_ms = now_ms if now_ms is not None else int(time.time() * 1000)
    days = pending_days(store, city)
    log(f"{city.id}: {len(days)} day(s) to derive; doing up to {max_days}")
    failed = 0
    for date, _ in days[:max_days]:
        t0 = time.monotonic()
        try:
            m = derive_day(store, city, date, workdir / date, now_ms=now_ms, code_version=code_version)
        except Exception as e:
            failed += 1
            log(f"\n### {date}: FAILED\n\n{type(e).__name__}: {e}")
            continue
        finally:
            shutil.rmtree(workdir / date, ignore_errors=True)
        log(f"\n### {date} ({time.monotonic() - t0:.0f} s, {m['parse_failures']} unparseable snapshots)\n")
        log("| Table | Rows | MB |\n| --- | ---: | ---: |")
        for name, t in m["tables"].items():
            log(f"| {name} | {t['rows']} | {t['bytes'] / 1e6:.1f} |")
    return failed
