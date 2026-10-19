"""Archive every version of the city's static GTFS timetable.

Delays and ghost trips can only be computed against the timetable that was in force at the
time, and the city replaces it without notice, so each download is compared with what is
already archived:
- a new version (by canonical hash) is stored once, as the original zip plus `meta.json`
  with its validity period (feed_info.txt, and calendar dates as a fallback);
- every download, new or not, leaves a small "seen" record, so the archive shows which
  version was being published on each day.

The canonical hash covers file names and contents only, so a re-zipped copy of the same data
(new timestamps, different compression) is not a new version. It leaves out feed_info.txt:
Sofia regenerates it daily with a rolling "today to a year from today" validity, which would
make every day a new version. Its contents go in every seen record instead, along with the
hash of every file, so any other file that churns can be spotted later. Logs show hashes and
counts only.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import time
import urllib.request
import zipfile
from datetime import UTC, datetime
from typing import Callable

from . import keys as K
from .cities import City
from .store import Store

USER_AGENT = "transit-pulse (+https://github.com/mike2906/transit-pulse)"
MAX_BYTES = 200 * 1024 * 1024
# Files whose content is metadata about the download rather than the timetable.
VOLATILE = {"feed_info.txt"}


def fetch(url: str) -> tuple[bytes, dict[str, str]]:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=120) as r:
        body = r.read(MAX_BYTES + 1)
        if len(body) > MAX_BYTES:
            raise ValueError(f"static feed is over {MAX_BYTES >> 20} MB")
        headers = {k.lower(): v for k, v in r.headers.items() if k.lower() in
                   ("content-type", "etag", "last-modified", "date", "content-length")}
        return body, headers


def canonical_hash(zf: zipfile.ZipFile) -> tuple[str, dict[str, str]]:
    """sha256 over sorted (file name, content sha256); also returns the per-file hashes."""
    files = {
        i.filename: hashlib.sha256(zf.read(i)).hexdigest()
        for i in zf.infolist() if not i.is_dir()
    }
    h = hashlib.sha256()
    for name in sorted(set(files) - VOLATILE):
        h.update(f"{name}\0{files[name]}\n".encode())
    return h.hexdigest(), files


def _rows(zf: zipfile.ZipFile, name: str):
    if name not in zf.namelist():
        return []
    with zf.open(name) as f:
        return list(csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig")))


def validity(zf: zipfile.ZipFile) -> dict:
    """What the feed says about when it applies. Dates are GTFS YYYYMMDD strings."""
    info = (_rows(zf, "feed_info.txt") or [{}])[0]
    dates = [r[k] for r in _rows(zf, "calendar.txt") for k in ("start_date", "end_date") if r.get(k)]
    dates += [r["date"] for r in _rows(zf, "calendar_dates.txt") if r.get("date")]
    return {
        "feed_start_date": info.get("feed_start_date") or None,
        "feed_end_date": info.get("feed_end_date") or None,
        "feed_version": info.get("feed_version") or None,
        "service_first_date": min(dates) if dates else None,
        "service_last_date": max(dates) if dates else None,
    }


def capture(
    store: Store, city: City, *, now_ms: int | None = None, code_version: str = "local",
    download: Callable[[str], tuple[bytes, dict[str, str]]] = fetch, log: Callable[[str], None] = print,
) -> bool:
    """Download the static feed once; store it if it is a new version. Returns True if new."""
    if not city.static_url:
        log(f"{city.id}: no static_url configured")
        return False
    now_ms = now_ms if now_ms is not None else int(time.time() * 1000)
    body, headers = download(city.static_url)
    try:
        zf = zipfile.ZipFile(io.BytesIO(body))
        if zf.testzip() is not None:
            raise zipfile.BadZipFile("corrupt member")
    except zipfile.BadZipFile as e:  # e.g. an HTML challenge page: store nothing
        raise ValueError(f"static feed download is not a valid zip ({len(body)} bytes): {e}") from e
    sha, files = canonical_hash(zf)
    prefix = K.static_version_prefix(city.id, sha)
    retrieved = datetime.fromtimestamp(now_ms / 1000, UTC).isoformat()

    new = not store.exists(prefix + "meta.json")
    if new:
        store.write(prefix + "original.zip", body, "application/zip")
        if store.read(prefix + "original.zip") != body:
            raise ValueError(f"{prefix}original.zip read back differently")
        meta = {
            "schema": "transit-pulse/static-version/v1", "city": city.id, "canonical_sha256": sha,
            "zip_sha256": hashlib.sha256(body).hexdigest(), "zip_bytes": len(body),
            "first_retrieved_at": retrieved, "source_url": city.static_url, "http": headers,
            "files": files, "canonical_excludes": sorted(VOLATILE),
            # As published in the first download; later downloads' feed_info is in seen records.
            "validity": validity(zf), "code_version": code_version,
        }
        # Written after the zip, so a version with meta.json always has its zip.
        store.write(prefix + "meta.json", json.dumps(meta, indent=1, sort_keys=True).encode(), "application/json")

    store.write(K.static_seen_key(city.id, now_ms), json.dumps({
        "canonical_sha256": sha, "zip_sha256": hashlib.sha256(body).hexdigest(), "retrieved_at": retrieved,
        "new_version": new, "http": headers, "files": files, "validity": validity(zf),
    }, sort_keys=True).encode(), "application/json")
    log(f"{city.id}: static GTFS {sha[:12]} ({len(body) / 1e6:.1f} MB, {len(files)} files): "
        + ("NEW version archived" if new else "unchanged"))
    return new


def versions_seen(store: Store, city: City, date: str) -> list[str]:
    """Canonical hashes of the timetables downloaded on `date` (UTC), in first-seen order."""
    out: list[str] = []
    for o in sorted(store.list(K.static_seen_prefix(city.id, date)), key=lambda o: o.key):
        sha = json.loads(store.read(o.key))["canonical_sha256"]
        if sha not in out:
            out.append(sha)
    return out
