"""Daily raw preservation: bundle each closed UTC day, verify the upload, write a manifest, and
only then (if asked) delete the landing objects that manifest covers.

Order per day, so a crash at any point loses nothing:
1. List the day's raw/ and errors/ objects not already covered by a manifest.
2. Download them (the GET returns the Worker's metadata too) and hash each one.
3. Write one deterministic tar.zst per feed (plus one for error records), with `index.jsonl`
   first: key, size, sha256 and metadata of every member.
4. Upload, download again, decompress and check every member against the inventory.
5. Write the manifest last. It is the only thing deletion trusts.
6. Delete exactly the keys listed in the day's manifests, and only with `delete=True`.

A rerun after a crash before step 5 repeats the day from scratch; nothing was deleted. A rerun
after a crash during step 6 finds every remaining object already covered and just finishes the
deletes. Objects that turn up after a manifest was written get their own later manifest.
Parquet is a separate stage (derive.py), so a decoding bug can never hold up cleanup.
"""

from __future__ import annotations

import hashlib
import io
import json
import shutil
import tarfile
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Callable

import zstandard as zstd

from . import keys as K
from .cities import City
from .coverage import coverage, markdown_table
from .store import ObjectInfo, Store, sha256_file

DAY_MS = 86_400_000
WINDOW_LOG = 27  # same as `zstd --long`; `tar --zstd -xf` restores it with default limits
INDEX = "index.jsonl"
MANIFEST_SCHEMA = "transit-pulse/raw-manifest/v1"


def day_start_ms(date: str) -> int:
    return int(datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=UTC).timestamp() * 1000)


def outstanding_days(store: Store, city: City, now_ms: int, grace_ms: int) -> list[str]:
    """Closed UTC days that still have landing objects, oldest first."""
    dates: set[str] = set(store.list_dirs(K.errors_prefix(city.id)))
    for feed in city.feed_ids:
        dates |= set(store.list_dirs(K.raw_prefix(city.id, feed)))
    return sorted(d for d in dates if day_start_ms(d) + DAY_MS + grace_ms <= now_ms)


def load_manifests(store: Store, city: City, date: str) -> list[tuple[str, dict]]:
    found = sorted(o.key for o in store.list(K.raw_manifests_prefix(city.id, date)) if o.key.endswith(".json"))
    return [(k, json.loads(store.read(k))) for k in found]


def _tarinfo(name: str, size: int) -> tarfile.TarInfo:
    # Fixed ownership and times so the same objects always give the same bytes.
    info = tarfile.TarInfo(name)
    info.size, info.mtime, info.mode = size, 0, 0o644
    info.uid = info.gid = 0
    info.uname = info.gname = ""
    return info


def write_bundle(entries: list[dict], files: dict[str, Path], out: Path, level: int, threads: int) -> int:
    """Write `out` as tar.zst; returns the uncompressed tar size. `entries` must be sorted by key."""
    params = zstd.ZstdCompressionParameters.from_level(
        level, window_log=WINDOW_LOG, enable_ldm=True, threads=threads
    )
    index = "".join(json.dumps(e, sort_keys=True) + "\n" for e in entries).encode()
    with out.open("wb") as fh:
        with zstd.ZstdCompressor(compression_params=params).stream_writer(fh, closefd=False) as zw:
            with tarfile.open(fileobj=zw, mode="w|", format=tarfile.PAX_FORMAT) as tar:
                tar.addfile(_tarinfo(INDEX, len(index)), io.BytesIO(index))
                for e in entries:
                    with files[e["key"]].open("rb") as f:
                        tar.addfile(_tarinfo(f"objects/{e['key']}", e["size"]), f)
            tar_bytes = tar.offset
    return tar_bytes


def verify_bundle(path: Path, entries: list[dict]) -> None:
    """Decompress `path` and check it holds exactly `entries`, byte for byte. Raises on any doubt."""
    expected = {e["key"]: e for e in entries}
    seen: set[str] = set()
    index: list[dict] | None = None
    dctx = zstd.ZstdDecompressor(max_window_size=1 << WINDOW_LOG)
    with path.open("rb") as fh, dctx.stream_reader(fh) as zr, tarfile.open(fileobj=zr, mode="r|") as tar:
        for member in tar:
            data = tar.extractfile(member)
            if data is None:
                raise ValueError(f"bundle has a non-file member {member.name}")
            if index is None:
                if member.name != INDEX:
                    raise ValueError(f"bundle starts with {member.name}, not {INDEX}")
                index = [json.loads(line) for line in data.read().splitlines()]
                if index != entries:
                    raise ValueError("bundle index differs from the inventory")
                continue
            key = member.name.removeprefix("objects/")
            if key not in expected or key in seen:
                raise ValueError(f"unexpected or repeated member {member.name}")
            h = hashlib.sha256()
            size = 0
            for chunk in iter(lambda: data.read(1 << 20), b""):
                h.update(chunk)
                size += len(chunk)
            if size != expected[key]["size"] or h.hexdigest() != expected[key]["sha256"]:
                raise ValueError(f"member {key} does not match its inventory entry")
            seen.add(key)
    if index is None or seen != expected.keys():
        raise ValueError(f"bundle holds {len(seen)} of {len(expected)} objects")


def read_bundle(path: Path):
    """Yields (index entry, member bytes) for every object in a bundle."""
    dctx = zstd.ZstdDecompressor(max_window_size=1 << WINDOW_LOG)
    with path.open("rb") as fh, dctx.stream_reader(fh) as zr, tarfile.open(fileobj=zr, mode="r|") as tar:
        index: dict[str, dict] = {}
        for member in tar:
            data = tar.extractfile(member).read()
            if member.name == INDEX:
                index = {e["key"]: e for e in map(json.loads, data.splitlines())}
                continue
            yield index[member.name.removeprefix("objects/")], data


def verify_day(store: Store, city: City, date: str, workdir: Path) -> tuple[str, bool]:
    """Independent restore test: re-download every bundle of a day, check each object against
    the manifests, and compare with any landing copies still in the bucket. Run it on a day
    before turning deletion on."""
    want = {o["key"]: o for _, m in load_manifests(store, city, date) for o in m["objects"]}
    if not want:
        return f"{date}: no manifest", False
    restored: dict[str, str] = {}
    workdir.mkdir(parents=True, exist_ok=True)
    for _, m in load_manifests(store, city, date):
        for b in m["bundles"]:
            local = workdir / "bundle.tar.zst"
            store.download(b["key"], local)
            if sha256_file(local) != b["sha256"]:
                return f"{date}: bundle {b['key']} does not match its manifest hash", False
            for entry, data in read_bundle(local):
                restored[entry["key"]] = hashlib.sha256(data).hexdigest()
            local.unlink()
    bad = [k for k, o in want.items() if restored.get(k) != o["sha256"]]
    landing = [o for f in [*city.feed_ids] for o in store.list(K.raw_prefix(city.id, f, date))]
    landing += list(store.list(K.errors_prefix(city.id, date)))
    differ = 0
    for o in landing:
        path = workdir / "landing"
        store.download(o.key, path)
        if sha256_file(path) != restored.get(o.key):
            differ += 1
    ok = not bad and not differ and len(restored) == len(want)
    return (
        f"{date}: restored {len(restored)} of {len(want)} manifest objects, {len(bad)} mismatched; "
        f"{len(landing)} landing objects compared, {differ} differ or are not in a bundle",
        ok,
    )


def _same(entry: dict, listed: ObjectInfo) -> bool:
    """The landing object is still the one the manifest preserved: same size and, where both
    sides have one, same ETag."""
    if entry["size"] != listed.size:
        return False
    return not (entry.get("etag") and listed.etag) or entry["etag"] == listed.etag


@dataclass
class DayResult:
    date: str
    packed: int = 0  # objects newly preserved by this run
    bundles: list[dict] = field(default_factory=list)
    covered: int = 0  # objects covered by all of the day's manifests
    remaining: int = 0  # landing objects still present after this run
    deleted: int = 0
    skipped_deletes: int = 0  # covered keys whose size or ETag no longer matches: never deleted
    coverage_md: str = ""
    uncovered: int = 0  # landing objects no manifest covered when the run started


def _preserve(
    store: Store, city: City, date: str, new: dict[str, list[ObjectInfo]], workdir: Path,
    level: int, threads: int, workers: int,
) -> tuple[list[dict], list[dict]]:
    """Steps 2-4 for each part with new objects. Returns (bundles, object entries)."""
    bundles, all_entries = [], []
    for part, objs in new.items():
        if not objs:
            continue
        pdir = workdir / part
        files = {o.key: pdir / "objects" / o.key for o in objs}
        for p in files.values():
            p.parent.mkdir(parents=True, exist_ok=True)

        def fetch(o: ObjectInfo) -> dict:
            meta = store.download(o.key, files[o.key])
            size = files[o.key].stat().st_size
            if size != o.size or meta.size != o.size:
                raise ValueError(f"{o.key}: listed {o.size} bytes, downloaded {size}")
            return {
                "key": o.key, "size": size, "sha256": sha256_file(files[o.key]),
                "content_type": meta.content_type, "etag": meta.etag or o.etag,
                "last_modified": meta.last_modified, "metadata": meta.metadata,
            }

        with ThreadPoolExecutor(workers) as ex:
            entries = sorted(ex.map(fetch, objs), key=lambda e: e["key"])

        local = workdir / f"{part}.tar.zst"
        tar_bytes = write_bundle(entries, files, local, level, threads)
        sha = sha256_file(local)
        key = K.bundle_key(city.id, date, part, sha)
        store.upload(key, local, "application/zstd")

        readback = workdir / f"{part}.readback.tar.zst"
        store.download(key, readback)
        if sha256_file(readback) != sha:
            raise ValueError(f"{key}: uploaded bundle differs from the local one")
        verify_bundle(readback, entries)

        bundles.append({
            "part": part, "key": key, "sha256": sha, "bytes": local.stat().st_size,
            "tar_bytes": tar_bytes, "objects": len(entries),
            "object_bytes": sum(e["size"] for e in entries),
        })
        all_entries += [
            {"key": e["key"], "size": e["size"], "etag": e["etag"], "sha256": e["sha256"], "bundle": sha}
            for e in entries
        ]
        shutil.rmtree(pdir)
        readback.unlink()
        local.unlink()
    return bundles, all_entries


def pack_day(
    store: Store, city: City, date: str, workdir: Path, *, delete: bool, now_ms: int,
    code_version: str, level: int = 19, threads: int = -1, workers: int = 16, allow_pack: bool = True,
) -> DayResult:
    """Preserve the day's uncovered objects (if `allow_pack`), then delete covered ones (if `delete`)."""
    res = DayResult(date)
    manifests = load_manifests(store, city, date)
    covered = {o["key"]: o for _, m in manifests for o in m["objects"]}

    parts = {feed: list(store.list(K.raw_prefix(city.id, feed, date))) for feed in city.feed_ids}
    parts["errors"] = list(store.list(K.errors_prefix(city.id, date)))
    present = {o.key: o for objs in parts.values() for o in objs}
    new = {part: [o for o in objs if o.key not in covered] for part, objs in parts.items()}

    res.uncovered = sum(map(len, new.values()))
    if res.uncovered and allow_pack:
        workdir.mkdir(parents=True, exist_ok=True)
        bundles, entries = _preserve(store, city, date, new, workdir, level, threads, workers)
        all_keys = sorted(covered.keys() | {e["key"] for e in entries})
        start = day_start_ms(date)
        cov = coverage(city, all_keys, start, start + DAY_MS)
        manifest = {
            "schema": MANIFEST_SCHEMA,
            "city": city.id,
            "date": date,
            "created_at": datetime.fromtimestamp(now_ms / 1000, UTC).isoformat(),
            "code_version": code_version,
            # Earlier manifests for this day; this one covers only what they did not.
            "earlier_manifests": [k for k, _ in manifests],
            "zstd": {"level": level, "window_log": WINDOW_LOG, "long_distance_matching": True},
            "bundles": bundles,
            "objects": entries,
            # Coverage of the whole day so far: preserved is not the same as complete.
            "coverage": {f: c.to_json() for f, c in cov.items()},
            "verification": "every bundle re-downloaded, decompressed and matched member by member",
        }
        mkey = K.raw_manifest_key(city.id, date, now_ms)
        body = json.dumps(manifest, indent=1, sort_keys=True).encode()
        store.write(mkey, body, "application/json")
        if store.read(mkey) != body:
            raise ValueError(f"{mkey}: manifest read back differently")
        covered.update({e["key"]: e for e in entries})
        res.packed, res.bundles, res.coverage_md = len(entries), bundles, markdown_table(cov)

    res.covered = len(covered)
    if delete:
        doomed = [k for k, o in present.items() if k in covered and _same(covered[k], o)]
        res.skipped_deletes = sum(1 for k in present if k in covered) - len(doomed)
        res.deleted = store.delete(doomed)
    res.remaining = len(present) - res.deleted
    return res


def run(
    store: Store, city: City, workdir: Path, *, delete: bool, max_days: int, grace_ms: int,
    code_version: str, level: int = 19, log: Callable[[str], None] = print, now_ms: int | None = None,
) -> int:
    """Pack every outstanding closed day, oldest first. Returns the number of days that failed.

    Logs counts and coverage only, never object contents.
    """
    now_ms = now_ms if now_ms is not None else int(time.time() * 1000)
    days = outstanding_days(store, city, now_ms, grace_ms)
    log(f"{city.id}: {len(days)} closed day(s) with landing objects; packing up to {max_days}")
    failed = packed_days = 0
    for date in days:
        # Days already preserved stay listed until deletion is on, so the budget counts only
        # days that need packing; covered days just get their deletes finished.
        allow_pack = packed_days < max_days
        t0 = time.monotonic()
        try:
            r = pack_day(
                store, city, date, workdir / date, delete=delete, now_ms=now_ms,
                code_version=code_version, level=level, allow_pack=allow_pack,
            )
        except Exception as e:  # keep going: later days can still be preserved
            failed += 1
            packed_days += 1
            log(f"\n### {date}: FAILED, nothing deleted\n\n{type(e).__name__}: {e}")
            continue
        finally:
            shutil.rmtree(workdir / date, ignore_errors=True)
        if r.packed:
            packed_days += 1
        elif not r.deleted and not (r.uncovered and not allow_pack):
            continue  # already preserved, nothing to do: keep the log short
        size = sum(b["bytes"] for b in r.bundles)
        raw = sum(b["object_bytes"] for b in r.bundles)
        log(
            f"\n### {date}\n\npacked {r.packed} new objects into {len(r.bundles)} bundle(s), "
            f"{raw / 1e6:.1f} MB -> {size / 1e6:.1f} MB; {r.covered} covered by manifests; "
            f"deleted {r.deleted}, skipped {r.skipped_deletes}, {r.remaining} still in landing; "
            f"{time.monotonic() - t0:.0f} s"
            + (f"; {r.uncovered} objects left for a later run" if r.uncovered and not r.packed else "")
        )
        if r.coverage_md:
            log("\n" + r.coverage_md)
    return failed
