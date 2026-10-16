import hashlib
import json
import tarfile

import pytest
import zstandard as zstd

from transit_pulse import pack
from transit_pulse.store import LocalStore

from conftest import CITY, DAY, fill_hours, put_capture

HOUR = 3_600_000


def run(store, tmp_path, now, delete=False):
    return pack.run(store, CITY, tmp_path / "work", delete=delete, max_days=10, grace_ms=HOUR,
                    code_version="test", level=3, log=lambda _: None, now_ms=now)


def manifests(store, date=DAY):
    return [m for _, m in pack.load_manifests(store, CITY, date)]


def landing(store):
    return sorted(o.key for p in ("raw/", "errors/") for o in store.list(p))


def test_packs_verifies_and_keeps_landing_without_delete(store, tmp_path, after_day):
    keys = fill_hours(store, DAY, range(2))
    assert run(store, tmp_path, after_day) == 0

    [m] = manifests(store)
    assert sorted(o["key"] for o in m["objects"]) == sorted(keys)
    assert {b["part"] for b in m["bundles"]} == {"vehicle-positions", "trip-updates", "alerts", "errors"}
    vp = m["coverage"]["vehicle-positions"]
    assert (vp["captured"], vp["errors"]) == (2 * 240 - 2, 2)
    assert vp["missing"] == 22 * 240  # the 22 hours with no data at all
    assert landing(store) == sorted(keys)  # delete is off by default

    # A second run finds everything covered and writes nothing new.
    assert run(store, tmp_path, after_day) == 0
    assert len(manifests(store)) == 1


def test_bundle_restores_every_object_byte_for_byte(store, tmp_path, after_day):
    keys = fill_hours(store, DAY, range(1))
    run(store, tmp_path, after_day)
    [m] = manifests(store)
    restored = {}
    for b in m["bundles"]:
        raw = store.read(b["key"])
        assert hashlib.sha256(raw).hexdigest() == b["sha256"]
        with zstd.ZstdDecompressor().stream_reader(raw) as zr, tarfile.open(fileobj=zr, mode="r|") as tar:
            for member in tar:
                data = tar.extractfile(member).read()
                if member.name == "index.jsonl":
                    index = [json.loads(line) for line in data.splitlines()]
                    if b["part"] != "errors":
                        assert index[0]["metadata"]["status"] == "200"
                else:
                    restored[member.name.removeprefix("objects/")] = data
    assert sorted(restored) == sorted(keys)
    assert all(restored[k] == store.read(k) for k in keys)


def test_delete_removes_only_covered_closed_day_objects(store, tmp_path, after_day):
    fill_hours(store, DAY, range(1))
    today = put_capture(store, "vehicle-positions", after_day - 60_000)  # the day still open
    assert run(store, tmp_path, after_day, delete=True) == 0
    assert landing(store) == [today]
    assert len(manifests(store)) == 1


def test_late_object_gets_its_own_manifest(store, tmp_path, after_day):
    fill_hours(store, DAY, range(1))
    run(store, tmp_path, after_day, delete=True)
    late = put_capture(store, "trip-updates", pack.day_start_ms(DAY) + 23 * HOUR)
    run(store, tmp_path, after_day + HOUR, delete=True)
    first, second = manifests(store)
    assert [o["key"] for o in second["objects"]] == [late]
    assert second["earlier_manifests"] and not first["earlier_manifests"]
    assert second["coverage"]["trip-updates"]["captured"] == 61
    assert landing(store) == []


def test_crash_during_delete_resumes_from_manifest(store, tmp_path, after_day, monkeypatch):
    keys = fill_hours(store, DAY, range(1))
    real_delete = LocalStore.delete

    def crash_halfway(self, ks):
        ks = list(ks)
        real_delete(self, ks[: len(ks) // 2])
        raise RuntimeError("runner lost")

    monkeypatch.setattr(LocalStore, "delete", crash_halfway)
    assert run(store, tmp_path, after_day, delete=True) == 1
    assert 0 < len(landing(store)) < len(keys)

    monkeypatch.setattr(LocalStore, "delete", real_delete)
    assert run(store, tmp_path, after_day, delete=True) == 0
    assert landing(store) == []
    [m] = manifests(store)  # no second, partial manifest built from the leftovers
    assert len(m["objects"]) == len(keys)


def test_corrupt_upload_writes_no_manifest_and_deletes_nothing(store, tmp_path, after_day, monkeypatch):
    keys = fill_hours(store, DAY, range(1))
    real_upload = LocalStore.upload

    def corrupt(self, key, path, content_type):
        real_upload(self, key, path, content_type)
        p = self.root / key
        p.write_bytes(p.read_bytes()[:-10])

    monkeypatch.setattr(LocalStore, "upload", corrupt)
    assert run(store, tmp_path, after_day, delete=True) == 1
    assert manifests(store) == []
    assert landing(store) == sorted(keys)


def test_bundle_rejects_a_changed_member(tmp_path):
    f = tmp_path / "a.pb"
    f.write_bytes(b"abc")
    entries = [{"key": "raw/x/a.pb", "size": 3, "sha256": pack.sha256_file(f)}]
    out = tmp_path / "b.tar.zst"
    pack.write_bundle(entries, {"raw/x/a.pb": f}, out, level=3, threads=0)
    pack.verify_bundle(out, entries)
    with pytest.raises(ValueError):
        pack.verify_bundle(out, [{**entries[0], "sha256": "0" * 64}])
    with pytest.raises(ValueError):
        pack.verify_bundle(out, entries + [{"key": "raw/x/b.pb", "size": 1, "sha256": "x"}])


def test_bundles_are_deterministic(tmp_path):
    f = tmp_path / "a.pb"
    f.write_bytes(b"abc" * 1000)
    entries = [{"key": "raw/x/a.pb", "size": 3000, "sha256": pack.sha256_file(f)}]
    shas = set()
    for i in range(2):
        out = tmp_path / f"{i}.tar.zst"
        pack.write_bundle(entries, {"raw/x/a.pb": f}, out, level=3, threads=2)
        shas.add(pack.sha256_file(out))
    assert len(shas) == 1


def test_open_day_is_not_packed(store, tmp_path):
    fill_hours(store, DAY, range(1))
    end_of_day = pack.day_start_ms(DAY) + pack.DAY_MS
    assert pack.outstanding_days(store, CITY, end_of_day + HOUR - 1, HOUR) == []
    assert pack.outstanding_days(store, CITY, end_of_day + HOUR, HOUR) == [DAY]


def test_verify_day_passes_then_catches_a_changed_landing_object(store, tmp_path, after_day):
    keys = fill_hours(store, DAY, range(1))
    assert pack.verify_day(store, CITY, DAY, tmp_path / "v")[1] is False  # nothing packed yet
    run(store, tmp_path, after_day)
    report, ok = pack.verify_day(store, CITY, DAY, tmp_path / "v")
    assert ok, report
    (store.root / keys[0]).write_bytes(b"changed")
    assert pack.verify_day(store, CITY, DAY, tmp_path / "v")[1] is False


def test_day_budget_skips_days_already_preserved(store, tmp_path, after_day):
    fill_hours(store, DAY, range(1))
    fill_hours(store, "2026-10-16", range(1))
    later = after_day + pack.DAY_MS

    def one_day():
        return pack.run(store, CITY, tmp_path / "w", delete=False, max_days=1, grace_ms=HOUR,
                        code_version="t", level=3, log=lambda _: None, now_ms=later)

    one_day()
    assert len(manifests(store)) == 1 and manifests(store, "2026-10-16") == []
    one_day()  # DAY is still in landing (delete off) but must not use up the budget
    assert len(manifests(store, "2026-10-16")) == 1
