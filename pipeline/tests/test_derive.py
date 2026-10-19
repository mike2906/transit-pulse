import json

import pyarrow.parquet as pq

from transit_pulse import derive, pack
from transit_pulse import keys as K

from conftest import CITY, DAY, fill_hours, put_capture

HOUR = 3_600_000


def pack_and_derive(store, tmp_path, now):
    assert pack.run(store, CITY, tmp_path / "p", delete=True, max_days=5, grace_ms=HOUR,
                    code_version="t", level=3, log=lambda _: None, now_ms=now) == 0
    return derive.run(store, CITY, tmp_path / "d", max_days=5, code_version="t", log=lambda _: None, now_ms=now)


def table(store, tmp_path, name):
    p = tmp_path / f"{name}.parquet"
    p.write_bytes(store.read(K.derived_key(CITY.id, derive.SCHEMA_VERSION, name, DAY)))
    return pq.read_table(p).to_pylist()


def test_tables_from_packed_day(store, tmp_path, after_day):
    start = pack.day_start_ms(DAY)
    fill_hours(store, DAY, range(1))
    put_capture(store, "vehicle-positions", start + 59 * 60_000 + 50_000, body=b"\xff not protobuf")
    assert pack_and_derive(store, tmp_path, after_day) == 0

    m = json.loads(store.read(K.derived_manifest_key(CITY.id, "v1", DAY)))
    assert m["parse_failures"] == 1
    snaps = table(store, tmp_path, "snapshots")
    assert len(snaps) == 239 + 60 + 12 + 1
    bad = [s for s in snaps if not s["parse_ok"]]
    assert len(bad) == 1 and bad[0]["feed"] == "vehicle-positions"

    vp = table(store, tmp_path, "vehicle_positions")
    assert len(vp) == 239 * 3
    assert {r["speed"] for r in vp} == {0.0, None}  # set-to-zero stays 0, absent stays null
    assert vp[0]["trip_schedule_relationship"] is None and vp[0]["route_id"] == "94"

    # t1's delay cycles through 4 values, so 4 versions of it, plus one of the cancelled t2.
    tus = table(store, tmp_path, "trip_updates")
    assert len(tus) == 5
    assert [t["trip_schedule_relationship"] for t in tus if t["trip_id"] == "trip-cancelled"] == ["CANCELED"]
    stus = table(store, tmp_path, "stop_time_updates")
    assert len(stus) == 4 * 3 and [s["position"] for s in stus[:3]] == [0, 1, 2]
    member = table(store, tmp_path, "entity_membership")
    assert sum(r["kind"] == "trip_update" for r in member) == 60 * 2

    [alert] = table(store, tmp_path, "alerts")
    assert json.loads(alert["alert_json"])["header_text"]["translation"][0]["text"] == "Ремонт"
    [err] = table(store, tmp_path, "fetch_errors")
    assert err["http_status"] == 503


def test_rederives_only_when_raw_manifests_change(store, tmp_path, after_day):
    fill_hours(store, DAY, range(1))
    assert pack_and_derive(store, tmp_path, after_day) == 0
    assert derive.pending_days(store, CITY) == []
    put_capture(store, "alerts", pack.day_start_ms(DAY) + 5 * HOUR)  # late arrival
    assert pack_and_derive(store, tmp_path, after_day + HOUR) == 0
    m = json.loads(store.read(K.derived_manifest_key(CITY.id, "v1", DAY)))
    assert len(m["source_manifests"]) == 2 and m["tables"]["snapshots"]["rows"] == 239 + 60 + 13
