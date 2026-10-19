import io
import json
import zipfile

import pytest

from transit_pulse import keys as K
from transit_pulse import pack, static
from transit_pulse.cities import City

from conftest import CITY, DAY, fill_hours

CITY_S = City(CITY.id, CITY.feeds, "https://example/static")
T0 = pack.day_start_ms(DAY) + 3_600_000


def gtfs_zip(files: dict[str, str], date_time=(2026, 1, 1, 0, 0, 0)) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, text in files.items():
            zf.writestr(zipfile.ZipInfo(name, date_time), text)
    return buf.getvalue()


FEED = {
    "agency.txt": "agency_id,agency_name\n1,ЦГМ\n",
    "feed_info.txt": "﻿feed_publisher_name,feed_start_date,feed_end_date,feed_version\nЦГМ,20261001,20271014,v42\n",
    "calendar.txt": "service_id,start_date,end_date\nwk,20261005,20270630\nwe,20261001,20271014\n",
}


def capture(store, body, ms=T0):
    return static.capture(store, CITY_S, now_ms=ms, download=lambda _: (body, {"etag": "x"}), log=lambda _: None)


def test_new_version_stored_once_with_validity(store):
    assert capture(store, gtfs_zip(FEED)) is True
    # Same content re-zipped with other timestamps: not a new version, but still recorded as seen.
    assert capture(store, gtfs_zip(FEED, (2026, 5, 5, 5, 5, 4)), T0 + 1000) is False
    [sha] = static.versions_seen(store, CITY_S, DAY)
    meta = json.loads(store.read(K.static_version_prefix(CITY.id, sha) + "meta.json"))
    assert meta["validity"] == {
        "feed_start_date": "20261001", "feed_end_date": "20271014", "feed_version": "v42",
        "service_first_date": "20261001", "service_last_date": "20271014",
    }
    assert store.read(K.static_version_prefix(CITY.id, sha) + "original.zip") == gtfs_zip(FEED)
    assert len(list(store.list(K.static_seen_prefix(CITY.id, DAY)))) == 2


def test_changed_timetable_is_a_new_version(store):
    capture(store, gtfs_zip(FEED))
    changed = {**FEED, "calendar.txt": FEED["calendar.txt"] + "hol,20261224,20261226\n"}
    assert capture(store, gtfs_zip(changed), T0 + 60_000) is True
    assert len(static.versions_seen(store, CITY_S, DAY)) == 2
    assert len(store.list_dirs(f"archive/{CITY.id}/static/versions/")) == 2


def test_rolling_feed_info_alone_is_not_a_new_version(store):
    capture(store, gtfs_zip(FEED))
    next_day = {**FEED, "feed_info.txt": FEED["feed_info.txt"].replace("20261001,20271014", "20261002,20271015")}
    assert capture(store, gtfs_zip(next_day), T0 + 86_400_000) is False
    [seen] = list(store.list(K.static_seen_prefix(CITY.id, "2026-10-16")))
    assert json.loads(store.read(seen.key))["validity"]["feed_start_date"] == "20261002"


def test_html_page_instead_of_zip_stores_nothing(store):
    with pytest.raises(ValueError, match="not a valid zip"):
        capture(store, b"<html>Just a moment...</html>")
    assert list(store.list("archive/")) == []


def test_raw_manifest_records_the_timetable_seen_that_day(store, tmp_path, after_day):
    capture(store, gtfs_zip(FEED))
    fill_hours(store, DAY, range(1))
    pack.run(store, CITY, tmp_path, delete=False, max_days=1, grace_ms=0, code_version="t", level=1,
             log=lambda _: None, now_ms=after_day)
    [(_, m)] = pack.load_manifests(store, CITY, DAY)
    assert m["static_versions_seen"] == static.versions_seen(store, CITY_S, DAY) != []
