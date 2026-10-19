import html
import json

import pytest

from transit_pulse import notices
from transit_pulse.cities import City

from conftest import CITY as BASE_CITY

CITY = City(BASE_CITY.id, BASE_CITY.feeds, notices="sofiatraffic")

NOW = 1_792_000_000_000  # 2026-10-15


def page(props: dict) -> str:
    return f'<div id="app" data-page="{html.escape(json.dumps({"component": "X", "props": props}))}"></div>'


def site(changes, articles, detail=None):
    detail = detail or {}

    def get(url):
        if url == f"{notices.BASE}/bg":
            return page({"routesChanges": {"data": [{"id": c["id"]} for c in changes], "next_page_url": None}})
        if "/transport/change/article/" in url:
            i = int(url.rsplit("/", 1)[1])
            return json.dumps(next(c for c in changes if c["id"] == i))
        if url.endswith("/info-center/news"):
            return page({"list": [{"id": 8, "pages": articles}]})
        slug = url.rsplit("/", 1)[1]
        return page({"page": detail.get(slug, {"slug": slug, "documents": []}), "list": articles})
    return get


CHANGE = {"id": 815, "date_from": "2026-10-13", "date_to": "2026-11-03", "cause": "Maintenance",
          "user_trans": {"description": "<p>Трамвайна линия № 3 ...</p>"}}
ARTICLES = [
    {"id": 557, "slug": "promeni-maraton", "date": "2026-10-08 17:07:14", "title": "Маратон", "header_article": "<p>a</p>"},
    {"id": 12, "slug": "old", "date": "2024-01-01 00:00:00", "title": "Old", "header_article": "<p>b</p>"},
]


def versions(store, kind, item):
    return list(store.list(f"archive/{CITY.id}/notices/sofiatraffic/{kind}/{item}/"))


def test_stores_once_and_again_only_on_edit(store):
    r = notices.capture(store, CITY, now_ms=NOW, get=site([CHANGE], ARTICLES), log=lambda _: None)
    assert r == {"route_changes": 1, "info_center": 2, "info_center_pages": 1, "new_versions": 4}
    # Unchanged run: nothing new, and no full-page fetch.
    r = notices.capture(store, CITY, now_ms=NOW + 60_000, get=site([CHANGE], ARTICLES), log=lambda _: None)
    assert r["new_versions"] == 0 and r["info_center_pages"] == 0
    # The operator edits the detour text: a second version, the first kept.
    edited = {**CHANGE, "user_trans": {"description": "<p>Трамвайна линия № 3 и № 4 ...</p>"}}
    notices.capture(store, CITY, now_ms=NOW + 120_000, get=site([edited], ARTICLES), log=lambda _: None)
    assert len(versions(store, "route-changes", 815)) == 2
    assert len(list(store.list(f"archive/{CITY.id}/notices/sofiatraffic/runs/"))) == 3


def test_removed_notice_is_kept_and_dated_by_run_records(store):
    notices.capture(store, CITY, now_ms=NOW, get=site([CHANGE], ARTICLES), log=lambda _: None)
    notices.capture(store, CITY, now_ms=NOW + 60_000, get=site([], ARTICLES), log=lambda _: None)
    assert len(versions(store, "route-changes", 815)) == 1
    runs = sorted(o.key for o in store.list(f"archive/{CITY.id}/notices/sofiatraffic/runs/"))
    assert json.loads(store.read(runs[-1]))["route_changes"] == {}


def test_layout_change_fails_loudly(store):
    def broken(url):
        return "<html>new design</html>"

    with pytest.raises(notices.LayoutChanged):
        notices.capture(store, CITY, now_ms=NOW, get=broken, log=lambda _: None)
    assert list(store.list("archive/")) == []
