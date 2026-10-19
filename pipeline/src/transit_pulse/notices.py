"""Archive the transport operator's notices: route changes (closures, detours, diversions) and
info-centre news (roadworks, bus lanes, schedule changes).

These pages are edited and removed after the fact, so every run stores each item again only
when its content changed: an item's history is the set of versions under its folder, and each
run's record lists which items were live and at which version, so removals are dated too.

Source today: sofiatraffic.bg (Sofia's Urban Mobility Centre). Its pages are an Inertia.js app,
so the data comes from the JSON the page embeds (`data-page`) and from the JSON endpoint its
route-change pop-up uses, not from scraping HTML layout. If that JSON stops having the shape
read here, the run fails loudly instead of archiving nothing.

Logs carry counts only. Stored under archive/<city>/notices/ in the private bucket.
"""

from __future__ import annotations

import hashlib
import html
import json
import re
import time
import urllib.request
from datetime import UTC, datetime, timedelta
from typing import Callable

from .cities import City
from .store import Store

USER_AGENT = "transit-pulse (+https://github.com/mike2906/transit-pulse)"
BASE = "https://www.sofiatraffic.bg"
SOURCE = "sofiatraffic"
DETAIL_DAYS = 30  # full pages are fetched for new or changed news dated this recently
MAX_PAGES = 20


class LayoutChanged(RuntimeError):
    """The site no longer looks the way this code expects."""


def http_get(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "text/html,application/json"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return r.read(20 * 1024 * 1024).decode("utf-8")


def page_props(text: str, url: str) -> dict:
    m = re.search(r'data-page="([^"]+)"', text)
    if not m:
        raise LayoutChanged(f"{url}: no Inertia data-page")
    return json.loads(html.unescape(m.group(1)))["props"]


def _canonical(obj) -> bytes:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


class Archive:
    def __init__(self, store: Store, city: City):
        self.store, self.prefix = store, f"archive/{city.id}/notices/{SOURCE}/"
        self.new = 0

    def put(self, kind: str, item_id, obj) -> tuple[str, bool]:
        """Store `obj` as a version of item `kind/item_id` unless that exact version exists."""
        body = _canonical(obj)
        sha = hashlib.sha256(body).hexdigest()[:32]
        key = f"{self.prefix}{kind}/{item_id}/{sha}.json"
        if self.store.exists(key):
            return sha, False
        self.store.write(key, body, "application/json")
        self.new += 1
        return sha, True


def capture(
    store: Store, city: City, *, now_ms: int | None = None, get: Callable[[str], str] = http_get,
    log: Callable[[str], None] = print,
) -> dict:
    if city.notices != SOURCE:
        log(f"{city.id}: no notices source configured")
        return {}
    now_ms = now_ms if now_ms is not None else int(time.time() * 1000)
    now = datetime.fromtimestamp(now_ms / 1000, UTC)
    arc = Archive(store, city)
    run: dict = {"retrieved_at": now.isoformat(), "route_changes": {}, "info_center": {}, "info_center_pages": {}}

    # Route changes: the home page lists the active ones (paginated); each has a JSON article.
    url, pages = f"{BASE}/bg", 0
    while url and pages < MAX_PAGES:
        props = page_props(get(url), url)
        rc = props.get("routesChanges")
        if not isinstance(rc, dict) or not isinstance(rc.get("data"), list):
            raise LayoutChanged(f"{url}: no routesChanges list")
        for item in rc["data"]:
            article = json.loads(get(f"{BASE}/bg/transport/change/article/{int(item['id'])}"))
            if article.get("id") != item["id"]:
                raise LayoutChanged(f"route change {item['id']}: article JSON has another id")
            sha, _ = arc.put("route-changes", item["id"], article)
            run["route_changes"][str(item["id"])] = sha
        url, pages = rc.get("next_page_url"), pages + 1

    # Info centre: one page embeds every article's title, date and main text.
    url = f"{BASE}/bg/info-center/news"
    listing = page_props(get(url), url).get("list")
    if not isinstance(listing, list) or not listing:
        raise LayoutChanged(f"{url}: no info-centre list")
    recent = (now - timedelta(days=DETAIL_DAYS)).strftime("%Y-%m-%d")
    for category in listing:
        for entry in category["pages"]:
            sha, changed = arc.put("info-center", entry["id"], entry)
            run["info_center"][str(entry["id"])] = sha
            # The full page adds attachments and secondary text; fetch it when the summary is
            # new or edited, for recent items only (each page also embeds the whole list).
            if changed and (entry.get("date") or "") >= recent and entry.get("slug"):
                purl = f"{BASE}/bg/info-center/{entry['slug']}"
                page = page_props(get(purl), purl).get("page")
                if not isinstance(page, dict):
                    raise LayoutChanged(f"{purl}: no page object")
                run["info_center_pages"][str(entry["id"])] = arc.put("info-center-pages", entry["id"], page)[0]

    if not run["info_center"]:
        raise LayoutChanged("info-centre list has no articles")
    store.write(f"{arc.prefix}runs/{now:%Y-%m-%d}/{now_ms}.json", _canonical(run), "application/json")
    counts = {k: len(run[k]) for k in ("route_changes", "info_center", "info_center_pages")}
    log(f"{city.id}: notices from {SOURCE}: {counts['route_changes']} active route changes, "
        f"{counts['info_center']} info-centre articles, {counts['info_center_pages']} full pages fetched; "
        f"{arc.new} new versions stored")
    return {**counts, "new_versions": arc.new}
