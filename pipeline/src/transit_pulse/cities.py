"""City configs (`cities/<id>.json`), shared with the recorder Worker."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path

CITIES_DIR = Path(os.environ.get("CITIES_DIR") or Path(__file__).resolve().parents[3] / "cities")


@dataclass(frozen=True)
class Feed:
    feed: str
    every_minutes: int
    offsets_s: tuple[int, ...]


@dataclass(frozen=True)
class City:
    id: str
    feeds: tuple[Feed, ...]
    static_url: str = ""

    @property
    def feed_ids(self) -> list[str]:
        return [f.feed for f in self.feeds]


def load_city(city_id: str, cities_dir: Path = CITIES_DIR) -> City:
    raw = json.loads((cities_dir / f"{city_id}.json").read_text())
    feeds = tuple(Feed(f["feed"], f["every_minutes"], tuple(f["offsets_s"])) for f in raw["realtime"])
    return City(raw["id"], feeds, raw.get("static_url", ""))
