"""How much of each feed's schedule was actually captured, from object keys alone."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field

from .cities import City
from .keys import ERROR_RE, RAW_RE

HOUR_MS = 3_600_000


def expected_slots(city: City, feed: str, start_ms: int, end_ms: int) -> int:
    """Fetches the recorder schedules for `feed` in [start_ms, end_ms), both on whole minutes."""
    f = next(f for f in city.feeds if f.feed == feed)
    minutes = range(start_ms // 60_000, end_ms // 60_000)
    return sum(len(f.offsets_s) for m in minutes if m % f.every_minutes == 0)


@dataclass
class FeedCoverage:
    feed: str
    expected: int = 0
    captured: int = 0
    errors: int = 0
    newest_ms: int = 0
    by_hour: dict[int, list[int]] = field(default_factory=dict)  # hour start ms -> [expected, captured, errors]

    @property
    def missing(self) -> int:
        """Slots with neither a capture nor an error record: the recorder never ran for them."""
        return max(0, self.expected - self.captured - self.errors)

    @property
    def ratio(self) -> float:
        return self.captured / self.expected if self.expected else 1.0

    def to_json(self) -> dict:
        return {
            "expected": self.expected,
            "captured": self.captured,
            "errors": self.errors,
            "missing": self.missing,
            "captured_ratio": round(self.ratio, 4),
            "hours": {str(h): v for h, v in sorted(self.by_hour.items())},
        }


def coverage(city: City, keys: list[str], start_ms: int, end_ms: int) -> dict[str, FeedCoverage]:
    """Per feed, captures and error records among `keys` for the window [start_ms, end_ms)."""
    out = {f: FeedCoverage(f) for f in city.feed_ids}
    hits: dict[tuple[str, int], list[int]] = defaultdict(lambda: [0, 0])
    for key in keys:
        if m := RAW_RE.match(key):
            kind = 0
        elif m := ERROR_RE.match(key):
            kind = 1
        else:
            continue
        feed, ms = m["feed"], int(m["ms"])
        if m["city"] != city.id or feed not in out or not start_ms <= ms < end_ms:
            continue
        hits[(feed, ms - ms % HOUR_MS)][kind] += 1
        if kind == 0:
            out[feed].newest_ms = max(out[feed].newest_ms, ms)
    for feed, cov in out.items():
        for hour in range(start_ms - start_ms % HOUR_MS, end_ms, HOUR_MS):
            lo, hi = max(hour, start_ms), min(hour + HOUR_MS, end_ms)
            exp = expected_slots(city, feed, lo, hi)
            got, err = hits.get((feed, hour), (0, 0))
            cov.by_hour[hour] = [exp, got, err]
            cov.expected += exp
            cov.captured += got
            cov.errors += err
    return out


def markdown_table(cov: dict[str, FeedCoverage]) -> str:
    """Counts only: safe to print in public logs."""
    lines = ["| Feed | Expected | Captured | Errors | Missing | Captured % |", "| --- | ---: | ---: | ---: | ---: | ---: |"]
    for c in cov.values():
        lines.append(f"| {c.feed} | {c.expected} | {c.captured} | {c.errors} | {c.missing} | {c.ratio:.1%} |")
    return "\n".join(lines)
