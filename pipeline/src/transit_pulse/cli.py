"""`transit-pulse <command>`: pack, derive, check, coverage.

Storage is `--store s3://<bucket>` (R2 credentials from the environment) or a local folder
laid out like the bucket, for trying things on sample data.
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import mkdtemp

from . import check, derive, pack, static
from .cities import load_city
from .store import open_store


def _out(text: str) -> None:
    """Print, and append to the GitHub job summary when running in Actions."""
    print(text, flush=True)
    if summary := os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(summary, "a") as f:
            f.write(text + "\n")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="transit-pulse")
    ap.add_argument("--city", default="sofia")
    ap.add_argument("--store", default=os.environ.get("TRANSIT_PULSE_STORE", "s3://transit-pulse-raw"))
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("pack", help="preserve closed days as verified bundles (+ manifests)")
    p.add_argument("--delete", action="store_true", help="delete landing objects covered by a manifest")
    p.add_argument("--max-days", type=int, default=4)
    p.add_argument("--grace-hours", type=float, default=1.0, help="wait this long after UTC midnight")
    p.add_argument("--level", type=int, default=19, help="zstd level")
    p.add_argument("--workdir", type=Path)

    d = sub.add_parser("derive", help="decode packed days into Parquet")
    d.add_argument("--max-days", type=int, default=4)
    d.add_argument("--workdir", type=Path)

    sub.add_parser("static", help="download the static GTFS; archive it if it is a new version")

    v = sub.add_parser("verify", help="restore test: re-check a packed day against manifests and landing")
    v.add_argument("--date", required=True)

    sub.add_parser("check", help="per-feed freshness and last-hour coverage; exit 1 on a problem")

    c = sub.add_parser("coverage", help="daily coverage report; exit 1 if a feed is under 90%%")
    c.add_argument("--date", help="UTC date, default yesterday")

    args = ap.parse_args(argv)
    city = load_city(args.city)
    store = open_store(args.store)
    version = os.environ.get("GITHUB_SHA", "local")

    if args.cmd == "pack":
        failed = pack.run(
            store, city, args.workdir or Path(mkdtemp(prefix="pack-")), delete=args.delete,
            max_days=args.max_days, grace_ms=int(args.grace_hours * 3_600_000), code_version=version,
            level=args.level, log=_out,
        )
        return 1 if failed else 0
    if args.cmd == "derive":
        failed = derive.run(store, city, args.workdir or Path(mkdtemp(prefix="derive-")),
                            max_days=args.max_days, code_version=version, log=_out)
        return 1 if failed else 0
    if args.cmd == "static":
        static.capture(store, city, code_version=version, log=_out)
        return 0
    if args.cmd == "verify":
        report, ok = pack.verify_day(store, city, args.date, Path(mkdtemp(prefix="verify-")))
        _out(report)
        return 0 if ok else 1
    if args.cmd == "check":
        report, problems = check.check(store, city)
        _out(report)
        return 1 if problems else 0
    date = args.date or (datetime.now(UTC) - timedelta(days=1)).strftime("%Y-%m-%d")
    report, ok = check.daily_coverage(store, city, date)
    _out(report)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
