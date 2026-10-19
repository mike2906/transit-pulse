"""Bucket key layout. The raw/ and errors/ shapes are written by the recorder Worker
(recorder/src/schedule.ts); everything under archive/ and derived/ is written here."""

from __future__ import annotations

import re

RAW_RE = re.compile(r"^raw/(?P<city>[^/]+)/(?P<feed>[^/]+)/(?P<date>\d{4}-\d{2}-\d{2})/\d{2}/(?P<ms>\d+)\.pb$")
ERROR_RE = re.compile(r"^errors/(?P<city>[^/]+)/(?P<date>\d{4}-\d{2}-\d{2})/(?P<ms>\d+)-(?P<feed>.+)\.json$")


def raw_prefix(city: str, feed: str, date: str = "") -> str:
    return f"raw/{city}/{feed}/{date + '/' if date else ''}"


def errors_prefix(city: str, date: str = "") -> str:
    return f"errors/{city}/{date + '/' if date else ''}"


def bundle_key(city: str, date: str, part: str, sha256: str) -> str:
    return f"archive/{city}/raw/{date}/{part}/{sha256}.tar.zst"


def raw_manifests_prefix(city: str, date: str = "") -> str:
    return f"archive/{city}/manifests/{date + '/' if date else ''}"


def raw_manifest_key(city: str, date: str, created_ms: int) -> str:
    # Several per day are possible: a later one covers only objects that arrived after the first.
    return f"{raw_manifests_prefix(city, date)}raw-v1-{created_ms}.json"


def static_version_prefix(city: str, canonical_sha: str) -> str:
    return f"archive/{city}/static/versions/{canonical_sha}/"


def static_seen_key(city: str, retrieved_ms: int) -> str:
    # One small record per download, so "which timetable was published on day D" is a listing.
    from datetime import UTC, datetime

    date = datetime.fromtimestamp(retrieved_ms / 1000, UTC).strftime("%Y-%m-%d")
    return f"{static_seen_prefix(city, date)}{retrieved_ms}.json"


def static_seen_prefix(city: str, date: str = "") -> str:
    return f"archive/{city}/static/seen/{date + '/' if date else ''}"


def derived_key(city: str, schema: str, table: str, date: str) -> str:
    return f"derived/{city}/{schema}/{table}/{date}/part-0.parquet"


def derived_manifest_key(city: str, schema: str, date: str) -> str:
    return f"derived/{city}/{schema}/manifests/{date}.json"
