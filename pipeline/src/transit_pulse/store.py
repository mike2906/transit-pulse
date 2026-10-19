"""Object storage the pipeline reads and writes: R2 through its S3 API, or a local folder.

The local folder mirrors the bucket's key layout and exists so every stage can be run and
tested against sample data without touching R2.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Iterator, Protocol


@dataclass(frozen=True)
class ObjectInfo:
    key: str
    size: int
    etag: str = ""  # from the listing; MD5 for single-part uploads, which is all the Worker does


@dataclass
class ObjectMeta:
    """What the bucket knows about an object besides its bytes."""

    size: int
    content_type: str = ""
    etag: str = ""
    last_modified: str = ""  # ISO 8601 UTC
    metadata: dict[str, str] = field(default_factory=dict)  # the Worker's customMetadata


class Store(Protocol):
    def list(self, prefix: str) -> Iterator[ObjectInfo]: ...
    def list_dirs(self, prefix: str) -> list[str]: ...
    def exists(self, key: str) -> bool: ...
    def download(self, key: str, path: Path) -> ObjectMeta: ...
    def read(self, key: str) -> bytes: ...
    def upload(self, key: str, path: Path, content_type: str) -> None: ...
    def write(self, key: str, data: bytes, content_type: str) -> None: ...
    def delete(self, keys: Iterable[str]) -> int: ...


def open_store(url: str) -> Store:
    """`s3://bucket` (R2 credentials from the environment) or `file:///path` / a plain path."""
    if url.startswith("s3://"):
        return S3Store(url.removeprefix("s3://").strip("/"))
    return LocalStore(Path(url.removeprefix("file://")))


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


class S3Store:
    """R2 via the S3 API, so a bucket-scoped R2 token is enough (the REST API needs more).

    Environment: R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, and R2_ENDPOINT or R2_ACCOUNT_ID.
    """

    def __init__(self, bucket: str, client=None):
        self.bucket = bucket
        if client is None:
            import boto3
            from botocore.config import Config

            endpoint = os.environ.get("R2_ENDPOINT") or (
                f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"
            )
            client = boto3.client(
                "s3",
                endpoint_url=endpoint,
                aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
                aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
                region_name="auto",
                config=Config(
                    retries={"max_attempts": 3, "mode": "standard"},
                    max_pool_connections=32,
                    # R2 does not support every checksum newer boto3 sends by default.
                    request_checksum_calculation="when_required",
                    response_checksum_validation="when_required",
                ),
            )
        self.client = client

    def list(self, prefix: str) -> Iterator[ObjectInfo]:
        pages = self.client.get_paginator("list_objects_v2").paginate(Bucket=self.bucket, Prefix=prefix)
        for page in pages:
            for o in page.get("Contents", []):
                yield ObjectInfo(o["Key"], o["Size"], o.get("ETag", "").strip('"'))

    def list_dirs(self, prefix: str) -> list[str]:
        pages = self.client.get_paginator("list_objects_v2").paginate(
            Bucket=self.bucket, Prefix=prefix, Delimiter="/"
        )
        return sorted(
            p["Prefix"].removeprefix(prefix).rstrip("/") for page in pages for p in page.get("CommonPrefixes", [])
        )

    def exists(self, key: str) -> bool:
        from botocore.exceptions import ClientError

        try:
            self.client.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
                return False
            raise

    def download(self, key: str, path: Path) -> ObjectMeta:
        # One GET returns body and metadata, so no HEAD is needed (saves a Class B op per object).
        r = self.client.get_object(Bucket=self.bucket, Key=key)
        with path.open("wb") as f:
            shutil.copyfileobj(r["Body"], f, 1 << 20)
        return ObjectMeta(
            size=r["ContentLength"],
            content_type=r.get("ContentType", ""),
            etag=r.get("ETag", "").strip('"'),
            last_modified=r["LastModified"].isoformat() if r.get("LastModified") else "",
            metadata=dict(r.get("Metadata", {})),
        )

    def read(self, key: str) -> bytes:
        return self.client.get_object(Bucket=self.bucket, Key=key)["Body"].read()

    def upload(self, key: str, path: Path, content_type: str) -> None:
        self.client.upload_file(str(path), self.bucket, key, ExtraArgs={"ContentType": content_type})

    def write(self, key: str, data: bytes, content_type: str) -> None:
        self.client.put_object(Bucket=self.bucket, Key=key, Body=data, ContentType=content_type)

    def delete(self, keys: Iterable[str]) -> int:
        keys = list(keys)
        deleted = 0
        for i in range(0, len(keys), 1000):
            batch = keys[i : i + 1000]
            r = self.client.delete_objects(
                Bucket=self.bucket, Delete={"Objects": [{"Key": k} for k in batch], "Quiet": True}
            )
            if r.get("Errors"):
                raise RuntimeError(f"{len(r['Errors'])} of {len(batch)} deletes failed: {r['Errors'][0]}")
            deleted += len(batch)
        return deleted


class LocalStore:
    """A folder laid out like the bucket. Object metadata lives in `<root>/.meta/<key>.json`."""

    def __init__(self, root: Path):
        self.root = root

    def _path(self, key: str) -> Path:
        return self.root / key

    def _meta_path(self, key: str) -> Path:
        return self.root / ".meta" / f"{key}.json"

    def list(self, prefix: str) -> Iterator[ObjectInfo]:
        # Walk only the deepest folder the prefix names, like a prefix listing would.
        base = self.root / prefix.rpartition("/")[0]
        if not base.is_dir():
            return
        found = []
        for p in base.rglob("*"):
            key = p.relative_to(self.root).as_posix()
            if p.is_file() and key.startswith(prefix) and not key.startswith(".meta/"):
                found.append(ObjectInfo(key, p.stat().st_size))
        yield from sorted(found, key=lambda o: o.key)

    def list_dirs(self, prefix: str) -> list[str]:
        base = self.root / prefix
        return sorted(p.name for p in base.iterdir() if p.is_dir()) if base.is_dir() else []

    def exists(self, key: str) -> bool:
        return self._path(key).is_file()

    def download(self, key: str, path: Path) -> ObjectMeta:
        shutil.copyfile(self._path(key), path)
        meta = self._meta_path(key)
        extra = json.loads(meta.read_text()) if meta.is_file() else {}
        return ObjectMeta(size=path.stat().st_size, **extra)

    def read(self, key: str) -> bytes:
        return self._path(key).read_bytes()

    def put(self, key: str, data: bytes, **meta) -> None:
        """Test and sample-data helper: write an object with metadata, like the Worker does."""
        self.write(key, data, meta.get("content_type", ""))
        self._meta_path(key).parent.mkdir(parents=True, exist_ok=True)
        self._meta_path(key).write_text(json.dumps(meta))

    def upload(self, key: str, path: Path, content_type: str) -> None:
        self._path(key).parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(path, self._path(key))

    def write(self, key: str, data: bytes, content_type: str) -> None:
        self._path(key).parent.mkdir(parents=True, exist_ok=True)
        self._path(key).write_bytes(data)

    def delete(self, keys: Iterable[str]) -> int:
        n = 0
        for key in keys:
            self._path(key).unlink(missing_ok=True)
            self._meta_path(key).unlink(missing_ok=True)
            n += 1
        return n
