"""S3Store against moto's fake S3, so the boto3 calls R2 will see are exercised."""

import boto3
import pytest
from moto import mock_aws

from transit_pulse import pack
from transit_pulse.store import S3Store

from conftest import CITY, DAY


@pytest.fixture
def s3():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="raw")
        yield S3Store("raw", client=client)


def test_listing_metadata_and_batched_deletes(s3, tmp_path):
    for i in range(1205):  # more than one page and one delete batch
        s3.client.put_object(Bucket="raw", Key=f"raw/c/f/2026-10-15/00/{i:05}.pb", Body=b"x" * i,
                             Metadata={"slot": str(i)})
    s3.write("raw/c/g/2026-10-16/00/1.pb", b"y", "application/x-protobuf")
    assert s3.list_dirs("raw/c/") == ["f", "g"]
    assert s3.list_dirs("raw/c/f/") == ["2026-10-15"]
    objs = list(s3.list("raw/c/f/"))
    assert len(objs) == 1205 and objs[3].size == 3

    meta = s3.download(objs[7].key, tmp_path / "o")
    assert (meta.size, meta.metadata, (tmp_path / "o").read_bytes()) == (7, {"slot": "7"}, b"x" * 7)
    assert s3.exists(objs[0].key) and not s3.exists("nope")

    assert s3.delete(o.key for o in objs) == 1205
    assert list(s3.list("raw/c/f/")) == []


def test_pack_end_to_end_on_s3(s3, tmp_path):
    from conftest import BUILDERS

    start = pack.day_start_ms(DAY)
    for i in range(20):
        ms = start + i * 15_000
        s3.client.put_object(Bucket="raw", Key=f"raw/{CITY.id}/vehicle-positions/{DAY}/00/{ms}.pb",
                             Body=BUILDERS["vehicle-positions"](ms // 1000), Metadata={"slot": str(ms)})
    failed = pack.run(s3, CITY, tmp_path, delete=True, max_days=5, grace_ms=0, code_version="t",
                      level=3, log=lambda _: None, now_ms=start + pack.DAY_MS)
    assert failed == 0
    assert list(s3.list("raw/")) == []
    [(_, m)] = pack.load_manifests(s3, CITY, DAY)
    assert len(m["objects"]) == 20 and s3.exists(m["bundles"][0]["key"])
