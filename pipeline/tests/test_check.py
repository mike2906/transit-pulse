from transit_pulse import check, pack
from transit_pulse.cli import main

from conftest import BUILDERS, CITY, DAY, fill_hours, put_capture

HOUR = 3_600_000
START = pack.day_start_ms(DAY)


def test_healthy_hour(store):
    fill_hours(store, DAY, range(2))
    report, problems = check.check(store, CITY, now_ms=START + 2 * HOUR + 30_000)
    assert problems == [], report
    assert "All feeds healthy." in report


def test_stale_feed_and_frozen_feed(store):
    fill_hours(store, DAY, range(1))
    # Trip updates keep arriving for another 30 min, but their header timestamp never moves.
    for i in range(30):
        ms = START + HOUR + i * 60_000 + 5_000
        put_capture(store, "trip-updates", ms, body=BUILDERS["trip-updates"](START // 1000))
    _, problems = check.check(store, CITY, now_ms=START + HOUR + 31 * 60_000)
    by_feed = {p.feed: p.text for p in problems}
    assert "no capture for over 10 min" in by_feed["vehicle-positions"]
    assert "upstream feed frozen" in by_feed["trip-updates"]


def test_low_coverage_in_last_hour(store):
    fill_hours(store, DAY, range(1))
    for i in range(0, 60, 3):  # positions in only every third minute of hour 1
        put_capture(store, "vehicle-positions", START + HOUR + i * 60_000 + 300)
    _, problems = check.check(store, CITY, now_ms=START + 2 * HOUR + 30_000)
    assert any("scheduled captures in the last hour" in p.text for p in problems if p.feed == "vehicle-positions")


def test_daily_coverage_reads_manifests_after_packing(store, tmp_path, after_day):
    fill_hours(store, DAY, range(24))
    report_before, ok_before = check.daily_coverage(store, CITY, DAY)
    pack.run(store, CITY, tmp_path, delete=True, max_days=1, grace_ms=HOUR, code_version="t", level=1,
             log=lambda _: None, now_ms=after_day)
    report_after, ok_after = check.daily_coverage(store, CITY, DAY)
    assert ok_before and ok_after and report_before == report_after
    assert "| vehicle-positions | 5760 | 5736 | 24 | 0 | 99.6% |" in report_after


def test_cli_runs_against_a_local_folder(store, tmp_path, capsys, monkeypatch):
    import transit_pulse.cli as cli

    monkeypatch.setattr(cli, "load_city", lambda _: CITY)
    fill_hours(store, DAY, range(2))
    assert main(["--store", str(store.root), "coverage", "--date", DAY]) == 1  # 22 empty hours
    assert "Below 90%" in capsys.readouterr().out


def test_real_city_configs_load():
    from transit_pulse.cities import load_city

    sofia = load_city("sofia")
    assert sofia.feed_ids == ["vehicle-positions", "trip-updates", "alerts"]
