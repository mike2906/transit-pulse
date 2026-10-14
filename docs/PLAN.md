# transit-pulse: recorder plan (Sofia first)

Draft 2, 2026-10-14 ~19:30 UTC (city-generic, reuse check, private data). Status: awaiting Codex review. Repo: mike2906/transit-pulse
(public, created by Mike 2026-10-14 19:33 UTC; site and visualisations live there too). May move to its own Claude project.

## Goal

Record everything a city publishes (Sofia first) about its public transport, continuously, so a history
exists for later charts, blog posts and the school site's rush-hour tuning. Running cost about $0,
no recurring human tasks.

## City-generic, Sofia only for now

GTFS and GTFS-Realtime are standards, so nothing in the code names Sofia. Each city is one config
file, `cities/sofia.toml`: feed URLs, poll intervals, timezone, licence, credit line and any
naming/colour rules. Storage keys and published files carry the city id. Sofia is the only city;
no multi-city features (city picker, cross-city charts) until a second city exists. This also
serves the school site later: other cities with GTFS-RT get rush-hour tuning by adding a config.

## Reuse check: Kurt Raschke's transit-data-archivers

github.com/kurtraschke/transit-data-archivers (BlueOak-1.0.0 licence, 3 stars) is the code behind
his ~170-feed archive. It is a Java/Maven suite (fetcher, protobuf bindings, dump tool, query
proxy) writing into ClickHouse with custom UDFs, run on a rented server in Québec. Reusing it means
an always-on server plus a ClickHouse instance: a few $/month at minimum, plus upkeep, or Mike's
laptop, which sleeps. Our design needs no server at all. Verdict: don't reuse the code; borrow the
lessons from his write-up: poll ~30 s, parse leniently (`parsePartialFrom`-style) so a malformed
snapshot is kept rather than dropped, keep raw bytes so parsing can be redone, and expect
StopTimeUpdates to dominate volume (~350 billion of his rows vs 9 billion positions).

## What exists (checked from Mike's PC, 2026-10-14)

All at `https://gtfs.sofiatraffic.bg/api/v1/`, no key, Cloudflare-fronted, no rate-limit or cache
headers. Licence CC BY 4.0 (details at the end).

| Feed | Size | Notes |
| --- | --- | --- |
| `static` | 18.8 MB zip | GTFS timetable incl. metro M1-M4; valid to 2027-10-14; no caching headers |
| `vehicle-positions` | ~37-44 KB | ~320 vehicles at ~21:20 Sofia time (evening); daytime will be higher |
| `trip-updates` | ~440-480 KB | ~650 trips, mostly per-stop predictions |
| `alerts` | ~120 KB | changes rarely |

Realtime covers bus, tram and trolleybus only. Metro is timetable-only.

## Design

```
Cloudflare Worker (cron, every minute)          GitHub Actions (public repo, free)
  fetch feeds -> put raw protobuf in R2   --->    daily: raw -> Parquet (private), checks
  no parsing (10 ms CPU limit)                    daily: static GTFS change check
                                                  every 3 h: "is the recorder alive?"
```

### 1. Recorder: Cloudflare Worker on the free plan

- One cron trigger, `* * * * *`. Each invocation:
  - vehicle positions at t = 0, 15, 30, 45 s (sleeping between, which is wall time, not CPU;
    cron wall limit is 15 min). Cadence is set to match the feed's real refresh rate, measured in
    step 0 from `header.timestamp`; if it refreshes every 30 s we poll every 30 s.
  - trip updates on even minutes (every 2 min).
  - alerts every 5 min.
- The Worker never parses protobuf. It streams the response body straight into R2 (`put` with the
  body and content length), so CPU stays far below 10 ms. Dedupe of unchanged snapshots happens
  later in the daily job.
- Object key: `raw/{city}/{feed}/{YYYY-MM-DD}/{HH}/{fetch_epoch_ms}.pb` (UTC). Metadata: HTTP status,
  `Last-Modified`/`ETag` if any, fetch duration.
- On failure (non-200, timeout, empty body) it writes a tiny `errors/{city}/{date}/{epoch_ms}.json`
  instead, so gaps are explainable.
- Polite: `User-Agent: transit-pulse (+https://github.com/mike2906/transit-pulse)`,
  timeout 10 s, no retries inside the minute.
- Bucket: new `transit-pulse-raw` (R2 is already enabled on Mike's account, so no new billing
  step). Lifecycle rule: delete `raw/` after 7 days (the daily job has a week to catch up).

Budget at 15 s positions: ~5,760 + 720 + 288 = ~6.8k writes/day = ~205k Class A ops/month
(free: 1M). Worker requests: 1,440/day (free: 100k). Subrequests per invocation: <= 9 fetches +
9 puts (limit 50). Raw volume: positions ~215 MB/day, trip updates ~320 MB/day, alerts ~35 MB/day,
so ~0.6 GB/day, ~4 GB at the 7-day cap, leaving room for the private archive. Retention is adjusted (decided in step 0 from measured sizes).

### 2. Daily compaction: GitHub Actions

Runs ~03:30 UTC for the previous UTC day. UTC day boundaries are deliberate for Sofia: UTC midnight is
02:00/03:00 Sofia time, the quietest part of the night, so few trips are split across files. For other cities the
config sets the cut-off hour (UTC) so it lands in their night lull.

- Python, `gtfs-realtime-bindings` + `pyarrow`, zstd Parquet.
- Tables, all timestamps UTC:
  - `vehicle_positions`: fetch_ts, feed_ts, vehicle_ts, vehicle_id, label, trip_id, route_id,
    direction_id, start_date, lat, lon, bearing, speed, stop_id, current_stop_sequence,
    current_status, occupancy_status (any field the feed fills). Duplicate rows (same vehicle and
    vehicle_ts) dropped.
  - `trip_updates`: one row per trip/stop **only when a value changed** since the previous
    snapshot (fetch_ts, feed_ts, trip_id, route_id, start_date, trip schedule_relationship,
    vehicle_id, stop_sequence, stop_id, arrival/departure delay and time, stop
    schedule_relationship). Nothing is lost: forward-fill rebuilds every snapshot.
  - `alerts`: one row per distinct alert version with first_seen/last_seen.
  - `fetch_log`: every fetch attempt incl. errors, for coverage stats.
- Unknown fields are kept as raw bytes (protobuf extensions) rather than dropped.
- Checks before publishing: snapshot coverage per hour, row counts vs previous days, schema
  unchanged. A failed check opens a GitHub issue (label `recorder`) and leaves the raw data in R2.
- Output (default: PRIVATE): the code repo is public, the data is not. Daily Parquet goes to the
  same R2 bucket under `archive/{city}/{table}/{date}.parquet`, which is private. Only charts and
  summary numbers are published. Publishing the files later stays possible; un-publishing is not.
  Estimated ~10-40 MB/day of Parquet (to be measured), ~4-15 GB/year. To leave room, raw retention
  is 7 days (~4 GB), leaving ~6 GB for the archive: roughly 5-18 months before the free 10 GB fills.
  The liveness job alerts at 8 GB. Overflow options, decided when it nears: a separate private
  GitHub repo's Releases (free), or Google Drive.
- Open choice for Mike: keep the data private (default, keeps a sale option open) or publish daily
  files openly. Note: CC BY 4.0 lets anyone sell the data too, with credit, so what is scarce is
  only our history from the start date; anyone can start their own recording at any time.
- Static GTFS: same daily job downloads the zip, hashes the sorted contents of its files (zip bytes
  can differ for the same data), and on change uploads it to a `gtfs-static` release as
  `gtfs_{first_seen_date}.zip`. Each realtime day records which static version it pairs with.

### 3. Liveness check

Actions cron every 3 h lists the newest object under `raw/{city}/vehicle-positions/`. Older than 15 min
opens (or updates) one `recorder-down` issue; recovery closes it. Mike gets the GitHub email; no
routine human task.

## Repo layout

```
cities/        one config per city (sofia.toml)
recorder/      Cloudflare Worker (TypeScript, wrangler config)
pipeline/      daily packing, static GTFS check, liveness check (Python)
site/          visualisations, later (static site, reads the published Parquet)
docs/          this plan, schema, attribution
.github/workflows/
```

## Credentials (one-time, Mike)

1. Create the empty public repo (done: mike2906/transit-pulse).
2. Cloudflare API token limited to "Workers Scripts: Edit" for deploys from Actions, as repo secret
   `CLOUDFLARE_API_TOKEN` (+ `CLOUDFLARE_ACCOUNT_ID`).
3. R2 S3 token limited to the `transit-pulse-raw` bucket (read + delete), as `R2_ACCESS_KEY_ID`,
   `R2_SECRET_ACCESS_KEY`.
The R2 token also needs write on `archive/`. I can create the bucket and lifecycle rule myself
through the Cloudflare connector.

## Build steps

0. **Smoke test** (before trusting anything): deploy the Worker, run 2 h, then measure: does the
   feed answer Workers (Cloudflare bot rules could block other Workers), real refresh interval of
   each feed, real sizes, CPU per invocation from Worker metrics. Adjust cadence and retention.
1. Worker + bucket + lifecycle + deploy workflow.
2. Compaction job + Parquet schema + checks, tested on the smoke-test data.
3. Static GTFS versioning.
4. Liveness check.
5. README: what's recorded, schema, known gaps, how to load a day in DuckDB/pandas,
   attribution. Code MIT; data CC BY 4.0 as published by the city.

Each step a PR, reviewed before merge (project habit).

## Licence obligations

CC BY 4.0 per Sofia's mobility data terms (order СОА25-РД09-3745/03.09.2025,
https://www.sofia.bg/web/guest/transport-data). Every published file, chart and post credits
"Център за градска мобилност ЕАД / Столична община", links CC BY 4.0 and says the data was
modified (reformatted to Parquet, deduplicated). Official stop names/codes, line numbers/names and
line colours stay unchanged. The city may add access limits later (Art. 14), which the liveness
check would surface.

## Cost

$0 expected: Workers free plan, R2 free tier (watch: 10 GB storage, 1M Class A/month), Actions
free for public repos. Risks to cost: none automatic, since the free Workers plan
fails rather than bills; R2 bills above 10 GB only if usage passes the free tier, which the
lifecycle rule prevents.

## Open questions for review

1. Is 15 s positions worth 2x the writes over 30 s? (Codex earlier: 15 s helps dwell analysis.)
2. Trip updates every 2 min: enough to learn anything, or is the prediction stream not worth
   storing at all beyond cancellations? Mike asked to record everything, so default is keep.
3. Should raw protobuf also be archived long-term (e.g. zstd tar per day) in case the Parquet
   conversion has bugs? Adds maybe 30-60 MB/day of storage. Default: no, 7 days of raw only.
4. Private archive in R2 (10 GB free) with overflow later: sound, or is there a better free private
   store for ~10 GB/year of Parquet?
5. Cron Worker vs a Durable Object alarm loop for steadier 15 s spacing.
