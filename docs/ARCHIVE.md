# Raw archive: packing, Parquet and monitoring

How the raw captures in the private R2 bucket `transit-pulse-raw` are preserved, decoded and
watched. Follows the Codex review of 2026-10-16 (preserve first, Parquet separately, per-feed
heartbeats). Code: `pipeline/` (Python, uv) and `recorder/` (Worker).

## Stages

| Stage | Where it runs | What it does |
| --- | --- | --- |
| Recorder | Cloudflare Worker, every minute | Stores snapshots under `raw/`, failures under `errors/`; pings healthchecks.io per feed |
| `pack` | private ops repo, 03:37/09:37/15:37/21:37 UTC | Bundles each closed UTC day, verifies it, writes a manifest, then (if enabled) deletes landing objects |
| `static` | private ops repo, before each `pack` run | Downloads the static GTFS timetable; archives it when its content changed |
| `notices` | private ops repo, before each `pack` run | Saves sofiatraffic.bg route changes and news; a new version only when an item changed |
| `derive` | private ops repo, after `pack` | Decodes bundles into Parquet under `derived/` |
| `check` | this repo, every 3 h | Per-feed freshness, last-hour coverage, frozen upstream feed |
| `coverage` | this repo, 04:47 UTC | Yesterday's coverage per feed and hour; fails under 90% |

Packing and Parquet are separate on purpose: a decoding bug fails `derive` and nothing else, so
it can never block preservation or cleanup. Any day can be re-derived from the bundles.

## Bucket layout

```text
raw/<city>/<feed>/<UTC-date>/<hour>/<fetch-ms>.pb           recorder, deleted after packing
errors/<city>/<UTC-date>/<slot-ms>-<feed>.json              recorder, deleted after packing
archive/<city>/raw/<UTC-date>/<feed|errors>/<sha256>.tar.zst  kept forever
archive/<city>/manifests/<UTC-date>/raw-v1-<created-ms>.json  kept forever
archive/<city>/static/versions/<canonical-sha256>/original.zip  kept forever, one per timetable version
archive/<city>/static/versions/<canonical-sha256>/meta.json     validity dates, per-file hashes, HTTP headers
archive/<city>/static/seen/<UTC-date>/<retrieved-ms>.json       every download: which version was published
archive/<city>/notices/sofiatraffic/<kind>/<id>/<sha>.json      every version of each notice, kept forever
archive/<city>/notices/sofiatraffic/runs/<UTC-date>/<ms>.json  per run: which notices were live, at which version
derived/<city>/v1/<table>/<UTC-date>/part-0.parquet         rebuildable
derived/<city>/v1/manifests/<UTC-date>.json
```

A bundle is a tar compressed with zstd level 19 and a 128 MB window (`zstd --long`). Its first
member, `index.jsonl`, lists every object's key, size, SHA-256, ETag, upload time and the
Worker's metadata (scheduled slot, HTTP status, fetch time); the objects follow under
`objects/<original key>`. Bundles are deterministic, so a rerun produces the same file.

A raw manifest lists every object it covers (key, size, SHA-256, bundle), the bundles, and the
day's coverage per feed and hour. **Preserved is not the same as complete**: a day with gaps is
still preserved, and its manifest says how much is missing. A day can have several manifests
if objects arrived after the first was written; each covers only what the earlier ones did not.

## Timetable versions

Delays and ghost trips need the timetable that was in force at the time, and the city replaces
it without notice, so each `pack` run first downloads the static GTFS (four times a day). The
**canonical hash** covers file names and contents only, so a re-zipped copy of the same data is
not a new version. It leaves out `feed_info.txt`, which Sofia regenerates daily with a rolling
one-year validity; that file's dates and every file's hash go in each `seen` record instead.
A new version is stored once, original zip first and `meta.json` last, with
`feed_info.txt` start/end dates and version, and the first and last service dates from the
calendars. Every download also leaves a `seen` record, and each day's raw manifest lists
`static_versions_seen` that day. A day with none uses the latest version first seen before it.

Matching a realtime trip to a version is left to analysis: during a changeover both versions
can apply on the same day, so it should check the trip's own service date and trip id, not
pick one version per day. A download that is not a valid zip (for example a bot-check page)
stores nothing and turns the `sofia-static` heartbeat red; packing still runs.

## Operator notices

Closures, detours, roadworks and bus-lane news date the before/after stories, and the operator
edits and removes them, so each `pack` run saves them from sofiatraffic.bg:

- `route-changes`: the active route changes (dates, cause, effect, affected lines, full text),
  from the JSON the site's own pop-up loads.
- `info-center`: every info-centre article's title, date and main text, from the list the
  news page embeds; `info-center-pages`: the full page (attachments too) for articles new or
  edited in the last 30 days.

Nothing is parsed out of HTML layout: the site is an Inertia.js app, so the data is the JSON it
embeds. An item is stored again only when its content changes, and each run record lists which
items were live at which version, so edits and removals are both dated. If the JSON stops
having the expected shape, the step fails and `sofia-notices` turns red; packing still runs.
First run stores about 320 small files (about 3 MB); later runs store only changes.

sofia.bg (the municipality) is not included: it answers "403 Unsupported Browser" to honestly
named bots, even on its RSS feed.

## Safety rules in the code

- Only closed days are packed: a UTC day plus 1 hour.
- Each bundle is uploaded, downloaded again, decompressed and checked member by member against
  the inventory before the manifest is written. The manifest is written last and read back.
- Deletion trusts only manifests: it deletes exactly the keys they list whose size still
  and ETag still match, and only with `--delete` (repo variable `PACK_DELETE=true`). A crash mid-delete is
  finished by the next run; it never repacks the leftovers as a smaller "complete" day.
- A failed day deletes nothing and does not stop later days.
- Logs and job summaries carry counts only, never object contents. No workflow uploads
  artifacts or uses caches.

## Restore a day by hand

```sh
# with an R2 token and the AWS CLI (or rclone), after reading the manifest for the bundle keys:
aws s3 cp --endpoint-url https://<account>.r2.cloudflarestorage.com s3://transit-pulse-raw/<bundle-key> day.tar.zst
tar --zstd -xf day.tar.zst            # GNU tar + zstd; default limits cover the 128 MB window
```

Or `uv run transit-pulse verify --date <day>` (in `pipeline/`), which restores every bundle of
the day in memory, checks each object against the manifest, and compares it with any landing
copy still in the bucket.

## Parquet tables (schema v1)

All capture times are UTC timestamps; times the feed publishes stay as epoch seconds; fields the
feed left out are null, never 0. Nothing is forward-filled. Enum values newer than the
bindings also come out null (they survive in the raw bundles).

- `snapshots`: every capture, with HTTP status, scheduled slot, fetch time, feed header and
  whether it parsed.
- `fetch_errors`: the recorder's error records.
- `vehicle_positions`: every vehicle entity in every snapshot.
- `trip_updates`, `stop_time_updates`: each distinct trip-update version once (stop updates in
  published order); `entity_membership` says which versions each snapshot held.
- `alerts`: each distinct alert version as complete JSON; membership as above.

Not yet: matching trips to timetable versions, service dates, per-entity quality events, and the 30-day
retention for detailed Parquet. Sizes are unmeasured until the first real day is derived.

## Monitoring

healthchecks.io checks (free plan: 20 checks), slugs created automatically on first ping:

| Check | Pinged by | Period | Grace |
| --- | --- | --- | --- |
| `sofia-vehicle-positions` | Worker, each minute a snapshot was stored | 1 min | 5 min |
| `sofia-trip-updates` | Worker | 1 min | 5 min |
| `sofia-alerts` | Worker | 5 min | 10 min |
| `sofia-auditor` | `recorder-health` workflow, 3-hourly check | 3 h | 3 h |
| `sofia-coverage` | `recorder-health` workflow, daily report | 1 day | 6 h |
| `sofia-pack` | `pack` workflow | 6 h | 6 h |
| `sofia-static` | `pack` workflow, timetable download | 6 h | 18 h |
| `sofia-notices` | `pack` workflow, operator notices | 6 h | 18 h |
| `sofia-derive` | `derive` workflow | 1 day | 6 h |

A minute where a feed stored nothing sends a `/log` ping with the errors: it shows in the
check's log without alerting, and the check goes down only after period plus grace with no
stored snapshot. The Worker also throws when a fetch left no record at all (R2 down), so that
invocation shows as an error in Workers metrics. The auditor catches what heartbeats cannot:
objects missing from R2, low coverage, and a feed whose own timestamp stopped moving.

GitHub disables scheduled workflows in a public repo after 60 days without commits. If that
happens, `sofia-auditor` goes down, which is the alert.

## Setup checklist (Mike)

Nothing here runs against R2 until these are done. Order matters for the last two.

1. **Bucket privacy.** Cloudflare dashboard, R2, `transit-pulse-raw`, Settings: Public
   Development URL (r2.dev) disabled, and no Custom Domains. They are separate access paths.
2. **R2 tokens.** R2, Manage API tokens, two tokens scoped to `transit-pulse-raw` only:
   - "Object Read only": this repo's secrets `R2_READ_ACCESS_KEY_ID` and
     `R2_READ_SECRET_ACCESS_KEY` (for `recorder-health`, which no longer uses
     `CLOUDFLARE_API_TOKEN`).
   - "Object Read & Write": the private repo's secrets `R2_PACK_ACCESS_KEY_ID` and
     `R2_PACK_SECRET_ACCESS_KEY`.
   - While there, check that `CLOUDFLARE_API_TOKEN` (deploys) has only Workers Scripts: Edit.
3. **Private ops repo**, e.g. `mike2906/transit-pulse-ops`: copy
   `ops/private-repo/.github/workflows/*.yml` into its `.github/workflows/` unchanged. Add the
   variable `R2_ACCOUNT_ID` = `2ecf50dd0473ff451ea98336863d40fc` and the secrets above. Leave
   `PACK_DELETE` unset for now. In Settings, Billing, keep the Actions budget at $0 so it
   stops rather than bills (about 2,000 free minutes/month).
4. **healthchecks.io**: sign up, open the project's Settings and create a ping key. Store it as
   the Worker secret `HC_PING_KEY` (`npx wrangler secret put HC_PING_KEY` in `recorder/`) and
   as the Actions secret `HC_PING_KEY` in both repos. After the first pings, set each check's
   period and grace as in the table above.
5. **Merge** this repo's PR only after step 2's read-only secrets exist, since the merge
   switches `recorder-health` to them and deploys the Worker with heartbeats.
6. **Turn on deletion** only after one manual delete has worked: R2's handling of the checksum
   boto3 sends with DeleteObjects is unverified (a rejection fails safely, deleting nothing). Run
   `pack` once by hand with `PACK_DELETE=true` set for that run, then keep it on if it deleted.
   Do this after the first packed day passes
   the restore test (Actions, `pack`, Run workflow, with `verify_date` set to that day): set the private repo variable `PACK_DELETE=true`.
