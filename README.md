# transit-pulse

Records public transport realtime feeds (GTFS-Realtime) so a history exists for charts, analysis
and blog posts. Sofia is the first city.

Status: being built. Plan: [docs/PLAN.md](docs/PLAN.md). Archive, Parquet and monitoring:
[docs/ARCHIVE.md](docs/ARCHIVE.md).

- `recorder/`: Cloudflare Worker that stores raw snapshots in a private R2 bucket.
- `pipeline/`: packs, decodes and checks the archive (`uv run transit-pulse --help`).
- `ops/private-repo/`: workflows that touch the archive; they run in a private repo.

## Data credit

Sofia data: Център за градска мобилност ЕАД / Столична община, licensed
[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/). Data shown here is modified
(reformatted, deduplicated, aggregated).
