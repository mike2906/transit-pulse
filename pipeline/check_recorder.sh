#!/usr/bin/env bash
# Liveness check for the recorder: counts recent raw snapshots per feed in R2 via the
# Cloudflare API and fails if vehicle positions have stopped arriving.
# Needs CLOUDFLARE_API_TOKEN (R2 read) and CLOUDFLARE_ACCOUNT_ID. Prints a Markdown summary.
set -euo pipefail

CITY="${CITY:-sofia}"
BUCKET="${BUCKET:-transit-pulse-raw}"
MAX_AGE_S="${MAX_AGE_S:-600}"
API="https://api.cloudflare.com/client/v4/accounts/${CLOUDFLARE_ACCOUNT_ID}/r2/buckets/${BUCKET}/objects"

# All objects under a prefix, as "key<TAB>size" lines (follows the cursor).
list() {
  local prefix="$1" cursor="" page
  while :; do
    page=$(curl -fsS -G "$API" -H "Authorization: Bearer ${CLOUDFLARE_API_TOKEN}" \
      --data-urlencode "prefix=${prefix}" --data-urlencode "per_page=1000" \
      ${cursor:+--data-urlencode "cursor=${cursor}"})
    jq -r '.result[] | "\(.key)\t\(.size)"' <<<"$page"
    cursor=$(jq -r '.result_info.cursor // empty' <<<"$page")
    [ "$(jq -r '.result_info.is_truncated // false' <<<"$page")" = true ] && [ -n "$cursor" ] || break
  done
}

now=$(date -u +%s)
hours=("$(date -u -d "@$((now - 3600))" +%Y-%m-%d/%H)" "$(date -u -d "@$now" +%Y-%m-%d/%H)")

echo "## Recorder check, $(date -u '+%Y-%m-%d %H:%M UTC')"
echo
echo "| Feed | Hour (UTC) | Snapshots | Avg KB |"
echo "| --- | --- | ---: | ---: |"
newest=0
for feed in vehicle-positions trip-updates alerts; do
  for hour in "${hours[@]}"; do
    rows=$(list "raw/${CITY}/${feed}/${hour}/")
    n=$(grep -c . <<<"$rows" || true)
    kb=$(awk -F'\t' 'NF { s += $2; n++ } END { if (n) printf "%.0f", s / n / 1024; else print "-" }' <<<"$rows")
    echo "| ${feed} | ${hour/\// } | ${n} | ${kb} |"
    if [ "$feed" = vehicle-positions ] && [ "$n" -gt 0 ]; then
      last=$(cut -f1 <<<"$rows" | sed -E 's#.*/([0-9]+)\.pb#\1#' | sort -n | tail -1)
      [ $((last / 1000)) -gt "$newest" ] && newest=$((last / 1000))
    fi
  done
done
errors=$(list "errors/${CITY}/$(date -u +%Y-%m-%d)/" | grep -c . || true)
echo
echo "Error records today (UTC): ${errors}"
echo

age=$((now - newest))
if [ "$newest" -eq 0 ] || [ "$age" -gt "$MAX_AGE_S" ]; then
  echo "**Recorder looks down:** newest vehicle-positions snapshot is ${age} s old (limit ${MAX_AGE_S} s)."
  exit 1
fi
echo "Newest vehicle-positions snapshot: ${age} s ago."
