#!/bin/bash
# Show files currently downloading, with live progress.
#
# Reads in-flight rows from file_downloads, joins file_hashes for the
# expected size, and stats the partial file on the target host to report
# bytes done and percent complete. The staging root is auto-detected from
# the container's bind mounts (falls back to STAGING_HOST_ROOT).
#
# Usage:
#   ./check-downloading.sh [hostname]
#
# Env overrides:
#   DB_PATH             container-side DB path (default /data/pipeline.db)
#   STAGING_HOST_ROOT   host-side staging root; skips auto-detect when set
#
# Defaults: hostname = physalis

set -e

HOST="${1:-physalis}"
DB_PATH="${DB_PATH:-/data/pipeline.db}"

sql_csv() {
    ssh "$HOST" "docker exec media-pipeline sqlite3 -readonly -separator $'\\t' \"$DB_PATH\" \"$1\""
}

# ---- 0. Auto-detect the host-side staging root from the container's mounts ----
if [[ -z "${STAGING_HOST_ROOT:-}" ]]; then
    STAGING_HOST_ROOT=$(ssh "$HOST" \
        "docker inspect media-pipeline --format '{{range .Mounts}}{{if eq .Destination \"/staging\"}}{{.Source}}{{end}}{{end}}'")
    if [[ -z "$STAGING_HOST_ROOT" ]]; then
        echo "error: no /staging mount on container media-pipeline; set STAGING_HOST_ROOT" >&2
        exit 1
    fi
fi

# ---- 1. Per-file rows: dir_id, rel_path, container staging_path, expected size, started ----
# file_hashes carries the remote's expected size for each rel_path; a missing
# row (manifest not yet collected) shows as expected size 0.
ROWS=$(sql_csv "
SELECT fd.dir_id, fd.rel_path, d.staging_path, COALESCE(fh.size, 0), fd.downloading_at
  FROM file_downloads fd
  JOIN directories d ON d.id = fd.dir_id
  LEFT JOIN file_hashes fh ON fh.dir_id = fd.dir_id AND fh.rel_path = fd.rel_path
 WHERE fd.state = 'downloading'
 ORDER BY d.category, d.staging_path, fd.rel_path;") || ROWS=""

if [[ -z "$ROWS" ]]; then
    echo "No files currently downloading on $HOST."
    exit 0
fi

echo "=== Currently downloading on $HOST ==="

# ---- 2. Stat each partial file host-side in one ssh round trip ----
# staging_path is container-side (/staging/...); rewrite the /staging prefix
# to the host root. stat emits one size per line in argument order, so sizes
# are paired with paths positionally (busybox stat does not interpret \t
# escapes in the format string).
PATHS=()
while IFS=$'\t' read -r _dir_id _rel_path staging_path _expected _started; do
    PATHS+=("${staging_path/#\/staging/$STAGING_HOST_ROOT}/${_rel_path}")
done <<< "$ROWS"

STAT_QUOTE=""
for p in "${PATHS[@]}"; do
    esc="${p//\'/\'\\\'\'}"
    STAT_QUOTE+=" '$esc'"
done
mapfile -t SIZES < <(ssh "$HOST" "stat -c '%s' --$STAT_QUOTE 2>/dev/null") || SIZES=()

# ---- 3. Report ----
TOTAL_BYTES=0
TOTAL_EXPECTED=0
IDX=0
while IFS=$'\t' read -r dir_id rel_path staging_path expected started; do
    bytes="${SIZES[$IDX]:-0}"
    IDX=$(( IDX + 1 ))
    if [[ "$expected" -gt 0 ]]; then
        pct=$(( bytes * 100 / expected ))
        pct_str="${pct}%"
        TOTAL_BYTES=$(( TOTAL_BYTES + bytes ))
        TOTAL_EXPECTED=$(( TOTAL_EXPECTED + expected ))
    else
        pct_str="?"
    fi
    printf "%-12s %12s of %-12s %-5s (started %s)\n  %s\n" \
        "$dir_id" "$bytes" "$expected" "$pct_str" "$started" "$rel_path"
done <<< "$ROWS"

echo ""
if [[ "$TOTAL_EXPECTED" -gt 0 ]]; then
    pct=$(( TOTAL_BYTES * 100 / TOTAL_EXPECTED ))
    echo "Progress: $TOTAL_BYTES / $TOTAL_EXPECTED bytes (${pct}%), $(wc -l <<< "$ROWS") files"
else
    echo "Files: $(wc -l <<< "$ROWS") (no expected sizes recorded)"
fi

# ---- 4. Per-directory rollup ----
echo ""
echo "=== Per-directory ==="
sql_csv "
SELECT d.category, d.staging_path,
       COUNT(fd.id) AS total,
       SUM(fd.state = 'downloading') AS downloading,
       SUM(fd.state = 'synced') AS synced,
       SUM(fd.state = 'failed') AS failed
  FROM file_downloads fd
  JOIN directories d ON d.id = fd.dir_id
 WHERE fd.dir_id IN (SELECT dir_id FROM file_downloads WHERE state = 'downloading')
 GROUP BY fd.dir_id
 ORDER BY d.category, d.staging_path;"