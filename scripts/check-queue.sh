#!/bin/bash
# Show pending work at both levels of the pipeline:
#   1. queued directories (oldest first)
#   2. queued individual file jobs, with live progress for in-flight ones
#   3. the post-download backlog awaiting analysis/rename/move
#
# In-flight progress stats the partial file on the target host against
# file_hashes.size. The staging root is auto-detected from the container's
# bind mounts (falls back to STAGING_HOST_ROOT).
#
# Usage:
#   ./check-queue.sh [hostname]
#
# Env overrides:
#   DB_PATH             container-side DB path (default /data/pipeline.db)
#   STAGING_HOST_ROOT   host-side staging root; skips auto-detect when set
#
# Defaults: hostname = physalis

set -e

HOST="${1:-physalis}"
DB_PATH="${DB_PATH:-/data/pipeline.db}"

sql() {
    ssh "$HOST" "docker exec media-pipeline sqlite3 -readonly -header -column \"$DB_PATH\" \"$1\""
}
sql_csv() {
    ssh "$HOST" "docker exec media-pipeline sqlite3 -readonly -separator $'\\t' \"$DB_PATH\" \"$1\""
}

echo "=== Download queue on $HOST (oldest first, max 20) ==="
sql "SELECT id, category, staging_path, detected_at
  FROM directories
 WHERE state = 'detected'
 ORDER BY detected_at ASC
 LIMIT 20;"

echo ""
echo "=== Queued file jobs (max 20) ==="
# A file is eligible for claiming when both the file row and its directory
# are 'detected' (see claim_file). Rows carry no per-file timestamp, so the
# queue order is the directory's detected_at, then the path.
QUEUED=$(sql_csv "
SELECT d.id, fd.rel_path, d.detected_at
  FROM file_downloads fd
  JOIN directories d ON d.id = fd.dir_id
 WHERE fd.state = 'detected'
   AND d.state = 'detected'
 ORDER BY d.detected_at ASC, fd.rel_path ASC
 LIMIT 20;") || QUEUED=""
if [[ -z "$QUEUED" ]]; then
    echo "No queued file jobs."
else
    echo "$QUEUED"
fi

echo ""
echo "=== In-progress downloads ==="
# Live progress for rows in 'downloading', same shape as check-downloading.sh:
# expected size comes from file_hashes (0 = manifest not yet collected), bytes
# done from a host-side stat of the partial file.
ROWS=$(sql_csv "
SELECT fd.dir_id, fd.rel_path, d.staging_path, COALESCE(fh.size, 0), fd.downloading_at
  FROM file_downloads fd
  JOIN directories d ON d.id = fd.dir_id
  LEFT JOIN file_hashes fh ON fh.dir_id = fd.dir_id AND fh.rel_path = fd.rel_path
 WHERE fd.state = 'downloading'
 ORDER BY d.category, d.staging_path, fd.rel_path;") || ROWS=""

if [[ -z "$ROWS" ]]; then
    echo "No files currently downloading."
else
    # Auto-detect the host-side staging root from the container's mounts.
    if [[ -z "${STAGING_HOST_ROOT:-}" ]]; then
        STAGING_HOST_ROOT=$(ssh "$HOST" \
            "docker inspect media-pipeline --format '{{range .Mounts}}{{if eq .Destination \"/staging\"}}{{.Source}}{{end}}{{end}}'")
        if [[ -z "$STAGING_HOST_ROOT" ]]; then
            echo "error: no /staging mount on container media-pipeline; set STAGING_HOST_ROOT" >&2
            exit 1
        fi
    fi

    # Stat each partial file host-side in one ssh round trip. staging_path is
    # container-side (/staging/...); rewrite the prefix to the host root.
    # stat emits one size per line in argument order, so sizes pair with
    # paths positionally (busybox stat does not interpret \t escapes).
    PATHS=()
    while IFS=$'\t' read -r _dir_id _rel_path staging_path _expected _started; do
        PATHS+=("${staging_path/#\/staging/$STAGING_HOST_ROOT}/${_rel_path}")
    done <<< "$ROWS"

    STAT_QUOTE=""
    for p in "${PATHS[@]}"; do
        esc="${p//\'/\'\\\'}"
        STAT_QUOTE+=" '$esc'"
    done
    mapfile -t SIZES < <(ssh "$HOST" "stat -c '%s' --$STAT_QUOTE 2>/dev/null") || SIZES=()

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
fi

echo ""
echo "=== Post-download backlog ==="
sql "SELECT state, COUNT(*) AS count
  FROM directories
 WHERE state IN ('synced', 'analyzed', 'renamed')
 GROUP BY state;"

echo ""
echo "=== Queue totals ==="
sql "SELECT
       (SELECT COUNT(*) FROM directories WHERE state = 'detected')   AS queued,
       (SELECT COUNT(*) FROM directories WHERE state = 'syncing')    AS syncing,
       (SELECT COUNT(*) FROM directories WHERE state LIKE '%_failed') AS failed;"