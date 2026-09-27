#!/usr/bin/env bash
# Mark a directory as `in_library` in the media-pipeline DB.
#
# Usage:
#   mark-in-library.sh [-v|--verbose] <staging-path> <library-path>
#   mark-in-library.sh --help
#
# Updates the directories row identified by `staging_path`:
#   - state        = 'in_library'
#   - moved_at     = CURRENT_TIMESTAMP
#   - library_path = <library-path>
#
# The script does NOT touch the filesystem. It's a DB-only marker
# for the case where a directory has been moved to the library by
# hand (or by some path the pipeline doesn't know about) and you
# just want the DB to reflect that fact.
#
# Flags:
#   -v, --verbose   Print each step to stderr as it runs.
#
# Defaults:
#   CONTAINER  media-pipeline
#   DB_PATH    /data/pipeline.db
#   CONTEXT    deploy-physalis
#
# Override via env vars:
#   CONTAINER=foo DB_PATH=/tmp/test.db CONTEXT=deploy-fenneko \
#       mark-in-library.sh "/staging/movies/Foo" "/library/Movies/Foo"

set -euo pipefail

CONTAINER="${CONTAINER:-media-pipeline}"
DB_PATH="${DB_PATH:-/data/pipeline.db}"
CONTEXT="${CONTEXT:-deploy-physalis}"

# ---- Argument parsing ----
VERBOSE=0
STAGING_PATH=""
LIBRARY_PATH=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        -v|--verbose)
            VERBOSE=1
            shift
            ;;
        -h|--help)
            sed -n '2,12p' "$0"
            exit 0
            ;;
        --)
            shift
            [[ $# -ge 1 ]] && STAGING_PATH="$1" && shift
            [[ $# -ge 1 ]] && LIBRARY_PATH="$1" && shift
            break
            ;;
        -*)
            echo "error: unknown flag '$1'" >&2
            echo "usage: $0 [-v|--verbose] <staging-path> <library-path>" >&2
            exit 64
            ;;
        *)
            if [[ -z "$STAGING_PATH" ]]; then
                STAGING_PATH="$1"
            elif [[ -z "$LIBRARY_PATH" ]]; then
                LIBRARY_PATH="$1"
            else
                echo "error: too many positional args" >&2
                echo "usage: $0 [-v|--verbose] <staging-path> <library-path>" >&2
                exit 64
            fi
            shift
            ;;
    esac
done

if [[ -z "$STAGING_PATH" || -z "$LIBRARY_PATH" ]]; then
    echo "usage: $0 [-v|--verbose] <staging-path> <library-path>" >&2
    exit 64
fi

# log: print a step line to stderr when verbose. No-op otherwise.
log() {
    if [[ "$VERBOSE" -eq 1 ]]; then
        echo "[$(date +%H:%M:%S)] $*" >&2
    fi
}

# Escape single quotes for SQL string literal embedding (SQL
# standard: double them). Same approach as rename-revert.sh.
escape_sql() {
    printf "%s" "$1" | sed "s/'/''/g"
}
ESCAPED_STAGING="$(escape_sql "$STAGING_PATH")"
ESCAPED_LIBRARY="$(escape_sql "$LIBRARY_PATH")"

log "marking directory in_library"
log "  staging_path = '$STAGING_PATH'"
log "  library_path = '$LIBRARY_PATH'"
log "  container=$CONTAINER  db=$DB_PATH  context=$CONTEXT"

# ---- 1. Verify the row exists (and show its current state) ----
log "checking current row state"
CURRENT=$(docker --context "$CONTEXT" exec "$CONTAINER" \
    sqlite3 "$DB_PATH" -separator $'\t' \
    "SELECT state, library_path FROM directories WHERE staging_path = '$ESCAPED_STAGING';" \
    | tr -d '\r')

if [[ -z "$CURRENT" ]]; then
    echo "error: no row in directories with staging_path = '$STAGING_PATH'" >&2
    exit 1
fi
log "  current state = $CURRENT"

# ---- 2. Update state, moved_at, and library_path ----
log "updating row"
docker --context "$CONTEXT" exec "$CONTAINER" \
    sqlite3 "$DB_PATH" \
    "UPDATE directories
        SET state = 'in_library',
            moved_at = CURRENT_TIMESTAMP,
            library_path = '$ESCAPED_LIBRARY'
      WHERE staging_path = '$ESCAPED_STAGING';" \
    >/dev/null

# ---- 3. Show the new state to confirm ----
log "verifying update"
AFTER=$(docker --context "$CONTEXT" exec "$CONTAINER" \
    sqlite3 "$DB_PATH" -separator $'\t' \
    "SELECT state, moved_at, library_path FROM directories WHERE staging_path = '$ESCAPED_STAGING';" \
    | tr -d '\r')
log "  after  state = $AFTER"

echo "marked in_library: $STAGING_PATH"
echo "         library: $LIBRARY_PATH"
echo "done."