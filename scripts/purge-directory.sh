#!/usr/bin/env bash
# Purge a directory and all its files from the media-pipeline DB.
#
# Usage:
#   purge-directory.sh [--db-path PATH] [--running 0|1] [-v|--verbose] <staging-path>
#   purge-directory.sh --help
#
# Deletes from the directories table by staging_path. Deletion cascades
# via FK to files and file_hashes.
#
# WARNING: This is destructive. There is no undo.
#
# Flags:
#   --db-path PATH   Path to the SQLite DB file (default: /data/pipeline.db)
#   --running 0|1    0=open DB directly, 1=docker exec into container (default: 1)
#   -v, --verbose    Print each step to stderr as it runs.
#
# Defaults:
#   DB_PATH  /data/pipeline.db
#   RUNNING  1  (docker exec)
#   CONTEXT  deploy-physalis
#   LOG_FILE ""  (if set, append all operations to this file)
#
# Override via env vars or --db-path / --running flags:
#   ./purge-directory.sh --db-path /tmp/test.db --running 0 -v "/staging/movies/Foo"

set -euo pipefail

CONTAINER="${CONTAINER:-media-pipeline}"
DB_PATH="${DB_PATH:-/data/pipeline.db}"
CONTEXT="${CONTEXT:-deploy-physalis}"
RUNNING="${RUNNING:-1}"
LOG_FILE="${LOG_FILE:-}"

# ---- Argument parsing ----
VERBOSE=0
STAGING_PATH=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --db-path)
            DB_PATH="$2"
            shift 2
            ;;
        --running)
            RUNNING="$2"
            shift 2
            ;;
        -v|--verbose)
            VERBOSE=1
            shift
            ;;
        -h|--help)
            sed -n '2,25p' "$0"
            exit 0
            ;;
        -*)
            echo "error: unknown flag '$1'" >&2
            echo "usage: $0 [--db-path PATH] [--running 0|1] [-v] <staging-path>" >&2
            exit 64
            ;;
        *)
            if [[ -z "$STAGING_PATH" ]]; then
                STAGING_PATH="$1"
            else
                echo "error: too many positional args" >&2
                echo "usage: $0 [--db-path PATH] [--running 0|1] [-v] <staging-path>" >&2
                exit 64
            fi
            shift
            ;;
    esac
done

if [[ -z "$STAGING_PATH" ]]; then
    echo "usage: $0 [--db-path PATH] [--running 0|1] [-v] <staging-path>" >&2
    exit 64
fi

# log: print to stderr (verbose) AND append to LOG_FILE if set.
log() {
    ts="$(date +%Y-%m-%dT%H:%M:%S)"
    if [[ "$VERBOSE" -eq 1 ]]; then
        echo "[$ts] $*" >&2
    fi
    if [[ -n "$LOG_FILE" ]]; then
        echo "[$ts] $*" >> "$LOG_FILE"
    fi
}

# sqlite: run sqlite3 — either inside the container or directly on the host.
sqlite() {
    if [[ "$RUNNING" -eq 1 ]]; then
        docker --context "$CONTEXT" exec "$CONTAINER" sqlite3 "$DB_PATH" "$@"
    else
        sqlite3 "$DB_PATH" "$@"
    fi
}

# Escape single quotes for SQL string literal embedding.
escape_sql() {
    printf "%s" "$1" | sed "s/'/''/g"
}
ESCAPED="$(escape_sql "$STAGING_PATH")"

MODE="host:$DB_PATH"
if [[ "$RUNNING" -eq 1 ]]; then
    MODE="docker:$CONTEXT:$CONTAINER"
fi
log "purging directory from DB"
log "  staging_path = '$STAGING_PATH'"
log "  mode = $MODE"

# ---- 1. Verify the row exists and show what will be deleted ----
log "looking up row"
ROW=$(sqlite -separator $'\t' \
    "SELECT id, state, category, remote_path FROM directories WHERE staging_path = '$ESCAPED';" \
    | tr -d '\r')

if [[ -z "$ROW" ]]; then
    echo "error: no row found with staging_path = '$STAGING_PATH'" >&2
    exit 1
fi

IFS=$'\t' read -r ID STATE CATEGORY REMOTE <<< "$ROW"
log "  id=$ID  state=$STATE  category=$CATEGORY"
log "  remote_path = $REMOTE"

FILE_COUNT=$(sqlite \
    "SELECT COUNT(*) FROM files WHERE dir_id = $ID;" \
    | tr -d '\r')
log "  $FILE_COUNT file(s) in DB for this directory"

HASH_COUNT=$(sqlite \
    "SELECT COUNT(*) FROM file_hashes WHERE dir_id = $ID;" \
    | tr -d '\r')
log "  $HASH_COUNT hash(es) in DB for this directory"

# ---- 2. Delete ----
log "deleting directory row (cascades to files + file_hashes)"
sqlite "DELETE FROM directories WHERE staging_path = '$ESCAPED';"

# ---- 3. Verify deletion ----
log "verifying deletion"
REMAINING=$(sqlite \
    "SELECT COUNT(*) FROM directories WHERE staging_path = '$ESCAPED';" \
    | tr -d '\r')
if [[ "$REMAINING" -ne 0 ]]; then
    echo "error: row still present after delete — this should not happen" >&2
    exit 1
fi
log "  row gone"

echo "purged from DB: $STAGING_PATH"
echo "         id: $ID  state: $STATE  category: $CATEGORY"
echo "         $FILE_COUNT file(s), $HASH_COUNT hash(es) also deleted"
echo "done."
