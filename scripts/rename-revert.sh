#!/usr/bin/env bash
# Revert a renamed media file back to its original (pre-rename) name.
#
# Usage:
#   rename-revert.sh [-v|--verbose] <renamed-filename>
#   rename-revert.sh --help
#
# Looks up the file in the media-pipeline SQLite database by its
# current (post-rename) name in `files.renamed_name`, finds the
# parent directory in `directories.staging_path`, and renames the
# file on disk back to `files.original_name`. Also clears
# `files.renamed_name` in the DB so it reflects the reverted state.
#
# Flags:
#   -v, --verbose   Print each step (DB query, sanity checks, rename,
#                   DB update) to stderr as it runs. Default output is
#                   concise: only the rename summary + the final status.
#
# Assumes the media-pipeline is running in a Docker container
# reachable via the `deploy-physalis` context, and that sqlite3 is
# installed inside the container (it is — the image is based on
# Debian with the binary's runtime deps + sqlite3 CLI).
#
# Defaults:
#   CONTAINER  media-pipeline
#   DB_PATH    /data/pipeline.db
#   CONTEXT    deploy-physalis
#
# Override via env vars:
#   CONTAINER=foo DB_PATH=/tmp/test.db CONTEXT=deploy-fenneko \
#       rename-revert.sh "Movie.S01E01-NXELE.mkv"
#
# Note on the rename path: the `mv` runs INSIDE the container
# (via `docker exec`) so the on-disk path matches the
# container's view of `directories.staging_path`. Running the
# rename from the host would require translating between the
# container's `/staging/...` and the host's
# `/mnt/mediaserver/Staging/...` (or wherever the bind mount
# appears locally), which is host-specific and brittle. The
# container's pipeline user has write perms on /staging (it's
# in gid 1111, the same group that owns the bind mount), so
# the rename is straightforward from inside.

set -euo pipefail

CONTAINER="${CONTAINER:-media-pipeline}"
DB_PATH="${DB_PATH:-/data/pipeline.db}"
CONTEXT="${CONTEXT:-deploy-physalis}"

# ---- Argument parsing ----
VERBOSE=0
RENAMED_NAME=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        -v|--verbose)
            VERBOSE=1
            shift
            ;;
        -h|--help)
            sed -n '2,9p' "$0"
            exit 0
            ;;
        --)
            shift
            RENAMED_NAME="${1:-}"
            break
            ;;
        -*)
            echo "error: unknown flag '$1'" >&2
            echo "usage: $0 [-v|--verbose] <renamed-filename>" >&2
            exit 64
            ;;
        *)
            RENAMED_NAME="$1"
            shift
            ;;
    esac
done

if [[ -z "$RENAMED_NAME" ]]; then
    echo "usage: $0 [-v|--verbose] <renamed-filename>" >&2
    exit 64
fi

# log: print a step line to stderr when verbose. No-op otherwise.
log() {
    if [[ "$VERBOSE" -eq 1 ]]; then
        echo "[$(date +%H:%M:%S)] $*" >&2
    fi
}

# ---- 1. Look up the file in the DB ----
# Pull the original_name and staging_path for the file whose
# renamed_name matches. We do this in one query so the script
# doesn't race the DB between lookups.
log "querying DB for renamed_name = '$RENAMED_NAME'"
log "  container=$CONTAINER  db=$DB_PATH  context=$CONTEXT"
# SQLite's CLI treats positional args as input files, not bind
# params, so we escape the value into the SQL as a quoted literal
# instead of using a `?` placeholder. Single quotes are doubled
# per SQL standard; backslashes are escaped for the CLI string.
ESCAPED_RENAMED="${RENAMED_NAME//\'/\'\'}"
read -r ORIGINAL_NAME STAGING_PATH < <(
    docker --context "$CONTEXT" exec "$CONTAINER" \
        sqlite3 "$DB_PATH" -separator $'\t' -readonly \
        "SELECT f.original_name, d.staging_path
           FROM files f
           JOIN directories d ON d.id = f.dir_id
          WHERE f.renamed_name = '$ESCAPED_RENAMED';" \
        | tr -d '\r'
)

if [[ -z "$ORIGINAL_NAME" || -z "$STAGING_PATH" ]]; then
    echo "error: no row in files table with renamed_name = '$RENAMED_NAME'" >&2
    echo "hint: the rename pool is currently disabled on physalis." >&2
    echo "hint: new files have renamed_name = NULL and are not matchable." >&2
    echo "hint: pass the basename as it appears in /staging, not the full path." >&2
    exit 1
fi
log "DB match: original_name='$ORIGINAL_NAME'  staging_path='$STAGING_PATH'"

CURRENT_PATH="$STAGING_PATH/$RENAMED_NAME"
TARGET_PATH="$STAGING_PATH/$ORIGINAL_NAME"

# ---- 2. Sanity-check the on-disk file (run inside the container
# so the path matches `staging_path` as the container sees it) ----
log "checking source path inside container: $CURRENT_PATH"
if ! docker --context "$CONTEXT" exec "$CONTAINER" \
        test -e "$CURRENT_PATH"; then
    echo "error: DB says file is at $CURRENT_PATH but it does not exist inside container $CONTAINER" >&2
    exit 1
fi

log "checking target path not already taken: $TARGET_PATH"
if docker --context "$CONTEXT" exec "$CONTAINER" \
        test -e "$TARGET_PATH"; then
    echo "error: target already exists inside container: $TARGET_PATH" >&2
    echo "hint: remove it first, or rename the original manually." >&2
    exit 1
fi

# ---- 3. Rename on disk + clear renamed_name in DB ----
# Both run inside the container so the on-disk path matches
# `staging_path` as the DB sees it.
log "renaming on disk (inside container)"
docker --context "$CONTEXT" exec "$CONTAINER" \
    mv -n "$CURRENT_PATH" "$TARGET_PATH"
log "  ok: $CURRENT_PATH -> $TARGET_PATH"

log "clearing renamed_name in DB"
docker --context "$CONTEXT" exec "$CONTAINER" \
    sqlite3 "$DB_PATH" \
    "UPDATE files SET renamed_name = NULL WHERE renamed_name = '$ESCAPED_RENAMED';" \
    >/dev/null
log "  ok: DB row updated"

echo "renamed: $CURRENT_PATH"
echo "     to: $TARGET_PATH"
echo "done."
