#!/bin/bash
# Unrar all .rar files under a directory tree, in-place.
#
# Usage:
#   ./unrar-all.sh [-v] /path/to/dir
#
# Options:
#   -v, --verbose   show unrar per-file extraction output
#
# Requires: unrar

set -e

VERBOSE="no"
if [[ "${1:-}" == "-v" ]] || [[ "${1:-}" == "--verbose" ]]; then
    VERBOSE="yes"
    shift
fi

if [[ "${1:-}" == "--help" ]] || [[ "${1:-}" == "-h" ]]; then
    sed -n '2,9p' "$0"
    exit 0
fi

ROOT="${1:-}"
if [[ -z "$ROOT" ]]; then
    echo "Usage: $0 [-v] <directory>"
    exit 1
fi

if [[ ! -d "$ROOT" ]]; then
    echo "Error: $ROOT is not a directory"
    exit 1
fi

if ! command -v unrar &>/dev/null; then
    echo "Error: unrar is not installed"
    echo "  apt: sudo apt install unrar"
    exit 1
fi

log() { echo "[$(date '+%H:%M:%S')] $*"; }

RAR_LIST=()
while IFS= read -r -d '' rar; do
    RAR_LIST+=("$rar")
done < <(find "$ROOT" -type f -name "*.rar" -print0)

log "Found ${#RAR_LIST[@]} .rar archive(s) in $ROOT"

TOTAL=${#RAR_LIST[@]}
FAILED=0

if [[ $TOTAL -eq 0 ]]; then
    log "Nothing to do"
    exit 0
fi

IDX=0
for rar in "${RAR_LIST[@]}"; do
    IDX=$((IDX + 1))
    DIR="$(dirname "$rar")"
    FILE="$(basename "$rar")"

    UNRAR_OPTS=(x -o+)
    [[ "$VERBOSE" == "yes" ]] || UNRAR_OPTS+=(-y)

    log "[$IDX/$TOTAL] Extracting: $FILE"
    log "         in: $DIR"

    before_count=$(ls -1A "$DIR" 2>/dev/null | wc -l)

    if [[ "$VERBOSE" == "yes" ]]; then
        unrar "${UNRAR_OPTS[@]}" "$rar" "$DIR" 2>&1 | while IFS= read -r line; do
            log "         $line"
        done
        RC=${PIPESTATUS[0]}
    else
        unrar "${UNRAR_OPTS[@]}" "$rar" "$DIR" >/dev/null 2>&1
        RC=$?
    fi

    after_count=$(ls -1A "$DIR" 2>/dev/null | wc -l)

    if [[ $RC -ne 0 ]]; then
        log "[$IDX/$TOTAL] FAILED: $FILE (unrar exit code: $RC)"
        FAILED=$((FAILED + 1))
    elif [[ $after_count -le $before_count ]]; then
        log "[$IDX/$TOTAL] FAILED: $FILE (no files extracted — archive may be password-protected or corrupt)"
        FAILED=$((FAILED + 1))
    else
        log "[$IDX/$TOTAL] Extracted: $FILE"
    fi
done

log "Done: $TOTAL archives, $FAILED failures"
exit $((FAILED > 0 ? 1 : 0))
