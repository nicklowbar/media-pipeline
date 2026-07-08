#!/bin/bash
# Rename TV episode files matching "Title - NNN - Episode Title.ext" to
# "Title - SXXEYY - Episode Title.ext".
#
# Usage:
#   ./rename-sxxeyy.sh /path/to/dir [--dry-run]
#
# Examples:
#   Gargoyles - 101 - Awakening, Part I.avi  → Gargoyles - S01E01 - Awakening, Part I.avi
#   Foo - 002 - Bar.avi                      → Foo - S00E02 - Bar.avi

set -e

if [[ "${1:-}" == "--help" ]] || [[ "${1:-}" == "-h" ]]; then
    sed -n '2,10p' "$0"
    exit 0
fi

DRY_RUN=""
if [[ "${1:-}" == "--dry-run" ]]; then
    DRY_RUN="yes"
    shift
fi

ROOT="${1:-.}"
EXT_PATTERN='\.(avi|mkv|mp4|m4v|srt|sub|ass)$'

if [[ ! -d "$ROOT" ]]; then
    echo "Error: $ROOT is not a directory"
    exit 1
fi

renamed=0
skipped=0

for f in "$ROOT"/*; do
    [[ -f "$f" ]] || continue
    name=$(basename "$f")

    # Skip if no extension match
    ext=$(echo "$name" | sed -E "s/.*($EXT_PATTERN)$/\1/")
    [[ "$ext" == "$name" ]] && continue

    # Match: "Title - NNN - Episode Title.ext"
    # Non-greedy .+? stops at the first " - " so titles with spaces work.
    # Handles single or double spaces around the dashes.
    if [[ "$name" =~ ^(.+?)[[:space:]]+-[[:space:]]([0-9]{3})[[:space:]]*-[[:space:]](.+)\.([a-zA-Z0-9]+)$ ]]; then
        title="${BASH_REMATCH[1]}"
        num="${BASH_REMATCH[2]}"
        episode="${BASH_REMATCH[3]}"
        ext="${BASH_REMATCH[4]}"

        # Strip extension from episode if present (group 3 captured up to the last dot)
        episode="${episode%.${ext}}"

        # Pad to 2 digits each (NNN → SXXEYY, e.g. 101 → S01E01)
        season=$(( num / 100 ))
        episode_num=$(( num % 100 ))
        new_num=$(printf "S%02dE%02d" "$season" "$episode_num")

        new_name="${title} - ${new_num} - ${episode}.${ext}"

        if [[ -n "$DRY_RUN" ]]; then
            echo "[DRY-RUN] $name → $new_name"
        else
            mv -i "$f" "$ROOT/$new_name"
            echo "[RENAMED] $new_name"
        fi
        renamed=$((renamed + 1))
    else
        echo "[SKIP] $name (no match)"
        skipped=$((skipped + 1))
    fi
done

echo ""
echo "Done: $renamed renamed, $skipped skipped"
