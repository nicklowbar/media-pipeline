#!/usr/bin/env python3
"""Bulk-mark directories as 'in_library' when their library_path exists on disk.

Usage:
    ./mark-in-library-bulk.py <hostname>
    pipelinedb: DB path inside the container (default: /data/pipeline.db)
"""

import subprocess
import sys
import os


def run(cmd: list[str]) -> str:
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"Command failed: {' '.join(cmd)}\n{result.stderr}")
    return result.stdout


def main():
    if len(sys.argv) < 2:
        print("Usage: mark-in-library-bulk.py <hostname>", file=sys.stderr)
        sys.exit(1)

    host = sys.argv[1]
    db_path = "/data/pipeline.db"
    container = "media-pipeline"

    print(f"=== mark-in-library-bulk on {host} ===\n")

    # Query candidates: analyzed/synced dirs with a non-null library_path
    query = (
        "SELECT id, library_path, staging_path, state "
        "FROM directories "
        "WHERE state IN ('analyzed', 'synced') "
        "AND library_path IS NOT NULL "
        "AND library_path != '';"
    )

    sql = f'docker exec {container} sqlite3 -separator "|" {db_path} "{query}"'
    output = run(["ssh", host, sql])

    lines = [l for l in output.strip().split("\n") if l and not l.startswith("id")]
    print(f"Found {len(lines)} candidate(s)\n")

    marked = 0
    missing = 0

    for line in lines:
        parts = line.strip().split("|")
        if len(parts) != 4:
            continue
        id_, library_path, staging_path, state = parts

        # Check existence inside the container (container mount paths, not host paths)
        test_cmd = f'docker exec {container} test -e "{library_path}"'
        rc = subprocess.run(
            ["ssh", host, test_cmd],
            capture_output=True,
        )

        if rc.returncode == 0:
            update = (
                f"UPDATE directories "
                f"SET state = 'in_library', moved_at = CURRENT_TIMESTAMP "
                f"WHERE id = {id_};"
            )
            upd_cmd = f'docker exec {container} sqlite3 {db_path} "{update}"'
            run(["ssh", host, upd_cmd])
            print(f"[{id_}] [{state}] marked in_library: {library_path}")
            marked += 1
        else:
            print(f"[{id_}] [{state}] skipped (not on disk): {library_path}")
            missing += 1

    print(f"\n=== Summary ===")
    print(f"  Marked in_library:  {marked}")
    print(f"  Skipped (missing):  {missing}")
    print(f"  Total checked:      {len(lines)}")


if __name__ == "__main__":
    main()
