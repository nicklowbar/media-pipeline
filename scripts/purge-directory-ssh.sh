#!/usr/bin/env bash
# Thin wrapper: copy purge-directory.sh to the remote host and run it there.
# Python subprocess handles all SSH quoting — no shell-escaping issues with
# paths containing spaces, apostrophes, backslashes, etc.
#
# Usage:
#   purge-directory-ssh.sh [-v|--verbose] <ssh-host> [--db-path PATH] <staging-path>
#   purge-directory-ssh.sh --help
#
# Example:
#   ./purge-directory-ssh.sh physalis -v "/staging/TvShows/Foo"
#   ./purge-directory-ssh.sh physalis --db-path /opt/media-pipeline/data/pipeline.db "/staging/TvShows/Foo"

set -euo pipefail
exec python3 "$(dirname "$0")/purge-directory-ssh.py" "${@:-}"
