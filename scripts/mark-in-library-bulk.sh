#!/bin/bash
set -euo pipefail
exec python3 "$(dirname "$0")/mark-in-library-bulk.py" "${@:-physalis}"
