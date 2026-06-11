#!/usr/bin/env bash
# Builds, then starts the broker on port 9092.
# Usage: scripts/run.sh [dataDir]   (default: ./data)
set -euo pipefail
cd "$(dirname "$0")/.."

scripts/build.sh
java -cp out minibroker.Main "${1:-data}"
