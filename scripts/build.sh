#!/usr/bin/env bash
# Compiles all main + test sources into out/.
set -euo pipefail
cd "$(dirname "$0")/.."

JAR="lib/junit-platform-console-standalone.jar"
[ -f "$JAR" ] || { echo "Missing $JAR — run scripts/setup.sh first." >&2; exit 1; }

rm -rf out && mkdir -p out
javac -d out -cp "$JAR" $(find src -name '*.java')
echo "Compiled to out/"
