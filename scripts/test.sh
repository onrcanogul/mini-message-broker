#!/usr/bin/env bash
# Builds, then runs the full JUnit suite.
set -euo pipefail
cd "$(dirname "$0")/.."

JAR="lib/junit-platform-console-standalone.jar"
scripts/build.sh
java -jar "$JAR" execute -cp out --scan-classpath
