#!/usr/bin/env bash
# Downloads the JUnit Platform Console Standalone jar (not committed; see .gitignore).
set -euo pipefail
cd "$(dirname "$0")/.."

JUNIT_VERSION="1.11.3"
JAR="lib/junit-platform-console-standalone.jar"
URL="https://repo1.maven.org/maven2/org/junit/platform/junit-platform-console-standalone/${JUNIT_VERSION}/junit-platform-console-standalone-${JUNIT_VERSION}.jar"

mkdir -p lib
if [ -f "$JAR" ]; then
  echo "JUnit jar already present: $JAR"
else
  echo "Downloading JUnit $JUNIT_VERSION ..."
  curl -sSL -o "$JAR" "$URL"
  echo "Saved to $JAR"
fi
