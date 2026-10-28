#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Build and run GenerateThetaFixtures.java directly via javac + java (no Gradle).
# Requires that apache/iceberg has been built once via `./gradlew :iceberg-api:jar`
# and that Gradle has downloaded the datasketches dependency Iceberg pins.

set -euo pipefail

ICEBERG_REPO="${ICEBERG_REPO:-$HOME/Desktop/work/iceberg}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT_DIR="${OUT_DIR:-$SCRIPT_DIR/build/theta-fixtures}"
TESTDATA_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)/table/stats/testdata"

# Versions Iceberg-Java pins (gradle/libs.versions.toml) and the memory
# release datasketches-java depends on.
DATASKETCHES_VERSION="$(sed -n 's/^datasketches = "\(.*\)"/\1/p' "$ICEBERG_REPO/gradle/libs.versions.toml")"
DATASKETCHES_MEMORY_VERSION="${DATASKETCHES_MEMORY_VERSION:-3.0.2}"

resolve_jar() {
  local pattern="$1"
  local found
  found="$(find "$ICEBERG_REPO" -path "*/build/libs/*" -name "$pattern" \
    ! -name "*-tests.jar" ! -name "*-sources.jar" ! -name "*-javadoc.jar" ! -name "*-empty.jar" \
    -print -quit 2>/dev/null)"
  if [[ -z "$found" ]]; then
    echo "ERROR: could not find $pattern under $ICEBERG_REPO/*/build/libs/" >&2
    echo "       Build it once: (cd $ICEBERG_REPO && ./gradlew :iceberg-api:jar :iceberg-bundled-guava:jar)" >&2
    exit 1
  fi
  echo "$found"
}

resolve_dep() {
  local artifact="$1"
  local version="$2"
  local found
  found="$(find "$HOME/.gradle/caches/modules-2/files-2.1/org.apache.datasketches/$artifact/$version" \
    -name "${artifact}-${version}.jar" -print -quit 2>/dev/null)"
  if [[ -z "$found" ]]; then
    echo "ERROR: could not find ${artifact}-${version}.jar in the Gradle cache" >&2
    echo "       Resolve it once: (cd $ICEBERG_REPO && ./gradlew :iceberg-spark:iceberg-spark-4.1_2.13:dependencies)" >&2
    exit 1
  fi
  echo "$found"
}

ICEBERG_API_JAR="$(resolve_jar 'iceberg-api-*.jar')"
ICEBERG_GUAVA_JAR="$(resolve_jar 'iceberg-bundled-guava-*.jar')"
DATASKETCHES_JAR="$(resolve_dep datasketches-java "$DATASKETCHES_VERSION")"
MEMORY_JAR="$(resolve_dep datasketches-memory "$DATASKETCHES_MEMORY_VERSION")"

CP="$ICEBERG_API_JAR:$ICEBERG_GUAVA_JAR:$DATASKETCHES_JAR:$MEMORY_JAR"

CLASSES_DIR="$OUT_DIR/classes"
mkdir -p "$CLASSES_DIR"

echo "[generate.sh] compiling against:"
echo "  api:          $ICEBERG_API_JAR"
echo "  guava:        $ICEBERG_GUAVA_JAR"
echo "  datasketches: $DATASKETCHES_JAR"
echo "  memory:       $MEMORY_JAR"

javac -d "$CLASSES_DIR" -cp "$CP" "$SCRIPT_DIR/GenerateThetaFixtures.java"

echo "[generate.sh] running"
java -cp "$CLASSES_DIR:$CP" \
  -Dtheta.fixtures.outputDir="$OUT_DIR" \
  GenerateThetaFixtures

echo "[generate.sh] mirroring into $TESTDATA_DIR"
cp "$OUT_DIR"/*.sk "$TESTDATA_DIR/"
