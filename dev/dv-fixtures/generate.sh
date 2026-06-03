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

# Build and run GenerateDVFixtures.java directly via javac + java (no Gradle).
# Requires that apache/iceberg has been built once via `./gradlew :iceberg-core:jar`
# so that the JARs in core/build/libs/, api/build/libs/, etc. exist.

set -euo pipefail

ICEBERG_REPO="${ICEBERG_REPO:-$HOME/Desktop/work/iceberg}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT_DIR="${OUT_DIR:-$SCRIPT_DIR/build/dv-fixtures}"
TESTDATA_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)/table/dv/testdata/deletes"

resolve_jar() {
  local pattern="$1"
  local found
  found="$(find "$ICEBERG_REPO" -path "*/build/libs/*" -name "$pattern" \
    ! -name "*-tests.jar" ! -name "*-sources.jar" ! -name "*-javadoc.jar" ! -name "*-empty.jar" \
    -print -quit 2>/dev/null)"
  if [[ -z "$found" ]]; then
    echo "ERROR: could not find $pattern under $ICEBERG_REPO/*/build/libs/" >&2
    echo "       Build iceberg-core once: (cd $ICEBERG_REPO && ./gradlew :iceberg-core:jar :iceberg-api:jar :iceberg-bundled-guava:jar)" >&2
    exit 1
  fi
  echo "$found"
}

resolve_dep() {
  # Find a versioned dep JAR under the Gradle cache. Picks the most recently downloaded.
  local group_dir="$1"
  local artifact="$2"
  local found
  found="$(find "$HOME/.gradle/caches/modules-2/files-2.1/$group_dir" -name "${artifact}-*.jar" \
    ! -name "*-sources.jar" ! -name "*-javadoc.jar" \
    -print 2>/dev/null | sort -V | tail -n 1)"
  if [[ -z "$found" ]]; then
    echo "ERROR: could not find ${artifact}-*.jar in ~/.gradle/caches/modules-2/files-2.1/$group_dir" >&2
    exit 1
  fi
  echo "$found"
}

ICEBERG_CORE_JAR="$(resolve_jar 'iceberg-core-*.jar')"
ICEBERG_API_JAR="$(resolve_jar 'iceberg-api-*.jar')"
ICEBERG_COMMON_JAR="$(resolve_jar 'iceberg-common-*.jar')"
ICEBERG_GUAVA_JAR="$(resolve_jar 'iceberg-bundled-guava-*.jar')"
ROARING_JAR="$(resolve_dep 'org.roaringbitmap' 'RoaringBitmap')"
AIRCOMPRESSOR_JAR="$(resolve_dep 'io.airlift' 'aircompressor')"
JACKSON_CORE_JAR="$(resolve_dep 'com.fasterxml.jackson.core/jackson-core' 'jackson-core')"
JACKSON_DATABIND_JAR="$(resolve_dep 'com.fasterxml.jackson.core/jackson-databind' 'jackson-databind')"
JACKSON_ANNOTATIONS_JAR="$(resolve_dep 'com.fasterxml.jackson.core/jackson-annotations' 'jackson-annotations')"
# Avro itself is unused but PuffinWriter.fileFlags reflects over an enum that references
# org.apache.avro.generic.IndexedRecord, so it must be loadable at runtime.
AVRO_JAR="$(resolve_dep 'org.apache.avro/avro' 'avro')"
# PuffinFormat.Flag's static initializer touches Caffeine via Iceberg's BaseFileScanTaskParser.
CAFFEINE_JAR="$(resolve_dep 'com.github.ben-manes.caffeine/caffeine' 'caffeine')"

CP="$ICEBERG_CORE_JAR:$ICEBERG_API_JAR:$ICEBERG_COMMON_JAR:$ICEBERG_GUAVA_JAR"
CP="$CP:$ROARING_JAR:$AIRCOMPRESSOR_JAR:$AVRO_JAR:$CAFFEINE_JAR"
CP="$CP:$JACKSON_CORE_JAR:$JACKSON_DATABIND_JAR:$JACKSON_ANNOTATIONS_JAR"

mkdir -p "$OUT_DIR"
CLASSES_DIR="$OUT_DIR/classes"
mkdir -p "$CLASSES_DIR"
# Iceberg's localOutput refuses to overwrite, so clear any prior run's outputs.
rm -f "$OUT_DIR/single-blob-dv.puffin" "$OUT_DIR/multi-blob-dv.puffin"

echo "[generate.sh] compiling against:"
echo "  core:        $ICEBERG_CORE_JAR"
echo "  api:         $ICEBERG_API_JAR"
echo "  common:      $ICEBERG_COMMON_JAR"
echo "  guava:       $ICEBERG_GUAVA_JAR"
echo "  roaring:     $ROARING_JAR"
echo "  airlift:     $AIRCOMPRESSOR_JAR"
echo "  avro:        $AVRO_JAR"
echo "  caffeine:    $CAFFEINE_JAR"
echo "  jackson-c:   $JACKSON_CORE_JAR"
echo "  jackson-d:   $JACKSON_DATABIND_JAR"
echo "  jackson-ann: $JACKSON_ANNOTATIONS_JAR"

javac -d "$CLASSES_DIR" -cp "$CP" "$SCRIPT_DIR/GenerateDVFixtures.java"

echo "[generate.sh] running"
java -cp "$CLASSES_DIR:$CP" \
  -Ddv.fixtures.outputDir="$OUT_DIR" \
  org.apache.iceberg.deletes.GenerateDVFixtures

echo "[generate.sh] mirroring into $TESTDATA_DIR"
cp "$OUT_DIR/single-blob-dv.puffin" "$TESTDATA_DIR/"
cp "$OUT_DIR/multi-blob-dv.puffin"  "$TESTDATA_DIR/"

echo "[generate.sh] done. fixtures:"
ls -la "$TESTDATA_DIR/single-blob-dv.puffin" "$TESTDATA_DIR/multi-blob-dv.puffin"