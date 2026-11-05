<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Theta sketch fixture generator

`GenerateThetaFixtures.java` produces the Theta sketch fixtures in
`table/stats/testdata/` used by iceberg-go's NDV parity tests in
`table/stats/sketch_test.go`.

It replays the aggregation in Iceberg-Java's
`org.apache.spark.sql.stats.ThetaSketchAgg` without a Spark runtime: an
`UpdateSketch.builder.setFamily(Family.ALPHA)` sketch per partition, non-null
values fed through `update(Conversions.toByteBuffer(type, value))`, partials
serialized with `compact().toByteArray()` and merged into a fresh buffer with
`SetOperationBuilder().buildUnion().union(buffer, input)`, and the result stored
as `NDVSketchUtil` stores it in the Puffin blob (before Puffin compression).

The value generators are mirrored in `table/stats/sketch_test.go`; change both
together.

## Regenerate

1. Build the Iceberg jars once in your `apache/iceberg` checkout and make sure
   Gradle has resolved the Spark module's dependencies, which include
   `datasketches-java`:

   ```bash
   ./gradlew :iceberg-api:jar :iceberg-bundled-guava:jar
   ```

2. From the iceberg-go repo root:

   ```bash
   ICEBERG_REPO=/path/to/iceberg dev/theta-fixtures/generate.sh
   ```

The script reads the DataSketches version from Iceberg's
`gradle/libs.versions.toml`, compiles and runs the generator with `javac` and
`java` (no Gradle), and copies the `.sk` files into `table/stats/testdata/`.
The output is deterministic, so regenerating without changes leaves the
fixtures byte-identical.
