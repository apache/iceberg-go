/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.deletes;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.iceberg.Files;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinWriter;
import org.apache.iceberg.puffin.StandardBlobTypes;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

/**
 * Standalone main() that produces Java-authored DV Puffin fixtures for iceberg-go's
 * cross-client tests. Lives in the {@code org.apache.iceberg.deletes} package so it
 * can construct the package-private {@code BitmapPositionDeleteIndex} directly.
 *
 * <p>Compiled and run by {@code dev/dv-fixtures/generate.sh} against the iceberg-core
 * JAR built at {@code /Users/tanmayrauth/Desktop/work/iceberg/core/build/libs/} (or
 * the path provided via the {@code ICEBERG_REPO} environment variable). No Gradle
 * runtime is required, so this works inside sandboxes that block Gradle's UDP loopback
 * bind.
 *
 * <p>Output directory defaults to {@code build/dv-fixtures/} under the current working
 * directory; override via {@code -Ddv.fixtures.outputDir=&lt;path&gt;}.
 *
 * <p>Compression is forced to NONE (null codec) so the output bytes are reproducible
 * across JVM and zstd library versions.
 */
public class GenerateDVFixtures {

  private static final String REFERENCED_DATA_FILE_KEY = "referenced-data-file";
  private static final String CARDINALITY_KEY = "cardinality";
  private static final String CREATED_BY = "iceberg-go DV cross-client fixtures (apache/iceberg)";

  public static void main(String[] args) throws Exception {
    Path dir = outputDir();
    dir.toFile().mkdirs();
    writeSingleBlobDV(dir);
    writeMultiBlobDV(dir);
  }

  private static Path outputDir() {
    String configured = System.getProperty("dv.fixtures.outputDir");
    if (configured != null && !configured.isEmpty()) {
      return Paths.get(configured);
    }
    return Paths.get("build", "dv-fixtures");
  }

  private static void writeSingleBlobDV(Path dir) throws Exception {
    File outFile = dir.resolve("single-blob-dv.puffin").toFile();
    OutputFile output = Files.localOutput(outFile);

    String referencedPath = "s3://warehouse/db/table/data/00000-0-abc.parquet";
    long[] positions = {1L, 3L, 5L, 7L, 9L};

    try (PuffinWriter writer = Puffin.write(output).createdBy(CREATED_BY).build()) {
      writer.add(buildBlob(referencedPath, positions));
      writer.finish();
    }
    System.out.println("[GenerateDVFixtures] wrote " + outFile.getAbsolutePath());
  }

  private static void writeMultiBlobDV(Path dir) throws Exception {
    File outFile = dir.resolve("multi-blob-dv.puffin").toFile();
    OutputFile output = Files.localOutput(outFile);

    String path1 = "s3://warehouse/db/table/data/file-001.parquet";
    long[] positions1 = {0L, 100L, 200L};
    String path2 = "s3://warehouse/db/table/data/file-002.parquet";
    long[] positions2 = {50L, 150L};

    try (PuffinWriter writer = Puffin.write(output).createdBy(CREATED_BY).build()) {
      writer.add(buildBlob(path1, positions1));
      writer.add(buildBlob(path2, positions2));
      writer.finish();
    }
    System.out.println("[GenerateDVFixtures] wrote " + outFile.getAbsolutePath());
  }

  private static Blob buildBlob(String referencedDataFile, long[] positions) {
    BitmapPositionDeleteIndex index = new BitmapPositionDeleteIndex();
    for (long pos : positions) {
      index.delete(pos);
    }
    return new Blob(
        StandardBlobTypes.DV_V1,
        ImmutableList.of(MetadataColumns.ROW_POSITION.fieldId()),
        -1L,
        -1L,
        index.serialize(),
        null,
        ImmutableMap.of(
            REFERENCED_DATA_FILE_KEY,
            referencedDataFile,
            CARDINALITY_KEY,
            String.valueOf(index.cardinality())));
  }
}
