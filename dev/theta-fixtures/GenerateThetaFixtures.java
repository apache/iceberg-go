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

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.IntFunction;
import org.apache.datasketches.common.Family;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.CompactSketch;
import org.apache.datasketches.theta.SetOperationBuilder;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Produces Theta sketch fixtures the way Iceberg-Java's Spark ComputeTableStats action does,
 * for iceberg-go's parity tests in {@code table/stats/sketch_test.go}.
 *
 * <p>Each fixture replays the Spark aggregation in {@code
 * org.apache.spark.sql.stats.ThetaSketchAgg} without a Spark runtime:
 *
 * <ul>
 *   <li>one {@code UpdateSketch.builder.setFamily(Family.ALPHA).build()} buffer per partition;
 *   <li>non-null values fed through {@code update(Conversions.toByteBuffer(type, value))};
 *   <li>each partial serialized with {@code compact().toByteArray()} and read back with {@code
 *       CompactSketch.wrap};
 *   <li>partials merged into a fresh Alpha buffer with {@code new
 *       SetOperationBuilder().buildUnion.union(buffer, input)};
 *   <li>the result written with {@code compact().toByteArray()}, as {@code NDVSketchUtil} stores
 *       it in the Puffin blob (before Puffin-level compression).
 * </ul>
 *
 * <p>Every value generator here is mirrored in the Go test; keep the two in sync.
 */
public class GenerateThetaFixtures {

  private static final long MICROS_BASE = 1_600_000_000_000_000L;

  public static void main(String[] args) throws IOException {
    Path dir = Paths.get(System.getProperty("theta.fixtures.outputDir", "build/theta-fixtures"));
    Files.createDirectories(dir);

    // All nulls: the sketch stays empty.
    write(dir, "null_only", Types.LongType.get(), 1, 3, i -> null);

    // One fixture per primitive type, hashed from Conversions.toByteBuffer. Rows are dealt
    // round-robin across partitions so the union merge path runs, with duplicates and nulls.
    write(dir, "boolean", Types.BooleanType.get(), 3, 100, i -> nullEvery(i, 7, i % 2 == 0));
    write(dir, "int", Types.IntegerType.get(), 3, 1000, i -> nullEvery(i, 11, (i % 600) * 7 - 2000));
    write(dir, "long", Types.LongType.get(), 3, 1000,
        i -> nullEvery(i, 11, (long) (i % 600) * 1_000_003L - 5_000_000_000L));
    write(dir, "float", Types.FloatType.get(), 3, 1000, i -> nullEvery(i, 11, floatValue(i)));
    write(dir, "double", Types.DoubleType.get(), 3, 1000, i -> nullEvery(i, 11, (i % 600) / 3.0));
    write(dir, "date", Types.DateType.get(), 3, 1000, i -> nullEvery(i, 11, 18000 + i % 400));
    write(dir, "timestamp", Types.TimestampType.withoutZone(), 3, 1000,
        i -> nullEvery(i, 11, MICROS_BASE + (long) (i % 900) * 1_000_000L));
    write(dir, "string", Types.StringType.get(), 3, 1000, i -> nullEvery(i, 11, stringValue(i)));
    write(dir, "binary", Types.BinaryType.get(), 3, 1000, i -> nullEvery(i, 11, binaryValue(i)));
    write(dir, "decimal", Types.DecimalType.of(20, 2), 3, 1000,
        i -> nullEvery(i, 11, new BigDecimal(BigInteger.valueOf((long) (i % 600 - 300) * 12345L), 2)));
    write(dir, "uuid", Types.UUIDType.get(), 3, 1000,
        i -> nullEvery(i, 11, new UUID(i % 600, (long) (i % 600) * 31L + 7L)));

    // Only empty values: Java skips them, so the sketch stays empty.
    write(dir, "string_empty_only", Types.StringType.get(), 2, 10, i -> "");

    // Every partition stays in exact mode, but the union holds more than 4096 distinct values
    // and trims to k.
    writePartitioned(dir, "merge_exact_partitions", 4, p -> range(p * 1200L, 1500));

    // One partition at exactly k distinct values, the largest that stays exact.
    writePartitioned(dir, "single_partition_at_k", 1, p -> range(0, 4096));

    // One partition past k: the Alpha sketch enters estimation mode before the final union.
    writePartitioned(dir, "single_partition_estimation", 1, p -> range(0, 6000));

    // Several estimation-mode partitions with overlapping ranges.
    writePartitioned(dir, "multi_partition_estimation", 3, p -> range(p * 3000L, 6000));
  }

  private static Object nullEvery(int i, int every, Object value) {
    return i % every == every - 1 ? null : value;
  }

  private static Object floatValue(int i) {
    switch (i % 600) {
      case 0:
        return 0.0f;
      case 1:
        return -0.0f;
      default:
        return (i % 600) * 0.5f + 1f;
    }
  }

  private static Object stringValue(int i) {
    switch (i % 500) {
      case 0:
        return "";
      case 1:
        return "é";
      default:
        return "value-" + (i % 500);
    }
  }

  private static Object binaryValue(int i) {
    if (i % 300 == 0) {
      return ByteBuffer.allocate(0);
    }
    return ByteBuffer.wrap(new byte[] {(byte) (i % 300), (byte) (i % 300 / 7)});
  }

  private static List<Object> range(long start, int count) {
    List<Object> values = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      values.add(start + i);
    }
    return values;
  }

  /** Deals rows 0..numRows-1 round-robin across numPartitions partitions. */
  private static void write(
      Path dir, String name, Type type, int numPartitions, int numRows, IntFunction<Object> value)
      throws IOException {
    writePartitioned(
        dir,
        name,
        type,
        numPartitions,
        p -> {
          List<Object> values = new ArrayList<>();
          for (int i = p; i < numRows; i += numPartitions) {
            values.add(value.apply(i));
          }
          return values;
        });
  }

  private static void writePartitioned(
      Path dir, String name, int numPartitions, IntFunction<List<Object>> partition)
      throws IOException {
    writePartitioned(dir, name, Types.LongType.get(), numPartitions, partition);
  }

  private static void writePartitioned(
      Path dir, String name, Type type, int numPartitions, IntFunction<List<Object>> partition)
      throws IOException {
    // ThetaSketchAgg.createAggregationBuffer / update / serialize, once per partition.
    List<Sketch> partials = new ArrayList<>();
    for (int p = 0; p < numPartitions; p++) {
      UpdateSketch buffer = UpdateSketch.builder().setFamily(Family.ALPHA).build();
      for (Object value : partition.apply(p)) {
        if (value != null) {
          buffer.update(Conversions.toByteBuffer(type, value));
        }
      }
      partials.add(CompactSketch.wrap(Memory.wrap(buffer.compact().toByteArray())));
    }

    // ThetaSketchAgg final aggregation: a fresh buffer merged with every partial.
    Sketch result = UpdateSketch.builder().setFamily(Family.ALPHA).build();
    for (Sketch partial : partials) {
      result = new SetOperationBuilder().buildUnion().union(result, partial);
    }

    // NDVSketchUtil: the blob holds the wrapped final sketch's toByteArray().
    Sketch blob = CompactSketch.wrap(Memory.wrap(result.compact().toByteArray()));
    byte[] bytes = blob.toByteArray();
    Files.write(dir.resolve(name + ".sk"), bytes);
    System.out.printf(
        "%-28s ndv=%-6d retained=%-5d theta=%d bytes=%d%n",
        name, (long) blob.getEstimate(), blob.getRetainedEntries(), blob.getThetaLong(), bytes.length);
  }
}
