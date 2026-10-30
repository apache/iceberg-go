// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package stats_test

import (
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/datasketches-go/theta"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table/stats"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// leInt64 encodes v the way Iceberg serializes a LONG value: 8 bytes,
// little-endian.
func leInt64(v int64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(v))

	return b
}

func TestSketchExactModeIsExact(t *testing.T) {
	s := stats.NewSketch()

	const n = 100
	for i := range int64(n) {
		s.Add(leInt64(i))
	}

	// Below the nominal entry count the sketch retains every distinct value, so
	// the estimate is exact.
	assert.Equal(t, float64(n), s.Estimate())
	assert.Equal(t, int64(n), s.NDV())
}

func TestSketchIgnoresDuplicates(t *testing.T) {
	s := stats.NewSketch()

	for range 1000 {
		s.Add(leInt64(42))
	}

	assert.Equal(t, int64(1), s.NDV())
}

func TestSketchEmpty(t *testing.T) {
	s := stats.NewSketch()

	assert.Equal(t, 0.0, s.Estimate())
	assert.Equal(t, int64(0), s.NDV())

	ndv, err := stats.NDVFromBytes(s.Serialize())
	require.NoError(t, err)
	assert.Equal(t, int64(0), ndv)
}

// TestSketchSkipsEmptyValues checks that empty values are not counted, as in
// Iceberg-Java: a string column with "", "a", "b" has an NDV of 2.
func TestSketchSkipsEmptyValues(t *testing.T) {
	s := stats.NewSketch()

	s.Add([]byte(""))
	s.Add([]byte("a"))
	s.Add([]byte("b"))
	s.Add(nil)
	s.Add([]byte{})

	assert.Equal(t, int64(2), s.NDV())

	onlyEmpty := stats.NewSketch()
	onlyEmpty.Add([]byte(""))
	assert.Equal(t, int64(0), onlyEmpty.NDV())
}

func TestSketchDistinctByteEncoding(t *testing.T) {
	s := stats.NewSketch()

	s.Add([]byte("apple"))
	s.Add([]byte("apple"))
	s.Add([]byte("banana"))

	assert.Equal(t, int64(2), s.NDV())
}

// TestSketchTrimsToNominalEntries checks that a sketch past 4096 distinct values
// is trimmed to 4096 retained entries, as Iceberg-Java's union does with every
// sketch it stores, so it reports an estimate rather than an exact count.
func TestSketchTrimsToNominalEntries(t *testing.T) {
	s := stats.NewSketch()
	for i := range int64(6000) {
		s.Add(leInt64(i))
	}

	cs, err := theta.Decode(s.Serialize(), theta.DefaultSeed)
	require.NoError(t, err)
	assert.Equal(t, uint32(4096), cs.NumRetained())
	assert.True(t, cs.IsEstimationMode())
	assert.Equal(t, cs.Estimate(), s.Estimate())
	assert.NotEqual(t, int64(6000), s.NDV())
}

func TestSerializeRoundTrip(t *testing.T) {
	s := stats.NewSketch()
	for i := range int64(5000) {
		s.Add(leInt64(i))
	}

	est, err := stats.EstimateFromBytes(s.Serialize())
	require.NoError(t, err)
	assert.Equal(t, s.Estimate(), est)
}

func TestUnionSketches(t *testing.T) {
	a := stats.NewSketch()
	b := stats.NewSketch()

	// [0,60) and [40,100): 100 distinct across the two, 20 overlapping.
	for i := range int64(60) {
		a.Add(leInt64(i))
	}
	for i := int64(40); i < 100; i++ {
		b.Add(leInt64(i))
	}

	u := stats.NewUnion()
	u.Add(a)
	u.Add(b)

	assert.Equal(t, int64(100), u.NDV())
}

func TestUnionAddBytes(t *testing.T) {
	a := stats.NewSketch()
	for i := range int64(60) {
		a.Add(leInt64(i))
	}

	b := stats.NewSketch()
	for i := int64(40); i < 100; i++ {
		b.Add(leInt64(i))
	}

	u := stats.NewUnion()
	require.NoError(t, u.AddBytes(a.Serialize()))
	u.Add(b)

	assert.Equal(t, int64(100), u.NDV())

	got, err := stats.NDVFromBytes(u.Serialize())
	require.NoError(t, err)
	assert.Equal(t, int64(100), got)
}

func TestUnionAddBytesRejectsInvalidInput(t *testing.T) {
	u := stats.NewUnion()
	assert.Error(t, u.AddBytes([]byte{1, 2, 3}))

	_, err := stats.EstimateFromBytes([]byte{1, 2, 3})
	assert.Error(t, err)
}

// javaFixture describes one sketch in testdata produced by
// dev/theta-fixtures/GenerateThetaFixtures.java. partitions mirrors the
// generator's input: one slice of rows per Spark partition.
type javaFixture struct {
	name       string
	ndv        int64
	partitions [][]row
}

// row is one column value, encoded with Iceberg's single-value serialization.
// Null is explicit because an encoded empty value (such as "") may be a nil
// slice, and empty values must still reach Sketch.Add.
type row struct {
	value []byte
	null  bool
}

var nullRow = row{null: true}

func lit[T iceberg.LiteralType](t *testing.T, v T) []byte {
	b, err := iceberg.NewLiteral(v).MarshalBinary()
	require.NoError(t, err)

	return b
}

// dealRows mirrors GenerateThetaFixtures.write: rows 0..numRows-1 are dealt
// round-robin across numPartitions partitions.
func dealRows(numPartitions, numRows int, value func(i int) row) [][]row {
	partitions := make([][]row, numPartitions)
	for i := range numRows {
		p := i % numPartitions
		partitions[p] = append(partitions[p], value(i))
	}

	return partitions
}

func nullEvery(i, every int, value func() []byte) row {
	if i%every == every-1 {
		return nullRow
	}

	return row{value: value()}
}

// longRanges mirrors GenerateThetaFixtures.range: partition p holds count
// consecutive longs starting at p*step.
func longRanges(t *testing.T, numPartitions int, step int64, count int) [][]row {
	partitions := make([][]row, numPartitions)
	for p := range numPartitions {
		start := int64(p) * step
		for i := range int64(count) {
			partitions[p] = append(partitions[p], row{value: lit(t, start+i)})
		}
	}

	return partitions
}

func javaFixtures(t *testing.T) []javaFixture {
	const microsBase = int64(1_600_000_000_000_000)

	return []javaFixture{
		{name: "null_only", ndv: 0, partitions: dealRows(1, 3, func(int) row { return nullRow })},
		{name: "boolean", ndv: 2, partitions: dealRows(3, 100, func(i int) row {
			return nullEvery(i, 7, func() []byte { return lit(t, i%2 == 0) })
		})},
		{name: "int", ndv: 582, partitions: dealRows(3, 1000, func(i int) row {
			return nullEvery(i, 11, func() []byte { return lit(t, int32((i%600)*7-2000)) })
		})},
		{name: "long", ndv: 582, partitions: dealRows(3, 1000, func(i int) row {
			return nullEvery(i, 11, func() []byte { return lit(t, int64(i%600)*1_000_003-5_000_000_000) })
		})},
		{name: "float", ndv: 582, partitions: dealRows(3, 1000, func(i int) row {
			return nullEvery(i, 11, func() []byte {
				switch i % 600 {
				case 0:
					return lit(t, float32(0))
				case 1:
					return lit(t, float32(math.Copysign(0, -1)))
				default:
					return lit(t, float32(i%600)*0.5+1)
				}
			})
		})},
		{name: "double", ndv: 582, partitions: dealRows(3, 1000, func(i int) row {
			return nullEvery(i, 11, func() []byte { return lit(t, float64(i%600)/3.0) })
		})},
		{name: "date", ndv: 400, partitions: dealRows(3, 1000, func(i int) row {
			return nullEvery(i, 11, func() []byte { return lit(t, iceberg.Date(18000+i%400)) })
		})},
		{name: "timestamp", ndv: 828, partitions: dealRows(3, 1000, func(i int) row {
			return nullEvery(i, 11, func() []byte {
				return lit(t, iceberg.Timestamp(microsBase+int64(i%900)*1_000_000))
			})
		})},
		{name: "string", ndv: 499, partitions: dealRows(3, 1000, func(i int) row {
			return nullEvery(i, 11, func() []byte {
				switch i % 500 {
				case 0:
					return lit(t, "")
				case 1:
					return lit(t, "é")
				default:
					return lit(t, fmt.Sprintf("value-%d", i%500))
				}
			})
		})},
		{name: "binary", ndv: 299, partitions: dealRows(3, 1000, func(i int) row {
			return nullEvery(i, 11, func() []byte {
				if i%300 == 0 {
					return lit(t, []byte{})
				}

				return lit(t, []byte{byte(i % 300), byte(i % 300 / 7)})
			})
		})},
		{name: "decimal", ndv: 582, partitions: dealRows(3, 1000, func(i int) row {
			return nullEvery(i, 11, func() []byte {
				unscaled := int64(i%600-300) * 12345

				return lit(t, iceberg.Decimal{Val: decimal128.FromI64(unscaled), Scale: 2})
			})
		})},
		{name: "uuid", ndv: 582, partitions: dealRows(3, 1000, func(i int) row {
			return nullEvery(i, 11, func() []byte {
				var u uuid.UUID
				binary.BigEndian.PutUint64(u[:8], uint64(i%600))
				binary.BigEndian.PutUint64(u[8:], uint64(i%600)*31+7)

				return lit(t, u)
			})
		})},
		{name: "string_empty_only", ndv: 0, partitions: dealRows(2, 10, func(int) row { return row{value: lit(t, "")} })},
		{name: "merge_exact_partitions", ndv: 5102, partitions: longRanges(t, 4, 1200, 1500)},
		{name: "single_partition_at_k", ndv: 4096, partitions: longRanges(t, 1, 0, 4096)},
	}
}

// buildUnion mirrors the Spark aggregation in GenerateThetaFixtures: one
// sketch per partition, nulls skipped, partials merged through a union.
func buildUnion(partitions [][]row) *stats.Union {
	u := stats.NewUnion()
	for _, rows := range partitions {
		s := stats.NewSketch()
		for _, r := range rows {
			if !r.null {
				s.Add(r.value)
			}
		}
		u.Add(s)
	}

	return u
}

func readFixture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", name+".sk"))
	require.NoError(t, err)

	return b
}

func decode(t *testing.T, b []byte) *theta.CompactSketch {
	t.Helper()
	cs, err := theta.Decode(b, theta.DefaultSeed)
	require.NoError(t, err)

	return cs
}

// assertSameSketch compares decoded state rather than raw bytes:
// DataSketches-Java writes a few preamble fields differently (for example p
// and the single-item flag), but equal emptiness, theta and retained hashes
// mean the sketches are identical.
func assertSameSketch(t *testing.T, java, ours *theta.CompactSketch) {
	t.Helper()
	assert.Equal(t, java.IsEmpty(), ours.IsEmpty(), "empty")
	assert.Equal(t, java.Theta64(), ours.Theta64(), "theta")
	assert.Equal(t, java.NumRetained(), ours.NumRetained(), "retained")
	assert.Equal(t, slices.Collect(java.All()), slices.Collect(ours.All()), "retained hashes")
}

// TestJavaFixtureParity rebuilds each Iceberg-Java fixture from the same
// values and checks that the merged sketch decodes to the identical sketch and
// NDV. Values are encoded with iceberg-go's literal serialization, so the test
// also pins that encoding to Java's Conversions.toByteBuffer for every type.
func TestJavaFixtureParity(t *testing.T) {
	for _, fx := range javaFixtures(t) {
		t.Run(fx.name, func(t *testing.T) {
			javaBytes := readFixture(t, fx.name)
			javaNDV, err := stats.NDVFromBytes(javaBytes)
			require.NoError(t, err)
			require.Equal(t, fx.ndv, javaNDV, "fixture ndv")

			u := buildUnion(fx.partitions)

			assert.Equal(t, javaNDV, u.NDV())
			assertSameSketch(t, decode(t, javaBytes), decode(t, u.Serialize()))

			if len(fx.partitions) == 1 {
				s := stats.NewSketch()
				for _, r := range fx.partitions[0] {
					if !r.null {
						s.Add(r.value)
					}
				}
				assert.Equal(t, javaNDV, s.NDV())
				assertSameSketch(t, decode(t, javaBytes), decode(t, s.Serialize()))
			}
		})
	}
}

// TestJavaFixtureReadAndMerge checks that sketches Iceberg-Java wrote can be
// read and merged with Go-built ones: a union of the Java fixture and the same
// values built in Go is unchanged.
func TestJavaFixtureReadAndMerge(t *testing.T) {
	javaBytes := readFixture(t, "merge_exact_partitions")

	s := stats.NewSketch()
	for i := range int64(5100) {
		s.Add(leInt64(i))
	}

	u := stats.NewUnion()
	require.NoError(t, u.AddBytes(javaBytes))
	u.Add(s)

	assertSameSketch(t, decode(t, javaBytes), decode(t, u.Serialize()))
}

// TestJavaEstimationModeWithinBounds covers fixtures where a single
// per-partition sketch goes past 4096 distinct values. Iceberg-Java's ALPHA
// sketch and our QuickSelect sketch then keep different samples, so the
// estimates are not identical; both must stay within the other's error bounds.
func TestJavaEstimationModeWithinBounds(t *testing.T) {
	for _, tc := range []struct {
		name       string
		partitions [][]row
		distinct   float64
	}{
		{"single_partition_estimation", longRanges(t, 1, 0, 6000), 6000},
		{"multi_partition_estimation", longRanges(t, 3, 3000, 6000), 12000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			java := decode(t, readFixture(t, tc.name))

			ours := decode(t, buildUnion(tc.partitions).Serialize())

			for _, cs := range []*theta.CompactSketch{java, ours} {
				lb, err := cs.LowerBound(3)
				require.NoError(t, err)
				ub, err := cs.UpperBound(3)
				require.NoError(t, err)
				assert.True(t, lb <= tc.distinct && tc.distinct <= ub,
					"true count %v outside [%v, %v]", tc.distinct, lb, ub)
			}

			lb, err := java.LowerBound(3)
			require.NoError(t, err)
			ub, err := java.UpperBound(3)
			require.NoError(t, err)
			assert.True(t, lb <= ours.Estimate() && ours.Estimate() <= ub,
				"our estimate %v outside Java's bounds [%v, %v]", ours.Estimate(), lb, ub)
		})
	}
}
