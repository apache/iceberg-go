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

package table

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// captureSlog redirects the default slog logger to an in-memory text handler
// for the duration of fn and returns the captured output. Note that
// slog.SetDefault mutates process-global state: callers must rely on the
// package's tests not running parallel slog-emitters during the window.
// Sibling test TestOverwriteRowCountWarning uses the same pattern.
func captureSlog(t *testing.T, fn func()) string {
	t.Helper()
	buf := &bytes.Buffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	fn()

	return buf.String()
}

// countWarnRecords counts WARN-level records in slog text-handler output.
// Each record is one line, so we scan lines rather than coupling to the
// exact key=value layout (which can shift if attributes are added).
func countWarnRecords(out string) int {
	n := 0
	for line := range strings.SplitSeq(out, "\n") {
		if strings.Contains(line, "level=WARN") {
			n++
		}
	}

	return n
}

// addUnsortedSortOrder gives a metadata builder the unsorted default order
// so v2+ "default sort order id must be set" validation passes. Position-
// delete writes in these tests do not depend on sort order.
func addUnsortedSortOrder(t *testing.T, mb *MetadataBuilder) {
	t.Helper()
	so := UnsortedSortOrder
	require.NoError(t, mb.AddSortOrder(&so))
	require.NoError(t, mb.SetDefaultSortOrderID(0))
}

func newPositionDeleteUnpartitionedMetadata(t *testing.T, formatVersion int) *MetadataBuilder {
	t.Helper()
	mb, err := NewMetadataBuilder(formatVersion)
	require.NoError(t, err)
	require.NoError(t, mb.AddSchema(iceberg.PositionalDeleteSchema))
	require.NoError(t, mb.SetCurrentSchemaID(0))
	require.NoError(t, mb.AddPartitionSpec(iceberg.UnpartitionedSpec, true))
	require.NoError(t, mb.SetDefaultSpecID(0))
	require.NoError(t, mb.SetLoc("file:///warn-test"))
	addUnsortedSortOrder(t, mb)

	return mb
}

func newPositionDeletePartitionedMetadata(t *testing.T, formatVersion int) *MetadataBuilder {
	t.Helper()
	mb, err := NewMetadataBuilder(formatVersion)
	require.NoError(t, err)
	require.NoError(t, mb.AddSchema(iceberg.NewSchema(0,
		append(iceberg.PositionalDeleteSchema.Fields(),
			iceberg.NestedField{Name: "age", ID: 2, Type: iceberg.Int64Type{}})...)))
	require.NoError(t, mb.SetCurrentSchemaID(0))
	partitionSpec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{2},
		Name:      "age_bucket",
		Transform: iceberg.BucketTransform{NumBuckets: 2},
	})
	require.NoError(t, mb.AddPartitionSpec(&partitionSpec, true))
	require.NoError(t, mb.SetDefaultSpecID(0))
	require.NoError(t, mb.SetLoc("file:///warn-test-partitioned"))
	addUnsortedSortOrder(t, mb)

	return mb
}

// runPositionDeleteWrite drives positionDeleteRecordsToDataFiles to completion.
// The for-range over seq is load-bearing: it drains the writer's goroutines.
// Do not "simplify" by discarding the iterator.
func runPositionDeleteWrite(t *testing.T, mb *MetadataBuilder, partitions map[string]partitionContext, itr func(yield func(arrow.RecordBatch, error) bool)) {
	t.Helper()
	seq := positionDeleteRecordsToDataFiles(context.Background(), t.TempDir(), mb, partitions,
		recordWritingArgs{
			sc:  PositionalDeleteArrowSchema,
			itr: itr,
			fs:  io.LocalFS{},
		})
	for _, err := range seq {
		require.NoError(t, err)
	}
}

// TestPositionDeleteV3Warning verifies the writer emits a single deduped
// slog.Warn naming the table when position-deletes are written on a v3 table,
// and stays silent on v2 where Parquet position-deletes are still canonical.
//
// The warning fires once at writer entry, before partition fanout, so a
// partitioned write logs once total — not once per partition. The partitioned
// subtest locks that contract: the issue specifically called out "deduped",
// and a future change that moved the warning into per-partition writers would
// silently regress this property without it.
func TestPositionDeleteV3Warning(t *testing.T) {
	emptyItr := func(yield func(arrow.RecordBatch, error) bool) {}

	t.Run("v3 unpartitioned warns once with table location", func(t *testing.T) {
		out := captureSlog(t, func() {
			runPositionDeleteWrite(t, newPositionDeleteUnpartitionedMetadata(t, 3), nil, emptyItr)
		})
		assert.Equal(t, 1, countWarnRecords(out),
			"expected exactly one WARN record, got: %s", out)
		assert.Contains(t, out, "Parquet position-delete")
		assert.Contains(t, out, "deletion vectors")
		assert.Contains(t, out, "table_location=file:///warn-test",
			"warning should name the table location")
	})

	t.Run("v2 does not warn", func(t *testing.T) {
		out := captureSlog(t, func() {
			runPositionDeleteWrite(t, newPositionDeleteUnpartitionedMetadata(t, 2), nil, emptyItr)
		})
		assert.Equal(t, 0, countWarnRecords(out),
			"v2 position-delete writes should not warn, got: %s", out)
	})

	t.Run("v3 partitioned write warns exactly once across multiple partitions", func(t *testing.T) {
		// Two batches each routed to a distinct partition. The fanout
		// writer's processBatch reads only the first row's file_path, so a
		// per-partition regression of the warn (e.g. moved into processBatch
		// or per-rolling-writer init) would emit 2 records here. One batch
		// with two rows would not exercise the fanout — it would all go to
		// one partition. Two batches force two processBatch invocations
		// targeting two different partition contexts.
		mb := newPositionDeletePartitionedMetadata(t, 3)
		partitions := map[string]partitionContext{
			"file://namespace/age_bucket=0/test.parquet": {
				partitionData: map[int]any{iceberg.PartitionDataIDStart: 0}, specID: 0,
			},
			"file://namespace/age_bucket=1/test.parquet": {
				partitionData: map[int]any{iceberg.PartitionDataIDStart: 1}, specID: 0,
			},
		}
		batch0 := mustLoadRecordBatchFromJSON(PositionalDeleteArrowSchema,
			`[{"file_path": "file://namespace/age_bucket=0/test.parquet", "pos": 0}]`)
		batch1 := mustLoadRecordBatchFromJSON(PositionalDeleteArrowSchema,
			`[{"file_path": "file://namespace/age_bucket=1/test.parquet", "pos": 0}]`)
		itr := func(yield func(arrow.RecordBatch, error) bool) {
			batch0.Retain()
			if !yield(batch0, nil) {
				return
			}
			batch1.Retain()
			yield(batch1, nil)
		}

		out := captureSlog(t, func() {
			runPositionDeleteWrite(t, mb, partitions, itr)
		})
		assert.Equal(t, 1, countWarnRecords(out),
			"expected exactly one WARN record across multiple partitions, got: %s", out)
	})
}
