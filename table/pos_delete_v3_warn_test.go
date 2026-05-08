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
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/internal"
	"github.com/apache/iceberg-go/io"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// captureSlog redirects the default slog logger to an in-memory text handler
// for the duration of fn and returns the captured output. Used to assert on
// log records emitted by the code under test.
func captureSlog(t *testing.T, fn func()) string {
	t.Helper()
	buf := &bytes.Buffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	fn()

	return buf.String()
}

// captureSlogMu serializes captureSlog calls because slog.SetDefault mutates
// process-global state. Run any test using captureSlog under this mutex so
// concurrent t.Parallel cases do not interleave handlers.
var captureSlogMu sync.Mutex

func newPositionDeleteMetadataBuilder(t *testing.T, formatVersion int) *MetadataBuilder {
	t.Helper()
	mb, err := NewMetadataBuilder(formatVersion)
	require.NoError(t, err)
	require.NoError(t, mb.AddSchema(iceberg.PositionalDeleteSchema))
	require.NoError(t, mb.SetCurrentSchemaID(0))
	unpartitioned := *iceberg.UnpartitionedSpec
	require.NoError(t, mb.AddPartitionSpec(&unpartitioned, true))
	require.NoError(t, mb.SetDefaultSpecID(0))
	require.NoError(t, mb.SetLoc("file:///warn-test"))
	sortOrder, err := NewSortOrder(1, []SortField{{
		SourceIDs: []int{2147483546}, // file_path
		Direction: SortASC,
		Transform: iceberg.IdentityTransform{},
		NullOrder: NullsFirst,
	}})
	require.NoError(t, err)
	require.NoError(t, mb.AddSortOrder(&sortOrder))
	require.NoError(t, mb.SetDefaultSortOrderID(-1))

	return mb
}

func runEmptyPositionDeleteWrite(t *testing.T, formatVersion int) {
	t.Helper()
	mb := newPositionDeleteMetadataBuilder(t, formatVersion)
	writeUUID := uuid.New()
	emptyItr := func(yield func(arrow.RecordBatch, error) bool) {}

	seq := positionDeleteRecordsToDataFiles(context.Background(), t.TempDir(), mb,
		map[string]partitionContext{}, recordWritingArgs{
			sc:        PositionalDeleteArrowSchema,
			itr:       emptyItr,
			fs:        &io.LocalFS{},
			writeUUID: &writeUUID,
			counter:   internal.Counter(0),
		})
	for _, err := range seq {
		require.NoError(t, err)
	}
}

// TestPositionDeleteV3Warning verifies the writer emits a single deduped
// slog.Warn naming the table when position-deletes are written on a v3
// table, and stays silent on v2 where Parquet position-deletes are still
// the canonical format. v3 prefers deletion vectors; the warning guides
// users toward DV writers as DV-write support lands.
func TestPositionDeleteV3Warning(t *testing.T) {
	captureSlogMu.Lock()
	defer captureSlogMu.Unlock()

	t.Run("v3 warns once with table location", func(t *testing.T) {
		out := captureSlog(t, func() { runEmptyPositionDeleteWrite(t, 3) })
		assert.Equal(t, 1, strings.Count(out, "level=WARN"),
			"expected exactly one WARN record, got: %s", out)
		assert.Contains(t, out, "Parquet position-delete")
		assert.Contains(t, out, "deletion vectors")
		assert.Contains(t, out, "table=file:///warn-test",
			"warning should name the table location")
	})

	t.Run("v2 does not warn", func(t *testing.T) {
		out := captureSlog(t, func() { runEmptyPositionDeleteWrite(t, 2) })
		assert.Empty(t, out, "v2 position-delete writes should not warn, got: %s", out)
	})
}
