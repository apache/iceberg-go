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
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newDataManifest(minSeqNum int64) iceberg.ManifestFile {
	return iceberg.NewManifestFile(2, "/path/to/manifest.avro", 1000, 0, 1).
		Content(iceberg.ManifestContentData).
		SequenceNum(minSeqNum, minSeqNum).
		Build()
}

func newDeleteManifest(minSeqNum int64) iceberg.ManifestFile {
	return iceberg.NewManifestFile(2, "/path/to/manifest.avro", 1000, 0, 1).
		Content(iceberg.ManifestContentDeletes).
		SequenceNum(minSeqNum, minSeqNum).
		Build()
}

func TestMinSequenceNum(t *testing.T) {
	tests := []struct {
		name      string
		manifests []iceberg.ManifestFile
		expected  int64
	}{
		{
			name:      "empty list returns 0",
			manifests: []iceberg.ManifestFile{},
			expected:  0,
		},
		{
			name: "single data manifest returns its sequence number",
			manifests: []iceberg.ManifestFile{
				newDataManifest(5),
			},
			expected: 5,
		},
		{
			name: "multiple data manifests returns minimum",
			manifests: []iceberg.ManifestFile{
				newDataManifest(10),
				newDataManifest(3),
				newDataManifest(7),
			},
			expected: 3,
		},
		{
			name: "only delete manifests returns 0",
			manifests: []iceberg.ManifestFile{
				newDeleteManifest(5),
				newDeleteManifest(10),
			},
			expected: 0,
		},
		{
			name: "mixed manifests only considers data manifests",
			manifests: []iceberg.ManifestFile{
				newDeleteManifest(1), // should be ignored
				newDataManifest(8),
				newDeleteManifest(2), // should be ignored
				newDataManifest(5),
			},
			expected: 5,
		},
		{
			name: "data manifest with sequence 0 is handled correctly",
			manifests: []iceberg.ManifestFile{
				newDataManifest(0),
				newDataManifest(5),
			},
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := minSequenceNum(tt.manifests)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestKeyDefaultMapRaceCondition(t *testing.T) {
	var factoryCallCount atomic.Int64
	factory := func(key string) int {
		factoryCallCount.Add(1)
		runtime.Gosched() // to widen the race window

		return 42
	}

	kdm := newKeyDefaultMap(factory)

	var wg sync.WaitGroup
	start := make(chan struct{})

	numGoroutines := 1000
	for range numGoroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_ = kdm.Get("same-key")
		}()
	}

	close(start)
	wg.Wait()

	callCount := factoryCallCount.Load()
	assert.Equal(t, int64(1), callCount,
		"factory should be called exactly once per key, but was called %d times", callCount)
}

func TestBuildPartitionProjectionWithInvalidSpecID(t *testing.T) {
	schema := iceberg.NewSchema(
		1,
		iceberg.NestedField{
			ID: 1, Name: "id",
			Type: iceberg.PrimitiveTypes.Int64, Required: true,
		},
	)

	metadata, err := NewMetadata(
		schema,
		iceberg.UnpartitionedSpec,
		UnsortedSortOrder,
		"s3://test-bucket/test_table",
		iceberg.Properties{},
	)
	require.NoError(t, err)

	scan := &Scan{
		metadata:      metadata,
		rowFilter:     iceberg.AlwaysTrue{},
		caseSensitive: true,
	}

	expr, err := scan.buildPartitionProjection(999)
	require.Error(t, err)
	assert.Nil(t, expr)
	assert.ErrorIs(t, err, ErrPartitionSpecNotFound)
	assert.ErrorContains(t, err, "id 999")
}

func TestBuildManifestEvaluatorWithInvalidSpecID(t *testing.T) {
	schema := iceberg.NewSchema(
		1,
		iceberg.NestedField{
			ID: 1, Name: "id",
			Type: iceberg.PrimitiveTypes.Int64, Required: true,
		},
	)

	metadata, err := NewMetadata(
		schema,
		iceberg.UnpartitionedSpec,
		UnsortedSortOrder,
		"s3://test-bucket/test_table",
		iceberg.Properties{},
	)
	require.NoError(t, err)

	scan := &Scan{
		metadata:      metadata,
		rowFilter:     iceberg.AlwaysTrue{},
		caseSensitive: true,
	}

	scan.partitionFilters = newKeyDefaultMapWrapErr(scan.buildPartitionProjection)

	evaluator, err := scan.buildManifestEvaluator(999)
	require.Error(t, err)
	assert.Nil(t, evaluator)
	assert.ErrorIs(t, err, ErrPartitionSpecNotFound)
	assert.ErrorContains(t, err, "id 999")
}

func TestBuildPartitionEvaluatorWithInvalidSpecID(t *testing.T) {
	schema := iceberg.NewSchema(
		1,
		iceberg.NestedField{
			ID: 1, Name: "id",
			Type: iceberg.PrimitiveTypes.Int64, Required: true,
		},
	)

	metadata, err := NewMetadata(
		schema,
		iceberg.UnpartitionedSpec,
		UnsortedSortOrder,
		"s3://test-bucket/test_table",
		iceberg.Properties{},
	)
	require.NoError(t, err)

	scan := &Scan{
		metadata:      metadata,
		rowFilter:     iceberg.AlwaysTrue{},
		caseSensitive: true,
	}

	evaluator, err := scan.buildPartitionEvaluator(999)
	require.Error(t, err)
	assert.Nil(t, evaluator)
	assert.ErrorIs(t, err, ErrPartitionSpecNotFound)
	assert.ErrorContains(t, err, "id 999")
}

// TestProjectionV3PreLineageFile verifies that Projection() succeeds and returns
// _row_id and _last_updated_sequence_number as nullable (all-null-capable) fields when
// the table is v3 with next-row-id set but the data file predates row lineage (those
// columns are absent from the schema).
func TestProjectionV3PreLineageFile(t *testing.T) {
	schema := iceberg.NewSchema(
		1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String, Required: false},
	)

	metadata, err := NewMetadata(
		schema,
		iceberg.UnpartitionedSpec,
		UnsortedSortOrder,
		"s3://test-bucket/test_table",
		iceberg.Properties{"format-version": "3"},
	)
	require.NoError(t, err)
	assert.Equal(t, 3, metadata.Version(), "sanity: must be v3")

	// Request the two user columns plus both row-lineage metadata columns.
	// These metadata columns do NOT exist in the physical schema of a pre-lineage file.
	scan := &Scan{
		metadata:       metadata,
		selectedFields: []string{"id", "payload", iceberg.RowIDColumnName, iceberg.LastUpdatedSequenceNumberColumnName},
		caseSensitive:  true,
	}

	proj, err := scan.Projection()
	require.NoError(t, err, "Projection must not error for pre-lineage metadata columns")
	require.NotNil(t, proj)

	fields := proj.Fields()
	require.Len(t, fields, 4, "projected schema must contain all four requested fields")

	fieldByName := make(map[string]iceberg.NestedField, len(fields))
	for _, f := range fields {
		fieldByName[f.Name] = f
	}

	// Regular columns must survive unchanged.
	idField, ok := fieldByName["id"]
	require.True(t, ok, "id must be in projection")
	assert.Equal(t, 1, idField.ID)

	payloadField, ok := fieldByName["payload"]
	require.True(t, ok, "payload must be in projection")
	assert.Equal(t, 2, payloadField.ID)

	// Row lineage columns must be present as optional (nullable) fields — the scanner
	// will return all-nulls for any data file that was written before row lineage existed.
	rowIDField, ok := fieldByName[iceberg.RowIDColumnName]
	require.True(t, ok, "_row_id must be in projection")
	assert.Equal(t, iceberg.RowIDFieldID, rowIDField.ID, "_row_id field ID")
	assert.False(t, rowIDField.Required, "_row_id must be optional (nullable) for pre-lineage files")

	seqField, ok := fieldByName[iceberg.LastUpdatedSequenceNumberColumnName]
	require.True(t, ok, "_last_updated_sequence_number must be in projection")
	assert.Equal(t, iceberg.LastUpdatedSequenceNumberFieldID, seqField.ID, "_last_updated_sequence_number field ID")
	assert.False(t, seqField.Required, "_last_updated_sequence_number must be optional (nullable) for pre-lineage files")
}

func TestProjectionV3PreLineageFileCaseSensitive(t *testing.T) {
	schema := iceberg.NewSchema(
		1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String, Required: false},
	)

	metadata, err := NewMetadata(
		schema,
		iceberg.UnpartitionedSpec,
		UnsortedSortOrder,
		"s3://test-bucket/test_table",
		iceberg.Properties{"format-version": "3"},
	)
	require.NoError(t, err)
	assert.Equal(t, 3, metadata.Version(), "sanity: must be v3")

	scan := &Scan{
		metadata:       metadata,
		selectedFields: []string{"id", "payload", "_Row_Id"},
		caseSensitive:  true,
	}

	_, err = scan.Projection()
	require.Error(t, err)
	require.ErrorContains(t, err, "could not find column _Row_Id")
}

func TestProjectionV3PreLineageFileCaseInsensitive(t *testing.T) {
	schema := iceberg.NewSchema(
		1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String, Required: false},
	)

	metadata, err := NewMetadata(
		schema,
		iceberg.UnpartitionedSpec,
		UnsortedSortOrder,
		"s3://test-bucket/test_table",
		iceberg.Properties{"format-version": "3"},
	)
	require.NoError(t, err)
	assert.Equal(t, 3, metadata.Version(), "sanity: must be v3")

	scan := &Scan{
		metadata:       metadata,
		selectedFields: []string{"id", "payload", "_Row_Id", "_Last_Updated_SEQUENCE_number"},
		caseSensitive:  false,
	}

	proj, err := scan.Projection()
	require.NoError(t, err)
	require.NotNil(t, proj)

	fields := proj.Fields()
	require.Len(t, fields, 4, "projected schema must contain all four requested fields")

	fieldByName := make(map[string]iceberg.NestedField, len(fields))
	for _, f := range fields {
		fieldByName[f.Name] = f
	}

	idField, ok := fieldByName["id"]
	require.True(t, ok, "id must be in projection")
	assert.Equal(t, 1, idField.ID)

	payloadField, ok := fieldByName["payload"]
	require.True(t, ok, "payload must be in projection")
	assert.Equal(t, 2, payloadField.ID)

	rowIDField, ok := fieldByName[iceberg.RowIDColumnName]
	require.True(t, ok, "_row_id must be in projection")
	assert.Equal(t, iceberg.RowIDFieldID, rowIDField.ID, "_row_id field ID")
	assert.False(t, rowIDField.Required, "_row_id must be optional (nullable) for pre-lineage files")

	seqField, ok := fieldByName[iceberg.LastUpdatedSequenceNumberColumnName]
	require.True(t, ok, "_last_updated_sequence_number must be in projection")
	assert.Equal(t, iceberg.LastUpdatedSequenceNumberFieldID, seqField.ID, "_last_updated_sequence_number field ID")
	assert.False(t, seqField.Required, "_last_updated_sequence_number must be optional (nullable) for pre-lineage files")
}

// TestProjectionV2RowLineage asserts that requesting row-lineage metadata columns on a v1 or v2
// table does not use the v3-only synthesis path
func TestProjectionV2RowLineage(t *testing.T) {
	schema := iceberg.NewSchema(
		1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String, Required: false},
	)

	for _, tc := range []struct {
		name string
		ver  int
	}{
		{name: "v1", ver: 1},
		{name: "v2", ver: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			metadata, err := NewMetadata(
				schema,
				iceberg.UnpartitionedSpec,
				UnsortedSortOrder,
				"s3://test-bucket/test_table",
				iceberg.Properties{PropertyFormatVersion: strconv.Itoa(tc.ver)},
			)
			require.NoError(t, err)
			assert.Equal(t, tc.ver, metadata.Version(), "sanity: metadata format version")

			scan := &Scan{
				metadata:       metadata,
				selectedFields: []string{"id", iceberg.RowIDColumnName},
				caseSensitive:  true,
			}

			_, err = scan.Projection()
			require.Error(t, err)
			assert.ErrorIs(t, err, iceberg.ErrInvalidSchema)
			assert.ErrorContains(t, err, iceberg.RowIDColumnName)
		})
	}
}

// TestProjectionV3SchemaWithRowIDOnly covers a v3 table whose schema
// already declares _row_id (reserved field id) but does not declare _last_updated_sequence_number.
func TestProjectionV3SchemaWithRowIDOnly(t *testing.T) {
	schema := iceberg.NewSchema(
		1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String, Required: false},
		iceberg.RowID(),
	)

	metadata, err := NewMetadata(
		schema,
		iceberg.UnpartitionedSpec,
		UnsortedSortOrder,
		"s3://test-bucket/test_table",
		iceberg.Properties{"format-version": "3"},
	)
	require.NoError(t, err)
	assert.Equal(t, 3, metadata.Version(), "sanity: must be v3")

	scan := &Scan{
		metadata: metadata,
		selectedFields: []string{
			"id", "payload",
			iceberg.RowIDColumnName,
			iceberg.LastUpdatedSequenceNumberColumnName,
		},
		caseSensitive: true,
	}

	var proj *iceberg.Schema
	require.NotPanics(t, func() {
		var perr error
		proj, perr = scan.Projection()
		require.NoError(t, perr)
	})
	require.NotNil(t, proj)

	fields := proj.Fields()
	require.Len(t, fields, 4, "projection must include id, payload, _row_id, _last_updated_sequence_number")

	fieldByName := make(map[string]iceberg.NestedField, len(fields))
	idsSeen := make(map[int]string, len(fields))
	for _, f := range fields {
		if prev, dup := idsSeen[f.ID]; dup {
			t.Fatalf("duplicate field id %d: %q and %q", f.ID, prev, f.Name)
		}
		idsSeen[f.ID] = f.Name
		fieldByName[f.Name] = f
	}

	idField, ok := fieldByName["id"]
	require.True(t, ok)
	assert.Equal(t, 1, idField.ID)

	payloadField, ok := fieldByName["payload"]
	require.True(t, ok)
	assert.Equal(t, 2, payloadField.ID)

	rowIDField, ok := fieldByName[iceberg.RowIDColumnName]
	require.True(t, ok)
	assert.Equal(t, iceberg.RowIDFieldID, rowIDField.ID)
	assert.False(t, rowIDField.Required)

	seqField, ok := fieldByName[iceberg.LastUpdatedSequenceNumberColumnName]
	require.True(t, ok)
	assert.Equal(t, iceberg.LastUpdatedSequenceNumberFieldID, seqField.ID)
	assert.False(t, seqField.Required)
}

func TestProjectionV3SchemaWithLastUpdatedSequenceNumberOnly(t *testing.T) {
	schema := iceberg.NewSchema(
		1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String, Required: false},
		iceberg.LastUpdatedSequenceNumber(),
	)

	metadata, err := NewMetadata(
		schema,
		iceberg.UnpartitionedSpec,
		UnsortedSortOrder,
		"s3://test-bucket/test_table",
		iceberg.Properties{"format-version": "3"},
	)
	require.NoError(t, err)
	assert.Equal(t, 3, metadata.Version(), "sanity: must be v3")

	scan := &Scan{
		metadata: metadata,
		selectedFields: []string{
			"id", "payload",
			iceberg.RowIDColumnName,
			iceberg.LastUpdatedSequenceNumberColumnName,
		},
		caseSensitive: true,
	}

	var proj *iceberg.Schema
	require.NotPanics(t, func() {
		var perr error
		proj, perr = scan.Projection()
		require.NoError(t, perr)
	})
	require.NotNil(t, proj)

	fields := proj.Fields()
	require.Len(t, fields, 4, "projection must include id, payload, _row_id, _last_updated_sequence_number")

	fieldByName := make(map[string]iceberg.NestedField, len(fields))
	idsSeen := make(map[int]string, len(fields))
	for _, f := range fields {
		if prev, dup := idsSeen[f.ID]; dup {
			t.Fatalf("duplicate field id %d: %q and %q", f.ID, prev, f.Name)
		}
		idsSeen[f.ID] = f.Name
		fieldByName[f.Name] = f
	}

	idField, ok := fieldByName["id"]
	require.True(t, ok)
	assert.Equal(t, 1, idField.ID)

	payloadField, ok := fieldByName["payload"]
	require.True(t, ok)
	assert.Equal(t, 2, payloadField.ID)

	rowIDField, ok := fieldByName[iceberg.RowIDColumnName]
	require.True(t, ok)
	assert.Equal(t, iceberg.RowIDFieldID, rowIDField.ID)
	assert.False(t, rowIDField.Required)

	seqField, ok := fieldByName[iceberg.LastUpdatedSequenceNumberColumnName]
	require.True(t, ok)
	assert.Equal(t, iceberg.LastUpdatedSequenceNumberFieldID, seqField.ID)
	assert.False(t, seqField.Required)
}

// TestSynthesizeRowLineageColumns verifies that _row_id and _last_updated_sequence_number
// are filled from task constants when those columns are present and null.
func TestSynthesizeRowLineageColumns(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	ctx := compute.WithAllocator(t.Context(), mem)
	defer mem.AssertSize(t, 0)
	firstRowID := int64(1000)
	dataSeqNum := int64(5)
	task := FileScanTask{FirstRowID: &firstRowID, DataSequenceNumber: &dataSeqNum}
	rowOffset := int64(0)

	// Build a batch with a data column plus _row_id and _last_updated_sequence_number (all nulls).
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "x", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: iceberg.RowIDColumnName, Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: iceberg.LastUpdatedSequenceNumberColumnName, Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		},
		nil,
	)
	const nrows = 3
	xBldr := array.NewInt64Builder(mem)
	defer xBldr.Release()
	xBldr.AppendValues([]int64{1, 2, 3}, nil)
	rowIDBldr := array.NewInt64Builder(mem)
	defer rowIDBldr.Release()
	rowIDBldr.AppendNulls(nrows)
	seqBldr := array.NewInt64Builder(mem)
	defer seqBldr.Release()
	seqBldr.AppendNulls(nrows)

	xArr := xBldr.NewArray()
	rowIDArr := rowIDBldr.NewArray()
	seqArr := seqBldr.NewArray()
	batch := array.NewRecordBatch(schema, []arrow.Array{xArr, rowIDArr, seqArr}, nrows)
	xArr.Release()
	rowIDArr.Release()
	seqArr.Release()
	defer batch.Release()

	out, err := synthesizeRowLineageColumns(ctx, &rowOffset, task, batch)
	require.NoError(t, err)
	defer out.Release()

	// _row_id should be 1000, 1001, 1002
	rowIDCol := out.Column(1).(*array.Int64)
	require.Equal(t, nrows, rowIDCol.Len())
	for i := 0; i < nrows; i++ {
		assert.False(t, rowIDCol.IsNull(i), "row %d", i)
		assert.EqualValues(t, 1000+int64(i), rowIDCol.Value(i), "row %d", i)
	}
	// _last_updated_sequence_number should be 5 for all
	seqCol := out.Column(2).(*array.Int64)
	for i := 0; i < nrows; i++ {
		assert.False(t, seqCol.IsNull(i), "row %d", i)
		assert.EqualValues(t, 5, seqCol.Value(i), "row %d", i)
	}
	assert.EqualValues(t, 3, rowOffset)
}
