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
	"math"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/variant"
	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

func normalizationRecord(t *testing.T, fields []arrow.Field, rows string, op ChangelogOperation) arrow.RecordBatch {
	t.Helper()
	data, _, err := array.RecordFromJSON(memory.DefaultAllocator, arrow.NewSchema(fields, nil), strings.NewReader(rows))
	require.NoError(t, err)
	defer data.Release()
	schema := arrow.NewSchema(append(fields, changelogArrowFields(false)...), nil)
	file := changelogTestDataFile(t, "data.parquet", iceberg.EntryContentData, iceberg.ParquetFile)
	var task ChangelogScanTask
	if op == ChangelogOpInsert {
		task, err = NewAddedRowsScanTask(file, nil, 0, 7)
	} else {
		task, err = NewDeletedDataFileScanTask(file, nil, 0, 7)
	}
	require.NoError(t, err)
	out, err := appendChangelogColumns(t.Context(), schema, false, data, task)
	require.NoError(t, err)
	t.Cleanup(out.Release)

	return out
}

func TestChangelogNormalizationCarryoverMultiplicity(t *testing.T) {
	fields := []arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}
	for _, tt := range []struct {
		name             string
		deletes, inserts int
		want             []ChangelogOperation
	}{
		{"one pair", 1, 1, nil},
		{"extra delete", 2, 1, []ChangelogOperation{ChangelogOpDelete}},
		{"extra insert", 1, 2, []ChangelogOperation{ChangelogOpInsert}},
		{"two pairs", 2, 2, nil},
		{"pure deletes", 2, 0, []ChangelogOperation{ChangelogOpDelete, ChangelogOpDelete}},
		{"pure inserts", 0, 2, []ChangelogOperation{ChangelogOpInsert, ChangelogOpInsert}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for _, insertsFirst := range []bool{false, true} {
				buffer := changelogSnapshotBuffer{opts: ChangelogNormalizationOptions{MaxBufferedBytes: 1 << 20, MaxBufferedRows: 100}}
				defer buffer.release()
				add := func(op ChangelogOperation, count int) {
					for range count {
						rec := normalizationRecord(t, fields, `[{"id":1}]`, op)
						require.NoError(t, buffer.add(t.Context(), rec, []int{0}))
					}
				}
				if insertsFirst {
					add(ChangelogOpInsert, tt.inserts)
					add(ChangelogOpDelete, tt.deletes)
				} else {
					add(ChangelogOpDelete, tt.deletes)
					add(ChangelogOpInsert, tt.inserts)
				}
				var got []ChangelogOperation
				for _, row := range buffer.rows {
					if row.operation != "" {
						got = append(got, row.operation)
					}
				}
				require.Equal(t, tt.want, got)
			}
		})
	}
}

func TestChangelogNormalizationRejectsAmbiguousUpdateImages(t *testing.T) {
	fields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "data", Type: arrow.BinaryTypes.String},
	}
	for _, tt := range []struct {
		name, deletes, inserts string
	}{
		{"two deletes", `[{"id":1,"data":"a"},{"id":1,"data":"b"}]`, `[]`},
		{"two inserts", `[]`, `[{"id":1,"data":"a"},{"id":1,"data":"b"}]`},
		{"one delete two inserts", `[{"id":1,"data":"a"}]`, `[{"id":1,"data":"b"},{"id":1,"data":"c"}]`},
		{"two deletes two inserts", `[{"id":1,"data":"a"},{"id":1,"data":"b"}]`, `[{"id":1,"data":"c"},{"id":1,"data":"d"}]`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			buffer := changelogSnapshotBuffer{opts: ChangelogNormalizationOptions{MaxBufferedBytes: 1 << 20, MaxBufferedRows: 100}}
			defer buffer.release()
			require.NoError(t, buffer.add(t.Context(), normalizationRecord(t, fields, tt.deletes, ChangelogOpDelete), []int{0, 1}))
			require.NoError(t, buffer.add(t.Context(), normalizationRecord(t, fields, tt.inserts, ChangelogOpInsert), []int{0, 1}))
			err := buffer.computeUpdates(t.Context(), []changelogIdentifier{{id: 1, name: "id", ref: arrowFieldRef{path: []int{0}}}})
			require.ErrorIs(t, err, ErrInvalidOperation)
			require.ErrorContains(t, err, "same identifier")
		})
	}
}

func TestChangelogNormalizationRemovesCarryoversBeforePairing(t *testing.T) {
	fields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "data", Type: arrow.BinaryTypes.String},
	}
	buffer := changelogSnapshotBuffer{opts: ChangelogNormalizationOptions{MaxBufferedBytes: 1 << 20, MaxBufferedRows: 100}}
	defer buffer.release()
	require.NoError(t, buffer.add(t.Context(), normalizationRecord(t, fields,
		`[{"id":1,"data":"same"},{"id":1,"data":"old"}]`, ChangelogOpDelete), []int{0, 1}))
	require.NoError(t, buffer.add(t.Context(), normalizationRecord(t, fields,
		`[{"id":1,"data":"new"},{"id":1,"data":"same"}]`, ChangelogOpInsert), []int{0, 1}))
	require.NoError(t, buffer.computeUpdates(t.Context(), []changelogIdentifier{{id: 1, name: "id", ref: arrowFieldRef{path: []int{0}}}}))
	require.Equal(t, ChangelogOperation(""), buffer.rows[0].operation)
	require.Equal(t, ChangelogOpUpdateBefore, buffer.rows[1].operation)
	require.Equal(t, ChangelogOpUpdateAfter, buffer.rows[2].operation)
	require.Equal(t, ChangelogOperation(""), buffer.rows[3].operation)
	require.Equal(t, 2, buffer.rows[1].partner)
}

func TestChangelogNormalizationKeyValues(t *testing.T) {
	for _, tt := range []struct {
		name  string
		type_ arrow.DataType
		rows  string
		same  bool
	}{
		{"null and empty string", arrow.BinaryTypes.String, `[{"v":null},{"v":""}]`, false},
		{"null and empty binary", arrow.BinaryTypes.Binary, `[{"v":null},{"v":""}]`, false},
		{"binary content", arrow.BinaryTypes.Binary, `[{"v":"YWI="},{"v":"YWI="}]`, true},
		{"list binary content", arrow.ListOf(arrow.BinaryTypes.Binary), `[{"v":["YWI=",null]},{"v":["YWI=",null]}]`, true},
		{"nested binary lists", arrow.ListOf(arrow.ListOf(arrow.BinaryTypes.Binary)), `[{"v":[["YWI="],null,[]]},{"v":[["YWI="],null,[]]}]`, true},
		{"map entry order", arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int64), `[{"v":[{"key":"a","value":1},{"key":"b","value":2}]},{"v":[{"key":"b","value":2},{"key":"a","value":1}]}]`, true},
		{"map values", arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int64), `[{"v":[{"key":"a","value":1}]},{"v":[{"key":"a","value":2}]}]`, false},
		{"list order", arrow.ListOf(arrow.PrimitiveTypes.Int64), `[{"v":[1,2]},{"v":[2,1]}]`, false},
		{"list length", arrow.ListOf(arrow.PrimitiveTypes.Int64), `[{"v":[1]},{"v":[1,null]}]`, false},
		{"null and empty list", arrow.ListOf(arrow.PrimitiveTypes.Int64), `[{"v":null},{"v":[]}]`, false},
		{"null and null child struct", arrow.StructOf(arrow.Field{Name: "x", Type: arrow.BinaryTypes.String}), `[{"v":null},{"v":{"x":null}}]`, false},
		{"struct binary content", arrow.StructOf(arrow.Field{Name: "x", Type: arrow.BinaryTypes.Binary}), `[{"v":{"x":"YWI="}},{"v":{"x":"YWI="}}]`, true},
		{"decimal equality", &arrow.Decimal128Type{Precision: 10, Scale: 2}, `[{"v":"12.30"},{"v":"12.30"}]`, true},
		{"decimal inequality", &arrow.Decimal128Type{Precision: 10, Scale: 2}, `[{"v":"12.30"},{"v":"12.31"}]`, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rec, _, err := array.RecordFromJSON(memory.DefaultAllocator,
				arrow.NewSchema([]arrow.Field{{Name: "v", Type: tt.type_}}, nil), strings.NewReader(tt.rows))
			require.NoError(t, err)
			defer rec.Release()
			var first, second bytes.Buffer
			require.NoError(t, encodeChangelogValue(t.Context(), &first, rec.Column(0), 0, 1<<20))
			require.NoError(t, encodeChangelogValue(t.Context(), &second, rec.Column(0), 1, 1<<20))
			require.Equal(t, tt.same, bytes.Equal(first.Bytes(), second.Bytes()))
		})
	}
}

func TestChangelogNormalizationFloatKeys(t *testing.T) {
	builder := array.NewFloat64Builder(memory.DefaultAllocator)
	defer builder.Release()
	builder.AppendValues([]float64{
		math.Float64frombits(0x7ff0000000000001), math.Float64frombits(0xfff8000000000002),
		0, math.Copysign(0, -1),
	}, nil)
	values := builder.NewArray()
	defer values.Release()
	list := arrow.ListOf(arrow.PrimitiveTypes.Float64)
	lists := array.NewListBuilder(memory.DefaultAllocator, arrow.PrimitiveTypes.Float64)
	defer lists.Release()
	for i := range values.Len() {
		lists.Append(true)
		lists.ValueBuilder().(*array.Float64Builder).Append(values.(*array.Float64).Value(i))
	}
	nested := lists.NewArray()
	defer nested.Release()
	require.True(t, arrow.TypeEqual(list, nested.DataType()))
	for _, column := range []arrow.Array{values, nested} {
		var keys [4]bytes.Buffer
		for i := range keys {
			require.NoError(t, encodeChangelogValue(t.Context(), &keys[i], column, i, 1<<20))
		}
		require.Equal(t, keys[0].String(), keys[1].String(), "all NaN payloads identify the same value")
		require.NotEqual(t, keys[2].String(), keys[3].String(), "signed zero remains distinct")
	}
}

func TestChangelogNormalizationDictionaryKeysUseValues(t *testing.T) {
	dictType := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.BinaryTypes.String}
	first, _, err := array.FromJSON(memory.DefaultAllocator, dictType, strings.NewReader(`["a","b"]`))
	require.NoError(t, err)
	defer first.Release()
	second, _, err := array.FromJSON(memory.DefaultAllocator, dictType, strings.NewReader(`["b","a"]`))
	require.NoError(t, err)
	defer second.Release()
	var a, b bytes.Buffer
	require.NoError(t, encodeChangelogValue(t.Context(), &a, first, 0, 1<<20))
	require.NoError(t, encodeChangelogValue(t.Context(), &b, second, 1, 1<<20))
	require.Equal(t, a.String(), b.String())
}

func TestChangelogNormalizationAccountsForRetainedParentBuffers(t *testing.T) {
	rec := normalizationRecord(t, []arrow.Field{{Name: "v", Type: arrow.BinaryTypes.String}},
		`[{"v":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},{"v":"b"}]`, ChangelogOpDelete)
	part := rec.NewSlice(1, 2)
	defer part.Release()
	require.Equal(t, changelogRetainedBytes(rec), changelogRetainedBytes(part))
	buffer := changelogSnapshotBuffer{opts: ChangelogNormalizationOptions{MaxBufferedBytes: changelogRetainedBytes(part) - 1, MaxBufferedRows: 10}}
	defer buffer.release()
	require.ErrorIs(t, buffer.add(t.Context(), part, []int{0}), ErrChangelogBufferLimit)
	require.Empty(t, buffer.batches)
}

func TestChangelogNormalizationCancelledBuffer(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	rec := normalizationRecord(t, []arrow.Field{{Name: "v", Type: arrow.BinaryTypes.String}}, `[{"v":"a"}]`, ChangelogOpInsert)
	buffer := changelogSnapshotBuffer{opts: ChangelogNormalizationOptions{MaxBufferedBytes: 1 << 20, MaxBufferedRows: 10}}
	defer buffer.release()
	require.ErrorIs(t, buffer.add(ctx, rec, []int{0}), context.Canceled)
}

func TestChangelogNormalizationBoundsKeyExpansion(t *testing.T) {
	values, _, err := array.FromJSON(memory.DefaultAllocator,
		arrow.ListOf(arrow.ListOf(arrow.BinaryTypes.String)),
		strings.NewReader(`[[["a long string", "another string"], ["third string"]]]`))
	require.NoError(t, err)
	defer values.Release()
	var key bytes.Buffer
	require.ErrorIs(t, encodeChangelogValue(t.Context(), &key, values, 0, 24), ErrChangelogBufferLimit)
	require.LessOrEqual(t, key.Len(), 24)
}

func TestChangelogNormalizationPreservesVariantNullStorage(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	builder := extensions.NewVariantBuilder(mem, extensions.NewDefaultVariantType())
	defer builder.Release()
	builder.AppendNull()
	var variantBuilder variant.Builder
	require.NoError(t, variantBuilder.Append(nil))
	encoded, err := variantBuilder.Build()
	require.NoError(t, err)
	builder.Append(encoded)
	values := builder.NewArray()
	defer values.Release()
	var physical, present bytes.Buffer
	require.NoError(t, encodeChangelogValue(t.Context(), &physical, values, 0, 1<<20))
	require.NoError(t, encodeChangelogValue(t.Context(), &present, values, 1, 1<<20))
	require.NotEqual(t, physical.String(), present.String())
	changeBuilder := array.NewStringBuilder(mem)
	defer changeBuilder.Release()
	changeBuilder.AppendValues([]string{"DELETE", "INSERT"}, nil)
	change := changeBuilder.NewStringArray()
	defer change.Release()
	ordinalBuilder := array.NewInt32Builder(mem)
	defer ordinalBuilder.Release()
	ordinalBuilder.AppendValues([]int32{0, 0}, nil)
	ordinal := ordinalBuilder.NewInt32Array()
	defer ordinal.Release()
	snapshotBuilder := array.NewInt64Builder(mem)
	defer snapshotBuilder.Release()
	snapshotBuilder.AppendValues([]int64{7, 7}, nil)
	snapshot := snapshotBuilder.NewInt64Array()
	defer snapshot.Release()
	schema := arrow.NewSchema(append([]arrow.Field{{Name: "payload", Type: values.DataType(), Nullable: true}}, changelogArrowFields(false)...), nil)
	input := array.NewRecordBatch(schema, []arrow.Array{values, change, ordinal, snapshot}, 2)
	defer input.Release()
	ctx := compute.WithAllocator(t.Context(), mem)
	out, err := gatherNormalizedChangelogRows(ctx, []arrow.RecordBatch{input}, []rowRef{newRowRef(0, 1), newRowRef(0, 0)},
		[]ChangelogOperation{ChangelogOpUpdateAfter, ChangelogOpUpdateBefore})
	require.NoError(t, err)
	defer out.Release()
	storage := out.Column(0).(array.ExtensionArray).Storage()
	require.False(t, storage.IsNull(0), "present Variant null must not become physical NULL")
	require.True(t, storage.IsNull(1))
	require.Equal(t, string(ChangelogOpUpdateAfter), out.Column(1).(*array.String).Value(0))
	require.Equal(t, string(ChangelogOpUpdateBefore), out.Column(1).(*array.String).Value(1))

	multi, err := gatherNormalizedChangelogRows(ctx, []arrow.RecordBatch{input, input},
		[]rowRef{newRowRef(0, 1), newRowRef(1, 0), newRowRef(0, 0)},
		[]ChangelogOperation{ChangelogOpUpdateBefore, ChangelogOpUpdateAfter, ChangelogOpDelete})
	require.NoError(t, err)
	defer multi.Release()
	multiStorage := multi.Column(0).(array.ExtensionArray).Storage()
	require.False(t, multiStorage.IsNull(0))
	require.True(t, multiStorage.IsNull(1))
	require.True(t, multiStorage.IsNull(2))
	require.Equal(t, string(ChangelogOpUpdateBefore), multi.Column(1).(*array.String).Value(0))
	require.Equal(t, string(ChangelogOpUpdateAfter), multi.Column(1).(*array.String).Value(1))
	require.Equal(t, string(ChangelogOpDelete), multi.Column(1).(*array.String).Value(2))
}

func TestGatherNormalizedChangelogRows(t *testing.T) {
	fields := []arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}
	first := normalizationRecord(t, fields, `[{"id":1},{"id":2}]`, ChangelogOpDelete)
	second := normalizationRecord(t, fields, `[{"id":3}]`, ChangelogOpInsert)
	batches := make([]arrow.RecordBatch, 10000)
	batches[12], batches[9999] = first, second
	refs := []rowRef{newRowRef(9999, 0), newRowRef(12, 1), newRowRef(12, 0)}
	ops := []ChangelogOperation{ChangelogOpUpdateAfter, ChangelogOpUpdateBefore, ChangelogOpDelete}
	out, err := gatherNormalizedChangelogRows(t.Context(), batches, refs, ops)
	require.NoError(t, err)
	defer out.Release()
	require.Equal(t, []int64{3, 2, 1}, out.Column(0).(*array.Int64).Int64Values())
	change := out.Column(1).(*array.String)
	for i, op := range ops {
		require.Equal(t, string(op), change.Value(i))
	}
	_, err = gatherNormalizedChangelogRows(t.Context(), batches, nil, nil)
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	_, err = gatherNormalizedChangelogRows(t.Context(), batches, refs, ops[:1])
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	_, err = gatherNormalizedChangelogRows(t.Context(), batches, []rowRef{newRowRef(0, 0)}, ops[:1])
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	_, err = gatherNormalizedChangelogRows(t.Context(), batches, []rowRef{newRowRef(12, 2)}, ops[:1])
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = gatherNormalizedChangelogRows(ctx, batches, refs, ops)
	require.ErrorIs(t, err, context.Canceled)
}

func TestChangelogNormalizationReusesByteBudgetForIdentifierMatching(t *testing.T) {
	fields := []arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "data", Type: arrow.BinaryTypes.String}}
	buffer := changelogSnapshotBuffer{opts: ChangelogNormalizationOptions{MaxBufferedBytes: 1 << 20, MaxBufferedRows: 10}}
	defer buffer.release()
	require.NoError(t, buffer.add(t.Context(), normalizationRecord(t, fields, `[{"id":1,"data":"old value"}]`, ChangelogOpDelete), []int{0, 1}))
	require.NoError(t, buffer.add(t.Context(), normalizationRecord(t, fields, `[{"id":1,"data":"new value"}]`, ChangelogOpInsert), []int{0, 1}))
	// This budget fits full-row matching and identifier matching separately,
	// without reserving both sets of keys at the same time.
	buffer.opts.MaxBufferedBytes = buffer.usedBytes
	require.NoError(t, buffer.computeUpdates(t.Context(), []changelogIdentifier{{id: 1, name: "id", ref: arrowFieldRef{path: []int{0}}}}))
	require.Equal(t, ChangelogOpUpdateBefore, buffer.rows[0].operation)
	require.Equal(t, ChangelogOpUpdateAfter, buffer.rows[1].operation)
}
