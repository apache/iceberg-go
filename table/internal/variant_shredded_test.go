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

package internal_test

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/arrow-go/v18/parquet/variant"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table/internal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var shreddedVariantCases = []struct {
	name        string
	parquetFile string
	variantFile string
}{
	{"boolean primitive (case-004)", "case-004.parquet", "case-004_row-0.variant.bin"},
	{"int8 primitive (case-006)", "case-006.parquet", "case-006_row-0.variant.bin"},
	{"int64 primitive (case-012)", "case-012.parquet", "case-012_row-0.variant.bin"},
	{"object with null + empty string (case-046)", "case-046.parquet", "case-046_row-0.variant.bin"},
	{"nested shredded object (case-044)", "case-044.parquet", "case-044_row-0.variant.bin"},
	{"partially shredded object — typed_value+residual merge (case-134)", "case-134.parquet", "case-134_row-0.variant.bin"},
}

func TestReassembleShreddedVariant(t *testing.T) {
	for _, tc := range shreddedVariantCases {
		t.Run(tc.name, func(t *testing.T) {
			fixture := openVariantArray(t, fixturePath(tc.parquetFile))
			defer fixture.Release()

			meta := fixture.Metadata().Value(0)
			var value []byte
			if u := fixture.UntypedValues(); u != nil && !u.IsNull(0) {
				value = u.Value(0)
			}
			got, err := internal.ReassembleShreddedVariant(meta, value, fixture.Shredded(), 0)
			require.NoError(t, err)

			expected := readVariantBin(t, fixturePath(tc.variantFile))
			assertVariantStructurallyEqual(t, expected, got)
		})
	}
}

func TestShreddedVariantInvisibleToScanner(t *testing.T) {
	ctx := context.Background()
	fm := internal.GetFileFormat(iceberg.ParquetFile)

	for _, tc := range shreddedVariantCases {
		t.Run(tc.name, func(t *testing.T) {
			path := fixturePath(tc.parquetFile)
			rdr, err := fm.Open(ctx, io.LocalFS{}, path)
			require.NoError(t, err)
			defer rdr.Close()

			cols := allColumnIndices(t, path)
			recs, err := rdr.GetRecords(ctx, cols, nil)
			require.NoError(t, err)
			defer recs.Release()

			require.True(t, recs.Next(), "expected at least one record")
			batch := recs.RecordBatch()

			variantArr := lastVariantColumn(t, batch)
			assert.False(t, variantArr.IsShredded(),
				"scanner should see a non-shredded variant column")

			got, err := variantArr.Value(0)
			require.NoError(t, err)

			expected := readVariantBin(t, fixturePath(tc.variantFile))
			assertVariantStructurallyEqual(t, expected, got)
		})
	}
}

// openVariantArray reads a fixture via raw pqarrow and returns the last column
// as a VariantArray. Fixtures use "id"+"var" so the variant is always last.
// Caller owns Release.
func openVariantArray(t *testing.T, path string) *extensions.VariantArray {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err, "open %s", path)
	t.Cleanup(func() { _ = f.Close() })

	reader, err := file.NewParquetReader(f)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Close() })

	arrReader, err := pqarrow.NewFileReader(reader, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	require.NoError(t, err)

	tbl, err := arrReader.ReadTable(context.Background())
	require.NoError(t, err)
	t.Cleanup(tbl.Release)

	col := tbl.Column(int(tbl.NumCols()) - 1)
	chunk := col.Data().Chunk(0)
	v, ok := chunk.(*extensions.VariantArray)
	require.True(t, ok, "expected VariantArray, got %T", chunk)
	v.Retain()

	return v
}

func lastVariantColumn(t *testing.T, rec arrow.RecordBatch) *extensions.VariantArray {
	t.Helper()
	col := rec.Column(int(rec.NumCols()) - 1)
	v, ok := col.(*extensions.VariantArray)
	require.True(t, ok, "expected VariantArray, got %T", col)

	return v
}

func allColumnIndices(t *testing.T, path string) []int {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err, "open %s", path)
	defer f.Close()

	r, err := file.NewParquetReader(f)
	require.NoError(t, err)
	defer r.Close()

	n := r.MetaData().Schema.NumColumns()
	out := make([]int, n)
	for i := 0; i < n; i++ {
		out[i] = i
	}

	return out
}

// readVariantBin parses a .variant.bin file (metadata bytes immediately
// followed by value bytes) by reading the metadata header to find the split.
func readVariantBin(t *testing.T, path string) variant.Value {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err, "read %s", path)
	require.NotEmpty(t, data)

	hdr := data[0]
	offsetSize := int(1 + ((hdr & 0b11000000) >> 6))
	dictSize := int(readVariantUnsigned(data[1 : 1+offsetSize]))
	offsetListOffset := 1 + offsetSize
	dataOffset := offsetListOffset + ((1 + dictSize) * offsetSize)

	idx := offsetListOffset + (offsetSize * dictSize)
	endOffset := dataOffset + int(readVariantUnsigned(data[idx:idx+offsetSize]))
	v, err := variant.New(data[:endOffset], data[endOffset:])
	require.NoError(t, err, "parse %s", path)

	return v
}

func readVariantUnsigned(b []byte) uint64 {
	var buf [8]byte
	copy(buf[:], b)

	return binary.LittleEndian.Uint64(buf[:])
}

// assertVariantStructurallyEqual walks object/array shape and compares
// primitive payloads. Bytes() is avoided because semantically-equal
// values may re-encode to different byte sequences.
func assertVariantStructurallyEqual(t *testing.T, expected, actual variant.Value) {
	t.Helper()
	switch expected.BasicType() {
	case variant.BasicObject:
		exp := expected.Value().(variant.ObjectValue)
		act, ok := actual.Value().(variant.ObjectValue)
		require.True(t, ok, "expected object, got %T", actual.Value())
		require.Equal(t, exp.NumElements(), act.NumElements())
		for i := range exp.NumElements() {
			expField, err := exp.FieldAt(i)
			require.NoError(t, err)
			actField, err := act.ValueByKey(expField.Key)
			require.NoError(t, err, "key %q missing in actual", expField.Key)
			assertVariantStructurallyEqual(t, expField.Value, actField.Value)
		}
	case variant.BasicArray:
		exp := expected.Value().(variant.ArrayValue)
		act, ok := actual.Value().(variant.ArrayValue)
		require.True(t, ok, "expected array")
		require.Equal(t, exp.Len(), act.Len())
		for i := range exp.Len() {
			expVal, err := exp.Value(i)
			require.NoError(t, err)
			actVal, err := act.Value(i)
			require.NoError(t, err)
			assertVariantStructurallyEqual(t, expVal, actVal)
		}
	default:
		ev, av := expected.Value(), actual.Value()
		// Integer types may differ in width between the Java reference encoding
		// and what arrow-go produces from the Parquet typed_value column type.
		// Compare by numeric value in that case.
		if ei := toInt64(ev); ei != nil {
			ai := toInt64(av)
			require.NotNil(t, ai, "primitive mismatch: expected integer %v (%T), got %v (%T)", ev, ev, av, av)
			assert.Equal(t, *ei, *ai, "integer value mismatch")
		} else {
			assert.Equal(t, ev, av,
				"primitive mismatch: expected %v (%T), got %v (%T)", ev, ev, av, av)
		}
	}
}

func fixturePath(name string) string {
	return filepath.Join("testdata", "shredded_variant", name)
}

func toInt64(v any) *int64 {
	var i int64
	switch x := v.(type) {
	case int8:
		i = int64(x)
	case int16:
		i = int64(x)
	case int32:
		i = int64(x)
	case int64:
		i = x
	default:
		return nil
	}

	return &i
}
