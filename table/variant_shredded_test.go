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

package table_test

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/arrow-go/v18/parquet/variant"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/sql"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun/driver/sqliteshim"
)

const shreddedFixtureDir = "testdata/shredded_variant"

func TestShreddedVariantTableScan(t *testing.T) {
	location := filepath.ToSlash(t.TempDir())

	dataDir := filepath.Join(location, "data")
	require.NoError(t, os.MkdirAll(dataDir, 0o755))
	dataPath := filepath.Join(dataDir, "shredded.parquet")
	const nData = 5
	writeShreddedVariantFile(t, dataPath, 1, nData)

	iceSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "payload", Type: iceberg.VariantType{}, Required: false},
	)

	meta, err := table.NewMetadata(iceSchema, iceberg.UnpartitionedSpec,
		table.UnsortedSortOrder, location,
		iceberg.Properties{table.PropertyFormatVersion: "3"})
	require.NoError(t, err)

	tbl := table.New(
		table.Identifier{"db", "shredded_variant_scan_test"},
		meta, location+"/metadata/v1.metadata.json",
		func(ctx context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil },
		&rowDeltaCatalog{metadata: meta},
	)

	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddFiles(t.Context(), []string{dataPath}, nil, false))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	scanned, err := tbl.Scan().ToArrowTable(t.Context())
	require.NoError(t, err)
	defer scanned.Release()

	require.Equal(t, int64(nData+1), scanned.NumRows(), "nData rows + one null row")
	require.Equal(t, 1, int(scanned.NumCols()))
	require.Equal(t, "payload", scanned.Schema().Field(0).Name)

	ext, ok := scanned.Schema().Field(0).Type.(arrow.ExtensionType)
	require.True(t, ok, "payload must be Variant extension type, got %T", scanned.Schema().Field(0).Type)
	assert.Equal(t, "parquet.variant", ext.ExtensionName())

	// Scan order is not guaranteed; check the set of "a" values is {0..nData-1}.
	col := scanned.Column(0).Data()
	seenA := map[int64]bool{}
	nullCount := 0
	for _, chunk := range col.Chunks() {
		varr, ok := chunk.(*extensions.VariantArray)
		require.True(t, ok, "expected *extensions.VariantArray, got %T", chunk)
		// The shredded layout must be invisible to the scanner: the column
		// reads back as a plain (non-shredded) variant.
		assert.False(t, varr.IsShredded(), "scanned variant column must not be shredded")
		for i := range varr.Len() {
			if varr.IsNull(i) {
				nullCount++

				continue
			}
			val, err := varr.Value(i)
			require.NoError(t, err)
			obj, ok := val.Value().(variant.ObjectValue)
			require.True(t, ok, "expected ObjectValue, got %T", val.Value())
			require.EqualValues(t, 3, obj.NumElements(), "row should have a, b, extra")

			aField, err := obj.ValueByKey("a")
			require.NoError(t, err, "missing field a")
			bField, err := obj.ValueByKey("b")
			require.NoError(t, err, "missing field b")
			eField, err := obj.ValueByKey("extra")
			require.NoError(t, err, "missing residual field extra")

			a := variantInt(t, aField.Value.Value())
			seenA[a] = true
			assert.EqualValues(t, "row-"+string(rune('A'+a%26)), bField.Value.Value(), "b for a=%d", a)
			assert.EqualValues(t, a*10, variantInt(t, eField.Value.Value()), "extra for a=%d", a)
		}
	}
	assert.Equal(t, 1, nullCount, "exactly one null row")
	assert.Len(t, seenA, nData, "distinct a values")
	for i := range int64(nData) {
		assert.True(t, seenA[i], "missing reassembled a=%d", i)
	}
}

func TestShreddedVariantScanNulls(t *testing.T) {
	location := filepath.ToSlash(t.TempDir())
	dataDir := filepath.Join(location, "data")
	require.NoError(t, os.MkdirAll(dataDir, 0o755))
	dataPath := filepath.Join(dataDir, "nulls.parquet")

	mem := memory.DefaultAllocator
	shreddedType := extensions.NewShreddedVariantType(arrow.StructOf(
		arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int64},
	))
	bldr := extensions.NewVariantBuilder(mem, shreddedType)
	defer bldr.Release()

	// row 0: present row whose variant value is JSON null.
	var nb variant.Builder
	require.NoError(t, nb.Append(nil))
	jsonNull, err := nb.Build()
	require.NoError(t, err)
	bldr.Append(jsonNull)
	// row 1: genuine physical null.
	bldr.AppendNull()
	// row 2: normal object.
	var ob variant.Builder
	require.NoError(t, ob.Append(map[string]any{"a": int64(7)}))
	obj, err := ob.Build()
	require.NoError(t, err)
	bldr.Append(obj)

	arr := bldr.NewArray()
	defer arr.Release()
	arrowSchema := arrow.NewSchema([]arrow.Field{{
		Name: "payload", Type: shreddedType, Nullable: true,
		Metadata: arrow.NewMetadata([]string{"PARQUET:field_id"}, []string{"1"}),
	}}, nil)
	rec := array.NewRecordBatch(arrowSchema, []arrow.Array{arr}, int64(arr.Len()))
	defer rec.Release()
	f, err := os.Create(dataPath)
	require.NoError(t, err)
	wr, err := pqarrow.NewFileWriter(arrowSchema, f, parquet.NewWriterProperties(), pqarrow.DefaultWriterProps())
	require.NoError(t, err)
	require.NoError(t, wr.Write(rec))
	require.NoError(t, wr.Close())

	iceSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "payload", Type: iceberg.VariantType{}, Required: false},
	)
	meta, err := table.NewMetadata(iceSchema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, location,
		iceberg.Properties{table.PropertyFormatVersion: "3"})
	require.NoError(t, err)
	tbl := table.New(table.Identifier{"db", "shredded_variant_nulls"}, meta,
		location+"/metadata/v1.metadata.json",
		func(ctx context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil },
		&rowDeltaCatalog{metadata: meta})

	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddFiles(t.Context(), []string{dataPath}, nil, false))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	scanned, err := tbl.Scan().ToArrowTable(t.Context())
	require.NoError(t, err)
	defer scanned.Release()
	require.Equal(t, int64(3), scanned.NumRows())

	varr, ok := scanned.Column(0).Data().Chunk(0).(*extensions.VariantArray)
	require.True(t, ok)
	require.Equal(t, 3, varr.Len())

	// row 0: present (physical validity bit set), reassembles to a null-typed variant.
	assert.False(t, varr.Storage().IsNull(0), "JSON-null row must stay physically present")
	v0, err := varr.Value(0)
	require.NoError(t, err)
	assert.Equal(t, variant.Null, v0.Type())
	// row 1: genuine physical null.
	assert.True(t, varr.Storage().IsNull(1), "physical null row must stay null")
	// row 2: object preserved.
	assert.False(t, varr.Storage().IsNull(2))
	v2, err := varr.Value(2)
	require.NoError(t, err)
	_, ok = v2.Value().(variant.ObjectValue)
	assert.True(t, ok)
}

func TestShreddedVariantReassemblyNoLeak(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	ctx := compute.WithAllocator(context.Background(), mem)

	shredded := extensions.NewShreddedVariantType(arrow.StructOf(
		arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int64},
	))
	bldr := extensions.NewVariantBuilder(mem, shredded)
	for i := range 4 {
		var b variant.Builder
		require.NoError(t, b.Append(map[string]any{"a": int64(i), "city": "NYC"}))
		v, err := b.Build()
		require.NoError(t, err)
		bldr.Append(v)
	}
	arr := bldr.NewArray()
	bldr.Release()

	sc := arrow.NewSchema([]arrow.Field{{
		Name: "payload", Type: shredded, Nullable: true,
		Metadata: arrow.NewMetadata([]string{"PARQUET:field_id"}, []string{"1"}),
	}}, nil)
	rec := array.NewRecordBatch(sc, []arrow.Array{arr}, 4)
	arr.Release()

	ice := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "payload", Type: iceberg.VariantType{}})
	out, err := table.ToRequestedSchema(ctx, ice, ice, rec, table.SchemaOptions{})
	require.NoError(t, err)
	out.Release()
	rec.Release()
}

func TestShreddedVariantGoldenReassembly(t *testing.T) {
	cases := []struct {
		parquet string
		goldens []string
	}{
		{"case-001.parquet", []string{"case-001_row-0.variant.bin"}},
		{"case-002.parquet", []string{"case-002_row-0.variant.bin"}},
		{"case-013.parquet", []string{"case-013_row-0.variant.bin"}},
		{"case-133.parquet", []string{"case-133_row-0.variant.bin"}},
		{"case-138.parquet", []string{"case-138_row-0.variant.bin"}},
		{"case-136.parquet", []string{"case-136_row-0.variant.bin"}},
		{"case-135.parquet", []string{"case-135_row-0.variant.bin"}},
		{"case-089.parquet", []string{"case-089_row-0.variant.bin"}},
		{"case-083.parquet", []string{
			"", // row 0 is a null variant
			"case-083_row-1.variant.bin",
			"case-083_row-2.variant.bin",
			"case-083_row-3.variant.bin",
		}},
	}

	for _, tc := range cases {
		t.Run(tc.parquet, func(t *testing.T) {
			scanned := scanShreddedFixture(t, tc.parquet)
			defer scanned.Release()

			varr := shreddedVarColumn(t, scanned)
			require.False(t, varr.IsShredded(), "scanned variant must read back unshredded")
			require.Equal(t, len(tc.goldens), varr.Len(), "row count")

			for i, golden := range tc.goldens {
				if golden == "" {
					assert.True(t, varr.Storage().IsNull(i), "row %d should be a null variant", i)

					continue
				}
				require.False(t, varr.Storage().IsNull(i), "row %d should not be null", i)
				got, err := varr.Value(i)
				require.NoError(t, err, "row %d", i)
				gotJSON, err := got.MarshalJSON()
				require.NoError(t, err, "row %d", i)
				assert.JSONEq(t, goldenVariantJSON(t, golden), string(gotJSON), "row %d", i)
			}
		})
	}
}

// Fixtures whose conflicting/invalid shredded layout the reader must reject.
func TestShreddedVariantRejectsInvalid(t *testing.T) {
	for _, pq := range []string{
		"case-040.parquet", "case-042.parquet", "case-087.parquet",
		"case-128.parquet", "case-084-INVALID.parquet",
	} {
		t.Run(pq, func(t *testing.T) {
			require.Error(t, scanShreddedFixtureErr(t, pq),
				"reader must reject conflicting/invalid shredded data")
		})
	}
}

// arrow-go reads some spec-invalid / unsupported-type layouts that Java rejects.
// This pins that documented leniency so a future tightening is flagged.
func TestShreddedVariantArrowGoLeniency(t *testing.T) {
	// 127/137: unsupported scalar typed_value with null data reassembles to null.
	for _, pq := range []string{"case-127.parquet", "case-137.parquet"} {
		t.Run(pq, func(t *testing.T) {
			scanned := scanShreddedFixture(t, pq)
			defer scanned.Release()

			varr := shreddedVarColumn(t, scanned)
			require.Equal(t, 1, varr.Len())
			require.False(t, varr.Storage().IsNull(0), "row is a present variant, not a physical null")
			v, err := varr.Value(0)
			require.NoError(t, err)
			assert.Equal(t, variant.Null, v.Type())
		})
	}

	// 043: arrow-go's read differs from the Java golden; the spec pins no value
	// for invalid cases, so assert arrow-go's actual read.
	t.Run("case-043-INVALID.parquet", func(t *testing.T) {
		scanned := scanShreddedFixture(t, "case-043-INVALID.parquet")
		defer scanned.Release()

		varr := shreddedVarColumn(t, scanned)
		require.Equal(t, 1, varr.Len())
		require.False(t, varr.Storage().IsNull(0))
		v, err := varr.Value(0)
		require.NoError(t, err)
		gotJSON, err := v.MarshalJSON()
		require.NoError(t, err)
		assert.JSONEq(t, `{"a":null,"b":"2024-01-30"}`, string(gotJSON))
	})
}

// writeVariantColumnParquet writes bldr's contents as a single "payload" variant column (field id fieldID) of shreddedType to path.
func writeVariantColumnParquet(t *testing.T, path string, fieldID int, shreddedType arrow.DataType, bldr *extensions.VariantBuilder) {
	t.Helper()
	arr := bldr.NewArray()
	defer arr.Release()

	arrowSchema := arrow.NewSchema([]arrow.Field{{
		Name: "payload", Type: shreddedType, Nullable: true,
		Metadata: arrow.NewMetadata([]string{"PARQUET:field_id"}, []string{strconv.Itoa(fieldID)}),
	}}, nil)
	rec := array.NewRecordBatch(arrowSchema, []arrow.Array{arr}, int64(arr.Len()))
	defer rec.Release()

	f, err := os.Create(path)
	require.NoError(t, err)
	defer f.Close()

	wr, err := pqarrow.NewFileWriter(arrowSchema, f,
		parquet.NewWriterProperties(parquet.WithStats(true)),
		pqarrow.DefaultWriterProps())
	require.NoError(t, err)
	require.NoError(t, wr.Write(rec))
	require.NoError(t, wr.Close())
}

// writeShreddedVariantFile writes a shredded variant column "payload" (typed
// {a, b} + residual "extra") with nRows data rows then one null row.
func writeShreddedVariantFile(t *testing.T, path string, fieldID, nRows int) {
	t.Helper()

	shreddedType := extensions.NewShreddedVariantType(arrow.StructOf(
		arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int64},
		arrow.Field{Name: "b", Type: arrow.BinaryTypes.String},
	))

	bldr := extensions.NewVariantBuilder(memory.DefaultAllocator, shreddedType)
	defer bldr.Release()

	for i := range nRows {
		var b variant.Builder
		require.NoError(t, b.Append(map[string]any{
			"a":     int64(i),
			"b":     "row-" + string(rune('A'+i%26)),
			"extra": int64(i * 10),
		}))
		v, err := b.Build()
		require.NoError(t, err)
		bldr.Append(v)
	}
	bldr.AppendNull()

	writeVariantColumnParquet(t, path, fieldID, shreddedType, bldr)
}

// variantInt normalizes the integer Go value a variant scalar may surface as
// (int8/16/32/64) into int64 for comparison.
func variantInt(t *testing.T, v any) int64 {
	t.Helper()
	switch n := v.(type) {
	case int8:
		return int64(n)
	case int16:
		return int64(n)
	case int32:
		return int64(n)
	case int64:
		return n
	default:
		t.Fatalf("expected integer variant value, got %T", v)

		return 0
	}
}

// scanShreddedFixture registers a fixture, asserts it is stored shredded, and
// returns the full Scan().ToArrowTable() result.
func scanShreddedFixture(t *testing.T, fixture string) arrow.Table {
	t.Helper()
	requireFixtureShredded(t, fixture)

	scanned, err := scanShreddedFixtureRaw(t, fixture)
	require.NoError(t, err)

	return scanned
}

// scanShreddedFixtureErr returns the error from AddFiles or the scan instead of
// failing, for the rejection cases.
func scanShreddedFixtureErr(t *testing.T, fixture string) error {
	t.Helper()
	scanned, err := scanShreddedFixtureRaw(t, fixture)
	if scanned != nil {
		scanned.Release()
	}

	return err
}

func scanShreddedFixtureRaw(t *testing.T, fixture string) (arrow.Table, error) {
	t.Helper()
	src := filepath.Join(shreddedFixtureDir, fixture)
	if _, err := os.Stat(src); err != nil {
		t.Skipf("fixture missing; run %s/gen_fixtures.sh: %v", shreddedFixtureDir, err)
	}

	location := filepath.ToSlash(t.TempDir())
	dataDir := filepath.Join(location, "data")
	require.NoError(t, os.MkdirAll(dataDir, 0o755))
	raw, err := os.ReadFile(src)
	require.NoError(t, err)
	dataPath := filepath.Join(dataDir, fixture)
	require.NoError(t, os.WriteFile(dataPath, raw, 0o644))

	iceSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: false},
		iceberg.NestedField{ID: 2, Name: "var", Type: iceberg.VariantType{}, Required: false},
	)
	meta, err := table.NewMetadata(iceSchema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder,
		location, iceberg.Properties{table.PropertyFormatVersion: "3"})
	require.NoError(t, err)
	tbl := table.New(table.Identifier{"db", "shredded_variant_golden"}, meta,
		location+"/metadata/v1.metadata.json",
		func(ctx context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil },
		&rowDeltaCatalog{metadata: meta})

	tx := tbl.NewTransaction()
	if err := tx.AddFiles(t.Context(), []string{dataPath}, nil, false); err != nil {
		return nil, err
	}
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	return tbl.Scan().ToArrowTable(t.Context())
}

// requireFixtureShredded fails unless the fixture's var column is stored shredded.
func requireFixtureShredded(t *testing.T, fixture string) {
	t.Helper()
	pf, err := file.OpenParquetFile(filepath.Join(shreddedFixtureDir, fixture), false)
	require.NoError(t, err)
	defer pf.Close()
	rdr, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	require.NoError(t, err)
	tbl, err := rdr.ReadTable(t.Context())
	require.NoError(t, err)
	defer tbl.Release()
	for c := range int(tbl.NumCols()) {
		if va, ok := tbl.Column(c).Data().Chunk(0).(*extensions.VariantArray); ok {
			require.True(t, va.IsShredded(), "fixture %s must be stored shredded", fixture)

			return
		}
	}
	t.Fatalf("fixture %s has no variant column", fixture)
}

// shreddedVarColumn returns the single-chunk VariantArray for the "var" column.
func shreddedVarColumn(t *testing.T, tbl arrow.Table) *extensions.VariantArray {
	t.Helper()
	for c := range int(tbl.NumCols()) {
		if tbl.Schema().Field(c).Name != "var" {
			continue
		}
		require.Len(t, tbl.Column(c).Data().Chunks(), 1, "expected a single chunk")
		varr, ok := tbl.Column(c).Data().Chunk(0).(*extensions.VariantArray)
		require.True(t, ok, "var column must be a *extensions.VariantArray, got %T", tbl.Column(c).Data().Chunk(0))

		return varr
	}
	t.Fatal("scanned table has no var column")

	return nil
}

// goldenVariantJSON decodes a *.variant.bin golden (metadata then value) to JSON.
func goldenVariantJSON(t *testing.T, name string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(shreddedFixtureDir, name))
	require.NoError(t, err)
	metaBytes, valueBytes := splitGoldenVariant(t, raw)
	v, err := variant.New(metaBytes, valueBytes)
	require.NoError(t, err, "decoding golden %s", name)
	j, err := v.MarshalJSON()
	require.NoError(t, err, "marshaling golden %s", name)

	return string(j)
}

// splitGoldenVariant separates the leading metadata from the trailing value.
func splitGoldenVariant(t *testing.T, b []byte) ([]byte, []byte) {
	t.Helper()
	require.NotEmpty(t, b, "empty golden")
	offsetSize := int((b[0]>>6)&0x03) + 1
	require.GreaterOrEqual(t, len(b), 1+offsetSize, "golden too short for dictionary size")
	dictSize := readLEUint(b[1 : 1+offsetSize])
	lastOffsetPos := 1 + offsetSize + dictSize*offsetSize
	require.GreaterOrEqual(t, len(b), lastOffsetPos+offsetSize, "golden too short for offsets")
	stringBytes := readLEUint(b[lastOffsetPos : lastOffsetPos+offsetSize])
	metaLen := 1 + offsetSize + (dictSize+1)*offsetSize + stringBytes
	require.GreaterOrEqual(t, len(b), metaLen, "golden too short for dictionary strings")

	return b[:metaLen], b[metaLen:]
}

// readLEUint reads a little-endian unsigned integer from a 1-to-4 byte slice.
func readLEUint(b []byte) int {
	var buf [4]byte
	copy(buf[:], b)

	return int(binary.LittleEndian.Uint32(buf[:]))
}

// writeFullyShreddedVariantFile writes rows that are EXACTLY {a,b} (both shredded),
// so the residual payload.value is all-null and the child bounds survive.
func writeFullyShreddedVariantFile(t *testing.T, path string, fieldID, nRows int) {
	t.Helper()
	shreddedType := extensions.NewShreddedVariantType(arrow.StructOf(
		arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int64},
		arrow.Field{Name: "b", Type: arrow.BinaryTypes.String},
	))
	bldr := extensions.NewVariantBuilder(memory.DefaultAllocator, shreddedType)
	defer bldr.Release()
	for i := range nRows {
		var b variant.Builder
		require.NoError(t, b.Append(map[string]any{"a": int64(i), "b": "row-" + string(rune('A'+i%26))}))
		v, err := b.Build()
		require.NoError(t, err)
		bldr.Append(v)
	}
	writeVariantColumnParquet(t, path, fieldID, shreddedType, bldr)
}

// TestShreddedVariantAddFilesBounds pins that AddFiles (which opens the file and
// recomputes) emits correct shredded variant child bounds under the parent field id.
func TestShreddedVariantAddFilesBounds(t *testing.T) {
	location := filepath.ToSlash(t.TempDir())
	dataDir := filepath.Join(location, "data")
	require.NoError(t, os.MkdirAll(dataDir, 0o755))
	dataPath := filepath.Join(dataDir, "shredded.parquet")
	const nData = 5
	writeFullyShreddedVariantFile(t, dataPath, 1, nData)

	iceSchema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "payload", Type: iceberg.VariantType{}, Required: false})
	meta, err := table.NewMetadata(iceSchema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, location,
		iceberg.Properties{table.PropertyFormatVersion: "3"})
	require.NoError(t, err)
	tbl := table.New(table.Identifier{"db", "shredded_addfiles_bounds"}, meta, location+"/metadata/v1.metadata.json",
		func(ctx context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, &rowDeltaCatalog{metadata: meta})

	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddFiles(t.Context(), []string{dataPath}, nil, false))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	tasks, err := tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)
	require.Len(t, tasks, 1)

	// bound object keyed by normalized JSON path: min/max of a (0..4) and b (row-A..row-E).
	buildObj := func(a int64, b string) []byte {
		var bld variant.Builder
		start := bld.Offset()
		entries := []variant.FieldEntry{bld.NextField(start, "$['a']")}
		require.NoError(t, bld.AppendInt(a))
		entries = append(entries, bld.NextField(start, "$['b']"))
		require.NoError(t, bld.AppendString(b))
		require.NoError(t, bld.FinishObject(start, entries))
		v, err := bld.Build()
		require.NoError(t, err)

		return append(append([]byte{}, v.Metadata().Bytes()...), v.Bytes()...)
	}

	assert.Equal(t, buildObj(0, "row-A"), tasks[0].File.LowerBoundValues()[1], "AddFiles lower bound")
	assert.Equal(t, buildObj(nData-1, "row-E"), tasks[0].File.UpperBoundValues()[1], "AddFiles upper bound")
}

// setupVariantScanTable writes a shredded variant table from rows and returns a closure reading the "id" column of a filtered scan.
func setupVariantScanTable(t *testing.T, name string, rows []map[string]any) (*table.Table, func(iceberg.BooleanExpression) []int64) {
	t.Helper()
	ctx := context.Background()
	loc := "file://" + t.TempDir()

	cat, err := catalog.Load(ctx, "default", iceberg.Properties{
		"uri":          ":memory:",
		"type":         "sql",
		sql.DriverKey:  sqliteshim.ShimName,
		sql.DialectKey: string(sql.SQLite),
		"warehouse":    loc,
	})
	require.NoError(t, err)

	iceSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.VariantType{}},
	)
	require.NoError(t, cat.CreateNamespace(ctx, table.Identifier{"ns"}, nil))
	tbl, err := cat.CreateTable(ctx, table.Identifier{"ns", name}, iceSchema,
		catalog.WithProperties(iceberg.Properties{
			table.PropertyFormatVersion:   "3",
			table.ParquetShredVariantsKey: "true",
		}),
		catalog.WithLocation(loc))
	require.NoError(t, err)

	arrSchema, err := table.SchemaToArrowSchema(iceSchema, nil, true, false)
	require.NoError(t, err)

	mem := memory.DefaultAllocator
	idb := array.NewInt64Builder(mem)
	defer idb.Release()
	vb := extensions.NewVariantBuilder(mem, extensions.NewDefaultVariantType())
	defer vb.Release()
	for i, m := range rows {
		idb.Append(int64(i))
		var b variant.Builder
		require.NoError(t, b.Append(m))
		v, err := b.Build()
		require.NoError(t, err)
		vb.Append(v)
	}
	idArr := idb.NewArray()
	defer idArr.Release()
	pArr := vb.NewArray()
	defer pArr.Release()
	rec := array.NewRecordBatch(arrSchema, []arrow.Array{idArr, pArr}, int64(len(rows)))
	defer rec.Release()

	rdr, err := array.NewRecordReader(arrSchema, []arrow.RecordBatch{rec})
	require.NoError(t, err)
	defer rdr.Release()
	tbl, err = tbl.Append(ctx, rdr, nil)
	require.NoError(t, err)

	return tbl, func(filter iceberg.BooleanExpression) []int64 {
		scan := tbl.Scan(table.WithRowFilter(filter))
		result, err := scan.ToArrowTable(ctx)
		require.NoError(t, err)
		defer result.Release()
		col := result.Column(result.Schema().FieldIndices("id")[0]).Data()
		out := make([]int64, 0, result.NumRows())
		for _, ch := range col.Chunks() {
			c := ch.(*array.Int64)
			for i := range c.Len() {
				out = append(out, c.Value(i))
			}
		}

		return out
	}
}

// TestVariantExtractScanEndToEnd covers variant predicate pushdown through the public API: write a shredded variant table, read it back via Scan.ToArrowTable with extract row filters.
func TestVariantExtractScanEndToEnd(t *testing.T) {
	_, idsFor := setupVariantScanTable(t, "variant_scan", []map[string]any{
		{"a": int64(1), "b": "x"},
		{"a": int64(2), "b": "y"},
		{"a": int64(3), "b": "z"},
		{"b": "only-b"}, // a absent
	})
	ext := iceberg.Extract("payload", "$.a", iceberg.PrimitiveTypes.Int64)

	require.Equal(t, []int64{2}, idsFor(iceberg.LiteralPredicate(iceberg.OpEQ, ext, iceberg.NewLiteral(int64(3)))),
		"$.a == 3 selects only row id 2")
	require.ElementsMatch(t, []int64{0, 1, 2}, idsFor(iceberg.NotNull(ext)),
		"NotNull($.a) selects the three a-present rows")
	require.Equal(t, []int64{3}, idsFor(iceberg.IsNull(ext)),
		"IsNull($.a) selects the a-absent row")
}

// TestVariantExtractResidualAndHeterogeneous covers extract over a non-uniform $.a, which stays in the residual value column, exercising arrow-go's reassembly and cast-to-null for the string-typed row.
func TestVariantExtractResidualAndHeterogeneous(t *testing.T) {
	_, idsFor := setupVariantScanTable(t, "variant_resid", []map[string]any{
		{"a": int64(10), "b": "x"},
		{"a": "hello", "b": "y"},
		{"a": int64(30), "b": "z"},
		{"b": "only-b"},
	})
	ext := iceberg.Extract("payload", "$.a", iceberg.PrimitiveTypes.Int64)

	require.Equal(t, []int64{0}, idsFor(iceberg.LiteralPredicate(iceberg.OpEQ, ext, iceberg.NewLiteral(int64(10)))),
		"$.a == 10 selects the residual-backed int row")
	require.Empty(t, idsFor(iceberg.LiteralPredicate(iceberg.OpEQ, ext, iceberg.NewLiteral(int64(0)))),
		"string $.a='hello' never matches an Int64 predicate")
	require.ElementsMatch(t, []int64{0, 2}, idsFor(iceberg.NotNull(ext)),
		"NotNull($.a as Int64) = the two int rows only")
	require.ElementsMatch(t, []int64{1, 3}, idsFor(iceberg.IsNull(ext)),
		"IsNull($.a as Int64) = the string row and the absent row")
}

// NOT($.a == 1) must keep the $.a == 2 row, not prune its row group.
func TestVariantExtractNegatedNotPruned(t *testing.T) {
	tbl, idsFor := setupVariantScanTable(t, "variant_neg", []map[string]any{
		{"a": int64(1), "b": "x"},
		{"a": int64(2), "b": "y"},
	})
	ext := iceberg.Extract("payload", "$.a", iceberg.PrimitiveTypes.Int64)
	filter := iceberg.NewNot(iceberg.LiteralPredicate(iceberg.OpEQ, ext, iceberg.NewLiteral(int64(1))))

	require.Equal(t, []int64{1}, idsFor(filter),
		"NOT($.a == 1) must keep the a=2 row (id 1), not prune its row group")

	result, err := tbl.Scan(table.WithRowFilter(filter)).ToArrowTable(context.Background())
	require.NoError(t, err)
	defer result.Release()
	require.Equal(t, int64(1), result.NumRows(), "exactly one surviving row")
	payload := result.Column(result.Schema().FieldIndices("payload")[0]).Data().Chunk(0).(*extensions.VariantArray)
	v, err := payload.Value(0)
	require.NoError(t, err)
	obj, ok := v.Value().(variant.ObjectValue)
	require.True(t, ok, "surviving payload must be a variant object")
	aField, err := obj.ValueByKey("a")
	require.NoError(t, err)
	require.EqualValues(t, 2, aField.Value.Value(), "surviving row's $.a must be 2")
}

// An extract filter on the incremental-scan read path must not abort with ErrNotImplemented.
func TestVariantExtractIncrementalScanReadTasks(t *testing.T) {
	ctx := context.Background()
	tbl, _ := setupVariantScanTable(t, "variant_incr", []map[string]any{
		{"a": int64(1), "b": "x"},
		{"a": int64(2), "b": "y"},
		{"a": int64(3), "b": "z"},
	})
	ext := iceberg.Extract("payload", "$.a", iceberg.PrimitiveTypes.Int64)
	filter := iceberg.LiteralPredicate(iceberg.OpEQ, ext, iceberg.NewLiteral(int64(2)))

	tasks, err := tbl.NewIncrementalAppendScan(table.WithRowFilter(filter)).PlanFiles(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, tasks)
	require.NotNil(t, tasks[0].Residual, "incremental tasks carry a non-nil residual")

	_, recs, err := tbl.Scan(table.WithRowFilter(filter)).ReadTasks(ctx, tasks)
	require.NoError(t, err)
	var ids []int64
	for rec, err := range recs {
		require.NoError(t, err, "extract filter on incremental read must not abort")
		if idxs := rec.Schema().FieldIndices("id"); len(idxs) == 1 {
			c := rec.Column(idxs[0]).(*array.Int64)
			for i := range c.Len() {
				ids = append(ids, c.Value(i))
			}
		}
		rec.Release()
	}
	require.Equal(t, []int64{1}, ids, "$.a == 2 selects the a=2 row (id 1) on the incremental path")
}
