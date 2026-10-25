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

package iceberg

import (
	"fmt"
	"reflect"
	"strconv"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/iceberg-go/internal"
	"github.com/stretchr/testify/require"
)

func TestSchemaCacheLRUBoundsSize(t *testing.T) {
	require.NoError(t, SetSchemaCacheSize(2))
	t.Cleanup(func() { require.NoError(t, SetSchemaCacheSize(defaultSchemaCacheSize)) })

	sc := NewSchema(0,
		NestedField{ID: 1, Name: "a", Type: Int64Type{}, Required: true},
		NestedField{ID: 2, Name: "b", Type: StringType{}, Required: true},
		NestedField{ID: 3, Name: "c", Type: BooleanType{}, Required: true},
	)
	for i, src := range []int{1, 2, 3} {
		spec := NewPartitionSpec(PartitionField{
			SourceIDs: []int{src},
			FieldID:   1000 + i,
			Name:      "part_" + strconv.Itoa(src),
			Transform: IdentityTransform{},
		})
		_, _, err := manifestEntrySchemaFor(spec, sc, 2)
		require.NoError(t, err)
	}
	require.LessOrEqual(t, dataFileSchemaCache.Len(), 2,
		"LRU must enforce its size bound by evicting least-recently-used entries on insert")
}

func TestSetSchemaCacheSizeRejectsNonPositive(t *testing.T) {
	for _, size := range []int{0, -1} {
		require.Error(t, SetSchemaCacheSize(size),
			"size %d must be rejected", size)
	}
}

func TestManifestEntrySchemaForCachesByPartitionAvroFingerprint(t *testing.T) {
	spec, schema, _ := fullyPopulatedDataFileForCodec(t, 2)
	s1, _, err := manifestEntrySchemaFor(spec, schema, 2)
	require.NoError(t, err)
	s2, _, err := manifestEntrySchemaFor(spec, schema, 2)
	require.NoError(t, err)
	require.Same(t, s1, s2,
		"identical (partition type, version) inputs must reuse the cached schema")
}

func TestDataFileCodecWithDroppedPartitionSource(t *testing.T) {
	spec := NewPartitionSpec(
		PartitionField{SourceIDs: []int{1}, FieldID: 1000, Name: "dropped", Transform: IdentityTransform{}},
		PartitionField{SourceIDs: []int{2}, FieldID: 1001, Name: "bucketed", Transform: BucketTransform{NumBuckets: 8}},
	)
	schema := NewSchema(0)

	builder, err := NewDataFileBuilder(
		spec,
		EntryContentData,
		"s3://bucket/table/data.parquet",
		ParquetFile,
		map[int]any{1000: "historical-value", 1001: int32(3)},
		nil,
		nil,
		1,
		1024,
	)
	require.NoError(t, err)

	encoded, err := builder.Build().(*dataFile).MarshalAvroEntry(spec, schema, 2)
	require.NoError(t, err)
	decoded, err := unmarshalAvroDataFileEntry(encoded, spec, schema, 2)
	require.NoError(t, err)
	require.Equal(t, map[int]any{1000: nil, 1001: int32(3)}, decoded.Partition())
}

// TestMarshalAvroEntryOmitsDistinctCounts checks that the data_file schema the
// entry codec encodes with has no distinct_counts (field 111), which the spec
// deprecates in every version (apache/iceberg#12182). The encoder and decoder
// share this schema, so a round trip alone could not show the field leaking;
// strict readers such as PyIceberg reject manifests that emit it.
func TestMarshalAvroEntryOmitsDistinctCounts(t *testing.T) {
	for _, version := range []int{1, 2, 3} {
		t.Run("v"+strconv.Itoa(version), func(t *testing.T) {
			spec, schema, _ := fullyPopulatedDataFileForCodec(t, version)
			s, _, err := manifestEntrySchemaFor(spec, schema, version)
			require.NoError(t, err)

			root := s.Root()
			found := false
			for _, field := range root.Fields {
				if field.Name != "data_file" {
					continue
				}
				found = true
				for _, dfField := range field.Type.Fields {
					require.NotEqual(t, "distinct_counts", dfField.Name)
					require.NotEqual(t, 111, dfField.Props["field-id"])
				}
			}
			require.True(t, found, "data_file field not found on manifest_entry v%d schema", version)
		})
	}
}

func TestUnmarshalAvroDataFileEntryNormalizesFileFormat(t *testing.T) {
	spec := NewPartitionSpec()
	schema := NewSchema(0)

	const path = "s3://bucket/table/data.parquet"

	tests := []struct {
		name      string
		content   ManifestEntryContent
		written   FileFormat
		expected  FileFormat
		expectErr bool
	}{
		{name: "spec lowercase", written: "parquet", expected: ParquetFile},
		{name: "mixed case", written: "Parquet", expected: ParquetFile},
		{name: "uppercase", written: ParquetFile, expected: ParquetFile},
		{name: "lowercase avro", written: "avro", expected: AvroFile},
		{name: "lowercase orc", written: "orc", expected: OrcFile},
		{
			name: "lowercase puffin in delete entry", content: EntryContentPosDeletes,
			written: "puffin", expected: PuffinFile,
		},
		{name: "unknown format", written: "csv", expectErr: true},
		{name: "empty format", written: "", expectErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			content := tt.content
			if content == 0 {
				content = EntryContentData
			}
			builder, err := NewDataFileBuilder(spec, content,
				path, ParquetFile, nil, nil, nil, 1, 1024)
			require.NoError(t, err)
			source := builder.Build().(*dataFile)
			source.Format = tt.written

			encoded, err := source.MarshalAvroEntry(spec, schema, 2)
			require.NoError(t, err)

			decoded, err := unmarshalAvroDataFileEntry(encoded, spec, schema, 2)
			if tt.expectErr {
				require.EqualError(t, err, fmt.Sprintf(
					"iceberg: unmarshalAvroDataFileEntry: data file %q: unknown file format: %s",
					path, tt.written))

				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.expected, decoded.FileFormat())
		})
	}
}

// TestManifestEntrySchemaForDistinguishesSameSpecIDDifferentPartitionTypes
// guards against the cache-key collision that would occur if the cache
// were keyed only by spec.ID(). Two tables can both use
// InitialPartitionSpecID = 0 but expose different partition column
// types; keying on the partition-type fingerprint keeps their schemas
// separate.
func TestManifestEntrySchemaForDistinguishesSameSpecIDDifferentPartitionTypes(t *testing.T) {
	intSchema := NewSchema(0,
		NestedField{ID: 1, Name: "id", Type: Int64Type{}, Required: true},
	)
	intSpec := NewPartitionSpec(
		PartitionField{SourceIDs: []int{1}, FieldID: 1000, Name: "id_part", Transform: IdentityTransform{}},
	)
	require.Equal(t, InitialPartitionSpecID, intSpec.ID(),
		"sanity: NewPartitionSpec uses InitialPartitionSpecID")

	strSchema := NewSchema(0,
		NestedField{ID: 1, Name: "name", Type: StringType{}, Required: true},
	)
	strSpec := NewPartitionSpec(
		PartitionField{SourceIDs: []int{1}, FieldID: 1000, Name: "name_part", Transform: IdentityTransform{}},
	)
	require.Equal(t, intSpec.ID(), strSpec.ID(),
		"sanity: both specs share the same spec id, exposing the collision risk")

	intAvro, intMaps, err := manifestEntrySchemaFor(intSpec, intSchema, 2)
	require.NoError(t, err)
	strAvro, strMaps, err := manifestEntrySchemaFor(strSpec, strSchema, 2)
	require.NoError(t, err)

	require.NotSame(t, intAvro, strAvro,
		"different partition column types must not share a cached avro schema")
	require.NotEqual(t, intMaps.nameToID, strMaps.nameToID,
		"partition field name→id maps must differ between the two specs")
}

func TestMarshalAvroEntryDoesNotMutateAnyAvroField(t *testing.T) {
	for _, version := range []int{1, 2, 3} {
		t.Run("v"+strconv.Itoa(version), func(t *testing.T) {
			spec, schema, df := fullyPopulatedDataFileForCodec(t, version)
			impl := df.(*dataFile)

			before := snapshotAvroFields(impl)
			_, err := impl.MarshalAvroEntry(spec, schema, version)
			require.NoError(t, err)
			after := snapshotAvroFields(impl)

			require.Equal(t, before, after,
				"MarshalAvroEntry must not mutate any avro-tagged field of the source DataFile, "+
					"including pointer-typed fields whose backing storage is shared with the clone")
		})
	}
}

func TestCloneDataFileAvroFieldsCopiesOnlyAvroFields(t *testing.T) {
	src := &dataFile{}
	typ := reflect.TypeFor[dataFile]()
	srcVal := reflect.ValueOf(src).Elem()

	// Give every tagged field a nonzero value, including fields added later.
	for i := range typ.NumField() {
		field := typ.Field(i)
		if _, tagged := field.Tag.Lookup("avro"); !tagged {
			continue
		}

		value := srcVal.Field(i)
		switch value.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			value.SetInt(17)
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			value.SetUint(17)
		case reflect.String:
			value.SetString("sentinel")
		case reflect.Map:
			value.Set(reflect.MakeMap(value.Type()))
		case reflect.Pointer:
			value.Set(reflect.New(value.Type().Elem()))
		default:
			t.Fatalf("unhandled Avro field %s of kind %s", field.Name, value.Kind())
		}
	}

	// The clone must not carry state from lazily initialized source data.
	src.projected = true
	src.specID = 42
	src.fieldNameToID = map[string]int{"field": 1}
	src.initPartition.Do(func() {})
	src.initColumnStats.Do(func() {})

	cloneVal := reflect.ValueOf(cloneDataFileAvroFields(src)).Elem()
	for i := range typ.NumField() {
		field := typ.Field(i)
		if _, tagged := field.Tag.Lookup("avro"); tagged {
			source := srcVal.Field(i)
			cloned := cloneVal.Field(i)
			require.Equal(t, source.Interface(), cloned.Interface(),
				"Avro field %s must be copied", field.Name)
			switch source.Kind() {
			case reflect.Pointer:
				require.Same(t, source.Interface(), cloned.Interface(),
					"pointer-typed Avro field %s must be shallow-copied", field.Name)
			case reflect.Map:
				// Mutating the source map must be visible through the clone.
				// This detects accidental deep copies in this hot path.
				key := reflect.Zero(source.Type().Key())
				source.SetMapIndex(key, reflect.Zero(source.Type().Elem()))
				require.True(t, cloned.MapIndex(key).IsValid(),
					"map-typed Avro field %s must be shallow-copied", field.Name)
			}
		} else {
			require.True(t, cloneVal.Field(i).IsZero(),
				"internal field %s must be reset", field.Name)
		}
	}
}

func TestMarshalAvroEntryDecimalPartitionRoundTrip(t *testing.T) {
	schema := NewSchema(0,
		NestedField{ID: 1, Name: "price", Type: MustDecimalTypeOf(10, 2)},
	)
	spec := NewPartitionSpecID(1,
		PartitionField{SourceIDs: []int{1}, FieldID: 1000, Name: "price", Transform: IdentityTransform{}},
	)
	want := Decimal{Val: decimal128.FromI64(-123), Scale: 2}
	builder, err := NewDataFileBuilder(
		spec,
		EntryContentData,
		"s3://bucket/ns/tbl/data/price=-1.23.parquet",
		ParquetFile,
		map[int]any{1000: want},
		nil,
		nil,
		1,
		1024,
	)
	require.NoError(t, err)
	df := builder.Build()

	// Builder partition values are already Iceberg-typed and are returned
	// directly; decimal scale reconstruction is only needed after Avro decode.
	require.Equal(t, want, df.Partition()[1000])

	encoded, err := df.(*dataFile).MarshalAvroEntry(spec, schema, 2)
	require.NoError(t, err)
	decoded, err := unmarshalAvroDataFileEntry(encoded, spec, schema, 2)
	require.NoError(t, err)

	got, ok := decoded.Partition()[1000].(DecimalLiteral)
	require.True(t, ok)
	require.True(t, got.Equals(DecimalLiteral(want)))
}

// snapshotAvroFields returns a deep copy of every avro-tagged field on
// d, keyed by field name. Slices, maps, byte arrays, and pointer
// targets are reconstructed so the snapshot is fully independent of d
// and stays stable across subsequent mutations of d.
func snapshotAvroFields(d *dataFile) map[string]any {
	out := make(map[string]any)
	v := reflect.ValueOf(d).Elem()
	t := v.Type()
	for i := range t.NumField() {
		f := t.Field(i)
		if _, ok := f.Tag.Lookup("avro"); !ok {
			continue
		}
		out[f.Name] = deepCopyReflect(v.Field(i)).Interface()
	}

	return out
}

// deepCopyReflect returns a reflect.Value whose pointer/slice/map
// descendants do not alias src. Primitives and the inner non-mutable
// leaves (interface payloads, function values) are copied by value.
func deepCopyReflect(src reflect.Value) reflect.Value {
	switch src.Kind() {
	case reflect.Pointer:
		if src.IsNil() {
			return src
		}
		dst := reflect.New(src.Elem().Type())
		dst.Elem().Set(deepCopyReflect(src.Elem()))

		return dst
	case reflect.Slice:
		if src.IsNil() {
			return src
		}
		dst := reflect.MakeSlice(src.Type(), src.Len(), src.Len())
		for i := range src.Len() {
			dst.Index(i).Set(deepCopyReflect(src.Index(i)))
		}

		return dst
	case reflect.Map:
		if src.IsNil() {
			return src
		}
		dst := reflect.MakeMapWithSize(src.Type(), src.Len())
		iter := src.MapRange()
		for iter.Next() {
			dst.SetMapIndex(deepCopyReflect(iter.Key()), deepCopyReflect(iter.Value()))
		}

		return dst
	case reflect.Struct:
		dst := reflect.New(src.Type()).Elem()
		for i := range src.NumField() {
			if dst.Field(i).CanSet() {
				dst.Field(i).Set(deepCopyReflect(src.Field(i)))
			}
		}

		return dst
	default:
		return src
	}
}

func fullyPopulatedDataFileForCodec(t testing.TB, version int) (PartitionSpec, *Schema, DataFile) {
	t.Helper()
	schema := NewSchema(123,
		NestedField{ID: 1, Name: "id", Type: Int64Type{}, Required: true},
		NestedField{ID: 2, Name: "name", Type: StringType{}},
	)
	spec := NewPartitionSpecID(7,
		PartitionField{SourceIDs: []int{1}, FieldID: 1000, Name: "id_part", Transform: IdentityTransform{}},
	)
	builder, err := NewDataFileBuilder(
		spec,
		EntryContentData,
		"s3://bucket/ns/tbl/data/part-0000.parquet",
		ParquetFile,
		map[int]any{1000: int64(42)},
		map[int]string{},
		map[int]int{},
		1024,
		1024*1024,
	)
	require.NoError(t, err)
	builder.
		ColumnSizes(map[int]int64{1: 512, 2: 256}).
		ValueCounts(map[int]int64{1: 1024, 2: 1024}).
		NullValueCounts(map[int]int64{1: 0, 2: 4}).
		NaNValueCounts(map[int]int64{1: 0, 2: 0}).
		LowerBoundValues(map[int][]byte{1: {0x01}, 2: []byte("a")}).
		UpperBoundValues(map[int][]byte{1: {0xff}, 2: []byte("z")}).
		SplitOffsets([]int64{0, 4096}).
		SortOrderID(0).
		KeyMetadata([]byte("kms-key-1"))
	if version >= 2 {
		builder.EqualityFieldIDs([]int{1})
	}
	if version >= 3 {
		builder.FirstRowID(0).
			ReferencedDataFile("s3://bucket/ns/tbl/data/source.parquet").
			ContentOffset(128).
			ContentSizeInBytes(2048)
	}

	df := builder.Build()
	if version < 3 {
		// distinct_counts is deprecated in the spec for all versions
		// (apache/iceberg#12182) and has no public setter. The fixture sets
		// the Avro field directly to cover the read-compatibility path for
		// legacy manifests, not to model what a new DataFile should carry.
		df.(*dataFile).DistinctCounts = mapToAvroColMap(map[int]int64{1: 64, 2: 128})
	}

	return spec, schema, df
}

var (
	benchmarkDataFileCloneSink *dataFile
	benchmarkEncodedEntrySize  int
)

func BenchmarkCloneDataFileAvroFields(b *testing.B) {
	_, _, df := fullyPopulatedDataFileForCodec(b, 2)
	impl := df.(*dataFile)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		benchmarkDataFileCloneSink = cloneDataFileAvroFields(impl)
	}
}

func BenchmarkMarshalAvroEntry(b *testing.B) {
	for _, version := range []int{1, 2, 3} {
		b.Run("v"+strconv.Itoa(version), func(b *testing.B) {
			spec, schema, df := fullyPopulatedDataFileForCodec(b, version)
			impl := df.(*dataFile)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				encoded, err := impl.MarshalAvroEntry(spec, schema, version)
				if err != nil {
					b.Fatal(err)
				}
				benchmarkEncodedEntrySize = len(encoded)
			}
		})
	}
}

func TestManifestEntrySchemaForMatchesPartitionAvroShape(t *testing.T) {
	types := []Type{
		Int32Type{},
		Int64Type{},
		Float32Type{},
		Float64Type{},
		StringType{},
		DateType{},
		TimeType{},
		TimestampType{},
		TimestampTzType{},
		UUIDType{},
		BooleanType{},
		BinaryType{},
		MustFixedTypeOf(8), MustFixedTypeOf(16),
		MustDecimalTypeOf(10, 2), MustDecimalTypeOf(11, 2), MustDecimalTypeOf(10, 3),
		UnknownType{},
	}
	for _, version := range []int{1, 2, 3} {
		for _, typ := range types {
			t.Run("v"+strconv.Itoa(version)+"/"+typ.String(), func(t *testing.T) {
				schema := NewSchema(1, NestedField{ID: 1, Name: "source", Type: typ})
				spec := NewPartitionSpec(PartitionField{
					SourceIDs: []int{1}, FieldID: 1000, Name: "partition", Transform: IdentityTransform{},
				})
				partition, err := partitionTypeToAvroSchema(spec.PartitionType(schema))
				require.NoError(t, err)
				want, err := internal.NewManifestEntrySchema(partition, version)
				require.NoError(t, err)
				for range 2 {
					got, maps, err := manifestEntrySchemaFor(spec, schema, version)
					require.NoError(t, err)
					require.Equal(t, want.String(), got.String())
					require.Equal(t, getFieldIDMap(want), maps)
				}
			})
		}
	}
}

func TestManifestEntrySchemaForPartitionShapeEquivalence(t *testing.T) {
	schema := NewSchema(1,
		NestedField{ID: 1, Name: "source", Type: Int32Type{}},
		NestedField{ID: 2, Name: "ts", Type: TimestampType{}},
	)
	field := PartitionField{SourceIDs: []int{1}, FieldID: 1000, Name: "partition", Transform: IdentityTransform{}}
	spec := NewPartitionSpec(field)
	original, _, err := manifestEntrySchemaFor(spec, schema, 2)
	require.NoError(t, err)

	for _, tc := range []struct {
		name   string
		spec   PartitionSpec
		schema *Schema
	}{
		{"spec_id", NewPartitionSpecID(99, field), schema},
		{"schema_metadata", spec, NewSchema(99,
			NestedField{
				ID: 1, Name: "renamed_source", Type: Int32Type{}, Required: true,
				Doc: "documentation", InitialDefault: int32(1), WriteDefault: int32(2),
			},
			NestedField{ID: 3, Name: "unrelated", Type: StringType{}},
		)},
		{"bucket", NewPartitionSpec(PartitionField{
			SourceIDs: []int{1}, FieldID: 1000, Name: "partition", Transform: BucketTransform{NumBuckets: 8},
		}), schema},
		{"year", NewPartitionSpec(PartitionField{
			SourceIDs: []int{2}, FieldID: 1000, Name: "partition", Transform: YearTransform{},
		}), schema},
		{"dropped_bucket_source", NewPartitionSpec(PartitionField{
			SourceIDs: []int{3}, FieldID: 1000, Name: "partition", Transform: BucketTransform{NumBuckets: 16},
		}), schema},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, _, err := manifestEntrySchemaFor(tc.spec, tc.schema, 2)
			require.NoError(t, err)
			require.Same(t, original, got)
		})
	}

	for _, tc := range []struct {
		name    string
		fields  []PartitionField
		version int
	}{
		{"field_id", []PartitionField{{SourceIDs: []int{1}, FieldID: 1001, Name: "partition", Transform: IdentityTransform{}}}, 2},
		{"field_name", []PartitionField{{SourceIDs: []int{1}, FieldID: 1000, Name: "renamed", Transform: IdentityTransform{}}}, 2},
		{"field_count", []PartitionField{field, {SourceIDs: []int{2}, FieldID: 1001, Name: "year", Transform: YearTransform{}}}, 2},
		{"empty", nil, 2},
		{"v1", []PartitionField{field}, 1},
		{"v3", []PartitionField{field}, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, _, err := manifestEntrySchemaFor(NewPartitionSpec(tc.fields...), schema, tc.version)
			require.NoError(t, err)
			require.NotSame(t, original, got)
		})
	}

	other := PartitionField{SourceIDs: []int{2}, FieldID: 1001, Name: "year", Transform: YearTransform{}}
	ordered, _, err := manifestEntrySchemaFor(NewPartitionSpec(field, other), schema, 2)
	require.NoError(t, err)
	reversed, _, err := manifestEntrySchemaFor(NewPartitionSpec(other, field), schema, 2)
	require.NoError(t, err)
	require.NotSame(t, ordered, reversed)
}

type unsupportedCodecPartitionType struct{ Int64Type }

func TestManifestEntrySchemaForRejectsInvalidTypesAfterCacheHit(t *testing.T) {
	spec := NewPartitionSpec(PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "partition", Transform: IdentityTransform{},
	})
	_, _, err := manifestEntrySchemaFor(spec, NewSchema(1, NestedField{ID: 1, Name: "source", Type: Int64Type{}}), 2)
	require.NoError(t, err)

	for _, typ := range []Type{
		unsupportedCodecPartitionType{},
		VariantType{},
		&StructType{}, &ListType{ElementID: 2, Element: Int64Type{}},
		&MapType{KeyID: 2, KeyType: StringType{}, ValueID: 3, ValueType: Int64Type{}},
	} {
		schema := NewSchema(1, NestedField{ID: 1, Name: "source", Type: typ})
		_, wantErr := partitionTypeToAvroSchema(spec.PartitionType(schema))
		require.Error(t, wantErr)
		_, _, err := manifestEntrySchemaFor(spec, schema, 2)
		require.EqualError(t, err, wantErr.Error())
	}
}

func TestManifestEntrySchemaForConcurrentTableLocalIDs(t *testing.T) {
	spec := NewPartitionSpec(PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "partition", Transform: IdentityTransform{},
	})
	schemas := []*Schema{
		NewSchema(1, NestedField{ID: 1, Name: "source", Type: Int64Type{}}),
		NewSchema(1, NestedField{ID: 1, Name: "source", Type: StringType{}}),
	}
	const workers = 32
	results := make([]struct {
		schema string
		err    error
	}, workers)
	var wg sync.WaitGroup
	for i := range results {
		wg.Go(func() {
			got, _, err := manifestEntrySchemaFor(spec, schemas[i%len(schemas)], 2)
			results[i].err = err
			if err == nil {
				results[i].schema = got.String()
			}
		})
	}
	wg.Wait()
	for i, result := range results {
		require.NoError(t, result.err)
		require.Equal(t, results[i%len(schemas)].schema, result.schema)
	}
	require.NotEqual(t, results[0].schema, results[1].schema)
}
