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

package codec_test

import (
	"reflect"
	"sort"
	"strconv"
	"sync"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/codec"
	"github.com/stretchr/testify/require"
)

func TestEncodeDecodeDataFileRoundTrip(t *testing.T) {
	for _, version := range []int{1, 2, 3} {
		t.Run("v"+strconv.Itoa(version), func(t *testing.T) {
			spec, schema, original := fullyPopulatedDataFile(t, version)

			bytes, err := codec.EncodeDataFile(original, spec, schema, version)
			require.NoError(t, err)
			require.NotEmpty(t, bytes)

			decoded, err := codec.DecodeDataFile(bytes, spec, schema, version)
			require.NoError(t, err)
			require.NotNil(t, decoded)

			assertDataFileEqual(t, original, decoded, version)
		})
	}
}

func TestEncodeDataFileRejectsForeignImpl(t *testing.T) {
	spec, schema, _ := fullyPopulatedDataFile(t, 2)
	_, err := codec.EncodeDataFile(stubDataFile{}, spec, schema, 2)
	require.Error(t, err)
	require.Contains(t, err.Error(), "iceberg.AvroEntryMarshaler",
		"error should name the interface the caller needs to satisfy")
	require.Contains(t, err.Error(), "codec: EncodeDataFile:",
		"codec errors should be prefixed with the codec entry point name")
}

func TestEncodeDataFileRejectsBadVersion(t *testing.T) {
	spec, schema, df := fullyPopulatedDataFile(t, 2)
	for _, v := range []int{0, 4, -1} {
		_, err := codec.EncodeDataFile(df, spec, schema, v)
		require.Error(t, err, "version %d must be rejected", v)
		require.Contains(t, err.Error(), "codec: EncodeDataFile:")
		require.Contains(t, err.Error(), "unsupported format version")
	}
}

func TestDecodeDataFileRejectsBadVersion(t *testing.T) {
	spec, schema, df := fullyPopulatedDataFile(t, 2)
	encoded, err := codec.EncodeDataFile(df, spec, schema, 2)
	require.NoError(t, err)
	for _, v := range []int{0, 4, -1} {
		_, err := codec.DecodeDataFile(encoded, spec, schema, v)
		require.Error(t, err, "version %d must be rejected", v)
		require.Contains(t, err.Error(), "codec: DecodeDataFile:")
		require.Contains(t, err.Error(), "unsupported format version")
	}
}

func TestDecodeDataFileWrapsDecodeError(t *testing.T) {
	spec, schema, _ := fullyPopulatedDataFile(t, 2)
	_, err := codec.DecodeDataFile([]byte{0xCA, 0xFE}, spec, schema, 2)
	require.Error(t, err)
	require.Contains(t, err.Error(), "codec: DecodeDataFile:",
		"decode-side errors from the bridge should be wrapped with the codec entry point")
}

// TestEncodeDataFileAcceptsExternalMarshaler exercises the
// extensibility contract: any DataFile that also implements
// [iceberg.AvroEntryMarshaler] is accepted by the codec, not only the
// iceberg package's built-in implementation.
func TestEncodeDataFileAcceptsExternalMarshaler(t *testing.T) {
	spec, schema, _ := fullyPopulatedDataFile(t, 2)
	got, err := codec.EncodeDataFile(externalMarshaler{payload: []byte{0xCA, 0xFE}}, spec, schema, 2)
	require.NoError(t, err)
	require.Equal(t, []byte{0xCA, 0xFE}, got,
		"the marshaler interface contract must let external impls supply their own bytes")
}

type externalMarshaler struct {
	stubDataFile
	payload []byte
}

func (e externalMarshaler) MarshalAvroEntry(iceberg.PartitionSpec, *iceberg.Schema, int) ([]byte, error) {
	return e.payload, nil
}

func TestEncodeDataFileIdempotent(t *testing.T) {
	spec, schema, df := fullyPopulatedDataFile(t, 2)
	first, err := codec.EncodeDataFile(df, spec, schema, 2)
	require.NoError(t, err)
	second, err := codec.EncodeDataFile(df, spec, schema, 2)
	require.NoError(t, err)
	require.Equal(t, first, second, "repeated encodes must produce identical bytes")
}

func TestEncodeDataFileConcurrent(t *testing.T) {
	spec, schema, df := fullyPopulatedDataFile(t, 2)
	want, err := codec.EncodeDataFile(df, spec, schema, 2)
	require.NoError(t, err)

	const goroutines = 16
	const iterations = 32
	results := make(chan []byte, goroutines*iterations)
	errs := make(chan error, goroutines*iterations)
	var wg sync.WaitGroup
	for range goroutines {
		wg.Go(func() {
			for range iterations {
				b, err := codec.EncodeDataFile(df, spec, schema, 2)
				if err != nil {
					errs <- err

					return
				}
				results <- b
			}
		})
	}
	wg.Wait()
	close(results)
	close(errs)

	for err := range errs {
		t.Fatalf("concurrent encode failed: %v", err)
	}
	for b := range results {
		require.Equal(t, want, b, "concurrent encodes must produce identical bytes")
	}
}

func fullyPopulatedDataFile(t *testing.T, version int) (iceberg.PartitionSpec, *iceberg.Schema, iceberg.DataFile) {
	t.Helper()
	schema := iceberg.NewSchema(123,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.Int64Type{}, Required: true},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.StringType{}},
	)
	spec := iceberg.NewPartitionSpecID(7,
		iceberg.PartitionField{SourceIDs: []int{1}, FieldID: 1000, Name: "id_part", Transform: iceberg.IdentityTransform{}},
	)
	builder, err := iceberg.NewDataFileBuilder(
		spec,
		iceberg.EntryContentData,
		"s3://bucket/ns/tbl/data/part-0000.parquet",
		iceberg.ParquetFile,
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
	// distinct_counts (field 111) is deprecated in the spec for every
	// version (apache/iceberg#12182). The fixture populates it on every
	// version to assert that the wire codec drops the field on encode
	// regardless of what the in-memory DataFile carries — strict readers
	// (e.g. PyIceberg) reject manifests that emit it.
	builder.DistinctValueCounts(map[int]int64{1: 64, 2: 128})
	if version >= 2 {
		builder.EqualityFieldIDs([]int{1})
	}
	if version >= 3 {
		builder.FirstRowID(0).
			ReferencedDataFile("s3://bucket/ns/tbl/data/source.parquet").
			ContentOffset(128).
			ContentSizeInBytes(2048)
	}

	return spec, schema, builder.Build()
}

func assertDataFileEqual(t *testing.T, want, got iceberg.DataFile, version int) {
	t.Helper()
	require.Equal(t, want.FilePath(), got.FilePath())
	require.Equal(t, want.FileFormat(), got.FileFormat())
	require.Equal(t, want.Partition(), got.Partition())
	require.Equal(t, want.Count(), got.Count())
	require.Equal(t, want.FileSizeBytes(), got.FileSizeBytes())
	require.Equal(t, want.ColumnSizes(), got.ColumnSizes())
	require.Equal(t, want.ValueCounts(), got.ValueCounts())
	require.Equal(t, want.NullValueCounts(), got.NullValueCounts())
	require.Equal(t, want.NaNValueCounts(), got.NaNValueCounts())
	require.Equal(t, want.LowerBoundValues(), got.LowerBoundValues())
	require.Equal(t, want.UpperBoundValues(), got.UpperBoundValues())
	require.Equal(t, want.KeyMetadata(), got.KeyMetadata())
	require.Equal(t, want.SplitOffsets(), got.SplitOffsets())
	require.Equal(t, want.SortOrderID(), got.SortOrderID())
	require.Equal(t, want.SpecID(), got.SpecID())
	require.Equal(t, want.ContentType(), got.ContentType())
	require.Empty(t, got.DistinctValueCounts(),
		"distinct_counts (field 111) is deprecated in every version "+
			"(apache/iceberg#12182); the manifest-entry schemas in "+
			"internal/avro_schemas.go omit it for v1/v2/v3 so the encoder "+
			"drops it on the wire regardless of what the source DataFile carries")
	if version >= 2 {
		require.Equal(t, want.EqualityFieldIDs(), got.EqualityFieldIDs())
	}
	if version >= 3 {
		require.Equal(t, want.FirstRowID(), got.FirstRowID())
		require.Equal(t, want.ReferencedDataFile(), got.ReferencedDataFile())
		require.Equal(t, want.ContentOffset(), got.ContentOffset())
		require.Equal(t, want.ContentSizeInBytes(), got.ContentSizeInBytes())
	}

	require.Equal(t, expectedDataFileMethods, dataFileInterfaceMethods(),
		"DataFile interface drifted: either extend [codec.EncodeDataFile]/[codec.DecodeDataFile] "+
			"and the assertions above to cover the change, or update expectedDataFileMethods "+
			"with a comment explaining why the new method is intentionally not transported")
}

// expectedDataFileMethods is the sorted list of methods the
// [iceberg.DataFile] interface is known to export. The round-trip test
// asserts this against runtime reflection; any drift here forces a
// deliberate decision about whether the new method needs to be
// round-tripped by [codec.EncodeDataFile] / [codec.DecodeDataFile].
var expectedDataFileMethods = []string{
	"ColumnSizes",
	"ContentOffset",
	"ContentSizeInBytes",
	"ContentType",
	"Count",
	"DistinctValueCounts",
	"EqualityFieldIDs",
	"FileFormat",
	"FilePath",
	"FileSizeBytes",
	"FirstRowID",
	"KeyMetadata",
	"LowerBoundValues",
	"NaNValueCounts",
	"NullValueCounts",
	"Partition",
	"ReferencedDataFile",
	"SortOrderID",
	"SpecID",
	"SplitOffsets",
	"UpperBoundValues",
	"ValueCounts",
}

// dataFileInterfaceMethods returns the sorted names of every method on
// the DataFile interface. If a new method is added upstream the slice
// changes; the round-trip test then enforces it is covered by an
// explicit assertion above.
func dataFileInterfaceMethods() []string {
	t := reflect.TypeOf((*iceberg.DataFile)(nil)).Elem()
	out := make([]string, 0, t.NumMethod())
	for i := 0; i < t.NumMethod(); i++ {
		out = append(out, t.Method(i).Name)
	}
	sort.Strings(out)

	return out
}

type stubDataFile struct{}

func (stubDataFile) ContentType() iceberg.ManifestEntryContent { return iceberg.EntryContentData }
func (stubDataFile) FilePath() string                          { return "" }
func (stubDataFile) FileFormat() iceberg.FileFormat            { return iceberg.ParquetFile }
func (stubDataFile) Partition() map[int]any                    { return nil }
func (stubDataFile) Count() int64                              { return 0 }
func (stubDataFile) FileSizeBytes() int64                      { return 0 }
func (stubDataFile) ColumnSizes() map[int]int64                { return nil }
func (stubDataFile) ValueCounts() map[int]int64                { return nil }
func (stubDataFile) NullValueCounts() map[int]int64            { return nil }
func (stubDataFile) NaNValueCounts() map[int]int64             { return nil }
func (stubDataFile) DistinctValueCounts() map[int]int64        { return nil }
func (stubDataFile) LowerBoundValues() map[int][]byte          { return nil }
func (stubDataFile) UpperBoundValues() map[int][]byte          { return nil }
func (stubDataFile) KeyMetadata() []byte                       { return nil }
func (stubDataFile) SplitOffsets() []int64                     { return nil }
func (stubDataFile) EqualityFieldIDs() []int                   { return nil }
func (stubDataFile) SortOrderID() *int                         { return nil }
func (stubDataFile) SpecID() int32                             { return 0 }
func (stubDataFile) FirstRowID() *int64                        { return nil }
func (stubDataFile) ReferencedDataFile() *string               { return nil }
func (stubDataFile) ContentOffset() *int64                     { return nil }
func (stubDataFile) ContentSizeInBytes() *int64                { return nil }
