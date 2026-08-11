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
	"bytes"
	"maps"
	"os"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// rewriteManifestListMetadata decodes the metadata map from an OCF header,
// applies mutate to it, and re-encodes the header; everything from the sync
// marker onward is copied through byte-identical. Stripping the optional
// "format-version" entry this way reproduces the header shape of writers
// that omit the key (e.g. DuckDB's iceberg extension).
func rewriteManifestListMetadata(t *testing.T, data []byte, mutate func(map[string][]byte)) []byte {
	t.Helper()

	readLong := func(off int) (int64, int) {
		var u uint64
		var shift uint
		for {
			require.Less(t, off, len(data), "truncated varint in OCF header")
			b := data[off]
			off++
			u |= uint64(b&0x7f) << shift
			if b&0x80 == 0 {
				break
			}
			shift += 7
		}

		return int64(u>>1) ^ -int64(u&1), off
	}
	readBytes := func(off int) ([]byte, int) {
		n, off := readLong(off)

		return data[off : off+int(n)], off + int(n)
	}
	appendLong := func(dst []byte, v int64) []byte {
		u := uint64(v<<1) ^ uint64(v>>63)
		for u >= 0x80 {
			dst = append(dst, byte(u)|0x80)
			u >>= 7
		}

		return append(dst, byte(u))
	}

	require.True(t, bytes.HasPrefix(data, []byte("Obj\x01")), "not an OCF file")

	// The file metadata is an Avro map<bytes>: blocks of (count, count pairs
	// of string key and bytes value), terminated by a zero count. A negative
	// count is followed by the block's byte size.
	off := 4
	meta := map[string][]byte{}
	for {
		var n int64
		n, off = readLong(off)
		if n == 0 {
			break
		}
		if n < 0 {
			n = -n
			_, off = readLong(off)
		}
		for range n {
			var k, v []byte
			k, off = readBytes(off)
			v, off = readBytes(off)
			meta[string(k)] = v
		}
	}

	mutate(meta)

	out := append([]byte(nil), data[:4]...)
	out = appendLong(out, int64(len(meta)))
	for _, k := range slices.Sorted(maps.Keys(meta)) {
		out = appendLong(out, int64(len(k)))
		out = append(out, k...)
		out = appendLong(out, int64(len(meta[k])))
		out = append(out, meta[k]...)
	}
	out = append(out, 0)

	return append(out, data[off:]...)
}

func stripFormatVersion(t *testing.T, data []byte) []byte {
	t.Helper()

	return rewriteManifestListMetadata(t, data, func(meta map[string][]byte) {
		_, ok := meta["format-version"]
		require.True(t, ok, "manifest list carries no format-version to strip")
		delete(meta, "format-version")
	})
}

// TestReadManifestListInfersVersionWhenKeyAbsent covers the fix for a v2/v3
// manifest list that omits the optional "format-version" metadata key: the
// version must be inferred from the embedded writer schema instead of
// falling back to v1, which silently zeroed content, sequence_number,
// min_sequence_number, and first_row_id.
func TestReadManifestListInfersVersionWhenKeyAbsent(t *testing.T) {
	t.Run("v2", func(t *testing.T) {
		deletes := NewManifestFile(2, "s3://bucket/metadata/deletes-m0.avro", 100, 0, 42).
			Content(ManifestContentDeletes).
			SequenceNum(5, 3).
			Build()

		var buf bytes.Buffer
		seq := int64(5)
		require.NoError(t, WriteManifestList(2, &buf, 42, nil, &seq, 0, []ManifestFile{deletes}))

		got, err := ReadManifestList(bytes.NewReader(stripFormatVersion(t, buf.Bytes())))
		require.NoError(t, err)
		require.Len(t, got, 1)

		assert.Equal(t, 2, got[0].Version())
		assert.Equal(t, ManifestContentDeletes, got[0].ManifestContent())
		assert.Equal(t, int64(5), got[0].SequenceNum())
		assert.Equal(t, int64(3), got[0].MinSequenceNum())
		assert.Equal(t, int64(42), got[0].SnapshotID())
	})

	t.Run("v3", func(t *testing.T) {
		data := NewManifestFile(3, "s3://bucket/metadata/data-m0.avro", 100, 0, 42).
			AddedRows(10).
			SequenceNum(5, 5).
			Build()
		deletes := NewManifestFile(3, "s3://bucket/metadata/deletes-m1.avro", 100, 0, 42).
			Content(ManifestContentDeletes).
			SequenceNum(5, 3).
			Build()

		var buf bytes.Buffer
		seq := int64(5)
		require.NoError(t, WriteManifestList(3, &buf, 42, nil, &seq, 100, []ManifestFile{data, deletes}))

		got, err := ReadManifestList(bytes.NewReader(stripFormatVersion(t, buf.Bytes())))
		require.NoError(t, err)
		require.Len(t, got, 2)

		assert.Equal(t, 3, got[0].Version())
		assert.Equal(t, ManifestContentData, got[0].ManifestContent())
		require.NotNil(t, got[0].FirstRowID())
		assert.Equal(t, int64(100), *got[0].FirstRowID())

		assert.Equal(t, ManifestContentDeletes, got[1].ManifestContent())
		assert.Equal(t, int64(5), got[1].SequenceNum())
		assert.Equal(t, int64(3), got[1].MinSequenceNum())
		assert.Nil(t, got[1].FirstRowID())
	})
}

// TestReadManifestListKeylessV1ReadsAsV1 pins the pre-existing behavior: a
// genuine v1 manifest list without the optional key must still decode as v1,
// identically to the same list with the key present.
func TestReadManifestListKeylessV1ReadsAsV1(t *testing.T) {
	mf := NewManifestFile(1, "s3://bucket/metadata/data-m0.avro", 100, 0, 7).
		AddedFiles(1).
		AddedRows(10).
		Build()

	var buf bytes.Buffer
	require.NoError(t, WriteManifestList(1, &buf, 7, nil, nil, 0, []ManifestFile{mf}))

	withKey, err := ReadManifestList(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	withoutKey, err := ReadManifestList(bytes.NewReader(stripFormatVersion(t, buf.Bytes())))
	require.NoError(t, err)

	require.Len(t, withoutKey, 1)
	assert.Equal(t, 1, withoutKey[0].Version())
	assert.Equal(t, withKey, withoutKey)
}

// TestReadManifestListDuckDBFixture reads a real v3 manifest list written by
// DuckDB's iceberg extension (v1.5.4 with format-version 3, one INSERT
// followed by one DELETE), which omits the optional "format-version" metadata
// key. Under the previous v1 fallback this file's delete manifest read as a
// data manifest and every sequence number read as 0.
func TestReadManifestListDuckDBFixture(t *testing.T) {
	data, err := os.ReadFile("testdata/duckdb_v3_manifest_list.avro")
	require.NoError(t, err)

	got, err := ReadManifestList(bytes.NewReader(data))
	require.NoError(t, err)
	require.Len(t, got, 2)

	assert.Equal(t, 3, got[0].Version())
	assert.Equal(t, ManifestContentData, got[0].ManifestContent())
	assert.Equal(t, int64(1), got[0].SequenceNum())
	assert.Equal(t, int64(1), got[0].MinSequenceNum())
	require.NotNil(t, got[0].FirstRowID())
	assert.Equal(t, int64(0), *got[0].FirstRowID())

	assert.Equal(t, 3, got[1].Version())
	assert.Equal(t, ManifestContentDeletes, got[1].ManifestContent())
	assert.Equal(t, int64(2), got[1].SequenceNum())
	assert.Equal(t, int64(2), got[1].MinSequenceNum())
}

// TestReadManifestListKeyContradictsSchema: a "format-version" key claiming
// v1 for a list whose writer schema carries v2+/v3 fields must be an error;
// reading it through the v1 schema would silently drop those fields.
func TestReadManifestListKeyContradictsSchema(t *testing.T) {
	for _, version := range []int{2, 3} {
		mf := NewManifestFile(version, "s3://bucket/metadata/deletes-m0.avro", 100, 0, 42).
			Content(ManifestContentDeletes).
			SequenceNum(5, 3).
			Build()

		var buf bytes.Buffer
		seq := int64(5)
		require.NoError(t, WriteManifestList(version, &buf, 42, nil, &seq, 0, []ManifestFile{mf}))

		lied := rewriteManifestListMetadata(t, buf.Bytes(), func(meta map[string][]byte) {
			meta["format-version"] = []byte("1")
		})

		_, err := ReadManifestList(bytes.NewReader(lied))
		assert.ErrorContains(t, err, "'format-version' metadata says 1", "v%d", version)
	}
}
