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

package puffin_test

//go:generate go run gen_dv_fixture.go

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/iceberg-go/puffin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// dvFixturePayloadName is the standalone Java-produced 64-bit roaring DV
// payload lifted from apache/iceberg's test resources. The inner shape per
// Iceberg spec: 4-byte BE length, 4-byte 0xD1D33964 magic, serialized
// roaring bitmap, 4-byte BE CRC32. Source:
// https://github.com/apache/iceberg/blob/main/core/src/test/resources/org/apache/iceberg/deletes/small-alternating-values-position-index.bin
const dvFixturePayloadName = "deletion-vector-v1-payload.bin"

// dvFixturePuffinName is the complete puffin file wrapping the Java-produced
// DV payload. The envelope is what puffin.Writer emits today; this is a
// Go-writer wire-format pin, not a strong Java cross-impl pin. (The basic
// puffin envelope shape is cross-checked by TestWriterBitIdenticalWithJava,
// but that test does not exercise empty Fields arrays or multi-key blob
// Properties, both of which this fixture relies on. JSON key ordering of
// blob Properties is also encoder-defined.) Regenerate via
// `go generate ./puffin/...` (driven by gen_dv_fixture.go).
const dvFixturePuffinName = "deletion-vector-v1.puffin"

// dvFixtureReferencedDataFile is the placeholder data-file path stored in
// the blob's properties. Spec: every DV blob carries `referenced-data-file`
// pointing at the parquet file it deletes from. The exact string is a fixture
// choice; matching against any specific Java-emitted file would require a
// matching string, which is not currently checked in upstream.
const dvFixtureReferencedDataFile = "data/test.parquet"

// dvFixtureCardinality is the cardinality property — the count of deleted
// row positions encoded inside the roaring bitmap. String form because puffin
// blob properties are stringly-typed (map[string]string). The bitmap encodes
// 5 positions: 1, 3, 5, 7, 9.
const dvFixtureCardinality = "5"

// buildDVFixture returns a puffin envelope wrapping the given Java-produced
// DV payload, with the canonical metadata for a deletion-vector-v1 blob.
// The same builder is used by both the regen path and the wire-format pin,
// so any drift in puffin.Writer surfaces as a byte-mismatch against the
// checked-in fixture rather than calcifying into a regenerated golden.
func buildDVFixture(t *testing.T, payload []byte) []byte {
	t.Helper()
	buf := &bytes.Buffer{}
	w, err := puffin.NewWriter(buf)
	require.NoError(t, err)
	require.NoError(t, w.SetCreatedBy("iceberg-go test fixture"))

	_, err = w.AddBlob(puffin.BlobMetadataInput{
		Type:           puffin.BlobTypeDeletionVector,
		SnapshotID:     -1,
		SequenceNumber: -1,
		Fields:         []int32{},
		Properties: map[string]string{
			"referenced-data-file": dvFixtureReferencedDataFile,
			"cardinality":          dvFixtureCardinality,
		},
	}, payload)
	require.NoError(t, err)
	require.NoError(t, w.Finish())

	return buf.Bytes()
}

// TestDeletionVectorPuffinWireFormat is a cross-implementation wire-format
// pin for puffin envelopes wrapping deletion-vector-v1 blobs. Two layers of
// guarantee:
//
//   - The inner roaring payload is byte-equal to a Java-produced fixture
//     lifted directly from apache/iceberg test resources. If the puffin
//     reader ever mangles blob bytes on the way out, this fails.
//
//   - The on-disk envelope bytes are byte-equal to what puffin.Writer
//     re-emits today for the same input. Any drift in the writer (footer
//     JSON shape, key ordering, properties handling, magic placement)
//     surfaces here instead of calcifying silently into the next regen.
//
// Independent of #866 (the roaring decoder PR) — uses raw bytes only.
func TestDeletionVectorPuffinWireFormat(t *testing.T) {
	puffinBytes, err := os.ReadFile(filepath.Join("testdata", dvFixturePuffinName))
	require.NoError(t, err, "fixture missing — regenerate with `go generate ./puffin/...`")

	expectedPayload, err := os.ReadFile(filepath.Join("testdata", dvFixturePayloadName))
	require.NoError(t, err)

	// Writer-side pin: the checked-in envelope must equal what puffin.Writer
	// produces today for the same input. Without this, writer regressions
	// slip through the read-side assertions because both sides drift
	// together.
	freshBytes := buildDVFixture(t, expectedPayload)
	if !bytes.Equal(freshBytes, puffinBytes) {
		diffAt := -1
		for i := 0; i < len(freshBytes) && i < len(puffinBytes); i++ {
			if freshBytes[i] != puffinBytes[i] {
				diffAt = i

				break
			}
		}
		t.Fatalf("checked-in envelope no longer matches puffin.Writer output. "+
			"First diff at byte %d (fixture=%d bytes, fresh=%d bytes). "+
			"Either a deliberate format change (regenerate with "+
			"`go generate ./puffin/...` and review the diff) or a writer regression.",
			diffAt, len(puffinBytes), len(freshBytes))
	}

	// Magic bytes at file head and tail.
	r, err := puffin.NewReader(bytes.NewReader(puffinBytes))
	require.NoError(t, err)

	// Read-side pin: blob count, type, spec-mandated invariants.
	blobs := r.Blobs()
	require.Len(t, blobs, 1, "fixture should contain exactly one DV blob")

	blob := blobs[0]
	assert.Equal(t, puffin.BlobTypeDeletionVector, blob.Type)
	assert.Equal(t, int64(-1), blob.SnapshotID,
		"deletion-vector-v1 spec requires snapshot-id=-1")
	assert.Equal(t, int64(-1), blob.SequenceNumber,
		"deletion-vector-v1 spec requires sequence-number=-1")
	// Strict empty (not nil): Java's parser rejects "fields": null. Asserting
	// the concrete []int32{} value catches a future regression that would
	// emit null instead of [].
	assert.Equal(t, []int32{}, blob.Fields,
		"DV blob fields must be an explicit empty array per spec")
	assert.Nil(t, blob.CompressionCodec,
		"this fixture is uncompressed (per fixture choice, not spec)")
	assert.Len(t, blob.Properties, 2,
		"DV blob should carry exactly the two spec-canonical properties; "+
			"a writer regression that emits extra keys must fail here too")
	assert.Equal(t, dvFixtureReferencedDataFile, blob.Properties["referenced-data-file"])
	assert.Equal(t, dvFixtureCardinality, blob.Properties["cardinality"])
	assert.Equal(t, int64(len(expectedPayload)), blob.Length,
		"blob length should equal the Java-produced payload")

	// Reader returns the inner payload byte-for-byte equal to the Java fixture.
	got, err := r.ReadBlob(0)
	require.NoError(t, err)
	assert.Equal(t, expectedPayload, got.Data,
		"reader should round-trip the Java-produced payload bytes unmodified")
}
