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

import (
	"bytes"
	"testing"

	"github.com/apache/iceberg-go/puffin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// placeholderBlobType is exactly the same byte length as
// puffin.BlobTypeDeletionVector ("deletion-vector-v1", 18 chars) so a
// byte-replace in the footer JSON keeps the file's footer-payload-size field
// accurate. The puffin writer accepts arbitrary strings for non-DV blob types,
// so this synthetic type passes validation on the way out.
const placeholderBlobType puffin.BlobType = "filler-blob-type01"

// Compile-time guarantee that placeholderBlobType and the DV blob type have
// equal byte length, so the byte-replace in writeWithPatchedType doesn't
// shift the footer-payload-size trailer. Produces an "array bound must be
// non-negative" build error if the lengths ever diverge.
var _ = [len(puffin.BlobTypeDeletionVector) - len(placeholderBlobType)]byte{}

// rowPositionFieldID is the reserved Iceberg field ID for the row-position
// metadata column that a deletion vector covers. Spec-compliant DV blobs
// list this in `fields`; Java's BaseDVFileWriter always emits it.
const rowPositionFieldID int32 = 2147483546

// writeWithPatchedType writes a blob whose type is placeholderBlobType and
// carries the requested snapshot-id / sequence-number, then byte-rewrites the
// footer JSON to claim the blob is `deletion-vector-v1`. The puffin writer
// rejects DV-typed blobs that don't satisfy snapshot-id/sequence-number == -1,
// so this byte patch is how we construct a malformed puffin file the reader's
// validateBlobs must catch. Same-length swap keeps the footer-payload-size
// trailer field accurate.
func writeWithPatchedType(t *testing.T, snapshotID, sequenceNumber int64) []byte {
	t.Helper()

	buf := &bytes.Buffer{}
	w, err := puffin.NewWriter(buf)
	require.NoError(t, err)
	_, err = w.AddBlob(puffin.BlobMetadataInput{
		Type:           placeholderBlobType,
		SnapshotID:     snapshotID,
		SequenceNumber: sequenceNumber,
		Fields:         []int32{rowPositionFieldID},
	}, []byte("payload"))
	require.NoError(t, err)
	require.NoError(t, w.Finish())

	patched := bytes.Replace(
		buf.Bytes(),
		[]byte(`"type":"`+string(placeholderBlobType)+`"`),
		[]byte(`"type":"`+string(puffin.BlobTypeDeletionVector)+`"`),
		1,
	)
	require.NotEqual(t, buf.Bytes(), patched, "footer JSON did not contain the expected type substring")

	return patched
}

// TestReaderRejectsDeletionVectorWithBadHeader pins the reader-side enforcement
// of the puffin spec's `deletion-vector-v1` invariants: snapshot-id and
// sequence-number on the blob metadata MUST both be -1. The writer already
// rejects bad values on the way out; this is the symmetric reader-side check,
// so a malformed DV puffin produced by a third-party tool can't sneak
// arbitrary blob bytes into dv.DeserializeDV.
func TestReaderRejectsDeletionVectorWithBadHeader(t *testing.T) {
	t.Run("rejects snapshot-id != -1", func(t *testing.T) {
		_, err := puffin.NewReader(bytes.NewReader(writeWithPatchedType(t, 5, -1)))
		require.Error(t, err)
		// Assert on the actual value carried in the error (not the spec
		// substring), so the assertion fails if the formatter ever drops
		// the value rather than passing for the wrong reason.
		assert.Contains(t, err.Error(), "snapshot-id")
		assert.Contains(t, err.Error(), "got 5")
		assert.Contains(t, err.Error(), string(puffin.BlobTypeDeletionVector))
	})

	t.Run("rejects sequence-number != -1", func(t *testing.T) {
		_, err := puffin.NewReader(bytes.NewReader(writeWithPatchedType(t, -1, 10)))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "sequence-number")
		assert.Contains(t, err.Error(), "got 10")
		assert.Contains(t, err.Error(), string(puffin.BlobTypeDeletionVector))
	})

	t.Run("both fields wrong: snapshot-id check fires first", func(t *testing.T) {
		// Pins the precedence of the two checks. If a future refactor
		// reorders them, this test fails — the ordering is part of the
		// implicit contract for log/diagnostic consumers grepping for
		// "snapshot-id" before "sequence-number".
		_, err := puffin.NewReader(bytes.NewReader(writeWithPatchedType(t, 7, 13)))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "snapshot-id")
		assert.Contains(t, err.Error(), "got 7")
		assert.NotContains(t, err.Error(), "sequence-number",
			"snapshot-id check should short-circuit before sequence-number is reported")
	})

	t.Run("accepts well-formed deletion-vector-v1 blob", func(t *testing.T) {
		buf := &bytes.Buffer{}
		w, err := puffin.NewWriter(buf)
		require.NoError(t, err)
		_, err = w.AddBlob(puffin.BlobMetadataInput{
			Type:           puffin.BlobTypeDeletionVector,
			SnapshotID:     -1,
			SequenceNumber: -1,
			Fields:         []int32{rowPositionFieldID},
			Properties: map[string]string{
				"cardinality":          "0",
				"referenced-data-file": "data/x.parquet",
			},
		}, []byte("payload"))
		require.NoError(t, err)
		require.NoError(t, w.Finish())

		_, err = puffin.NewReader(bytes.NewReader(buf.Bytes()))
		require.NoError(t, err)
	})
}
