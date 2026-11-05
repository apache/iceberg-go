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

package puffin

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
)

func TestWriterStateGuardErrors(t *testing.T) {
	ops := []struct {
		name   string
		action string
		call   func(*Writer) error
	}{
		{"SetProperties", "set properties", func(w *Writer) error {
			return w.SetProperties(map[string]string{"key": "value"})
		}},
		{"ClearProperties", "clear properties", func(w *Writer) error {
			return w.ClearProperties()
		}},
		{"SetCreatedBy", "set created-by", func(w *Writer) error {
			return w.SetCreatedBy("")
		}},
		{"AddBlob", "add blob", func(w *Writer) error {
			_, err := w.AddBlob(BlobMetadataInput{}, nil)

			return err
		}},
		{"Finish", "finish", func(w *Writer) error {
			return w.Finish()
		}},
	}

	for _, op := range ops {
		t.Run(op.name, func(t *testing.T) {
			finalized, err := NewWriter(io.Discard)
			if err != nil {
				t.Fatal(err)
			}
			if err := finalized.Finish(); err != nil {
				t.Fatal(err)
			}

			wantFinalized := "puffin: cannot " + op.action + ": writer already finalized"
			if got := op.call(finalized); got == nil || got.Error() != wantFinalized {
				t.Fatalf("finalized: got %v, want %q", got, wantFinalized)
			}

			failed, err := NewWriter(io.Discard)
			if err != nil {
				t.Fatal(err)
			}
			underlying := errors.New("write failed")
			failed.failed = underlying
			wantFailed := "puffin: cannot " + op.action + ": writer previously failed: write failed"
			if got := op.call(failed); got == nil || got.Error() != wantFailed || !errors.Is(got, underlying) {
				t.Fatalf("failed: got %v, want error wrapping %q", got, wantFailed)
			}

			// When both flags are set, finalization still takes precedence.
			failed.done = true
			if got := op.call(failed); got == nil || got.Error() != wantFinalized {
				t.Fatalf("finalized and failed: got %v, want %q", got, wantFinalized)
			}
		})
	}
}

func TestWriterReportsCompletedSizes(t *testing.T) {
	var output bytes.Buffer
	writer, err := NewWriter(&output)
	if err != nil {
		t.Fatal(err)
	}
	if writer.FileSizeInBytes() != 0 || writer.FileFooterSizeInBytes() != 0 {
		t.Fatal("file sizes must be unavailable before Finish")
	}

	_, err = writer.AddBlob(BlobMetadataInput{
		Type:   BlobTypeDataSketchesTheta,
		Fields: []int32{1},
	}, []byte("blob"))
	if err != nil {
		t.Fatal(err)
	}
	if writer.FileSizeInBytes() != 0 || writer.FileFooterSizeInBytes() != 0 {
		t.Fatal("file sizes must be unavailable until Finish")
	}
	if err := writer.Finish(); err != nil {
		t.Fatal(err)
	}

	data := output.Bytes()
	trailer := data[len(data)-footerTrailerSize:]
	payloadSize := int64(binary.LittleEndian.Uint32(trailer[:4]))
	wantFooterSize := int64(MagicSize+footerTrailerSize) + payloadSize
	if got := writer.FileFooterSizeInBytes(); got != wantFooterSize {
		t.Fatalf("footer size = %d, want %d", got, wantFooterSize)
	}
	if got := writer.FileSizeInBytes(); got != int64(len(data)) {
		t.Fatalf("file size = %d, want %d", got, len(data))
	}
}

func TestWriterSizeAccessorsRoundTrip(t *testing.T) {
	cases := []struct {
		name  string
		codec string
		blob  bool
	}{
		{name: "no blobs"},
		{name: "lz4", codec: CompressionCodecLZ4, blob: true},
		{name: "zstd", codec: CompressionCodecZstd, blob: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var output bytes.Buffer
			writer, err := NewWriter(&output)
			if err != nil {
				t.Fatal(err)
			}

			input := bytes.Repeat([]byte("iceberg-puffin"), 64)
			if tc.blob {
				_, err := writer.AddBlob(BlobMetadataInput{
					Type:             BlobTypeDataSketchesTheta,
					Fields:           []int32{1},
					CompressionCodec: tc.codec,
				}, input)
				if err != nil {
					t.Fatal(err)
				}
			}

			if err := writer.Finish(); err != nil {
				t.Fatal(err)
			}
			reader, err := NewReader(bytes.NewReader(output.Bytes()))
			if err != nil {
				t.Fatal(err)
			}

			if got, want := writer.FileSizeInBytes(), int64(output.Len()); got != want {
				t.Fatalf("file size = %d, want %d", got, want)
			}
			if got, want := writer.FileFooterSizeInBytes(), int64(output.Len())-reader.footerStart; got != want {
				t.Fatalf("footer size = %d, want %d", got, want)
			}

			if tc.blob {
				result, err := reader.ReadBlob(0)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(result.Data, input) {
					t.Fatal("Puffin round trip changed blob contents")
				}
			} else if len(reader.Blobs()) != 0 {
				t.Fatal("empty Puffin file unexpectedly contains blobs")
			}
		})
	}
}
