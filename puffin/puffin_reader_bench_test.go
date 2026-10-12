// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/apache/iceberg-go/puffin"
)

func BenchmarkReadAllBlobs(b *testing.B) {
	for _, blobCount := range []int{16, 256, 1024} {
		b.Run(fmt.Sprintf("blobs=%d", blobCount), func(b *testing.B) {
			var buf bytes.Buffer
			writer, err := puffin.NewWriter(&buf)
			if err != nil {
				b.Fatal(err)
			}

			payload := make([]byte, 64)
			for i := range blobCount {
				binary.LittleEndian.PutUint64(payload, uint64(i))
				_, err := writer.AddBlob(puffin.BlobMetadataInput{
					Type:       puffin.BlobTypeDataSketchesTheta,
					SnapshotID: int64(i),
					Fields:     []int32{1},
					Properties: map[string]string{"ndv": "1"},
				}, payload)
				if err != nil {
					b.Fatal(err)
				}
			}
			dataEnd := buf.Len()
			if err := writer.Finish(); err != nil {
				b.Fatal(err)
			}

			reader, err := puffin.NewReader(bytes.NewReader(buf.Bytes()))
			if err != nil {
				b.Fatal(err)
			}

			// Keep payloads in physical order and shuffle only the footer metadata.
			// This exercises the sort beyond its already-sorted fast path.
			footer := puffin.Footer{Blobs: reader.Blobs(), Properties: reader.Properties()}
			rand.New(rand.NewPCG(0, 0)).Shuffle(len(footer.Blobs), func(i, j int) {
				footer.Blobs[i], footer.Blobs[j] = footer.Blobs[j], footer.Blobs[i]
			})
			footerPayload, err := json.Marshal(footer)
			if err != nil {
				b.Fatal(err)
			}
			shuffled := append(bytes.Clone(buf.Bytes()[:dataEnd]), fileWithFooterPayload(footerPayload)[puffin.MagicSize:]...)

			for _, test := range []struct {
				name string
				data []byte
			}{
				{name: "sorted", data: buf.Bytes()},
				{name: "shuffled", data: shuffled},
			} {
				b.Run(test.name, func(b *testing.B) {
					reader, err := puffin.NewReader(bytes.NewReader(test.data))
					if err != nil {
						b.Fatal(err)
					}
					metadata := reader.Blobs()
					var blobs []*puffin.BlobData

					b.ReportAllocs()
					for b.Loop() {
						blobs, err = reader.ReadAllBlobs()
						if err != nil {
							b.Fatal(err)
						}
					}

					if len(blobs) != blobCount {
						b.Fatalf("got %d blobs, want %d", len(blobs), blobCount)
					}
					for i, blob := range blobs {
						if len(blob.Data) != len(payload) ||
							binary.LittleEndian.Uint64(blob.Data) != uint64(metadata[i].SnapshotID) ||
							blob.Metadata.SnapshotID != metadata[i].SnapshotID {
							b.Fatalf("blob %d does not match its footer entry", i)
						}
					}
				})
			}
		})
	}
}
