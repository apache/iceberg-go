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
	"fmt"
	"testing"

	"github.com/apache/iceberg-go/puffin"
)

var readAllBlobsBenchmarkSink []*puffin.BlobData

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
			if err := writer.Finish(); err != nil {
				b.Fatal(err)
			}

			reader, err := puffin.NewReader(bytes.NewReader(buf.Bytes()))
			if err != nil {
				b.Fatal(err)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				blobs, err := reader.ReadAllBlobs()
				if err != nil {
					b.Fatal(err)
				}
				readAllBlobsBenchmarkSink = blobs
			}
		})
	}
}
