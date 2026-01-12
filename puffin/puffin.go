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
package puffin

var magic = [4]byte{'P', 'F', 'A', '1'}

type BlobMetadata struct {
	Type             string            `json:"type"`
	SnapshotID       int64             `json:"snapshot-id"`
	SequenceNumber   int64             `json:"sequence-number"`
	Fields           []int32           `json:"fields,omitempty"`
	Offset           int64             `json:"offset"` // absolute file offset
	Length           int64             `json:"length"`
	CompressionCodec *string           `json:"compression-codec,omitempty"`
	Properties       map[string]string `json:"properties,omitempty"`
}

// Footer describes the blobs and file-level properties stored in a Puffin file.
type Footer struct {
	Blobs      []BlobMetadata    `json:"blobs"`
	Properties map[string]string `json:"properties,omitempty"`
}
