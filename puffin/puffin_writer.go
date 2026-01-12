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

import (
	"fmt"
	"io"
)

// PuffinWriter writes a Puffin file to an output stream.
type PuffinWriter struct {
	w         io.Writer
	offset    int64
	blobs     []BlobMetadata
	props     map[string]string
	done      bool
	createdBy string
}

type BlobMetadataInput struct{}

// NewWriter creates a new PuffinWriter.
func NewWriter(w io.Writer) (*PuffinWriter, error) {
	if w == nil {
		return nil, fmt.Errorf("puffin: writer is nil")
	}
	return &PuffinWriter{
		w:         w,
		offset:    0,
		blobs:     make([]BlobMetadata, 0),
		props:     make(map[string]string),
		done:      false,
		createdBy: "iceberg-go",
	}, nil
}

func (w *PuffinWriter) AddBlob() {}

func (w *PuffinReader) Finish() {}
