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

//go:build ignore

// Regenerates puffin/testdata/deletion-vector-v1.puffin from the Java-
// produced inner payload (deletion-vector-v1-payload.bin). Invoked via
//
//	go generate ./puffin/...
//
// After regen, diff the file before committing to confirm only the
// intended bytes changed:
//
//	git diff -- puffin/testdata/deletion-vector-v1.puffin
package main

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/apache/iceberg-go/puffin"
)

const (
	payloadName        = "deletion-vector-v1-payload.bin"
	puffinName         = "deletion-vector-v1.puffin"
	referencedDataFile = "data/test.parquet"
	cardinality        = "5"
)

func main() {
	// go generate runs in the directory of the file carrying the
	// //go:generate directive (puffin/); testdata sits alongside.
	payloadPath := filepath.Join("testdata", payloadName)
	outPath := filepath.Join("testdata", puffinName)

	payload, err := os.ReadFile(payloadPath)
	if err != nil {
		log.Fatalf("read payload: %v", err)
	}

	buf := &bytes.Buffer{}
	w, err := puffin.NewWriter(buf)
	if err != nil {
		log.Fatalf("new writer: %v", err)
	}
	if err := w.SetCreatedBy("iceberg-go test fixture"); err != nil {
		log.Fatalf("set created-by: %v", err)
	}
	if _, err := w.AddBlob(puffin.BlobMetadataInput{
		Type:           puffin.BlobTypeDeletionVector,
		SnapshotID:     -1,
		SequenceNumber: -1,
		Fields:         []int32{},
		Properties: map[string]string{
			"referenced-data-file": referencedDataFile,
			"cardinality":          cardinality,
		},
	}, payload); err != nil {
		log.Fatalf("add blob: %v", err)
	}
	if err := w.Finish(); err != nil {
		log.Fatalf("finish: %v", err)
	}

	// Self-validate before overwriting: parse what we just produced, read
	// the blob back, and confirm both the envelope-level invariants and
	// the inner-payload bytes survive the round-trip. Without the ReadBlob
	// step a writer bug producing valid-but-mismatched blob offsets/lengths
	// would still pass parse-only validation and calcify into the fixture.
	r, err := puffin.NewReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		log.Fatalf("regen produced an unreadable puffin file: %v", err)
	}
	if n := len(r.Blobs()); n != 1 {
		log.Fatalf("regen produced %d blobs, want 1", n)
	}
	if got, want := r.Blobs()[0].Type, puffin.BlobTypeDeletionVector; got != want {
		log.Fatalf("regen produced blob type %q, want %q", got, want)
	}
	got, err := r.ReadBlob(0)
	if err != nil {
		log.Fatalf("regen produced an unreadable blob: %v", err)
	}
	if !bytes.Equal(payload, got.Data) {
		log.Fatal("regen round-trip mangled the inner payload")
	}

	if err := os.WriteFile(outPath, buf.Bytes(), 0o644); err != nil {
		log.Fatalf("write fixture: %v", err)
	}
	fmt.Printf("wrote %s (%d bytes)\n", outPath, buf.Len())
}
