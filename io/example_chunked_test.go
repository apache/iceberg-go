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

package io_test

import (
	"fmt"
	"strings"

	"github.com/apache/iceberg-go/io"
)

// Example demonstrates how the chunked reader handles AWS chunked transfer encoding
func ExampleChunkedReader() {
	// Simulate AWS S3 response with chunked encoding
	chunkedData := "1a;chunk-signature=abc123\r\n" +
		"abcdefghijklmnopqrstuvwxyz\r\n" +
		"0;chunk-signature=final\r\n\r\n"

	reader := io.NewChunkedReader(strings.NewReader(chunkedData))

	// Read the data - it will be transparently decoded
	buf := make([]byte, 100)
	n, _ := reader.Read(buf)

	fmt.Printf("Read %d bytes: %s\n", n, string(buf[:n]))
	// Output: Read 26 bytes: abcdefghijklmnopqrstuvwxyz
}

// Example_blobFileIO demonstrates how AVRO files are automatically handled
func Example_blobFileIO() {
	// When opening an AVRO file through the blob file system,
	// the chunked reader is automatically applied

	// This would happen internally when reading manifest files:
	// fs.Open("s3://bucket/path/to/manifest.avro")
	// The file would be wrapped with ChunkedReader automatically

	fmt.Println("AVRO files are automatically wrapped with ChunkedReader")
	// Output: AVRO files are automatically wrapped with ChunkedReader
}
