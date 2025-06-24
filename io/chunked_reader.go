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

package io

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
)

// ChunkedReader wraps a reader and handles AWS chunked transfer encoding.
// AWS S3 sometimes returns responses with chunked encoding, which needs to be
// decoded before passing to AVRO readers.
type ChunkedReader struct {
	r          io.Reader
	buf        *bufio.Reader
	chunkSize  int64
	remaining  int64
	isChunked  bool
	checkedEnc bool
}

// NewChunkedReader creates a new reader that handles AWS chunked encoding.
// It automatically detects if the input is chunked and decodes it transparently.
func NewChunkedReader(r io.Reader) *ChunkedReader {
	return &ChunkedReader{
		r:   r,
		buf: bufio.NewReader(r),
	}
}

// Read implements io.Reader, transparently handling chunked encoding if present.
func (cr *ChunkedReader) Read(p []byte) (int, error) {
	// First time reading, check if content is chunked
	if !cr.checkedEnc {
		if err := cr.detectChunkedEncoding(); err != nil {
			return 0, err
		}
		cr.checkedEnc = true
	}

	// If not chunked, just pass through
	if !cr.isChunked {
		return cr.buf.Read(p)
	}

	// Handle chunked encoding
	if cr.remaining == 0 {
		// Read next chunk size
		line, err := cr.buf.ReadString('\n')
		if err != nil {
			return 0, err
		}

		// Parse chunk size (hexadecimal)
		line = strings.TrimSpace(line)
		
		// Handle chunk extensions (e.g., "1a3;chunk-signature=...")
		if idx := strings.Index(line, ";"); idx != -1 {
			line = line[:idx]
		}

		size, err := strconv.ParseInt(line, 16, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid chunk size: %w", err)
		}

		// If size is 0, we've reached the end
		if size == 0 {
			// Read trailing headers until empty line
			for {
				trailer, err := cr.buf.ReadString('\n')
				if err != nil {
					return 0, err
				}
				if trailer == "\r\n" || trailer == "\n" {
					break
				}
			}
			return 0, io.EOF
		}

		cr.chunkSize = size
		cr.remaining = size
	}

	// Read from current chunk
	toRead := len(p)
	if int64(toRead) > cr.remaining {
		toRead = int(cr.remaining)
	}

	n, err := cr.buf.Read(p[:toRead])
	cr.remaining -= int64(n)

	// If we've read the entire chunk, consume the trailing CRLF
	if cr.remaining == 0 && err == nil {
		// Read the CRLF after the chunk data
		crlf := make([]byte, 2)
		_, err = io.ReadFull(cr.buf, crlf)
		if err != nil && err != io.EOF {
			return n, err
		}
	}

	return n, err
}

// detectChunkedEncoding checks if the input stream is using chunked encoding.
// For AWS S3, this is indicated by the presence of chunk size lines.
func (cr *ChunkedReader) detectChunkedEncoding() error {
	// Peek at the beginning of the stream
	peek, err := cr.buf.Peek(512)
	if err != nil && err != io.EOF {
		return err
	}

	// Check if it looks like AVRO magic bytes (raw content)
	if len(peek) >= 4 && bytes.Equal(peek[:4], []byte("Obj\x01")) {
		// This is raw AVRO content, not chunked
		cr.isChunked = false
		log.Printf("ChunkedReader: Detected raw AVRO content (magic bytes), not using chunked decoding")
		return nil
	}

	// Check if it looks like a chunk size (hexadecimal followed by optional extensions)
	// AWS chunked format starts with hex size followed by optional ;extensions
	firstLine := bytes.Split(peek, []byte("\n"))[0]
	firstLine = bytes.TrimSpace(firstLine)
	
	// Remove any chunk extensions
	if idx := bytes.Index(firstLine, []byte(";")); idx != -1 {
		firstLine = firstLine[:idx]
	}

	// Try to parse as hexadecimal
	if _, err := strconv.ParseInt(string(firstLine), 16, 64); err == nil {
		// Successfully parsed as hex, this is chunked encoding
		cr.isChunked = true
		log.Printf("ChunkedReader: Detected AWS chunked encoding (hex size: %s)", string(firstLine))
		return nil
	}

	// Default to not chunked
	cr.isChunked = false
	log.Printf("ChunkedReader: No chunked encoding detected, passing through raw content")
	return nil
}

// Close closes the underlying reader if it implements io.Closer.
func (cr *ChunkedReader) Close() error {
	if closer, ok := cr.r.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}