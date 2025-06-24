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
	"bytes"
	"io"
	"strings"
	"testing"
)

func TestChunkedReader(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		wantErr  bool
	}{
		{
			name:     "raw avro content",
			input:    "Obj\x01\x00\x00\x00\x00\x00\x00\x00\x00",
			expected: "Obj\x01\x00\x00\x00\x00\x00\x00\x00\x00",
			wantErr:  false,
		},
		{
			name: "simple chunked content",
			input: "5\r\nhello\r\n" +
				"6\r\n world\r\n" +
				"0\r\n\r\n",
			expected: "hello world",
			wantErr:  false,
		},
		{
			name: "chunked with extensions",
			input: "5;chunk-signature=abc123\r\nhello\r\n" +
				"6;chunk-signature=def456\r\n world\r\n" +
				"0;chunk-signature=final\r\n\r\n",
			expected: "hello world",
			wantErr:  false,
		},
		{
			name: "chunked with larger content",
			input: "1a\r\nabcdefghijklmnopqrstuvwxyz\r\n" +
				"10\r\n0123456789ABCDEF\r\n" +
				"0\r\n\r\n",
			expected: "abcdefghijklmnopqrstuvwxyz0123456789ABCDEF",
			wantErr:  false,
		},
		{
			name:     "non-chunked content",
			input:    "This is plain text content without any chunking",
			expected: "This is plain text content without any chunking",
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := NewChunkedReader(strings.NewReader(tt.input))
			
			// Read all content
			var buf bytes.Buffer
			_, err := io.Copy(&buf, reader)
			
			if (err != nil && err != io.EOF) != tt.wantErr {
				t.Errorf("ChunkedReader.Read() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			
			if got := buf.String(); got != tt.expected {
				t.Errorf("ChunkedReader.Read() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestChunkedReaderPartialReads(t *testing.T) {
	// Test reading in small chunks
	input := "5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n"
	expected := "hello world"
	
	reader := NewChunkedReader(strings.NewReader(input))
	
	// Read in 3-byte chunks
	var result []byte
	buf := make([]byte, 3)
	
	for {
		n, err := reader.Read(buf)
		if n > 0 {
			result = append(result, buf[:n]...)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}
	
	if string(result) != expected {
		t.Errorf("Got %q, want %q", string(result), expected)
	}
}

func TestChunkedReaderAWSFormat(t *testing.T) {
	// Test with actual AWS chunked encoding format
	// AWS uses chunk-signature extensions
	input := "400;chunk-signature=0055627c9e194cb4542bae2aa5492e3c1575bbb81b612b7d234b86a503ef5497\r\n" +
		strings.Repeat("x", 1024) + "\r\n" +
		"0;chunk-signature=b6c6ea8a5354eaf15b3cb7646744f4275b71ea724fed81ceb9323e279d449df9\r\n\r\n"
	
	expected := strings.Repeat("x", 1024)
	
	reader := NewChunkedReader(strings.NewReader(input))
	
	// Read all content
	var buf bytes.Buffer
	_, err := io.Copy(&buf, reader)
	
	if err != nil && err != io.EOF {
		t.Errorf("Unexpected error: %v", err)
	}
	
	if got := buf.String(); got != expected {
		t.Errorf("Got %d bytes, want %d bytes", len(got), len(expected))
	}
}