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
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gocloud.dev/blob/memblob"
)

func TestDefaultKeyExtractor(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expectedKey string
		shouldError bool
	}{
		{
			name:        "s3 URI with path",
			input:       "s3://my-bucket/path/to/file.parquet",
			expectedKey: "path/to/file.parquet",
		},
		{
			name:        "s3a URI with path",
			input:       "s3a://my-bucket/path/to/file.parquet",
			expectedKey: "path/to/file.parquet",
		},
		{
			name:        "s3n URI with path",
			input:       "s3n://my-bucket/path/to/file.parquet",
			expectedKey: "path/to/file.parquet",
		},
		{
			name:        "gs URI with path",
			input:       "gs://my-bucket/path/to/file.parquet",
			expectedKey: "path/to/file.parquet",
		},
		{
			name:        "URI with query and fragment",
			input:       "s3://my-bucket/path/to/file.parquet?param=value#fragment",
			expectedKey: "path/to/file.parquet?param=value#fragment",
		},
		{
			name:        "URI with empty path",
			input:       "s3://my-bucket/",
			shouldError: true,
		},
	}

	extractor := defaultKeyExtractor("my-bucket")
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			key, err := extractor(test.input)

			if test.shouldError {
				assert.Error(t, err, "Expected error for input: %s", test.input)
			} else {
				assert.NoError(t, err, "Unexpected error for input: %s", test.input)
				assert.Equal(t, test.expectedKey, key, "Key mismatch for input: %s", test.input)
			}
		})
	}
}

type trackingReadCloser struct {
	io.ReadCloser
	closed *bool
}

func (t *trackingReadCloser) Close() error {
	*t.closed = true

	return t.ReadCloser.Close()
}

func TestReadAtResourceCleanup(t *testing.T) {
	ctx := context.Background()

	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()

	content := []byte("short")
	err := bucket.WriteAll(ctx, "test-file", content, nil)
	require.NoError(t, err)

	tests := []struct {
		name    string
		offset  int64
		readLen int
		wantN   int
		wantErr error
	}{
		{
			name:    "success full read",
			offset:  0,
			readLen: len(content),
			wantN:   len(content),
			wantErr: nil,
		},
		{
			name:    "partial read /unexpected EOF",
			offset:  2,
			readLen: 2 * len(content),
			wantN:   len(content) - 2,
			wantErr: io.ErrUnexpectedEOF,
		},
		{
			name:    "EOF read at end of file",
			offset:  int64(len(content)),
			readLen: 2 * len(content),
			wantN:   0,
			wantErr: io.EOF,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var lastReaderClosed bool
			bfs := &blobFileIO{
				Bucket:       bucket,
				keyExtractor: func(path string) (string, error) { return path, nil },
				ctx:          ctx,
				newRangeReader: func(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
					r, err := bucket.NewRangeReader(ctx, key, offset, length, nil)
					if err != nil {
						return nil, err
					}
					lastReaderClosed = false

					return &trackingReadCloser{ReadCloser: r, closed: &lastReaderClosed}, nil
				},
			}

			file, err := bfs.Open("test-file")
			require.NoError(t, err)
			defer file.Close()

			bof := file.(*blobOpenFile)

			buf := make([]byte, tt.readLen)
			n, err := bof.ReadAt(buf, tt.offset)

			assert.Equal(t, tt.wantN, n, "read byte count mismatch")
			if tt.wantErr == nil {
				assert.NoError(t, err)
			} else {
				assert.ErrorIs(t, err, tt.wantErr)
			}

			assert.True(t, lastReaderClosed, "resource leak: range reader was not closed")
		})
	}
}
