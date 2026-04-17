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

package gocloud

import (
	"context"
	"io"
	"io/fs"
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

func TestNewWriterExistsError(t *testing.T) {
	ctx := context.Background()

	bucket := memblob.OpenBucket(nil)

	bfs := &blobFileIO{
		Bucket:       bucket,
		keyExtractor: func(path string) (string, error) { return path, nil },
		ctx:          ctx,
	}
	require.NoError(t, bucket.Close())

	_, err := bfs.NewWriter(ctx, "test-file", false, nil)

	var pathErr *fs.PathError
	require.ErrorAs(t, err, &pathErr, "error should be a PathError wrapping the Exists failure")
	require.Equal(t, "new writer", pathErr.Op)
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

func TestBlobFileIOWalkDir(t *testing.T) {
	ctx := context.Background()

	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()

	files := []string{
		"data/file1.parquet",
		"data/file2.parquet",
		"metadata/snap-123.avro",
	}
	for _, f := range files {
		require.NoError(t, bucket.WriteAll(ctx, f, []byte("content"), nil))
	}

	bfs := &blobFileIO{
		Bucket:       bucket,
		keyExtractor: defaultKeyExtractor("test-bucket"),
		ctx:          ctx,
	}

	var walked []string
	err := bfs.WalkDir("s3://test-bucket/", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if !d.IsDir() {
			walked = append(walked, path)
		}

		return nil
	})
	require.NoError(t, err)

	expected := []string{
		"s3://test-bucket/data/file1.parquet",
		"s3://test-bucket/data/file2.parquet",
		"s3://test-bucket/metadata/snap-123.avro",
	}
	assert.ElementsMatch(t, expected, walked)
}

func TestBlobFileIOWalkDirSubPath(t *testing.T) {
	ctx := context.Background()

	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()

	require.NoError(t, bucket.WriteAll(ctx, "data/file1.parquet", []byte("a"), nil))
	require.NoError(t, bucket.WriteAll(ctx, "data/file2.parquet", []byte("b"), nil))
	require.NoError(t, bucket.WriteAll(ctx, "metadata/v1.json", []byte("c"), nil))

	bfs := &blobFileIO{
		Bucket:       bucket,
		keyExtractor: defaultKeyExtractor("mybucket"),
		ctx:          ctx,
	}

	var walked []string
	err := bfs.WalkDir("s3://mybucket/data", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if !d.IsDir() {
			walked = append(walked, path)
		}

		return nil
	})
	require.NoError(t, err)

	expected := []string{
		"s3://mybucket/data/file1.parquet",
		"s3://mybucket/data/file2.parquet",
	}
	assert.ElementsMatch(t, expected, walked)
}

func TestBlobFileIOWalkDirAzureURI(t *testing.T) {
	ctx := context.Background()

	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()

	require.NoError(t, bucket.WriteAll(ctx, "path/to/file.parquet", []byte("data"), nil))

	bfs := &blobFileIO{
		Bucket:       bucket,
		keyExtractor: defaultKeyExtractor("container@account.dfs.core.windows.net"),
		ctx:          ctx,
	}

	var walked []string
	err := bfs.WalkDir("abfs://container@account.dfs.core.windows.net/path", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if !d.IsDir() {
			walked = append(walked, path)
		}

		return nil
	})
	require.NoError(t, err)

	expected := []string{
		"abfs://container@account.dfs.core.windows.net/path/to/file.parquet",
	}
	assert.Equal(t, expected, walked)
}
