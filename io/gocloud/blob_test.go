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
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	icebergio "github.com/apache/iceberg-go/io"
	"gocloud.dev/blob"
	"gocloud.dev/blob/memblob"
)

func TestDefaultKeyExtractor(t *testing.T) {
	tests := []struct {
		name            string
		bucketName      string
		input           string
		expectedKey     string
		wantErrContains string
		wantErrIs       error
	}{
		{
			name:        "relative key",
			input:       "path/to/file.parquet",
			expectedKey: "path/to/file.parquet",
		},
		{
			name:        "relative key with raw percent",
			input:       "data/100%off/file.parquet",
			expectedKey: "data/100%off/file.parquet",
		},
		{
			name:        "bucket-prefixed relative key",
			input:       "my-bucket/path/to/file.parquet",
			expectedKey: "path/to/file.parquet",
		},
		{
			name:        "s3 URI with path",
			input:       "s3://my-bucket/path/to/file.parquet",
			expectedKey: "path/to/file.parquet",
		},
		{
			name:        "s3 URI with raw percent in key",
			input:       "s3://my-bucket/data/100%off/file.parquet",
			expectedKey: "data/100%off/file.parquet",
		},
		{
			name:        "s3 URI with raw space in key",
			input:       "s3://my-bucket/data/city=New York/file.parquet",
			expectedKey: "data/city=New York/file.parquet",
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
			name:        "azure URI with path",
			bucketName:  "container@account.dfs.core.windows.net",
			input:       "abfs://container@account.dfs.core.windows.net/path/to/file.parquet",
			expectedKey: "path/to/file.parquet",
		},
		{
			name:            "s3 URI with different bucket",
			input:           "s3://other-bucket/path/to/file.parquet",
			wantErrContains: "does not match configured authority",
		},
		{
			name:            "gs URI with different bucket",
			input:           "gs://other-bucket/path/to/file.parquet",
			wantErrContains: "does not match configured authority",
		},
		{
			name:        "URI with query and fragment",
			input:       "s3://my-bucket/path/to/file.parquet?param=value#fragment",
			expectedKey: "path/to/file.parquet?param=value#fragment",
		},
		{
			name:            "URI with empty path",
			input:           "s3://my-bucket/",
			wantErrContains: "object key is empty",
			wantErrIs:       errEmptyObjectKey,
		},
		{
			name:            "URI with empty authority",
			input:           "s3:///path/to/file.parquet",
			wantErrContains: "URI authority is empty",
		},
		{
			name:            "URI authority followed by query",
			input:           "s3://my-bucket?prefix=data",
			wantErrContains: "must be followed by an object path",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bucketName := test.bucketName
			if bucketName == "" {
				bucketName = "my-bucket"
			}
			extractor := defaultKeyExtractor(bucketName)
			key, err := extractor(test.input)

			if test.wantErrContains != "" {
				require.ErrorContains(t, err, test.wantErrContains, "Expected error for input: %s", test.input)
				if test.wantErrIs != nil {
					require.ErrorIs(t, err, test.wantErrIs)
				}
			} else {
				require.NoError(t, err, "Unexpected error for input: %s", test.input)
				assert.Equal(t, test.expectedKey, key, "Key mismatch for input: %s", test.input)
			}
		})
	}
}

func testBlobFileIO(ctx context.Context, bucketName string, bucket *blob.Bucket) *blobFileIO {
	extractor := defaultObjectLocationExtractor(bucketName)

	return &blobFileIO{
		Bucket:        bucket,
		extractObject: extractor,
		ctx:           ctx,
	}
}

func testADLSBlobFileIO(t *testing.T, ctx context.Context, root string, bucket *blob.Bucket) *blobFileIO {
	t.Helper()

	parsed, err := url.Parse(root)
	require.NoError(t, err)

	extractor := adlsObjectLocationExtractor(parsed)

	return &blobFileIO{
		Bucket:        bucket,
		extractObject: extractor,
		ctx:           ctx,
	}
}

func identityObjectLocation(location string) (objectLocation, error) {
	return objectLocation{key: location, uriKey: location}, nil
}

func TestBlobFileIORejectsWrongBucketObjectPaths(t *testing.T) {
	ctx := context.Background()

	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()

	bfs := testBlobFileIO(ctx, "test-bucket", bucket)

	for _, tt := range []struct {
		name   string
		path   string
		oldKey string
	}{
		{
			name:   "s3",
			path:   "s3://other-bucket/data/file.parquet",
			oldKey: "other-bucket/data/file.parquet",
		},
		{
			name:   "gcs",
			path:   "gs://other-bucket/data/file.parquet",
			oldKey: "other-bucket/data/file.parquet",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.NoError(t, bucket.WriteAll(ctx, tt.oldKey, []byte("sentinel"), nil))

			err := bfs.WriteFile(tt.path, []byte("content"))
			require.ErrorContains(t, err, "does not match configured authority")

			got, err := bucket.ReadAll(ctx, tt.oldKey)
			require.NoError(t, err)
			assert.Equal(t, []byte("sentinel"), got)
		})
	}
}

func TestNewWriterExistsError(t *testing.T) {
	ctx := context.Background()

	bucket := memblob.OpenBucket(nil)

	bfs := &blobFileIO{
		Bucket:        bucket,
		extractObject: identityObjectLocation,
		ctx:           ctx,
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
				Bucket:        bucket,
				extractObject: identityObjectLocation,
				ctx:           ctx,
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
		"data/100%off/file.parquet",
		"data/city=New York/file.parquet",
		"metadata/snap-123.avro",
	}
	for _, f := range files {
		require.NoError(t, bucket.WriteAll(ctx, f, []byte("content"), nil))
	}

	bfs := testBlobFileIO(ctx, "test-bucket", bucket)

	var walked []string
	var sawRoot bool
	err := bfs.WalkDir("s3://test-bucket/", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() && path == "s3://test-bucket/" {
			sawRoot = true
		}

		if !d.IsDir() {
			walked = append(walked, path)
		}

		return nil
	})
	require.NoError(t, err)

	expected := []string{
		"s3://test-bucket/data/100%off/file.parquet",
		"s3://test-bucket/data/city=New York/file.parquet",
		"s3://test-bucket/data/file1.parquet",
		"s3://test-bucket/data/file2.parquet",
		"s3://test-bucket/metadata/snap-123.avro",
	}
	assert.ElementsMatch(t, expected, walked)
	assert.True(t, sawRoot)
}

func TestBlobFileIOWalkDirRejectsMalformedAuthorityURI(t *testing.T) {
	ctx := context.Background()

	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()

	bfs := testBlobFileIO(ctx, "test-bucket", bucket)

	err := bfs.WalkDir("s3://test-bucket?prefix=data", func(string, fs.DirEntry, error) error {
		t.Fatal("WalkDir callback should not be called")

		return nil
	})
	require.ErrorContains(t, err, "must be followed by an object path")

	var pathErr *fs.PathError
	require.ErrorAs(t, err, &pathErr)
	assert.Equal(t, "walk dir", pathErr.Op)
}

func TestBlobFileIOWalkDirRejectsWrongBucket(t *testing.T) {
	ctx := context.Background()

	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()

	require.NoError(t, bucket.WriteAll(ctx, "data/file1.parquet", []byte("content"), nil))

	bfs := testBlobFileIO(ctx, "test-bucket", bucket)

	for _, tt := range []struct {
		name string
		root string
	}{
		{name: "s3", root: "s3://other-bucket/"},
		{name: "gcs", root: "gs://other-bucket/data"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := bfs.WalkDir(tt.root, func(string, fs.DirEntry, error) error {
				t.Fatal("WalkDir callback should not be called")

				return nil
			})
			require.ErrorContains(t, err, "does not match configured authority")
		})
	}
}

func TestWalkedURIPathRejectsPathOutsideStorageRoot(t *testing.T) {
	location := objectLocation{
		authority:    "other-bucket",
		key:          "other-bucket/data",
		uriKey:       "data",
		uriPrefix:    "s3://other-bucket/",
		hasAuthority: true,
	}

	_, err := walkedURIPath(location, "unrelated/data/file.parquet")
	require.ErrorContains(t, err, "outside storage root")
}

func TestBlobFileIOWalkDirRejectsWrongAzureAuthority(t *testing.T) {
	ctx := context.Background()

	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()

	bfs := testADLSBlobFileIO(t, ctx, "abfs://container@account.dfs.core.windows.net/", bucket)

	err := bfs.WalkDir("abfs://other@account.dfs.core.windows.net/data", func(string, fs.DirEntry, error) error {
		t.Fatal("WalkDir callback should not be called")

		return nil
	})
	require.ErrorContains(t, err, "does not match configured authority")
}

func TestBlobFileIOWalkDirSubPath(t *testing.T) {
	ctx := context.Background()

	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()

	require.NoError(t, bucket.WriteAll(ctx, "data/file1.parquet", []byte("a"), nil))
	require.NoError(t, bucket.WriteAll(ctx, "data/file2.parquet", []byte("b"), nil))
	require.NoError(t, bucket.WriteAll(ctx, "metadata/v1.json", []byte("c"), nil))

	bfs := testBlobFileIO(ctx, "mybucket", bucket)

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

	bfs := testADLSBlobFileIO(t, ctx, "abfs://container@account.dfs.core.windows.net/", bucket)

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

func TestBlobFileIOImplementsBulkRemovableIO(t *testing.T) {
	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()

	extractor := defaultObjectLocationExtractor("test-bucket")
	bfs := createBlobFS(context.Background(), bucket, extractor)

	_, ok := bfs.(icebergio.BulkRemovableIO)
	assert.True(t, ok, "blobFileIO should implement BulkRemovableIO")
}

func TestBlobFileIODeleteFiles(t *testing.T) {
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()

	// Write test files.
	require.NoError(t, bucket.WriteAll(ctx, "data/file1.parquet", []byte("data1"), nil))
	require.NoError(t, bucket.WriteAll(ctx, "data/file2.parquet", []byte("data2"), nil))
	require.NoError(t, bucket.WriteAll(ctx, "data/file3.parquet", []byte("data3"), nil))

	extractor := defaultObjectLocationExtractor("test-bucket")
	bfs := createBlobFS(ctx, bucket, extractor)
	bulk := bfs.(icebergio.BulkRemovableIO)

	deleted, err := bulk.DeleteFiles(ctx, []string{
		"s3://test-bucket/data/file1.parquet",
		"s3://test-bucket/data/file2.parquet",
	})
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{
		"s3://test-bucket/data/file1.parquet",
		"s3://test-bucket/data/file2.parquet",
	}, deleted)

	// file3 should still exist.
	exists, err := bucket.Exists(ctx, "data/file3.parquet")
	require.NoError(t, err)
	assert.True(t, exists)

	// file1 should be gone.
	exists, err = bucket.Exists(ctx, "data/file1.parquet")
	require.NoError(t, err)
	assert.False(t, exists)
}

func TestBlobFileIODeleteFilesMissingFilesAreNotErrors(t *testing.T) {
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()

	extractor := defaultObjectLocationExtractor("test-bucket")
	bfs := createBlobFS(ctx, bucket, extractor)
	bulk := bfs.(icebergio.BulkRemovableIO)

	// Deleting non-existent files should succeed.
	deleted, err := bulk.DeleteFiles(ctx, []string{
		"s3://test-bucket/data/nonexistent.parquet",
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"s3://test-bucket/data/nonexistent.parquet"}, deleted)
}

func TestBlobFileIORemoveMissingFileReturnsNotExist(t *testing.T) {
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()

	extractor := defaultObjectLocationExtractor("test-bucket")
	bfs := createBlobFS(ctx, bucket, extractor)

	err := bfs.Remove("s3://test-bucket/data/nonexistent.parquet")
	require.ErrorIs(t, err, fs.ErrNotExist)
}

func TestBlobFileIODeleteFilesEmpty(t *testing.T) {
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()

	extractor := defaultObjectLocationExtractor("test-bucket")
	bfs := createBlobFS(ctx, bucket, extractor)
	bulk := bfs.(icebergio.BulkRemovableIO)

	deleted, err := bulk.DeleteFiles(ctx, nil)
	require.NoError(t, err)
	assert.Nil(t, deleted)
}
