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
	"fmt"
	"io"
	"io/fs"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	icebergio "github.com/apache/iceberg-go/io"
	"gocloud.dev/blob"
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
		buckets:      make(map[string]*blob.Bucket),
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

func TestBlobFileIOImplementsBulkRemovableIO(t *testing.T) {
	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()

	bfs := createBlobFS(context.Background(), bucket, defaultKeyExtractor("test-bucket"))

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

	bfs := createBlobFS(ctx, bucket, defaultKeyExtractor("test-bucket"))
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

	bfs := createBlobFS(ctx, bucket, defaultKeyExtractor("test-bucket"))
	bulk := bfs.(icebergio.BulkRemovableIO)

	// Deleting non-existent files should succeed.
	deleted, err := bulk.DeleteFiles(ctx, []string{
		"s3://test-bucket/data/nonexistent.parquet",
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"s3://test-bucket/data/nonexistent.parquet"}, deleted)
}

func TestBlobFileIODeleteFilesEmpty(t *testing.T) {
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()

	bfs := createBlobFS(ctx, bucket, defaultKeyExtractor("test-bucket"))
	bulk := bfs.(icebergio.BulkRemovableIO)

	deleted, err := bulk.DeleteFiles(ctx, nil)
	require.NoError(t, err)
	assert.Nil(t, deleted)
}

// TestMultiBucketWriteAndRead verifies that blobFileIO routes reads and writes
// to the correct bucket when a URI references a bucket different from the
// primary one. This is the scenario triggered by Iceberg's write.metadata.path
// table property pointing to a dedicated metadata bucket while data lives in
// the warehouse bucket.
func TestMultiBucketWriteAndRead(t *testing.T) {
	ctx := context.Background()

	warehouseBucket := memblob.OpenBucket(nil)
	defer warehouseBucket.Close()
	metadataBucket := memblob.OpenBucket(nil)
	defer metadataBucket.Close()

	bfs := &blobFileIO{
		Bucket:        warehouseBucket,
		primaryBucket: "warehouse",
		bucketOpener: func(_ context.Context, name string) (*blob.Bucket, error) {
			if name == "metadata" {
				return metadataBucket, nil
			}

			return nil, fmt.Errorf("unknown bucket: %s", name)
		},
		buckets:      make(map[string]*blob.Bucket),
		keyExtractor: defaultKeyExtractor("warehouse"),
		ctx:          ctx,
	}

	// Write a data file to the primary (warehouse) bucket.
	require.NoError(t, bfs.WriteFile("s3://warehouse/db/table/data/file.parquet", []byte("data-content")))

	// Write a manifest list to the metadata bucket (simulates write.metadata.path).
	require.NoError(t, bfs.WriteFile("s3://metadata/db/table/snap-123.avro", []byte("manifest-list")))

	// Verify the data file landed in the warehouse bucket, not the metadata bucket.
	exists, err := warehouseBucket.Exists(ctx, "db/table/data/file.parquet")
	require.NoError(t, err)
	assert.True(t, exists, "data file should exist in warehouse bucket")

	exists, err = metadataBucket.Exists(ctx, "db/table/data/file.parquet")
	require.NoError(t, err)
	assert.False(t, exists, "data file should NOT exist in metadata bucket")

	// Verify the manifest list landed in the metadata bucket, not the warehouse bucket.
	exists, err = metadataBucket.Exists(ctx, "db/table/snap-123.avro")
	require.NoError(t, err)
	assert.True(t, exists, "manifest list should exist in metadata bucket")

	exists, err = warehouseBucket.Exists(ctx, "db/table/snap-123.avro")
	require.NoError(t, err)
	assert.False(t, exists, "manifest list should NOT exist in warehouse bucket")

	// Read back via Open (uses resolveBucket).
	f, err := bfs.Open("s3://metadata/db/table/snap-123.avro")
	require.NoError(t, err)
	defer f.Close()
	content, err := io.ReadAll(f)
	require.NoError(t, err)
	assert.Equal(t, "manifest-list", string(content))

	// Read from the primary bucket.
	f2, err := bfs.Open("s3://warehouse/db/table/data/file.parquet")
	require.NoError(t, err)
	defer f2.Close()
	content2, err := io.ReadAll(f2)
	require.NoError(t, err)
	assert.Equal(t, "data-content", string(content2))
}

// TestMultiBucketDelete verifies that Remove and DeleteFiles route to the
// correct bucket based on the URI.
func TestMultiBucketDelete(t *testing.T) {
	ctx := context.Background()

	warehouseBucket := memblob.OpenBucket(nil)
	defer warehouseBucket.Close()
	metadataBucket := memblob.OpenBucket(nil)
	defer metadataBucket.Close()

	bfs := &blobFileIO{
		Bucket:        warehouseBucket,
		primaryBucket: "warehouse",
		bucketOpener: func(_ context.Context, name string) (*blob.Bucket, error) {
			if name == "metadata" {
				return metadataBucket, nil
			}

			return nil, fmt.Errorf("unknown bucket: %s", name)
		},
		buckets:      make(map[string]*blob.Bucket),
		keyExtractor: defaultKeyExtractor("warehouse"),
		ctx:          ctx,
	}

	// Seed files in both buckets.
	require.NoError(t, warehouseBucket.WriteAll(ctx, "data/file.parquet", []byte("d"), nil))
	require.NoError(t, metadataBucket.WriteAll(ctx, "snap-456.avro", []byte("m"), nil))

	// Remove from metadata bucket.
	require.NoError(t, bfs.Remove("s3://metadata/snap-456.avro"))
	exists, err := metadataBucket.Exists(ctx, "snap-456.avro")
	require.NoError(t, err)
	assert.False(t, exists, "metadata file should be deleted")

	// Warehouse file should be untouched.
	exists, err = warehouseBucket.Exists(ctx, "data/file.parquet")
	require.NoError(t, err)
	assert.True(t, exists, "warehouse file should still exist")

	// DeleteFiles across both buckets.
	require.NoError(t, warehouseBucket.WriteAll(ctx, "old-manifest.avro", []byte("x"), nil))
	require.NoError(t, metadataBucket.WriteAll(ctx, "old-snap.avro", []byte("y"), nil))

	deleted, err := bfs.DeleteFiles(ctx, []string{
		"s3://warehouse/old-manifest.avro",
		"s3://metadata/old-snap.avro",
	})
	require.NoError(t, err)
	assert.Len(t, deleted, 2)
}

// TestMultiBucketOpenerCaching verifies that the bucket opener is called only
// once per bucket name, and subsequent requests reuse the cached handle.
func TestMultiBucketOpenerCaching(t *testing.T) {
	ctx := context.Background()

	warehouseBucket := memblob.OpenBucket(nil)
	defer warehouseBucket.Close()
	metadataBucket := memblob.OpenBucket(nil)
	defer metadataBucket.Close()

	openCount := 0
	bfs := &blobFileIO{
		Bucket:        warehouseBucket,
		primaryBucket: "warehouse",
		bucketOpener: func(_ context.Context, name string) (*blob.Bucket, error) {
			openCount++
			if name == "metadata" {
				return metadataBucket, nil
			}

			return nil, fmt.Errorf("unknown bucket: %s", name)
		},
		buckets:      make(map[string]*blob.Bucket),
		keyExtractor: defaultKeyExtractor("warehouse"),
		ctx:          ctx,
	}

	require.NoError(t, metadataBucket.WriteAll(ctx, "file1.avro", []byte("a"), nil))
	require.NoError(t, metadataBucket.WriteAll(ctx, "file2.avro", []byte("b"), nil))

	// Two reads from the same secondary bucket.
	f1, err := bfs.Open("s3://metadata/file1.avro")
	require.NoError(t, err)
	f1.Close()

	f2, err := bfs.Open("s3://metadata/file2.avro")
	require.NoError(t, err)
	f2.Close()

	assert.Equal(t, 1, openCount, "bucket opener should be called exactly once for 'metadata'")
}

// TestMultiBucketFallbackWithoutOpener verifies backward compatibility: when
// no BucketOpener is configured, cross-bucket URIs fall back to the primary
// bucket with the legacy key extractor.
func TestMultiBucketFallbackWithoutOpener(t *testing.T) {
	ctx := context.Background()

	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()

	// No bucketOpener set: uses legacy createBlobFS path.
	bfs := createBlobFS(ctx, bucket, defaultKeyExtractor("warehouse"))

	// Write using a URI with a different bucket name. Without an opener,
	// the key extractor strips "://other-bucket/" and the file ends up
	// in the primary bucket under a mangled key (legacy behavior).
	wfs := bfs.(icebergio.WriteFileIO)
	require.NoError(t, wfs.WriteFile(
		"s3://other-bucket/path/file.txt", []byte("hello")))

	// The file lands in the primary bucket with the full "other-bucket/path/file.txt" key
	// because TrimPrefix("other-bucket/...", "warehouse/") is a no-op.
	exists, err := bucket.Exists(ctx, "other-bucket/path/file.txt")
	require.NoError(t, err)
	assert.True(t, exists, "without opener, cross-bucket URI should fall back to primary bucket with mangled key")
}
