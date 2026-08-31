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

package blobfs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/iceberg-go/internal/schemes"
	icebergio "github.com/apache/iceberg-go/io"
	"gocloud.dev/blob"
	"gocloud.dev/blob/driver"
	"gocloud.dev/blob/memblob"
	"gocloud.dev/gcerrors"
)

func TestDefaultKeyExtractor(t *testing.T) {
	tests := []struct {
		name            string
		bucketName      string
		allowedSchemes  []string
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
			wantErrIs:       ErrUnsupportedObjectAuthority,
		},
		{
			name:            "gs URI with different bucket",
			input:           "gs://other-bucket/path/to/file.parquet",
			wantErrContains: "does not match configured authority",
			wantErrIs:       ErrUnsupportedObjectAuthority,
		},
		{
			name:            "s3 extractor rejects gs URI with same bucket",
			allowedSchemes:  schemes.S3,
			input:           "gs://my-bucket/path/to/file.parquet",
			wantErrContains: `URI scheme "gs" is not supported`,
		},
		{
			name:            "gcs extractor rejects s3 URI with same bucket",
			allowedSchemes:  schemes.GCS,
			input:           "s3://my-bucket/path/to/file.parquet",
			wantErrContains: `URI scheme "s3" is not supported`,
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
			wantErrIs:       ErrEmptyObjectKey,
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
			extractor := defaultKeyExtractor(bucketName, test.allowedSchemes...)
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

func TestBlobFileIOOpenMissingReturnsPathError(t *testing.T) {
	t.Parallel()

	bucket := memblob.OpenBucket(nil)
	t.Cleanup(func() { require.NoError(t, bucket.Close()) })
	fileIO := testBlobFileIO(context.Background(), "my-bucket", bucket)
	name := "s3://my-bucket/missing.parquet"

	_, err := fileIO.Open(name)
	require.ErrorIs(t, err, fs.ErrNotExist)

	var pathErr *fs.PathError
	require.ErrorAs(t, err, &pathErr)
	assert.Equal(t, "open", pathErr.Op)
	assert.Equal(t, name, pathErr.Path)
}

func TestBlobFileIOOpenPreprocessErrorRetainsOriginalPath(t *testing.T) {
	t.Parallel()

	bucket := memblob.OpenBucket(nil)
	t.Cleanup(func() { require.NoError(t, bucket.Close()) })
	fileIO := testBlobFileIO(context.Background(), "my-bucket", bucket)
	name := "s3://other-bucket/file.parquet"

	_, err := fileIO.Open(name)
	require.Error(t, err)

	var pathErr *fs.PathError
	require.ErrorAs(t, err, &pathErr)
	assert.Equal(t, "open", pathErr.Op)
	assert.Equal(t, name, pathErr.Path)
}

func testBlobFileIO(ctx context.Context, bucketName string, bucket *blob.Bucket, allowedSchemes ...string) *FileIO {
	if len(allowedSchemes) == 0 {
		allowedSchemes = schemes.S3
	}
	extractor := DefaultObjectLocationExtractor(bucketName, allowedSchemes...)

	return &FileIO{
		Bucket:        bucket,
		extractObject: extractor,
		ctx:           ctx,
	}
}

func identityObjectLocation(location string) (ObjectLocation, error) {
	return ObjectLocation{key: location}, nil
}

func TestBlobFileIORejectsUnsupportedObjectPaths(t *testing.T) {
	ctx := context.Background()

	for _, tt := range []struct {
		name           string
		allowedSchemes []string
		path           string
		oldKey         string
		wantErr        error
		wantErrText    string
	}{
		{
			name:           "s3 different bucket",
			allowedSchemes: schemes.S3,
			path:           "s3://other-bucket/data/file.parquet",
			oldKey:         "other-bucket/data/file.parquet",
			wantErr:        ErrUnsupportedObjectAuthority,
			wantErrText:    "does not match configured authority",
		},
		{
			name:           "gcs different bucket",
			allowedSchemes: schemes.GCS,
			path:           "gs://other-bucket/data/file.parquet",
			oldKey:         "other-bucket/data/file.parquet",
			wantErr:        ErrUnsupportedObjectAuthority,
			wantErrText:    "does not match configured authority",
		},
		{
			name:           "s3 rejects gs same bucket",
			allowedSchemes: schemes.S3,
			path:           "gs://test-bucket/data/file.parquet",
			oldKey:         "data/file.parquet",
			wantErrText:    `URI scheme "gs" is not supported`,
		},
		{
			name:           "gcs rejects s3 same bucket",
			allowedSchemes: schemes.GCS,
			path:           "s3://test-bucket/data/file.parquet",
			oldKey:         "data/file.parquet",
			wantErrText:    `URI scheme "s3" is not supported`,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			bucket := memblob.OpenBucket(nil)
			defer bucket.Close()

			bfs := testBlobFileIO(ctx, "test-bucket", bucket, tt.allowedSchemes...)
			require.NoError(t, bucket.WriteAll(ctx, tt.oldKey, []byte("sentinel"), nil))

			err := bfs.WriteFile(tt.path, []byte("content"))
			require.ErrorContains(t, err, tt.wantErrText)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			}

			got, err := bucket.ReadAll(ctx, tt.oldKey)
			require.NoError(t, err)
			assert.Equal(t, []byte("sentinel"), got)
		})
	}
}

func TestNewWriterExistsError(t *testing.T) {
	ctx := context.Background()

	bucket := memblob.OpenBucket(nil)

	bfs := &FileIO{
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
			bfs := &FileIO{
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

	for _, tt := range []struct {
		name           string
		allowedSchemes []string
		root           string
	}{
		{name: "s3", allowedSchemes: schemes.S3, root: "s3://other-bucket/"},
		{name: "gcs", allowedSchemes: schemes.GCS, root: "gs://other-bucket/data"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			bfs := testBlobFileIO(ctx, "test-bucket", bucket, tt.allowedSchemes...)
			err := bfs.WalkDir(tt.root, func(string, fs.DirEntry, error) error {
				t.Fatal("WalkDir callback should not be called")

				return nil
			})
			require.ErrorContains(t, err, "does not match configured authority")
			require.ErrorIs(t, err, ErrUnsupportedObjectAuthority)
		})
	}
}

func TestBlobFileIOWalkDirRelativeRootReturnsBareKeys(t *testing.T) {
	ctx := context.Background()

	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()

	require.NoError(t, bucket.WriteAll(ctx, "data/file1.parquet", []byte("a"), nil))
	require.NoError(t, bucket.WriteAll(ctx, "data/file2.parquet", []byte("b"), nil))

	bfs := testBlobFileIO(ctx, "mybucket", bucket)

	var walked []string
	err := bfs.WalkDir("data", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if !d.IsDir() {
			walked = append(walked, path)
		}

		return nil
	})
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{
		"data/file1.parquet",
		"data/file2.parquet",
	}, walked)
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

func TestBlobFileIORawQueryFragmentRoundTrip(t *testing.T) {
	ctx := context.Background()

	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()

	bfs := testBlobFileIO(ctx, "test-bucket", bucket)
	location := "s3://test-bucket/path/to/file.parquet?param=value#fragment"
	content := []byte("data")
	require.NoError(t, bfs.WriteFile(location, content))

	file, err := bfs.Open(location)
	require.NoError(t, err)
	defer file.Close()

	got, err := io.ReadAll(file)
	require.NoError(t, err)
	assert.Equal(t, content, got)
}

func TestBlobFileIODeleteFiles(t *testing.T) {
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()

	// Write test files.
	require.NoError(t, bucket.WriteAll(ctx, "data/file1.parquet", []byte("data1"), nil))
	require.NoError(t, bucket.WriteAll(ctx, "data/file2.parquet", []byte("data2"), nil))
	require.NoError(t, bucket.WriteAll(ctx, "data/file3.parquet", []byte("data3"), nil))

	extractor := DefaultObjectLocationExtractor("test-bucket")
	bfs := New(ctx, bucket, extractor)
	var bulk icebergio.BulkRemovableIO = bfs

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

	extractor := DefaultObjectLocationExtractor("test-bucket")
	bfs := New(ctx, bucket, extractor)
	var bulk icebergio.BulkRemovableIO = bfs

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

	extractor := DefaultObjectLocationExtractor("test-bucket")
	bfs := New(ctx, bucket, extractor)

	err := bfs.Remove("s3://test-bucket/data/nonexistent.parquet")
	require.ErrorIs(t, err, fs.ErrNotExist)
}

func TestBlobFileIODeleteFilesEmpty(t *testing.T) {
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()

	extractor := DefaultObjectLocationExtractor("test-bucket")
	bfs := New(ctx, bucket, extractor)
	var bulk icebergio.BulkRemovableIO = bfs

	deleted, err := bulk.DeleteFiles(ctx, nil)
	require.NoError(t, err)
	assert.Nil(t, deleted)
}

type trackingDeleteBucket struct {
	started chan<- struct{}
	release chan struct{}

	mu          sync.Mutex
	inFlight    int
	maxInFlight int
}

var errTrackingDeleteUnsupported = errors.New("unsupported test operation")

func (b *trackingDeleteBucket) ErrorCode(error) gcerrors.ErrorCode { return gcerrors.Unknown }
func (b *trackingDeleteBucket) As(any) bool                        { return false }
func (b *trackingDeleteBucket) ErrorAs(error, any) bool            { return false }
func (b *trackingDeleteBucket) Attributes(context.Context, string) (*driver.Attributes, error) {
	return nil, errTrackingDeleteUnsupported
}

func (b *trackingDeleteBucket) ListPaged(context.Context, *driver.ListOptions) (*driver.ListPage, error) {
	return nil, errTrackingDeleteUnsupported
}

func (b *trackingDeleteBucket) NewRangeReader(context.Context, string, int64, int64, *driver.ReaderOptions) (driver.Reader, error) {
	return nil, errTrackingDeleteUnsupported
}

func (b *trackingDeleteBucket) NewTypedWriter(context.Context, string, string, *driver.WriterOptions) (driver.Writer, error) {
	return nil, errTrackingDeleteUnsupported
}

func (b *trackingDeleteBucket) Copy(context.Context, string, string, *driver.CopyOptions) error {
	return errTrackingDeleteUnsupported
}

func (b *trackingDeleteBucket) Delete(ctx context.Context, _ string) error {
	b.mu.Lock()
	b.inFlight++
	if b.inFlight > b.maxInFlight {
		b.maxInFlight = b.inFlight
	}
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		b.inFlight--
		b.mu.Unlock()
	}()

	select {
	case b.started <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case <-b.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *trackingDeleteBucket) SignedURL(context.Context, string, *driver.SignedURLOptions) (string, error) {
	return "", errTrackingDeleteUnsupported
}
func (b *trackingDeleteBucket) Close() error { return nil }

func TestBlobFileIODeleteFilesIsConcurrentAndBounded(t *testing.T) {
	const pathCount = deleteFilesMaxConcurrency * 2

	started := make(chan struct{}, pathCount)
	tracker := &trackingDeleteBucket{
		started: started,
		release: make(chan struct{}),
	}
	bucket := blob.NewBucket(tracker)
	defer bucket.Close()

	bfs := &FileIO{
		Bucket:        bucket,
		extractObject: DefaultObjectLocationExtractor("test-bucket"),
		ctx:           context.Background(),
	}
	paths := make([]string, pathCount)
	for i := range paths {
		paths[i] = fmt.Sprintf("s3://test-bucket/data/file%d.parquet", i)
	}

	deletedCh := make(chan struct{})
	var deleted []string
	var deleteErr error
	go func() {
		deleted, deleteErr = bfs.DeleteFiles(context.Background(), paths)
		close(deletedCh)
	}()

	for range deleteFilesMaxConcurrency {
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatal("DeleteFiles did not start the expected concurrent deletes")
		}
	}

	tracker.mu.Lock()
	maxInFlight := tracker.maxInFlight
	tracker.mu.Unlock()
	assert.Equal(t, deleteFilesMaxConcurrency, maxInFlight)

	select {
	case <-started:
		t.Fatal("DeleteFiles exceeded its concurrency limit")
	default:
	}

	close(tracker.release)
	select {
	case <-deletedCh:
	case <-time.After(5 * time.Second):
		t.Fatal("DeleteFiles did not finish after deletes were released")
	}

	require.NoError(t, deleteErr)
	assert.Equal(t, paths, deleted)
}

func TestBlobFileIOStat(t *testing.T) {
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()

	require.NoError(t, bucket.WriteAll(ctx, "data/file.parquet", []byte("content"), nil))

	bfs := New(ctx, bucket, DefaultObjectLocationExtractor("test-bucket"))

	fileInfo, err := bfs.Stat("s3://test-bucket/data/file.parquet")
	require.NoError(t, err)
	assert.Equal(t, "file.parquet", fileInfo.Name())
	assert.Equal(t, int64(len("content")), fileInfo.Size())
	assert.False(t, fileInfo.IsDir())

	dirInfo, err := bfs.Stat("s3://test-bucket/data")
	require.NoError(t, err)
	assert.Equal(t, "data", dirInfo.Name())
	assert.True(t, dirInfo.IsDir())

	require.NoError(t, bfs.MkdirAll("s3://test-bucket/foo"))
	markerInfoWithSlash, err := bfs.Stat("s3://test-bucket/foo/")
	require.NoError(t, err)
	assert.Equal(t, "foo", markerInfoWithSlash.Name())
	markerInfoWithoutSlash, err := bfs.Stat("s3://test-bucket/foo")
	require.NoError(t, err)
	assert.Equal(t, "foo", markerInfoWithoutSlash.Name())
	// The trailing slash in the path should not affect the
	// isDir result
	assert.True(t, markerInfoWithSlash.IsDir())
	assert.True(t, markerInfoWithoutSlash.IsDir())

	_, err = bfs.Stat("s3://test-bucket/missing")
	require.ErrorIs(t, err, fs.ErrNotExist)
}

func TestBlobFileIOMkdirAll(t *testing.T) {
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()

	bfs := New(ctx, bucket, DefaultObjectLocationExtractor("test-bucket"))

	require.NoError(t, bfs.MkdirAll("s3://test-bucket/a/b/c"))

	for _, key := range []string{"a/", "a/b/", "a/b/c/"} {
		exists, err := bucket.Exists(ctx, key)
		require.NoError(t, err)
		assert.True(t, exists, "%s should exist", key)
	}
}

func TestBlobFileIOWalkDirSkipsDirectoryMarker(t *testing.T) {
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()

	bfs := New(ctx, bucket, DefaultObjectLocationExtractor("test-bucket"))

	// MkdirAll leaves only a "warehouse/ns/" marker for an empty namespace.
	// Walking "warehouse/ns" should not report that marker as a child file.
	require.NoError(t, bfs.MkdirAll("s3://test-bucket/warehouse/ns"))

	var paths []string
	require.NoError(t, bfs.WalkDir("s3://test-bucket/warehouse/ns", func(path string, d fs.DirEntry, err error) error {
		require.NoError(t, err)
		paths = append(paths, path)

		return nil
	}))
	// There should be exactly one path reported and no dummy directory markers
	assert.Equal(t, []string{"s3://test-bucket/warehouse/ns"}, paths)
}

func TestBlobFileIORemoveAll(t *testing.T) {
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()

	require.NoError(t, bucket.WriteAll(ctx, "data/file.parquet", []byte("file"), nil))
	require.NoError(t, bucket.WriteAll(ctx, "warehouse/ns/", nil, nil))
	require.NoError(t, bucket.WriteAll(ctx, "warehouse/ns/tbl/metadata/v1.metadata.json", []byte("metadata"), nil))
	require.NoError(t, bucket.WriteAll(ctx, "warehouse/ns/tbl/data/00001.parquet", []byte("data"), nil))
	require.NoError(t, bucket.WriteAll(ctx, "warehouse/other/keep.parquet", []byte("keep"), nil))

	bfs := New(ctx, bucket, DefaultObjectLocationExtractor("test-bucket"))

	require.NoError(t, bfs.RemoveAll("s3://test-bucket/data/file.parquet"))
	exists, err := bucket.Exists(ctx, "data/file.parquet")
	require.NoError(t, err)
	assert.False(t, exists)

	require.NoError(t, bfs.RemoveAll("s3://test-bucket/warehouse/ns"))
	for _, key := range []string{
		"warehouse/ns/",
		"warehouse/ns/tbl/metadata/v1.metadata.json",
		"warehouse/ns/tbl/data/00001.parquet",
	} {
		exists, err := bucket.Exists(ctx, key)
		require.NoError(t, err)
		assert.False(t, exists, "%s should be removed", key)
	}

	exists, err = bucket.Exists(ctx, "warehouse/other/keep.parquet")
	require.NoError(t, err)
	assert.True(t, exists)

	require.NoError(t, bfs.RemoveAll("s3://test-bucket/missing"))
}

func TestBlobFileIOPreprocessErrorRetainsOriginalPath(t *testing.T) {
	t.Parallel()

	bucket := memblob.OpenBucket(nil)
	t.Cleanup(func() { require.NoError(t, bucket.Close()) })
	fileIO := testBlobFileIO(context.Background(), "my-bucket", bucket)
	name := "s3://other-bucket/file.parquet"

	for _, tt := range []struct {
		op  string
		run func() error
	}{
		{"remove", func() error { return fileIO.Remove(name) }},
		{"write file", func() error { return fileIO.WriteFile(name, nil) }},
		{"new writer", func() error {
			_, err := fileIO.NewWriter(context.Background(), name, true, nil)

			return err
		}},
	} {
		t.Run(tt.op, func(t *testing.T) {
			var pathErr *fs.PathError
			require.ErrorAs(t, tt.run(), &pathErr)
			assert.Equal(t, tt.op, pathErr.Op)
			assert.Equal(t, name, pathErr.Path)
		})
	}
}

func TestBlobFileIORemoveFallsBackToDirectoryMarker(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	t.Cleanup(func() { require.NoError(t, bucket.Close()) })
	require.NoError(t, bucket.WriteAll(ctx, "ns/tbl/", nil, nil))

	fileIO := testBlobFileIO(ctx, "test-bucket", bucket)
	require.NoError(t, fileIO.Remove("s3://test-bucket/ns/tbl"))

	exists, err := bucket.Exists(ctx, "ns/tbl/")
	require.NoError(t, err)
	assert.False(t, exists)
}
