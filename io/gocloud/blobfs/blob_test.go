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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"strings"
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

	_, err := fileIO.Open(context.Background(), name)
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

	_, err := fileIO.Open(context.Background(), name)
	require.Error(t, err)

	var pathErr *fs.PathError
	require.ErrorAs(t, err, &pathErr)
	assert.Equal(t, "open", pathErr.Op)
	assert.Equal(t, name, pathErr.Path)
}

func TestBlobFileIOWriteOperationsUseOperationContext(t *testing.T) {
	t.Parallel()

	bucket := memblob.OpenBucket(nil)
	t.Cleanup(func() { require.NoError(t, bucket.Close()) })
	lifetimeCtx, cancelLifetime := context.WithCancel(context.Background())
	fileIO := testBlobFileIO(lifetimeCtx, "my-bucket", bucket)
	cancelLifetime()

	ctx := context.Background()
	content := []byte("written with operation context")
	require.NoError(t, fileIO.WriteFile(ctx, "s3://my-bucket/write-file", content))

	writer, err := fileIO.Create(ctx, "s3://my-bucket/create-file")
	require.NoError(t, err)
	_, err = writer.Write(content)
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	for _, key := range []string{"write-file", "create-file"} {
		got, err := bucket.ReadAll(ctx, key)
		require.NoError(t, err)
		assert.Equal(t, content, got)
	}
}

type statContextKey struct{}

type contextAwareAttributesBucket struct {
	driver.Bucket
	attributesCtx chan context.Context
}

func (b *contextAwareAttributesBucket) ErrorCode(error) gcerrors.ErrorCode { return gcerrors.Unknown }

func (b *contextAwareAttributesBucket) Attributes(ctx context.Context, _ string) (*driver.Attributes, error) {
	b.attributesCtx <- ctx
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	return &driver.Attributes{Size: 1}, nil
}

func (b *contextAwareAttributesBucket) Close() error { return nil }

func TestBlobFileIOStatUsesOperationContext(t *testing.T) {
	driverBucket := &contextAwareAttributesBucket{attributesCtx: make(chan context.Context, 1)}
	bucket := blob.NewBucket(driverBucket)
	t.Cleanup(func() { require.NoError(t, bucket.Close()) })

	lifetimeCtx := context.WithValue(context.Background(), statContextKey{}, "lifetime")
	fileIO := New(lifetimeCtx, bucket, DefaultObjectLocationExtractor("my-bucket"))
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), statContextKey{}, "operation"))
	cancel()

	_, err := fileIO.Stat(ctx, "s3://my-bucket/data/file.parquet")
	assert.ErrorIs(t, err, context.Canceled)

	gotCtx := <-driverBucket.attributesCtx
	assert.Equal(t, "operation", gotCtx.Value(statContextKey{}))
}

type statListContextBucket struct {
	driver.Bucket
	listCtx context.Context
}

func (*statListContextBucket) ErrorCode(err error) gcerrors.ErrorCode {
	if errors.Is(err, fs.ErrNotExist) {
		return gcerrors.NotFound
	}

	return gcerrors.Unknown
}

func (*statListContextBucket) Attributes(context.Context, string) (*driver.Attributes, error) {
	return nil, fs.ErrNotExist
}

func (b *statListContextBucket) ListPaged(ctx context.Context, _ *driver.ListOptions) (*driver.ListPage, error) {
	b.listCtx = ctx

	return &driver.ListPage{
		Objects: []*driver.ListObject{{Key: "data/child.parquet"}},
	}, nil
}

func (*statListContextBucket) Close() error { return nil }

func TestBlobFileIOStatDirectoryListingUsesOperationContext(t *testing.T) {
	driverBucket := &statListContextBucket{}
	bucket := blob.NewBucket(driverBucket)
	t.Cleanup(func() { require.NoError(t, bucket.Close()) })

	lifetimeCtx, cancelLifetime := context.WithCancel(context.WithValue(context.Background(), statContextKey{}, "lifetime"))
	fileIO := New(lifetimeCtx, bucket, DefaultObjectLocationExtractor("my-bucket"))
	cancelLifetime()
	operationCtx := context.WithValue(context.Background(), statContextKey{}, "operation")

	info, err := fileIO.Stat(operationCtx, "s3://my-bucket/data")
	require.NoError(t, err)
	require.True(t, info.IsDir())
	require.NotNil(t, driverBucket.listCtx)
	assert.Equal(t, "operation", driverBucket.listCtx.Value(statContextKey{}))
}

type readFileContextKey struct{}

type contextAwareRangeReaderBucket struct {
	driver.Bucket
	readerCtx chan context.Context
}

func (b *contextAwareRangeReaderBucket) ErrorCode(error) gcerrors.ErrorCode { return gcerrors.Unknown }

func (b *contextAwareRangeReaderBucket) NewRangeReader(
	ctx context.Context,
	_ string,
	_, _ int64,
	_ *driver.ReaderOptions,
) (driver.Reader, error) {
	b.readerCtx <- ctx
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	return contextAwareBlobReader{ReadCloser: io.NopCloser(strings.NewReader("metadata"))}, nil
}

func (b *contextAwareRangeReaderBucket) Close() error { return nil }

type contextAwareBlobReader struct{ io.ReadCloser }

func (contextAwareBlobReader) Attributes() *driver.ReaderAttributes {
	return &driver.ReaderAttributes{}
}
func (contextAwareBlobReader) As(any) bool { return false }

func TestBlobFileIOReadFileUsesOperationContext(t *testing.T) {
	driverBucket := &contextAwareRangeReaderBucket{readerCtx: make(chan context.Context, 2)}
	bucket := blob.NewBucket(driverBucket)
	t.Cleanup(func() { require.NoError(t, bucket.Close()) })
	lifetimeCtx, cancelLifetime := context.WithCancel(context.Background())
	fileIO := New(lifetimeCtx, bucket, DefaultObjectLocationExtractor("my-bucket"))
	cancelLifetime()

	ctx := context.WithValue(context.Background(), readFileContextKey{}, "operation")
	ctx, cancelOperation := context.WithTimeout(ctx, time.Hour)
	defer cancelOperation()
	content, err := fileIO.ReadFile(ctx, "s3://my-bucket/data/file.parquet")
	require.NoError(t, err)
	assert.Equal(t, []byte("metadata"), content)
	readCtx := <-driverBucket.readerCtx
	assert.Equal(t, "operation", readCtx.Value(readFileContextKey{}))
	wantDeadline, ok := ctx.Deadline()
	require.True(t, ok)
	gotDeadline, ok := readCtx.Deadline()
	require.True(t, ok)
	assert.Equal(t, wantDeadline, gotDeadline)

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = fileIO.ReadFile(canceledCtx, "s3://my-bucket/data/file.parquet")
	assert.ErrorIs(t, err, context.Canceled)
	assert.ErrorIs(t, (<-driverBucket.readerCtx).Err(), context.Canceled)
}

func TestBlobFileIOWriteFileHonorsOperationCancellation(t *testing.T) {
	t.Parallel()

	bucket := memblob.OpenBucket(nil)
	t.Cleanup(func() { require.NoError(t, bucket.Close()) })
	fileIO := testBlobFileIO(context.Background(), "my-bucket", bucket)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := fileIO.WriteFile(ctx, "s3://my-bucket/canceled", []byte("content"))
	assert.ErrorIs(t, err, context.Canceled)
}

func TestBlobFileIOCreateHonorsOperationCancellation(t *testing.T) {
	t.Parallel()

	bucket := memblob.OpenBucket(nil)
	t.Cleanup(func() { require.NoError(t, bucket.Close()) })
	fileIO := testBlobFileIO(context.Background(), "my-bucket", bucket)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	writer, err := fileIO.Create(ctx, "s3://my-bucket/canceled")
	if err == nil {
		_, writeErr := writer.Write([]byte("content"))
		closeErr := writer.Close()
		err = errors.Join(writeErr, closeErr)
	}
	assert.ErrorIs(t, err, context.Canceled)
	exists, err := bucket.Exists(context.Background(), "canceled")
	require.NoError(t, err)
	assert.False(t, exists)
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

			err := bfs.WriteFile(context.Background(), tt.path, []byte("content"))
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

			file, err := bfs.Open(context.Background(), "test-file")
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
	err := bfs.WalkDir(context.Background(), "s3://test-bucket/", func(path string, d fs.DirEntry, err error) error {
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

type walkContextKey struct{}

type contextAwareListBucket struct {
	driver.Bucket
	listCtx chan context.Context
	release chan struct{}
}

func (b *contextAwareListBucket) ErrorCode(error) gcerrors.ErrorCode { return gcerrors.Unknown }

func (b *contextAwareListBucket) ListPaged(ctx context.Context, _ *driver.ListOptions) (*driver.ListPage, error) {
	b.listCtx <- ctx
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-b.release:
		return nil, context.Canceled
	}
}

func (b *contextAwareListBucket) Close() error { return nil }

func TestBlobFileIOWalkDirPassesOperationContextToListing(t *testing.T) {
	driverBucket := &contextAwareListBucket{
		listCtx: make(chan context.Context, 1),
		release: make(chan struct{}),
	}
	bucket := blob.NewBucket(driverBucket)
	defer bucket.Close()
	defer close(driverBucket.release)

	const contextValue = "walk operation"
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), walkContextKey{}, contextValue))
	defer cancel()
	bfs := New(context.Background(), bucket, DefaultObjectLocationExtractor("test-bucket"))

	walkErr := make(chan error, 1)
	go func() {
		walkErr <- bfs.WalkDir(ctx, "s3://test-bucket/", func(string, fs.DirEntry, error) error {
			return nil
		})
	}()

	var listCtx context.Context
	select {
	case listCtx = <-driverBucket.listCtx:
	case <-time.After(2 * time.Second):
		t.Fatal("WalkDir did not start the bucket listing")
	}
	assert.Equal(t, contextValue, listCtx.Value(walkContextKey{}))

	cancel()
	select {
	case err := <-walkErr:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("WalkDir did not cancel the bucket listing")
	}
}

func TestBlobFileIOWalkDirRejectsMalformedAuthorityURI(t *testing.T) {
	ctx := context.Background()

	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()

	bfs := testBlobFileIO(ctx, "test-bucket", bucket)

	err := bfs.WalkDir(context.Background(), "s3://test-bucket?prefix=data", func(string, fs.DirEntry, error) error {
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
			err := bfs.WalkDir(context.Background(), tt.root, func(string, fs.DirEntry, error) error {
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
	err := bfs.WalkDir(context.Background(), "data", func(path string, d fs.DirEntry, err error) error {
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
	err := bfs.WalkDir(context.Background(), "s3://mybucket/data", func(path string, d fs.DirEntry, err error) error {
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

func TestBlobFileIOWalkDirHierarchicalNamespace(t *testing.T) {
	ctx := context.Background()

	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()

	// ADLS Gen2 hierarchical namespaces store each directory as a 0-byte
	// blob named after it, with no trailing slash.
	for _, k := range []string{"tbl", "tbl/data", "tbl/metadata"} {
		require.NoError(t, bucket.WriteAll(ctx, k, nil, nil))
	}
	require.NoError(t, bucket.WriteAll(ctx, "tbl/data/a.parquet", []byte("a"), nil))
	require.NoError(t, bucket.WriteAll(ctx, "tbl/metadata/v1.metadata.json", []byte("b"), nil))
	require.NoError(t, bucket.WriteAll(ctx, "tbl/empty.txt", nil, nil))

	bfs := testBlobFileIO(ctx, "mybucket", bucket)

	var dirs, files []string
	err := bfs.WalkDir(context.Background(), "s3://mybucket/tbl", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() {
			dirs = append(dirs, path)
		} else {
			files = append(files, path)
		}

		return nil
	})
	require.NoError(t, err)

	assert.Equal(t, []string{
		"s3://mybucket/tbl",
		"s3://mybucket/tbl/data",
		"s3://mybucket/tbl/metadata",
	}, dirs)
	assert.Equal(t, []string{
		"s3://mybucket/tbl/data/a.parquet",
		"s3://mybucket/tbl/empty.txt",
		"s3://mybucket/tbl/metadata/v1.metadata.json",
	}, files)
}

func TestBlobFileIORawQueryFragmentRoundTrip(t *testing.T) {
	ctx := context.Background()

	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()

	bfs := testBlobFileIO(ctx, "test-bucket", bucket)
	location := "s3://test-bucket/path/to/file.parquet?param=value#fragment"
	content := []byte("data")
	require.NoError(t, bfs.WriteFile(context.Background(), location, content))

	file, err := bfs.Open(context.Background(), location)
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

	err := bfs.Remove(context.Background(), "s3://test-bucket/data/nonexistent.parquet")
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

func (b *trackingDeleteBucket) Delete(ctx context.Context, _ string, _ *driver.DeleteOptions) error {
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

	fileInfo, err := bfs.Stat(ctx, "s3://test-bucket/data/file.parquet")
	require.NoError(t, err)
	assert.Equal(t, "file.parquet", fileInfo.Name())
	assert.Equal(t, int64(len("content")), fileInfo.Size())
	assert.False(t, fileInfo.IsDir())

	dirInfo, err := bfs.Stat(ctx, "s3://test-bucket/data")
	require.NoError(t, err)
	assert.Equal(t, "data", dirInfo.Name())
	assert.True(t, dirInfo.IsDir())

	require.NoError(t, bfs.MkdirAll("s3://test-bucket/foo"))
	markerInfoWithSlash, err := bfs.Stat(ctx, "s3://test-bucket/foo/")
	require.NoError(t, err)
	assert.Equal(t, "foo", markerInfoWithSlash.Name())
	markerInfoWithoutSlash, err := bfs.Stat(ctx, "s3://test-bucket/foo")
	require.NoError(t, err)
	assert.Equal(t, "foo", markerInfoWithoutSlash.Name())
	// The trailing slash in the path should not affect the
	// isDir result
	assert.True(t, markerInfoWithSlash.IsDir())
	assert.True(t, markerInfoWithoutSlash.IsDir())

	_, err = bfs.Stat(ctx, "s3://test-bucket/missing")
	require.ErrorIs(t, err, fs.ErrNotExist)
}

func TestBlobFileIORename(t *testing.T) {
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()

	require.NoError(t, bucket.WriteAll(ctx, "data/source.parquet", []byte("source"), nil))
	require.NoError(t, bucket.WriteAll(ctx, "data/destination.parquet", []byte("old destination"), nil))
	bfs := testBlobFileIO(ctx, "test-bucket", bucket)

	require.NoError(t, bfs.Rename(ctx, "s3://test-bucket/data/source.parquet", "s3://test-bucket/data/destination.parquet"))

	data, err := bucket.ReadAll(ctx, "data/destination.parquet")
	require.NoError(t, err)
	assert.Equal(t, []byte("source"), data)
	exists, err := bucket.Exists(ctx, "data/source.parquet")
	require.NoError(t, err)
	assert.False(t, exists)
}

func TestBlobFileIORenameUsesOperationContext(t *testing.T) {
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()
	require.NoError(t, bucket.WriteAll(ctx, "data/source.parquet", []byte("source"), nil))
	bfs := testBlobFileIO(ctx, "test-bucket", bucket)

	opCtx, cancel := context.WithCancel(ctx)
	cancel()
	err := bfs.Rename(opCtx, "s3://test-bucket/data/source.parquet", "s3://test-bucket/data/destination.parquet")
	require.ErrorIs(t, err, context.Canceled)
	var pathErr *fs.PathError
	require.ErrorAs(t, err, &pathErr)
	assert.Equal(t, "Rename", pathErr.Op)
	assert.Equal(t, "s3://test-bucket/data/source.parquet", pathErr.Path)
	exists, err := bucket.Exists(ctx, "data/source.parquet")
	require.NoError(t, err)
	assert.True(t, exists)
}

type cancelAfterCopyBucket struct {
	driver.Bucket
	cancel                     context.CancelFunc
	copyCtx                    context.Context
	deleteCtx                  context.Context
	srcKey, dstKey, deletedKey string
}

func (b *cancelAfterCopyBucket) ErrorCode(err error) gcerrors.ErrorCode {
	if errors.Is(err, fs.ErrNotExist) {
		return gcerrors.NotFound
	}

	return gcerrors.Unknown
}

func (b *cancelAfterCopyBucket) Attributes(context.Context, string) (*driver.Attributes, error) {
	return nil, fs.ErrNotExist
}

func (b *cancelAfterCopyBucket) ListPaged(context.Context, *driver.ListOptions) (*driver.ListPage, error) {
	return &driver.ListPage{}, nil
}

func (b *cancelAfterCopyBucket) Copy(ctx context.Context, dstKey, srcKey string, _ *driver.CopyOptions) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b.copyCtx = ctx
	b.srcKey, b.dstKey = srcKey, dstKey
	b.cancel()

	return nil
}

func (b *cancelAfterCopyBucket) Delete(ctx context.Context, key string, _ *driver.DeleteOptions) error {
	b.deleteCtx = ctx
	if err := ctx.Err(); err != nil {
		return err
	}
	b.deletedKey = key

	return nil
}

func (b *cancelAfterCopyBucket) Close() error { return nil }

func TestBlobFileIORenameFinishesAfterCopyWhenCanceled(t *testing.T) {
	for _, tt := range []struct {
		name   string
		rename func(*FileIO, context.Context, string, string) error
	}{
		{name: "Rename", rename: (*FileIO).Rename},
		{name: "RenameNoReplace", rename: (*FileIO).RenameNoReplace},
	} {
		t.Run(tt.name, func(t *testing.T) {
			type renameContextKey struct{}
			const contextValue = "rename operation"
			opCtx, cancel := context.WithCancel(context.WithValue(context.Background(), renameContextKey{}, contextValue))
			defer cancel()

			testDriver := &cancelAfterCopyBucket{cancel: cancel}
			bucket := blob.NewBucket(testDriver)
			t.Cleanup(func() { require.NoError(t, bucket.Close()) })
			bfs := testBlobFileIO(context.Background(), "test-bucket", bucket)

			require.NoError(t, tt.rename(bfs, opCtx, "s3://test-bucket/data/source.parquet", "s3://test-bucket/data/destination.parquet"))
			require.ErrorIs(t, opCtx.Err(), context.Canceled)
			assert.Equal(t, "data/source.parquet", testDriver.srcKey)
			assert.Equal(t, "data/destination.parquet", testDriver.dstKey)
			assert.Equal(t, "data/source.parquet", testDriver.deletedKey)
			require.NotNil(t, testDriver.copyCtx)
			require.NotNil(t, testDriver.deleteCtx)
			assert.ErrorIs(t, testDriver.copyCtx.Err(), context.Canceled)
			assert.NoError(t, testDriver.deleteCtx.Err())
			assert.Nil(t, testDriver.deleteCtx.Done())
			assert.Equal(t, contextValue, testDriver.deleteCtx.Value(renameContextKey{}))
		})
	}
}

func TestBlobFileIORenameNoReplace(t *testing.T) {
	for _, tt := range []struct {
		name         string
		targetKey    string
		targetPath   string
		existingKey  string
		existingData []byte
		moved        bool
		wantErr      error
	}{
		{
			name:       "moves when destination is missing",
			targetKey:  "data/destination.parquet",
			targetPath: "s3://test-bucket/data/destination.parquet",
			moved:      true,
		},
		{
			name:         "keeps existing destination file",
			targetKey:    "data/destination.parquet",
			targetPath:   "s3://test-bucket/data/destination.parquet",
			existingKey:  "data/destination.parquet",
			existingData: []byte("existing"),
			wantErr:      fs.ErrExist,
		},
		{
			name:         "treats implicit directory as existing",
			targetKey:    "data/destination/child.parquet",
			targetPath:   "s3://test-bucket/data/destination",
			existingKey:  "data/destination/child.parquet",
			existingData: []byte("child"),
			wantErr:      fs.ErrExist,
		},
		{
			name:        "treats directory marker as existing",
			targetKey:   "data/destination/",
			targetPath:  "s3://test-bucket/data/destination",
			existingKey: "data/destination/",
			wantErr:     fs.ErrExist,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			bucket := memblob.OpenBucket(nil)
			defer bucket.Close()
			require.NoError(t, bucket.WriteAll(ctx, "data/source.parquet", []byte("source"), nil))
			if tt.existingKey != "" {
				require.NoError(t, bucket.WriteAll(ctx, tt.existingKey, tt.existingData, nil))
			}
			bfs := testBlobFileIO(ctx, "test-bucket", bucket)

			err := bfs.RenameNoReplace(ctx, "s3://test-bucket/data/source.parquet", tt.targetPath)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}

			sourceExists, err := bucket.Exists(ctx, "data/source.parquet")
			require.NoError(t, err)
			assert.Equal(t, !tt.moved, sourceExists)
			if tt.moved {
				data, err := bucket.ReadAll(ctx, tt.targetKey)
				require.NoError(t, err)
				assert.Equal(t, []byte("source"), data)
			} else if tt.existingData != nil {
				data, err := bucket.ReadAll(ctx, tt.existingKey)
				require.NoError(t, err)
				assert.Equal(t, tt.existingData, data)
			} else {
				exists, err := bucket.Exists(ctx, tt.existingKey)
				require.NoError(t, err)
				assert.True(t, exists)
			}
		})
	}
}

func TestBlobFileIORenameNoReplaceUsesOperationContext(t *testing.T) {
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()
	require.NoError(t, bucket.WriteAll(ctx, "data/source.parquet", []byte("source"), nil))
	require.NoError(t, bucket.WriteAll(ctx, "data/destination.parquet", []byte("existing"), nil))
	bfs := testBlobFileIO(ctx, "test-bucket", bucket)

	opCtx, cancel := context.WithCancel(ctx)
	cancel()
	err := bfs.RenameNoReplace(opCtx, "s3://test-bucket/data/source.parquet", "s3://test-bucket/data/destination.parquet")
	require.ErrorIs(t, err, context.Canceled)
	var pathErr *fs.PathError
	require.ErrorAs(t, err, &pathErr)
	assert.Equal(t, "RenameNoReplace", pathErr.Op)
	assert.Equal(t, "s3://test-bucket/data/destination.parquet", pathErr.Path)
	sourceExists, err := bucket.Exists(ctx, "data/source.parquet")
	require.NoError(t, err)
	assert.True(t, sourceExists)
	destination, err := bucket.ReadAll(ctx, "data/destination.parquet")
	require.NoError(t, err)
	assert.Equal(t, []byte("existing"), destination)
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
	require.NoError(t, bfs.WalkDir(context.Background(), "s3://test-bucket/warehouse/ns", func(path string, d fs.DirEntry, err error) error {
		require.NoError(t, err)
		paths = append(paths, path)

		return nil
	}))
	// There should be exactly one path reported and no dummy directory markers
	assert.Equal(t, []string{"s3://test-bucket/warehouse/ns"}, paths)
}

func TestBlobFileIOWalkDirSkipsSubdirectoryMarkers(t *testing.T) {
	ctx := context.Background()

	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()

	// "tbl/empty/" is a marker with no children, so it is an empty directory.
	for _, k := range []string{"tbl/", "tbl/data/", "tbl/empty/"} {
		require.NoError(t, bucket.WriteAll(ctx, k, nil, nil))
	}
	require.NoError(t, bucket.WriteAll(ctx, "tbl/data/a.parquet", []byte("a"), nil))
	require.NoError(t, bucket.WriteAll(ctx, "tbl/metadata/v1.metadata.json", []byte("b"), nil))

	bfs := testBlobFileIO(ctx, "mybucket", bucket)

	var dirs, files []string
	err := bfs.WalkDir(context.Background(), "s3://mybucket/tbl", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() {
			dirs = append(dirs, path)
		} else {
			files = append(files, path)
		}

		return nil
	})
	require.NoError(t, err)

	assert.Equal(t, []string{
		"s3://mybucket/tbl",
		"s3://mybucket/tbl/data",
		"s3://mybucket/tbl/empty",
		"s3://mybucket/tbl/metadata",
	}, dirs)
	assert.Equal(t, []string{
		"s3://mybucket/tbl/data/a.parquet",
		"s3://mybucket/tbl/metadata/v1.metadata.json",
	}, files)
}

// TestBlobFileIOWalkDirTrailingSlashRoot checks that a root written with a
// trailing slash walks the same paths as the root without one.
func TestBlobFileIOWalkDirTrailingSlashRoot(t *testing.T) {
	ctx := context.Background()

	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()

	require.NoError(t, bucket.WriteAll(ctx, "tbl/data/a.parquet", []byte("a"), nil))
	require.NoError(t, bucket.WriteAll(ctx, "tbl/metadata/v1.metadata.json", []byte("b"), nil))

	bfs := testBlobFileIO(ctx, "mybucket", bucket)

	walk := func(root string) []string {
		var paths []string
		require.NoError(t, bfs.WalkDir(context.Background(), root, func(path string, _ fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			paths = append(paths, path)

			return nil
		}), root)

		return paths
	}

	want := walk("s3://mybucket/tbl")
	require.Len(t, want, 5)
	assert.Equal(t, want, walk("s3://mybucket/tbl/"))
	assert.Equal(t, want, walk("s3://mybucket/tbl//"))
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
		{"remove", func() error { return fileIO.Remove(context.Background(), name) }},
		{"write file", func() error { return fileIO.WriteFile(context.Background(), name, nil) }},
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
	require.NoError(t, fileIO.Remove(context.Background(), "s3://test-bucket/ns/tbl"))

	exists, err := bucket.Exists(ctx, "ns/tbl/")
	require.NoError(t, err)
	assert.False(t, exists)
}

// countingFileIO returns a FileIO over bucket that counts the GETs it issues.
func countingFileIO(ctx context.Context, bucket *blob.Bucket, gets *int) *FileIO {
	return &FileIO{
		Bucket:        bucket,
		extractObject: identityObjectLocation,
		ctx:           ctx,
		newRangeReader: func(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
			*gets++

			return bucket.NewRangeReader(ctx, key, offset, length, nil)
		},
	}
}

func TestOpenReadAtGets(t *testing.T) {
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()

	small := []byte("0123456789")
	large := bytes.Repeat([]byte{7}, wholeObjectReadMaxSize+1)
	require.NoError(t, bucket.WriteAll(ctx, "small", small, nil))
	require.NoError(t, bucket.WriteAll(ctx, "large", large, nil))

	tests := []struct {
		key      string
		content  []byte
		wantGets int
	}{
		{"small", small, 1},
		{"large", large, 3},
	}
	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			var gets int
			f, err := countingFileIO(ctx, bucket, &gets).Open(ctx, tt.key)
			require.NoError(t, err)
			defer f.Close()

			info, err := f.Stat()
			require.NoError(t, err)
			assert.Equal(t, int64(len(tt.content)), info.Size())
			assert.Zero(t, gets, "Open must not read the object")

			for _, off := range []int64{int64(len(tt.content)) - 4, 0, 3} {
				buf := make([]byte, 4)
				_, err := f.ReadAt(buf, off)
				require.NoError(t, err)
				assert.Equal(t, tt.content[off:off+4], buf)
			}
			assert.Equal(t, tt.wantGets, gets)
		})
	}
}

func TestOpenReadSeek(t *testing.T) {
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()

	content := []byte("0123456789")
	require.NoError(t, bucket.WriteAll(ctx, "f", content, nil))

	var gets int
	f, err := countingFileIO(ctx, bucket, &gets).Open(ctx, "f")
	require.NoError(t, err)
	defer f.Close()

	end, err := f.Seek(0, io.SeekEnd)
	require.NoError(t, err)
	assert.Equal(t, int64(len(content)), end)
	assert.Zero(t, gets, "seeking must not read the object")

	_, err = f.Seek(6, io.SeekStart)
	require.NoError(t, err)
	got, err := io.ReadAll(f)
	require.NoError(t, err)
	assert.Equal(t, content[6:], got)

	_, err = f.Seek(-8, io.SeekCurrent)
	require.NoError(t, err)
	buf := make([]byte, 3)
	_, err = io.ReadFull(f, buf)
	require.NoError(t, err)
	assert.Equal(t, content[2:5], buf)
	assert.Equal(t, 2, gets, "one GET per stream started")

	_, err = f.Seek(-1, io.SeekStart)
	assert.Error(t, err)
}
