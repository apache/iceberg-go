package catalog

import (
	"context"
	"io"
	"strings"

	"github.com/thanos-io/objstore"
)

// icebucket is a wrapper around an objstore.Bucket.
// Iceberg files are written with the full uri path s3://bucket-name/data-warehouse, gs://bucket-name/data-warehouse, /Users/username/Documents/data-warehouse
// icebucket is used to strip the full path from the object name when interacting with the bucket, since buckets are only pointed at the warehouse level.
type icebucket struct {
	bucket objstore.Bucket

	// prefix is the full path prefix of the data warehouse. Ex. s3://bucket-name/data-warehouse, gs://bucket-name/data-warehouse, /Users/username/Documents/data-warehouse
	prefix string
}

// NewIcebucket creates a new icebucket with the given prefix and bucket.
// The warehouseURI is used to strip the full path of the data warehouse from the object name.
func NewIcebucket(warehouseURI string, bucket objstore.Bucket) *icebucket {
	return &icebucket{
		bucket: bucket,
		prefix: warehouseURI,
	}
}

// Upload the contents of the reader as an object into the bucket.
// Upload should be idempotent.
func (i *icebucket) Upload(ctx context.Context, name string, r io.Reader) error {
	return i.bucket.Upload(ctx, strings.TrimPrefix(name, i.prefix), r)
}

// Delete removes the object with the given name.
// If object does not exist in the moment of deletion, Delete should throw error.
func (i *icebucket) Delete(ctx context.Context, name string) error {
	// Strip the prefix from the name if one exists.
	return i.bucket.Delete(ctx, strings.TrimPrefix(name, i.prefix))
}

func (i *icebucket) Name() string { return i.bucket.Name() }

func (i *icebucket) Close() error { return i.bucket.Close() }

func (i *icebucket) Iter(ctx context.Context, dir string, f func(string) error, options ...objstore.IterOption) error {
	return i.bucket.Iter(ctx, strings.TrimPrefix(dir, i.prefix), f, options...)
}

// Get returns a reader for the given object name.
func (i *icebucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	return i.bucket.Get(ctx, strings.TrimPrefix(name, i.prefix))
}

// GetRange returns a new range reader for the given object name and range.
func (i *icebucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	return i.bucket.GetRange(ctx, strings.TrimPrefix(name, i.prefix), off, length)
}

// Exists checks if the given object exists in the bucket.
func (i *icebucket) Exists(ctx context.Context, name string) (bool, error) {
	return i.bucket.Exists(ctx, strings.TrimPrefix(name, i.prefix))
}

// IsObjNotFoundErr returns true if error means that object is not found. Relevant to Get operations.
func (i *icebucket) IsObjNotFoundErr(err error) bool { return i.bucket.IsObjNotFoundErr(err) }

// IsAccessDeniedErr returns true if access to object is denied.
func (i *icebucket) IsAccessDeniedErr(err error) bool { return i.bucket.IsAccessDeniedErr(err) }

// Attributes returns information about the specified object.
func (i *icebucket) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	return i.bucket.Attributes(ctx, strings.TrimPrefix(name, i.prefix))
}
