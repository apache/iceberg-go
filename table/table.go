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

package table

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"reflect"
	"time"

	"github.com/oklog/ulid"
	"github.com/parquet-go/parquet-go"
	"github.com/polarsignals/iceberg-go"
	"github.com/thanos-io/objstore"
	"golang.org/x/exp/slices"
)

const (
	manifestFileExt = ".avro"
	metadataFileExt = ".json"
	metadataDirName = "metadata"
	dataDirName     = "data"
)

type Identifier = []string

type Table interface {
	Identifier() Identifier
	Metadata() Metadata
	MetadataLocation() string
	Bucket() objstore.Bucket
	Schema() *iceberg.Schema
	Spec() iceberg.PartitionSpec
	SortOrder() SortOrder
	Properties() iceberg.Properties
	Location() string
	CurrentSnapshot() *Snapshot
	SnapshotByID(id int64) *Snapshot
	SnapshotByName(name string) *Snapshot
	Schemas() map[int]*iceberg.Schema
	Equals(other Table) bool

	SnapshotWriter(options ...WriterOption) (SnapshotWriter, error)
}

type ReadOnlyTable struct {
	*baseTable
}

func (r *ReadOnlyTable) SnapshotWriter(options ...WriterOption) (SnapshotWriter, error) {
	return nil, fmt.Errorf("table is read-only")
}

type baseTable struct {
	identifier       Identifier
	metadata         Metadata
	metadataLocation string
	bucket           objstore.Bucket
}

func (t *baseTable) Equals(other Table) bool {
	return slices.Equal(t.identifier, other.Identifier()) &&
		t.metadataLocation == other.MetadataLocation() &&
		reflect.DeepEqual(t.metadata, other.Metadata())
}

func (t *baseTable) Identifier() Identifier   { return t.identifier }
func (t *baseTable) Metadata() Metadata       { return t.metadata }
func (t *baseTable) MetadataLocation() string { return t.metadataLocation }
func (t *baseTable) Bucket() objstore.Bucket  { return t.bucket }

func (t *baseTable) Schema() *iceberg.Schema              { return t.metadata.CurrentSchema() }
func (t *baseTable) Spec() iceberg.PartitionSpec          { return t.metadata.PartitionSpec() }
func (t *baseTable) SortOrder() SortOrder                 { return t.metadata.SortOrder() }
func (t *baseTable) Properties() iceberg.Properties       { return t.metadata.Properties() }
func (t *baseTable) Location() string                     { return t.metadata.Location() }
func (t *baseTable) CurrentSnapshot() *Snapshot           { return t.metadata.CurrentSnapshot() }
func (t *baseTable) SnapshotByID(id int64) *Snapshot      { return t.metadata.SnapshotByID(id) }
func (t *baseTable) SnapshotByName(name string) *Snapshot { return t.metadata.SnapshotByName(name) }
func (t *baseTable) Schemas() map[int]*iceberg.Schema {
	m := make(map[int]*iceberg.Schema)
	for _, s := range t.metadata.Schemas() {
		m[s.ID] = s
	}
	return m
}

func New(ident Identifier, meta Metadata, location string, bucket objstore.Bucket) Table {
	return &ReadOnlyTable{
		baseTable: &baseTable{
			identifier:       ident,
			metadata:         meta,
			metadataLocation: location,
			bucket:           bucket,
		},
	}
}

func NewFromLocation(ident Identifier, metalocation string, bucket objstore.Bucket) (Table, error) {
	var meta Metadata

	r, err := bucket.Get(context.Background(), metalocation)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	if meta, err = ParseMetadata(r); err != nil {
		return nil, err
	}

	return New(ident, meta, metalocation, bucket), nil
}

// SnapshotWriter is an interface for writing a new snapshot to a table.
type SnapshotWriter interface {
	// Append accepts a ReaderAt object that should read the Parquet file that is to be added to the snapshot.
	Append(ctx context.Context, r io.Reader) error

	// Close writes the new snapshot to the table and closes the writer. It is an error to call Append after Close.
	Close(ctx context.Context) error
}

type WriterOption func(*writerOptions)

type writerOptions struct {
	fastAppendMode           bool
	manifestSizeBytes        int
	mergeSchema              bool
	expireSnapshotsOlderThan time.Duration
}

func WithExpireSnapshotsOlderThan(d time.Duration) WriterOption {
	return func(o *writerOptions) {
		o.expireSnapshotsOlderThan = d
	}
}

func WithMergeSchema() WriterOption {
	return func(o *writerOptions) {
		o.mergeSchema = true
	}
}

func WithFastAppend() WriterOption {
	return func(o *writerOptions) {
		o.fastAppendMode = true
	}
}

func WithManifestSizeBytes(size int) WriterOption {
	return func(o *writerOptions) {
		o.manifestSizeBytes = size
	}
}

func generateULID() ulid.ULID {
	t := time.Now()
	entropy := ulid.Monotonic(rand.New(rand.NewSource(t.UnixNano())), 0)
	return ulid.MustNew(ulid.Timestamp(t), entropy)
}

func parquetSchemaToIcebergSchema(id int, schema *parquet.Schema) *iceberg.Schema {
	fields := make([]iceberg.NestedField, 0, len(schema.Fields()))
	for i, f := range schema.Fields() {
		fields = append(fields, iceberg.NestedField{
			Type:     parquetTypeToIcebergType(f.Type()),
			ID:       i,
			Name:     f.Name(),
			Required: f.Required(),
		})
	}
	return iceberg.NewSchema(id, fields...)
}

func parquetTypeToIcebergType(t parquet.Type) iceberg.Type {
	switch tp := t.Kind(); tp {
	case parquet.Boolean:
		return iceberg.BooleanType{}
	case parquet.Int32:
		return iceberg.Int32Type{}
	case parquet.Int64:
		return iceberg.Int64Type{}
	case parquet.Float:
		return iceberg.Float32Type{}
	case parquet.Double:
		return iceberg.Float64Type{}
	case parquet.ByteArray:
		return iceberg.BinaryType{}
	default:
		panic(fmt.Sprintf("unsupported parquet type: %v", tp))
	}
}
