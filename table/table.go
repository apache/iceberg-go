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
	"iter"
	"runtime"
	"slices"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/internal"
	"github.com/apache/iceberg-go/io"
	tblutils "github.com/apache/iceberg-go/table/internal"
	"golang.org/x/sync/errgroup"
)

type Identifier = []string

type CatalogIO interface {
	LoadTable(context.Context, Identifier, iceberg.Properties) (*Table, error)
	CommitTable(context.Context, *Table, []Requirement, []Update) (Metadata, string, error)
}

type Table struct {
	identifier       Identifier
	metadata         Metadata
	metadataLocation string
	fs               io.IO
	cat              CatalogIO
}

func (t Table) Equals(other Table) bool {
	return slices.Equal(t.identifier, other.identifier) &&
		t.metadataLocation == other.metadataLocation &&
		t.metadata.Equals(other.metadata)
}

func (t Table) Identifier() Identifier               { return t.identifier }
func (t Table) Metadata() Metadata                   { return t.metadata }
func (t Table) MetadataLocation() string             { return t.metadataLocation }
func (t Table) FS() io.IO                            { return t.fs }
func (t Table) Schema() *iceberg.Schema              { return t.metadata.CurrentSchema() }
func (t Table) Spec() iceberg.PartitionSpec          { return t.metadata.PartitionSpec() }
func (t Table) SortOrder() SortOrder                 { return t.metadata.SortOrder() }
func (t Table) Properties() iceberg.Properties       { return t.metadata.Properties() }
func (t Table) NameMapping() iceberg.NameMapping     { return t.metadata.NameMapping() }
func (t Table) Location() string                     { return t.metadata.Location() }
func (t Table) CurrentSnapshot() *Snapshot           { return t.metadata.CurrentSnapshot() }
func (t Table) SnapshotByID(id int64) *Snapshot      { return t.metadata.SnapshotByID(id) }
func (t Table) SnapshotByName(name string) *Snapshot { return t.metadata.SnapshotByName(name) }
func (t Table) Schemas() map[int]*iceberg.Schema {
	m := make(map[int]*iceberg.Schema)
	for _, s := range t.metadata.Schemas() {
		m[s.ID] = s
	}

	return m
}

func (t Table) LocationProvider() (LocationProvider, error) {
	return LoadLocationProvider(t.metadata.Location(), t.metadata.Properties())
}

func (t Table) NewTransaction() *Transaction {
	meta, _ := MetadataBuilderFromBase(t.metadata)

	return &Transaction{
		tbl:  &t,
		meta: meta,
		reqs: []Requirement{},
	}
}

func (t Table) AllManifests() iter.Seq2[iceberg.ManifestFile, error] {
	type list = tblutils.Enumerated[[]iceberg.ManifestFile]
	g := errgroup.Group{}

	n := len(t.metadata.Snapshots())
	ch := make(chan list, n)
	for i, sn := range t.metadata.Snapshots() {
		g.Go(func() error {
			manifests, err := sn.Manifests(t.fs)
			if err != nil {
				return err
			}

			ch <- list{Index: i, Value: manifests, Last: i == n-1}

			return nil
		})
	}

	errch := make(chan error, 1)
	go func() {
		defer close(errch)
		defer close(ch)
		if err := g.Wait(); err != nil {
			errch <- err
		}
	}()

	results := tblutils.MakeSequencedChan(uint(n), ch,
		func(left, right *list) bool {
			switch {
			case left.Index < 0:
				return true
			case right.Index < 0:
				return false
			default:
				return left.Index < right.Index
			}
		}, func(prev, next *list) bool {
			if prev.Index < 0 {
				return next.Index == 0
			}

			return next.Index == prev.Index+1
		}, list{Index: -1})

	return func(yield func(iceberg.ManifestFile, error) bool) {
		defer func() {
			// drain channels if we exited early
			go func() {
				for range results {
				}
				for range errch {
				}
			}()
		}()

		for {
			select {
			case err := <-errch:
				if err != nil {
					yield(nil, err)

					return
				}
			case next, ok := <-results:
				for _, mf := range next.Value {
					if !yield(mf, nil) {
						return
					}
				}

				if next.Last || !ok {
					return
				}
			}
		}
	}
}

func (t Table) doCommit(ctx context.Context, updates []Update, reqs []Requirement) (*Table, error) {
	newMeta, newLoc, err := t.cat.CommitTable(ctx, &t, reqs, updates)
	if err != nil {
		return nil, err
	}

	if err := deleteOldMetadata(t.fs, t.metadata, newMeta); err != nil {
		return nil, err
	}

	return New(t.identifier, newMeta, newLoc, t.fs, t.cat), nil
}

func getFiles(it iter.Seq[MetadataLogEntry]) iter.Seq[string] {
	return func(yield func(string) bool) {
		next, stop := iter.Pull(it)
		defer stop()
		for {
			entry, ok := next()
			if !ok {
				return
			}
			if !yield(entry.MetadataFile) {
				return
			}
		}
	}
}

func deleteOldMetadata(fs io.IO, baseMeta, newMeta Metadata) error {
	deleteAfterCommit := newMeta.Properties().GetBool(MetadataDeleteAfterCommitEnabledKey,
		MetadataDeleteAfterCommitEnabledDefault)

	if deleteAfterCommit {
		removedPrevious := slices.Collect(getFiles(baseMeta.PreviousFiles()))
		currentMetadata := slices.Collect(getFiles(newMeta.PreviousFiles()))
		toRemove := internal.Difference(removedPrevious, currentMetadata)

		for _, file := range toRemove {
			if err := fs.Remove(file); err != nil {
				return fmt.Errorf("failed to delete old metadata file %s: %w", file, err)
			}
		}
	}

	return nil
}

type ScanOption func(*Scan)

func noopOption(*Scan) {}

func WithSelectedFields(fields ...string) ScanOption {
	if len(fields) == 0 || slices.Contains(fields, "*") {
		return noopOption
	}

	return func(scan *Scan) {
		scan.selectedFields = fields
	}
}

func WithRowFilter(e iceberg.BooleanExpression) ScanOption {
	if e == nil || e.Equals(iceberg.AlwaysTrue{}) {
		return noopOption
	}

	return func(scan *Scan) {
		scan.rowFilter = e
	}
}

func WithSnapshotID(n int64) ScanOption {
	if n == 0 {
		return noopOption
	}

	return func(scan *Scan) {
		scan.snapshotID = &n
	}
}

func WithCaseSensitive(b bool) ScanOption {
	return func(scan *Scan) {
		scan.caseSensitive = b
	}
}

func WithLimit(n int64) ScanOption {
	if n < 0 {
		return noopOption
	}

	return func(scan *Scan) {
		scan.limit = n
	}
}

// WitMaxConcurrency sets the maximum concurrency for table scan and plan
// operations. When unset it defaults to runtime.GOMAXPROCS.
func WitMaxConcurrency(n int) ScanOption {
	if n <= 0 {
		return noopOption
	}

	return func(scan *Scan) {
		scan.concurrency = n
	}
}

func WithOptions(opts iceberg.Properties) ScanOption {
	if opts == nil {
		return noopOption
	}

	return func(scan *Scan) {
		scan.options = opts
	}
}

func (t Table) Scan(opts ...ScanOption) *Scan {
	s := &Scan{
		metadata:       t.metadata,
		io:             t.fs,
		rowFilter:      iceberg.AlwaysTrue{},
		selectedFields: []string{"*"},
		caseSensitive:  true,
		limit:          ScanNoLimit,
		concurrency:    runtime.GOMAXPROCS(0),
	}

	for _, opt := range opts {
		opt(s)
	}

	s.partitionFilters = newKeyDefaultMapWrapErr(s.buildPartitionProjection)

	return s
}

func New(ident Identifier, meta Metadata, location string, fs io.IO, cat CatalogIO) *Table {
	return &Table{
		identifier:       ident,
		metadata:         meta,
		metadataLocation: location,
		fs:               fs,
		cat:              cat,
	}
}

func NewFromLocation(ident Identifier, metalocation string, fsys io.IO, cat CatalogIO) (*Table, error) {
	var meta Metadata

	if rf, ok := fsys.(io.ReadFileIO); ok {
		data, err := rf.ReadFile(metalocation)
		if err != nil {
			return nil, err
		}

		if meta, err = ParseMetadataBytes(data); err != nil {
			return nil, err
		}
	} else {
		f, err := fsys.Open(metalocation)
		if err != nil {
			return nil, err
		}
		defer f.Close()

		if meta, err = ParseMetadata(f); err != nil {
			return nil, err
		}
	}

	return New(ident, meta, metalocation, fsys, cat), nil
}
