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
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
)

type DeleteFilesTestSuite struct {
	suite.Suite

	ctx         context.Context
	tableSchema *iceberg.Schema
	arrSchema   *arrow.Schema
	location    string

	formatVersion int
}

func (t *DeleteFilesTestSuite) SetupSuite() {
	t.ctx = context.Background()

	t.tableSchema = iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "foo", Type: iceberg.PrimitiveTypes.Bool},
		iceberg.NestedField{ID: 2, Name: "bar", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 3, Name: "baz", Type: iceberg.PrimitiveTypes.Int32},
		iceberg.NestedField{ID: 4, Name: "qux", Type: iceberg.PrimitiveTypes.Date})

	t.arrSchema = arrow.NewSchema([]arrow.Field{
		{Name: "foo", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		{Name: "bar", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "baz", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "qux", Type: arrow.PrimitiveTypes.Date32, Nullable: true},
	}, nil)
}

func (t *DeleteFilesTestSuite) SetupTest() {
	t.location = filepath.ToSlash(strings.Replace(t.T().TempDir(), "#", "", -1))
}

func (t *DeleteFilesTestSuite) createTable(identifier Identifier, formatVersion int, spec iceberg.PartitionSpec, sc *iceberg.Schema) *Table {
	meta, err := NewMetadata(sc, &spec, UnsortedSortOrder,
		t.location, iceberg.Properties{"format-version": strconv.Itoa(formatVersion)})
	t.Require().NoError(err)

	return New(
		identifier,
		meta,
		fmt.Sprintf("%s/metadata/%05d-%s.metadata.json", t.location, 1, uuid.New().String()),
		func(ctx context.Context) (iceio.IO, error) {
			return iceio.LocalFS{}, nil
		},
		nil,
	)
}

func (t *DeleteFilesTestSuite) TestEmptyTable() {
	ident := Identifier{"default", "delete_files_table_empty_v" + strconv.Itoa(t.formatVersion)}
	table := t.createTable(ident, t.formatVersion, *iceberg.UnpartitionedSpec, t.tableSchema)
	io, err := table.FS(t.ctx)
	t.Require().NoError(err)

	tx := table.NewTransaction()
	updater := tx.updateSnapshot(io, nil).delete()
	df := updater.producerImpl.(*deleteFiles)
	err = df.computeDeletes(iceberg.EqualTo(iceberg.Reference("foo"), true), true)
	t.Require().NoError(err)

	keepManifests, err := df.existingManifests()
	t.Require().NoError(err)
	t.Assert().Empty(keepManifests)

	deletedEntries, err := df.deletedEntries()
	t.Require().NoError(err)
	t.Assert().Empty(deletedEntries)
}

func (t *DeleteFilesTestSuite) TestPredicateMatchedNone() {
	ident := Identifier{"default", "delete_files_table_matched_none_v" + strconv.Itoa(t.formatVersion)}
	table := t.createTable(ident, t.formatVersion, *iceberg.UnpartitionedSpec, t.tableSchema)
	io, err := table.FS(t.ctx)
	t.Require().NoError(err)

	mem := memory.DefaultAllocator
	arrTbl, err := array.TableFromJSON(mem, t.arrSchema, []string{
		`[{"foo": true, "bar": "bar_string", "baz": 123, "qux": "2024-03-07"}]`,
	})
	t.Require().NoError(err)
	defer arrTbl.Release()

	tx := table.NewTransaction()
	t.Require().NoError(tx.AppendTable(t.ctx, arrTbl, 1, nil))

	stagedTbl, err := tx.StagedTable()
	t.Require().NoError(err)

	updater := stagedTbl.NewTransaction().updateSnapshot(io, nil).delete()
	df := updater.producerImpl.(*deleteFiles)

	// predicate does not match any rows
	err = df.computeDeletes(iceberg.EqualTo(iceberg.Reference("baz"), int32(124)), true)
	t.Require().NoError(err)

	manifests, err := stagedTbl.CurrentSnapshot().Manifests(io)
	t.Require().NoError(err)

	// all previous manifests are kept
	keepManifests, err := df.existingManifests()
	t.Require().NoError(err)
	t.Assert().Equal(manifests, keepManifests)

	// no deleted entries
	deletedEntries, err := df.deletedEntries()
	t.Require().NoError(err)
	t.Assert().Empty(deletedEntries)
}

func (t *DeleteFilesTestSuite) TestPredicateMatchedManifest() {
	ident := Identifier{"default", "delete_files_table_matched_manifest_v" + strconv.Itoa(t.formatVersion)}
	table := t.createTable(ident, t.formatVersion, *iceberg.UnpartitionedSpec, t.tableSchema)
	io, err := table.FS(t.ctx)
	t.Require().NoError(err)

	mem := memory.DefaultAllocator
	arrTbl, err := array.TableFromJSON(mem, t.arrSchema, []string{
		`[
			{"foo": true,  "bar": "one",   "baz": 120, "qux": "2024-03-07"},
			{"foo": false, "bar": "two",   "baz": 121, "qux": "2024-03-08"},
			{"foo": true,  "bar": "three", "baz": 122, "qux": "2024-03-09"}
		]`,
	})
	t.Require().NoError(err)
	defer arrTbl.Release()

	tx := table.NewTransaction()
	t.Require().NoError(tx.AppendTable(t.ctx, arrTbl, 1, nil))

	stagedTbl, err := tx.StagedTable()
	t.Require().NoError(err)

	updater := stagedTbl.NewTransaction().updateSnapshot(io, nil).delete()
	df := updater.producerImpl.(*deleteFiles)
	err = df.computeDeletes(iceberg.LessThanEqual(iceberg.Reference("baz"), int32(122)), true)
	t.Require().NoError(err)

	manifests, err := stagedTbl.CurrentSnapshot().Manifests(io)
	t.Require().NoError(err)
	t.Assert().Equal(1, len(manifests))
	entries, err := manifests[0].FetchEntries(io, false)
	t.Require().NoError(err)

	// all previous manifests are not kept
	keepManifests, err := df.existingManifests()
	t.Require().NoError(err)
	t.Assert().Empty(keepManifests)

	// all entries in the manifest file are deleted
	deletedEntries, err := df.deletedEntries()
	t.Require().NoError(err)
	t.Assert().Equal(1, len(deletedEntries))

	// deleted entry use the new snapshot id and update the status to deleted
	t.Assert().Equal(df.base.snapshotID, deletedEntries[0].SnapshotID())
	t.Assert().Equal(iceberg.EntryStatusDELETED, deletedEntries[0].Status())
	t.Assert().Equal(entries[0].SequenceNum(), deletedEntries[0].SequenceNum())
	t.Assert().Equal(entries[0].FileSequenceNum(), deletedEntries[0].FileSequenceNum())
	t.Assert().Equal(entries[0].DataFile().ContentType(), deletedEntries[0].DataFile().ContentType())
	t.Assert().Equal(entries[0].DataFile().FilePath(), deletedEntries[0].DataFile().FilePath())
}

func (t *DeleteFilesTestSuite) TestPredicateMatchedManifestEntries() {
	ident := Identifier{"default", "delete_files_table_matched_manifest_entries_v" + strconv.Itoa(t.formatVersion)}
	table := t.createTable(ident, t.formatVersion, *iceberg.UnpartitionedSpec, t.tableSchema)
	io, err := table.FS(t.ctx)
	t.Require().NoError(err)

	mem := memory.DefaultAllocator
	arrTbl1, err := array.TableFromJSON(mem, t.arrSchema, []string{
		`[
			{"foo": true,  "bar": "one", "baz": 120, "qux": "2024-03-07"},
			{"foo": false, "bar": "two", "baz": 121, "qux": "2024-03-08"}
		]`,
	})
	t.Require().NoError(err)
	defer arrTbl1.Release()

	arrTbl2, err := array.TableFromJSON(mem, t.arrSchema, []string{
		`[{"foo": false,  "bar": "three",   "baz": 122, "qux": "2024-03-09"}]`,
	})
	t.Require().NoError(err)
	defer arrTbl2.Release()

	arrTbl3, err := array.TableFromJSON(mem, t.arrSchema, []string{
		`[{"foo": true,  "bar": "four",   "baz": 123, "qux": "2024-03-10"}]`,
	})
	t.Require().NoError(err)
	defer arrTbl3.Release()

	mergeProps := iceberg.Properties{
		ManifestMergeEnabledKey:  "true",
		ManifestMinMergeCountKey: "1",
	}

	tx := table.NewTransaction()

	err = tx.SetProperties(mergeProps)
	t.Require().NoError(err)

	// create one manifest file with three manifest entries
	t.Require().NoError(tx.AppendTable(t.ctx, arrTbl1, 1, mergeProps))
	t.Require().NoError(tx.AppendTable(t.ctx, arrTbl2, 1, mergeProps))
	t.Require().NoError(tx.AppendTable(t.ctx, arrTbl3, 1, mergeProps))

	stagedTbl, err := tx.StagedTable()
	t.Require().NoError(err)

	manifests, err := stagedTbl.CurrentSnapshot().Manifests(io)
	t.Require().NoError(err)
	t.Assert().Equal(1, len(manifests))

	entries, err := manifests[0].FetchEntries(io, false)
	t.Require().NoError(err)
	t.Assert().Equal(3, len(entries))

	updater := stagedTbl.NewTransaction().updateSnapshot(io, nil).delete()
	df := updater.producerImpl.(*deleteFiles)
	err = df.computeDeletes(iceberg.EqualTo(iceberg.Reference("baz"), int32(122)), true)
	t.Require().NoError(err)

	// new manifest file created rewritten with remaining two entries
	keepManifests, err := df.existingManifests()
	t.Require().NoError(err)
	t.Assert().Equal(1, len(keepManifests))
	t.Assert().Equal(iceberg.ManifestContentData, keepManifests[0].ManifestContent())
	t.Assert().Equal(df.base.snapshotID, keepManifests[0].SnapshotID())
	t.Assert().Equal(int64(-1), keepManifests[0].SequenceNum())
	t.Assert().Equal(int32(0), keepManifests[0].AddedDataFiles())
	t.Assert().Equal(int32(2), keepManifests[0].ExistingDataFiles())
	t.Assert().Equal(int32(0), keepManifests[0].DeletedDataFiles())

	newEntries, err := keepManifests[0].FetchEntries(io, false)
	t.Require().NoError(err)
	t.Assert().Equal(2, len(newEntries))

	t.Assert().Equal(iceberg.EntryStatusEXISTING, newEntries[0].Status())
	if t.formatVersion == 1 {
		t.Assert().Equal(int64(-1), newEntries[0].SequenceNum())
		t.Assert().Nil(newEntries[0].FileSequenceNum())
	}
	if t.formatVersion == 2 {
		t.Assert().Equal(entries[0].SequenceNum(), newEntries[0].SequenceNum())
		t.Assert().Equal(entries[0].FileSequenceNum(), newEntries[0].FileSequenceNum())
	}
	t.Assert().Equal(entries[0].DataFile().ContentType(), newEntries[0].DataFile().ContentType())
	t.Assert().Equal(entries[0].DataFile().FilePath(), newEntries[0].DataFile().FilePath())

	t.Assert().Equal(iceberg.EntryStatusEXISTING, newEntries[1].Status())
	if t.formatVersion == 1 {
		t.Assert().Equal(int64(-1), newEntries[1].SequenceNum())
		t.Assert().Nil(newEntries[1].FileSequenceNum())
	}
	if t.formatVersion == 2 {
		t.Assert().Equal(entries[2].SequenceNum(), newEntries[1].SequenceNum())
		t.Assert().Equal(entries[2].FileSequenceNum(), newEntries[1].FileSequenceNum())
	}
	t.Assert().Equal(entries[2].DataFile().ContentType(), newEntries[1].DataFile().ContentType())
	t.Assert().Equal(entries[2].DataFile().FilePath(), newEntries[1].DataFile().FilePath())

	// No data file require rewrite
	needRewrite, err := df.rewriteNeeded()
	t.Require().NoError(err)
	t.Assert().False(needRewrite)
}

func (t *DeleteFilesTestSuite) TestPredicateMatchedPartialDataFile() {
	ident := Identifier{"default", "delete_files_table_matched_manifest_entries_v" + strconv.Itoa(t.formatVersion)}
	table := t.createTable(ident, t.formatVersion, *iceberg.UnpartitionedSpec, t.tableSchema)
	io, err := table.FS(t.ctx)
	t.Require().NoError(err)

	mem := memory.DefaultAllocator
	arrTbl, err := array.TableFromJSON(mem, t.arrSchema, []string{
		`[
			{"foo": true,  "bar": "one",   "baz": 120, "qux": "2024-03-07"},
			{"foo": false, "bar": "two",   "baz": 121, "qux": "2024-03-08"},
			{"foo": true,  "bar": "three", "baz": 122, "qux": "2024-03-09"},
			{"foo": false, "bar": "four",  "baz": 123, "qux": "2024-03-10"}
		]`,
	})
	t.Require().NoError(err)
	defer arrTbl.Release()

	tx := table.NewTransaction()

	t.Require().NoError(tx.AppendTable(t.ctx, arrTbl, 1, nil))

	stagedTbl, err := tx.StagedTable()
	t.Require().NoError(err)

	manifests, err := stagedTbl.CurrentSnapshot().Manifests(io)
	t.Require().NoError(err)
	t.Assert().Equal(1, len(manifests))
	entries, err := manifests[0].FetchEntries(io, false)
	t.Require().NoError(err)
	t.Assert().Equal(1, len(entries))

	// delete baz >= 122
	updater := stagedTbl.NewTransaction().updateSnapshot(io, nil).delete()
	df := updater.producerImpl.(*deleteFiles)
	err = df.computeDeletes(iceberg.GreaterThanEqual(iceberg.Reference("baz"), int32(122)), true)
	t.Require().NoError(err)

	// nothing to delete on the manifest and manifest entry level
	keepManifests, err := df.existingManifests()
	t.Require().NoError(err)
	t.Assert().Equal(1, len(keepManifests))
	t.Assert().Equal(manifests[0], keepManifests[0])

	newEntries, err := keepManifests[0].FetchEntries(io, false)
	t.Require().NoError(err)
	t.Assert().Equal(1, len(newEntries))
	t.Assert().Equal(entries[0], newEntries[0])

	// predicate matched part of the data file, rewrite is needed
	t.Assert().True(df.rewriteNeeded())
}

func TestSnapshotProducer(t *testing.T) {
	suite.Run(t, &DeleteFilesTestSuite{formatVersion: 1})
	suite.Run(t, &DeleteFilesTestSuite{formatVersion: 2})
}
