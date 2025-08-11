package table

import (
	"context"
	"fmt"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

type DeleteFilesTestSuite struct {
	suite.Suite

	ctx         context.Context
	tableSchema *iceberg.Schema
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
	table := t.createTable(ident, t.formatVersion,
		*iceberg.UnpartitionedSpec, t.tableSchema)

	tx := table.NewTransaction()
	updater := tx.updateSnapshot(iceio.LocalFS{}, nil).delete()
	df := updater.producerImpl.(*deleteFiles)
	err := df.computeDeletes(iceberg.EqualTo(iceberg.Reference("foo"), true), true)
	t.Require().NoError(err)

	updates, reqs, err := updater.commit()
	t.Require().NoError(err)

	t.Assert().Equal(2, len(updates))
	t.Assert().Equal(UpdateAddSnapshot, updates[0].Action())
	snapshot := updates[0].(*addSnapshotUpdate).Snapshot
	t.Assert().Equal(OpDelete, snapshot.Summary.Operation)
	t.Assert().Equal(iceberg.Properties{
		totalDataFilesKey:   "0",
		totalDeleteFilesKey: "0",
		totalEqDeletesKey:   "0",
		totalFileSizeKey:    "0",
		totalPosDeletesKey:  "0",
		totalRecordsKey:     "0",
	}, snapshot.Summary.Properties)

	t.Assert().Equal(UpdateSetSnapshotRef, updates[1].Action())
	t.Assert().Equal(MainBranch, updates[1].(*setSnapshotRefUpdate).RefName)

	t.Assert().Equal(1, len(reqs))
	t.Assert().Equal(reqAssertRefSnapshotID, reqs[0].GetType())
	t.Assert().Equal(MainBranch, reqs[0].(*assertRefSnapshotID).Ref)
}

func (t *DeleteFilesTestSuite) TestPredicateMatchedNone() {

}

func (t *DeleteFilesTestSuite) TestPredicateMatchedPartition() {

}

func (t *DeleteFilesTestSuite) TestPredicateMatchedManifest() {

}

func (t *DeleteFilesTestSuite) TestPredicateMatchedManifestEntries() {

}

func TestSnapshotProducer(t *testing.T) {
	suite.Run(t, &DeleteFilesTestSuite{formatVersion: 1})
	suite.Run(t, &DeleteFilesTestSuite{formatVersion: 2})
}
