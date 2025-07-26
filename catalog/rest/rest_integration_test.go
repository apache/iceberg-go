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

//go:build integration

package rest_test

import (
	"context"
	"fmt"
	"net/url"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type RestIntegrationSuite struct {
	suite.Suite

	ctx context.Context
	cat *rest.Catalog
}

const TestNamespaceIdent = "TEST NS"

func (s *RestIntegrationSuite) loadCatalog(ctx context.Context) *rest.Catalog {
	cat, err := catalog.Load(ctx, "local", iceberg.Properties{
		"type":               "rest",
		"uri":                "http://localhost:8181",
		io.S3Region:          "us-east-1",
		io.S3AccessKeyID:     "admin",
		io.S3SecretAccessKey: "password",
		io.S3EndpointURL:     "http://localhost:9000",
	})
	s.Require().NoError(err)
	s.Require().IsType(&rest.Catalog{}, cat)
	return cat.(*rest.Catalog)
}

func (s *RestIntegrationSuite) SetupTest() {
	s.ctx = context.Background()
	s.cat = s.loadCatalog(s.ctx)
}

var (
	tableSchemaNested = iceberg.NewSchemaWithIdentifiers(1,
		[]int{1},
		iceberg.NestedField{
			ID: 1, Name: "foo", Type: iceberg.PrimitiveTypes.String, Required: true},
		iceberg.NestedField{
			ID: 2, Name: "bar", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{
			ID: 3, Name: "baz", Type: iceberg.PrimitiveTypes.Bool, Required: false},
		iceberg.NestedField{
			ID: 4, Name: "qux", Required: true, Type: &iceberg.ListType{
				ElementID: 5, Element: iceberg.PrimitiveTypes.String, ElementRequired: true}},
		iceberg.NestedField{
			ID: 6, Name: "quux",
			Type: &iceberg.MapType{
				KeyID:   7,
				KeyType: iceberg.PrimitiveTypes.String,
				ValueID: 8,
				ValueType: &iceberg.MapType{
					KeyID:         9,
					KeyType:       iceberg.PrimitiveTypes.String,
					ValueID:       10,
					ValueType:     iceberg.PrimitiveTypes.Int32,
					ValueRequired: true,
				},
				ValueRequired: true,
			},
			Required: true},
		iceberg.NestedField{
			ID: 11, Name: "location", Type: &iceberg.ListType{
				ElementID: 12, Element: &iceberg.StructType{
					FieldList: []iceberg.NestedField{
						{ID: 13, Name: "latitude", Type: iceberg.PrimitiveTypes.Float32, Required: false},
						{ID: 14, Name: "longitude", Type: iceberg.PrimitiveTypes.Float32, Required: false},
					},
				},
				ElementRequired: true},
			Required: true},
		iceberg.NestedField{
			ID:   15,
			Name: "person",
			Type: &iceberg.StructType{
				FieldList: []iceberg.NestedField{
					{ID: 16, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false},
					{ID: 17, Name: "age", Type: iceberg.PrimitiveTypes.Int32, Required: true},
				},
			},
			Required: false,
		},
	)
)

func (s *RestIntegrationSuite) ensureNamespace() {
	exists, err := s.cat.CheckNamespaceExists(s.ctx, catalog.ToIdentifier(TestNamespaceIdent))
	s.Require().NoError(err)
	if exists {
		s.Require().NoError(s.cat.DropNamespace(s.ctx, catalog.ToIdentifier(TestNamespaceIdent)))
	}

	s.NoError(s.cat.CreateNamespace(s.ctx, catalog.ToIdentifier(TestNamespaceIdent),
		iceberg.Properties{"foo": "bar", "prop": "yes"}))
}

func (s *RestIntegrationSuite) TestCreateNamespace() {
	exists, err := s.cat.CheckNamespaceExists(s.ctx, catalog.ToIdentifier(TestNamespaceIdent))
	s.Require().NoError(err)
	if exists {
		s.Require().NoError(s.cat.DropNamespace(s.ctx, catalog.ToIdentifier(TestNamespaceIdent)))
	}

	s.NoError(s.cat.CreateNamespace(s.ctx, catalog.ToIdentifier(TestNamespaceIdent), nil))
	s.NoError(s.cat.DropNamespace(s.ctx, catalog.ToIdentifier(TestNamespaceIdent)))
}

func (s *RestIntegrationSuite) TestLoadNamespaceProps() {
	s.ensureNamespace()

	props, err := s.cat.LoadNamespaceProperties(s.ctx, catalog.ToIdentifier(TestNamespaceIdent))
	s.Require().NoError(err)
	s.Require().NotNil(props)
	s.Equal("bar", props["foo"])
	s.Equal("yes", props["prop"])
}

func (s *RestIntegrationSuite) TestUpdateNamespaceProps() {
	s.ensureNamespace()

	summary, err := s.cat.UpdateNamespaceProperties(s.ctx, catalog.ToIdentifier(TestNamespaceIdent),
		[]string{"abc"}, iceberg.Properties{"prop": "no"})
	s.Require().NoError(err)
	s.Equal(catalog.PropertiesUpdateSummary{
		Removed: []string{},
		Updated: []string{"prop"},
		Missing: []string{"abc"},
	}, summary)
}

func (s *RestIntegrationSuite) TestCreateTable() {
	s.ensureNamespace()

	const location = "s3://warehouse/iceberg"

	tbl, err := s.cat.CreateTable(s.ctx,
		catalog.ToIdentifier(TestNamespaceIdent, "test-table"),
		tableSchemaSimple, catalog.WithProperties(iceberg.Properties{"foobar": "baz"}),
		catalog.WithLocation(location))
	s.Require().NoError(err)
	s.Require().NotNil(tbl)

	s.Equal(location, tbl.Location())
	s.Equal("baz", tbl.Properties()["foobar"])

	exists, err := s.cat.CheckTableExists(s.ctx, catalog.ToIdentifier(TestNamespaceIdent, "test-table"))
	s.Require().NoError(err)
	s.True(exists)

	s.Require().NoError(s.cat.DropTable(s.ctx, catalog.ToIdentifier(TestNamespaceIdent, "test-table")))
}

func (s *RestIntegrationSuite) TestCreateView() {
	s.ensureNamespace()

	const location = "s3://warehouse/iceberg"
	// create a table first
	tbl, err := s.cat.CreateTable(s.ctx,
		catalog.ToIdentifier(TestNamespaceIdent, "test-table"),
		tableSchemaSimple, catalog.WithProperties(iceberg.Properties{"foobar": "baz"}),
		catalog.WithLocation(location))
	s.Require().NoError(err)
	s.Require().NotNil(tbl)

	s.Equal(location, tbl.Location())
	s.Equal("baz", tbl.Properties()["foobar"])

	exists, err := s.cat.CheckTableExists(s.ctx, catalog.ToIdentifier(TestNamespaceIdent, "test-table"))
	s.Require().NoError(err)
	s.True(exists)

	// Create a view
	viewSQL := fmt.Sprintf("SELECT * FROM  %s.%s", TestNamespaceIdent, "test-table")

	err = s.cat.CreateView(s.ctx, catalog.ToIdentifier(TestNamespaceIdent, "test-view"), tableSchemaSimple, viewSQL, iceberg.Properties{"foobar": "baz"})
	s.Require().NoError(err)

	exists, err = s.cat.CheckViewExists(s.ctx, catalog.ToIdentifier(TestNamespaceIdent, "test-view"))
	s.Require().NoError(err)
	s.True(exists)

	s.Require().NoError(s.cat.DropTable(s.ctx, catalog.ToIdentifier(TestNamespaceIdent, "test-table")))
	s.Require().NoError(s.cat.DropView(s.ctx, catalog.ToIdentifier(TestNamespaceIdent, "test-view")))
}

func (s *RestIntegrationSuite) TestWriteCommitTable() {
	s.ensureNamespace()

	const location = "s3://warehouse/iceberg"

	tbl, err := s.cat.CreateTable(s.ctx,
		catalog.ToIdentifier(TestNamespaceIdent, "test-table-2"),
		tableSchemaNested, catalog.WithLocation(location))
	s.Require().NoError(err)
	s.Require().NotNil(tbl)

	defer func() {
		s.Require().NoError(s.cat.DropTable(s.ctx, catalog.ToIdentifier(TestNamespaceIdent, "test-table-2")))
	}()

	s.Equal(location, tbl.Location())

	arrSchema, err := table.SchemaToArrowSchema(tableSchemaNested, nil, false, false)
	s.Require().NoError(err)

	table, err := array.TableFromJSON(memory.DefaultAllocator, arrSchema,
		[]string{`[
		{
			"foo": "foo_string",
			"bar": 123,
			"baz": true,
			"qux": ["a", "b", "c"],
			"quux": [{"key": "gopher", "value": [
				{"key": "golang", "value": "1337"}]}],
			"location": [{"latitude": 37.7749, "longitude": -122.4194}],
			"person": {"name": "gopher", "age": 10}
		}
	]`})
	s.Require().NoError(err)
	defer table.Release()

	pqfile, err := url.JoinPath(location, "data", "test_commit_table_data", "test.parquet")
	s.Require().NoError(err)

	fw, err := mustFS(s.T(), tbl).(io.WriteFileIO).Create(pqfile)
	s.Require().NoError(err)
	s.Require().NoError(pqarrow.WriteTable(table, fw, table.NumRows(),
		nil, pqarrow.DefaultWriterProps()))
	defer mustFS(s.T(), tbl).Remove(pqfile)

	txn := tbl.NewTransaction()
	s.Require().NoError(txn.AddFiles(s.ctx, []string{pqfile}, nil, false))
	updated, err := txn.Commit(s.ctx)
	s.Require().NoError(err)

	mf := []iceberg.ManifestFile{}
	for m, err := range updated.AllManifests(s.ctx) {
		s.Require().NoError(err)
		s.Require().NotNil(m)
		mf = append(mf, m)
	}

	s.Len(mf, 1)
	s.EqualValues(1, mf[0].AddedDataFiles())
	entries, err := mf[0].FetchEntries(mustFS(s.T(), updated), false)
	s.Require().NoError(err)

	s.Len(entries, 1)
	s.Equal(pqfile, entries[0].DataFile().FilePath())
}

func (s *RestIntegrationSuite) TestUpdateSchema() {
	s.ensureNamespace()

	const location = "s3://warehouse/iceberg"

	// Create table with initial schema
	initialSchema := iceberg.NewSchemaWithIdentifiers(1, []int{2},
		iceberg.NestedField{ID: 1, Name: "foo", Type: iceberg.PrimitiveTypes.String, Required: false},
		iceberg.NestedField{ID: 2, Name: "bar", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 3, Name: "temp_col", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: "Temporary column"},
	)

	tbl, err := s.cat.CreateTable(s.ctx,
		catalog.ToIdentifier(TestNamespaceIdent, "test-schema-update"),
		initialSchema, catalog.WithLocation(location))
	s.Require().NoError(err)
	s.Require().NotNil(tbl)

	defer func() {
		s.Require().NoError(s.cat.DropTable(s.ctx, catalog.ToIdentifier(TestNamespaceIdent, "test-schema-update")))
	}()

	// Verify initial schema
	s.Equal(3, tbl.Schema().NumFields())
	s.Equal("foo", tbl.Schema().Field(0).Name)
	s.Equal("bar", tbl.Schema().Field(1).Name)
	s.Equal("temp_col", tbl.Schema().Field(2).Name)
	s.Equal(iceberg.PrimitiveTypes.String, tbl.Schema().Field(0).Type)
	s.Equal(iceberg.PrimitiveTypes.Int32, tbl.Schema().Field(1).Type)
	s.Equal(iceberg.PrimitiveTypes.String, tbl.Schema().Field(2).Type)

	// Write initial data with the original schema
	arrSchema, err := table.SchemaToArrowSchema(initialSchema, nil, false, false)
	s.Require().NoError(err)

	initialTable, err := array.TableFromJSON(memory.DefaultAllocator, arrSchema,
		[]string{`[
		{
			"foo": "test_string",
			"bar": 42,
			"temp_col": "temporary_value"
		},
		{
			"foo": "another_string",
			"bar": 123,
			"temp_col": "another_temp"
		}
	]`})
	s.Require().NoError(err)
	defer initialTable.Release()

	pqfile, err := url.JoinPath(location, "data", "test_schema_update_data", "initial.parquet")
	s.Require().NoError(err)

	fw, err := tbl.FS().(io.WriteFileIO).Create(pqfile)
	s.Require().NoError(err)
	s.Require().NoError(pqarrow.WriteTable(initialTable, fw, initialTable.NumRows(),
		nil, pqarrow.DefaultWriterProps()))
	defer tbl.FS().Remove(pqfile)

	// Add initial data to the table
	txn := tbl.NewTransaction()
	s.Require().NoError(txn.AddFiles(s.ctx, []string{pqfile}, nil, false))
	tbl, err = txn.Commit(s.ctx)
	s.Require().NoError(err)

	// Perform multiple schema operations in one transaction
	txn = tbl.NewTransaction()
	updateSchema := txn.UpdateSchema()

	// Update schema 1. Add a new column
	updated, err := updateSchema.AddColumn([]string{"baz"}, false, iceberg.PrimitiveTypes.Bool, "Boolean flag", nil)
	s.Require().NoError(err)

	// Update schema 2. Update existing column (type promotion: Int32 -> Int64)
	updated, err = updated.UpdateColumn([]string{"bar"}, table.ColumnUpdate{
		Type: iceberg.Optional[iceberg.Type]{Val: iceberg.PrimitiveTypes.Int64, Valid: true},
		Doc:  iceberg.Optional[string]{Val: "Updated bar column - promoted to int64", Valid: true},
	})
	s.Require().NoError(err)

	// Update schema 3. Update another column (documentation only)
	updated, err = updated.UpdateColumn([]string{"foo"}, table.ColumnUpdate{
		Doc: iceberg.Optional[string]{Val: "Updated foo column documentation", Valid: true},
	})
	s.Require().NoError(err)

	// Update schema 4. Delete the temporary column
	updated, err = updated.DeleteColumn([]string{"temp_col"})
	s.Require().NoError(err)

	// Commit schema changes
	s.Require().NoError(updated.Commit())

	// Commit the transaction
	updatedTbl, err := txn.Commit(s.ctx)
	s.Require().NoError(err)
	s.Require().NotNil(updatedTbl)

	s.Equal(3, updatedTbl.Schema().NumFields(), "Should have 3 fields after operations")

	s.Equal("foo", updatedTbl.Schema().Field(0).Name)
	s.Equal("bar", updatedTbl.Schema().Field(1).Name)
	s.Equal("baz", updatedTbl.Schema().Field(2).Name)

	s.Equal(iceberg.PrimitiveTypes.String, updatedTbl.Schema().Field(0).Type)
	s.Equal(iceberg.PrimitiveTypes.Int64, updatedTbl.Schema().Field(1).Type, "bar should be promoted to int64")
	s.Equal(iceberg.PrimitiveTypes.Bool, updatedTbl.Schema().Field(2).Type)

	s.False(updatedTbl.Schema().Field(0).Required)
	s.True(updatedTbl.Schema().Field(1).Required)
	s.False(updatedTbl.Schema().Field(2).Required)

	s.Equal("Updated foo column documentation", updatedTbl.Schema().Field(0).Doc)
	s.Equal("Updated bar column - promoted to int64", updatedTbl.Schema().Field(1).Doc)
	s.Equal("Boolean flag", updatedTbl.Schema().Field(2).Doc)

	_, found := updatedTbl.Schema().FindFieldByName("temp_col")
	s.False(found, "temp_col should be deleted")

	// Reload table from catalog
	reloadedTbl, err := s.cat.LoadTable(s.ctx, catalog.ToIdentifier(TestNamespaceIdent, "test-schema-update"), nil)
	s.Require().NoError(err)
	s.Require().NotNil(reloadedTbl)

	s.Equal(3, reloadedTbl.Schema().NumFields())

	s.Equal(iceberg.PrimitiveTypes.Int64, reloadedTbl.Schema().Field(1).Type)

	s.Equal("baz", reloadedTbl.Schema().Field(2).Name)
	s.Equal(iceberg.PrimitiveTypes.Bool, reloadedTbl.Schema().Field(2).Type)
	s.False(reloadedTbl.Schema().Field(2).Required)

	s.Equal("Updated foo column documentation", reloadedTbl.Schema().Field(0).Doc)
	s.Equal("Updated bar column - promoted to int64", reloadedTbl.Schema().Field(1).Doc)

	_, found = reloadedTbl.Schema().FindFieldByName("temp_col")
	s.False(found, "temp_col should remain deleted after reload")

	// Test reading the table
	scanResult, err := reloadedTbl.Scan().ToArrowTable(s.ctx)
	s.Require().NoError(err)
	s.Require().NotNil(scanResult)
	defer scanResult.Release()

	s.Equal(int64(3), scanResult.NumCols(), "Should have 3 columns after evolution")
	s.Equal("foo", scanResult.Schema().Field(0).Name)
	s.Equal("bar", scanResult.Schema().Field(1).Name)
	s.Equal("baz", scanResult.Schema().Field(2).Name)

	s.Equal(int64(2), scanResult.NumRows(), "Should read 2 rows of data")

	fooCol := scanResult.Column(0)
	barCol := scanResult.Column(1)
	bazCol := scanResult.Column(2)

	fooChunk := fooCol.Data().Chunk(0).(*array.String)
	s.Equal("test_string", fooChunk.Value(0))
	s.Equal("another_string", fooChunk.Value(1))

	barChunk := barCol.Data().Chunk(0).(*array.Int64)
	s.Equal(int64(42), barChunk.Value(0))
	s.Equal(int64(123), barChunk.Value(1))

	// Check that new boolean column is null for existing records
	bazChunk := bazCol.Data().Chunk(0).(*array.Boolean)
	s.True(bazChunk.IsNull(0), "New column should be null for existing records")
	s.True(bazChunk.IsNull(1), "New column should be null for existing records")

	// Verify temp_col is not present in the results
	for i := 0; i < int(scanResult.NumCols()); i++ {
		s.NotEqual("temp_col", scanResult.Schema().Field(i).Name, "temp_col should not be present in results")
	}
func mustFS(t *testing.T, tbl *table.Table) io.IO {
	r, err := tbl.FS(context.Background())
	require.NoError(t, err)
	return r
}

func TestRestIntegration(t *testing.T) {
	suite.Run(t, new(RestIntegrationSuite))
}
