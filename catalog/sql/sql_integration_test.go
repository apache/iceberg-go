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

package sql_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/sql"
	"github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/suite"
)

type SQLIntegrationSuite struct {
	suite.Suite

	ctx context.Context
	cat *sql.Catalog
	dir string
}

const (
	TestNamespaceIdent = "TEST_NS"
	location           = "s3://warehouse/iceberg"
)

var tableSchemaSimple = iceberg.NewSchemaWithIdentifiers(1, []int{2},
	iceberg.NestedField{ID: 1, Name: "foo", Type: iceberg.StringType{}, Required: false},
	iceberg.NestedField{ID: 2, Name: "bar", Type: iceberg.PrimitiveTypes.Int32, Required: true},
	iceberg.NestedField{ID: 3, Name: "baz", Type: iceberg.PrimitiveTypes.Bool, Required: false},
)

func (s *SQLIntegrationSuite) loadCatalog(ctx context.Context) *sql.Catalog {
	// Create a temp dir for SQLite database
	var err error
	s.dir, err = os.MkdirTemp("", "iceberg-sql-test-*")
	s.Require().NoError(err)

	// Create catalog
	dbPath := filepath.Join(s.dir, "iceberg-catalog.db")
	cat, err := catalog.Load(ctx, "local", iceberg.Properties{
		"type":               "sql",
		"uri":                "file:" + dbPath,
		"sql.dialect":        "sqlite",
		"sql.driver":         "sqlite",
		io.S3Region:          "us-east-1",
		io.S3AccessKeyID:     "admin",
		io.S3SecretAccessKey: "password",
		"warehouse":          location,
	})
	s.Require().NoError(err)
	s.Require().IsType(&sql.Catalog{}, cat)

	return cat.(*sql.Catalog)
}

func (s *SQLIntegrationSuite) SetupTest() {
	s.ctx = context.Background()
	s.cat = s.loadCatalog(s.ctx)
}

func (s *SQLIntegrationSuite) TearDownTest() {
	if s.dir != "" {
		os.RemoveAll(s.dir)
	}
}

var tableSchemaNestedTest = iceberg.NewSchemaWithIdentifiers(1,
	[]int{1},
	iceberg.NestedField{
		ID: 1, Name: "foo", Type: iceberg.PrimitiveTypes.String, Required: true,
	},
	iceberg.NestedField{
		ID: 2, Name: "bar", Type: iceberg.PrimitiveTypes.Int32, Required: true,
	},
	iceberg.NestedField{
		ID: 3, Name: "baz", Type: iceberg.PrimitiveTypes.Bool, Required: false,
	},
	iceberg.NestedField{
		ID: 4, Name: "qux", Required: true, Type: &iceberg.ListType{
			ElementID: 5, Element: iceberg.PrimitiveTypes.String, ElementRequired: true,
		},
	},
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
		Required: true,
	},
	iceberg.NestedField{
		ID: 11, Name: "location", Type: &iceberg.ListType{
			ElementID: 12, Element: &iceberg.StructType{
				FieldList: []iceberg.NestedField{
					{ID: 13, Name: "latitude", Type: iceberg.PrimitiveTypes.Float32, Required: false},
					{ID: 14, Name: "longitude", Type: iceberg.PrimitiveTypes.Float32, Required: false},
				},
			},
			ElementRequired: true,
		},
		Required: true,
	},
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

func (s *SQLIntegrationSuite) ensureNamespace() {
	exists, err := s.cat.CheckNamespaceExists(s.ctx, catalog.ToIdentifier(TestNamespaceIdent))
	s.Require().NoError(err)
	if exists {
		s.Require().NoError(s.cat.DropNamespace(s.ctx, catalog.ToIdentifier(TestNamespaceIdent)))
	}

	s.NoError(s.cat.CreateNamespace(s.ctx, catalog.ToIdentifier(TestNamespaceIdent),
		iceberg.Properties{"foo": "bar", "prop": "yes"}))
}

func (s *SQLIntegrationSuite) TestCreateNamespace() {
	exists, err := s.cat.CheckNamespaceExists(s.ctx, catalog.ToIdentifier(TestNamespaceIdent))
	s.Require().NoError(err)
	if exists {
		s.Require().NoError(s.cat.DropNamespace(s.ctx, catalog.ToIdentifier(TestNamespaceIdent)))
	}

	s.NoError(s.cat.CreateNamespace(s.ctx, catalog.ToIdentifier(TestNamespaceIdent), nil))
	s.NoError(s.cat.DropNamespace(s.ctx, catalog.ToIdentifier(TestNamespaceIdent)))
}

func (s *SQLIntegrationSuite) TestLoadNamespaceProps() {
	s.ensureNamespace()

	props, err := s.cat.LoadNamespaceProperties(s.ctx, catalog.ToIdentifier(TestNamespaceIdent))
	s.Require().NoError(err)
	s.Require().NotNil(props)
	s.Equal("bar", props["foo"])
	s.Equal("yes", props["prop"])
}

func (s *SQLIntegrationSuite) TestUpdateNamespaceProps() {
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

func (s *SQLIntegrationSuite) TestCreateTable() {
	s.ensureNamespace()

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

func (s *SQLIntegrationSuite) TestCreateView() {
	s.ensureNamespace()

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
	viewSQL := fmt.Sprintf("SELECT * FROM %s.%s", TestNamespaceIdent, "test-table")

	err = s.cat.CreateView(s.ctx, catalog.ToIdentifier(TestNamespaceIdent, "test-view"), tableSchemaSimple, viewSQL, iceberg.Properties{"foobar": "baz"})
	s.Require().NoError(err)

	exists, err = s.cat.CheckViewExists(s.ctx, catalog.ToIdentifier(TestNamespaceIdent, "test-view"))
	s.Require().NoError(err)
	s.True(exists)

	s.Require().NoError(s.cat.DropTable(s.ctx, catalog.ToIdentifier(TestNamespaceIdent, "test-table")))
	s.Require().NoError(s.cat.DropView(s.ctx, catalog.ToIdentifier(TestNamespaceIdent, "test-view")))
}

func (s *SQLIntegrationSuite) TestWriteCommitTable() {
	s.ensureNamespace()

	tbl, err := s.cat.CreateTable(s.ctx,
		catalog.ToIdentifier(TestNamespaceIdent, "test-table-2"),
		tableSchemaNestedTest, catalog.WithLocation(location))
	s.Require().NoError(err)
	s.Require().NotNil(tbl)

	defer func() {
		s.Require().NoError(s.cat.DropTable(s.ctx, catalog.ToIdentifier(TestNamespaceIdent, "test-table-2")))
	}()

	s.Equal(location, tbl.Location())

	arrSchema, err := table.SchemaToArrowSchema(tableSchemaNestedTest, nil, false, false)
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

	fs, err := tbl.FS(s.ctx)
	s.Require().NoError(err)
	fw, err := fs.(io.WriteFileIO).Create(pqfile)
	s.Require().NoError(err)
	s.Require().NoError(pqarrow.WriteTable(table, fw, table.NumRows(),
		nil, pqarrow.DefaultWriterProps()))
	defer func(fs io.IO, name string) {
		err = fs.Remove(name)
		s.Require().NoError(err)
	}(fs, pqfile)

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
	updatedFS, err := updated.FS(s.ctx)
	s.Require().NoError(err)

	entries, err := mf[0].FetchEntries(updatedFS, false)
	s.Require().NoError(err)

	s.Len(entries, 1)
	s.Equal(pqfile, entries[0].DataFile().FilePath())
}

func TestSQLIntegration(t *testing.T) {
	suite.Run(t, new(SQLIntegrationSuite))
}
