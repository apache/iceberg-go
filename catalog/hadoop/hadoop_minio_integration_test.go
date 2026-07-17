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

package hadoop_test

import (
	"context"
	"fmt"
	"io/fs"
	"strings"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/hadoop"
	icebergio "github.com/apache/iceberg-go/io"
	_ "github.com/apache/iceberg-go/io/gocloud"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
)

type HadoopMinIOIntegrationSuite struct {
	suite.Suite

	ctx       context.Context
	cat       *hadoop.Catalog
	props     iceberg.Properties
	warehouse string
}

func (s *HadoopMinIOIntegrationSuite) SetupTest() {
	s.ctx = context.Background()
	s.warehouse = fmt.Sprintf("s3a://warehouse/hadoop-minio/%s", uuid.NewString())
	s.props = iceberg.Properties{
		"allow-unsafe-commits":      "true",
		icebergio.S3Region:          "local",
		icebergio.S3AccessKeyID:     "admin",
		icebergio.S3SecretAccessKey: "password",
		// Endpoint is provided by the env var AWS_S3_ENDPOINT
		// from `make integration-env` or recipe.Start.
	}

	cat, err := hadoop.NewCatalog("hadoop_minio_test", s.warehouse, s.props)
	s.Require().NoError(err)
	s.cat = cat
}

func (s *HadoopMinIOIntegrationSuite) testSchema() *iceberg.Schema {
	return iceberg.NewSchema(
		1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
}

func (s *HadoopMinIOIntegrationSuite) TearDownTest() {
	fs, err := icebergio.LoadFS(s.ctx, s.props, s.warehouse)
	if err != nil {
		return
	}

	if remover, ok := fs.(icebergio.RemoveAllIO); ok {
		_ = remover.RemoveAll(s.warehouse)
	}
}

func (s *HadoopMinIOIntegrationSuite) TestNamespaceTableRoundTrip() {
	ns := table.Identifier{"minio_ns"}
	ident := table.Identifier{"minio_ns", "tbl"}

	s.Require().NoError(s.cat.CreateNamespace(s.ctx, ns, nil))

	exists, err := s.cat.CheckNamespaceExists(s.ctx, ns)
	s.Require().NoError(err)
	s.True(exists)

	tbl, err := s.cat.CreateTable(s.ctx, ident, s.testSchema())
	s.Require().NoError(err)
	s.NotNil(tbl)
	s.True(strings.HasPrefix(tbl.MetadataLocation(), s.warehouse+"/minio_ns/tbl/metadata/v1.metadata.json"))

	loaded, err := s.cat.LoadTable(s.ctx, ident)
	s.Require().NoError(err)
	s.Equal(tbl.MetadataLocation(), loaded.MetadataLocation())
	s.Equal(2, len(loaded.Schema().Fields()))

	var tables []table.Identifier
	for found, err := range s.cat.ListTables(s.ctx, ns) {
		s.Require().NoError(err)
		tables = append(tables, found)
	}
	s.Equal([]table.Identifier{ident}, tables)

	s.Require().NoError(s.cat.DropTable(s.ctx, ident))

	tableExists, err := s.cat.CheckTableExists(s.ctx, ident)
	s.Require().NoError(err)
	s.False(tableExists)
}

func (s *HadoopMinIOIntegrationSuite) TestListTablesEmptyNamespace() {
	ns := table.Identifier{"empty_ns"}
	s.Require().NoError(s.cat.CreateNamespace(s.ctx, ns, nil))

	var tables []table.Identifier
	for found, err := range s.cat.ListTables(s.ctx, ns) {
		s.Require().NoError(err)
		tables = append(tables, found)
	}
	s.Empty(tables)
}

func (s *HadoopMinIOIntegrationSuite) TestListTablesMissingNamespace() {
	for _, err := range s.cat.ListTables(s.ctx, table.Identifier{"missing_ns"}) {
		s.ErrorIs(err, catalog.ErrNoSuchNamespace)

		break
	}
}

func (s *HadoopMinIOIntegrationSuite) TestListTablesOnlyDirectNamespaceTables() {
	parent := table.Identifier{"parent_ns"}
	child := table.Identifier{"parent_ns", "child_ns"}
	directTable := table.Identifier{"parent_ns", "direct_tbl"}
	childTable := table.Identifier{"parent_ns", "child_ns", "nested_tbl"}

	s.Require().NoError(s.cat.CreateNamespace(s.ctx, parent, nil))
	s.Require().NoError(s.cat.CreateNamespace(s.ctx, child, nil))
	_, err := s.cat.CreateTable(s.ctx, directTable, s.testSchema())
	s.Require().NoError(err)
	_, err = s.cat.CreateTable(s.ctx, childTable, s.testSchema())
	s.Require().NoError(err)

	var tables []table.Identifier
	for found, err := range s.cat.ListTables(s.ctx, parent) {
		s.Require().NoError(err)
		tables = append(tables, found)
	}
	s.Equal([]table.Identifier{directTable}, tables)
}

func (s *HadoopMinIOIntegrationSuite) TestDropTableRemovesTableFromListingAndLoad() {
	ns := table.Identifier{"drop_ns"}
	ident := table.Identifier{"drop_ns", "tbl"}

	s.Require().NoError(s.cat.CreateNamespace(s.ctx, ns, nil))
	_, err := s.cat.CreateTable(s.ctx, ident, s.testSchema())
	s.Require().NoError(err)

	s.Require().NoError(s.cat.DropTable(s.ctx, ident))

	_, err = s.cat.LoadTable(s.ctx, ident)
	s.ErrorIs(err, catalog.ErrNoSuchTable)

	var tables []table.Identifier
	for found, err := range s.cat.ListTables(s.ctx, ns) {
		s.Require().NoError(err)
		tables = append(tables, found)
	}
	s.Empty(tables)
}

func (s *HadoopMinIOIntegrationSuite) TestPurgeTableRemovesTableRoot() {
	ns := table.Identifier{"purge_ns"}
	ident := table.Identifier{"purge_ns", "tbl"}

	s.Require().NoError(s.cat.CreateNamespace(s.ctx, ns, nil))
	tbl, err := s.cat.CreateTable(s.ctx, ident, s.testSchema())
	s.Require().NoError(err)

	fileIO, err := icebergio.LoadFS(s.ctx, s.props, s.warehouse)
	s.Require().NoError(err)
	statFS, ok := fileIO.(icebergio.StatIO)
	s.Require().True(ok)
	writeFS, ok := fileIO.(icebergio.WriteFileIO)
	s.Require().True(ok)

	dataPath := tbl.Location() + "/data/file.parquet"
	s.Require().NoError(writeFS.WriteFile(dataPath, []byte("data")))

	s.Require().NoError(s.cat.PurgeTable(s.ctx, ident))

	_, err = statFS.Stat(tbl.MetadataLocation())
	s.ErrorIs(err, fs.ErrNotExist)
	_, err = statFS.Stat(dataPath)
	s.ErrorIs(err, fs.ErrNotExist)
	_, err = statFS.Stat(tbl.Location())
	s.ErrorIs(err, fs.ErrNotExist)

	exists, err := s.cat.CheckTableExists(s.ctx, ident)
	s.Require().NoError(err)
	s.False(exists)
}

func (s *HadoopMinIOIntegrationSuite) TestDropNamespaceAfterDropTable() {
	ns := table.Identifier{"drop_table_then_ns"}
	ident := table.Identifier{"drop_table_then_ns", "tbl"}

	s.Require().NoError(s.cat.CreateNamespace(s.ctx, ns, nil))
	_, err := s.cat.CreateTable(s.ctx, ident, s.testSchema())
	s.Require().NoError(err)

	s.Require().NoError(s.cat.DropTable(s.ctx, ident))

	// Ensure that the namespace can be dropped after dropping the table
	// and that directory marker logic is handled properly, not preventing
	// the drop from occurring.
	s.Require().NoError(s.cat.DropNamespace(s.ctx, ns))
}

func TestHadoopIntegrationMinIO(t *testing.T) {
	suite.Run(t, new(HadoopMinIOIntegrationSuite))
}
