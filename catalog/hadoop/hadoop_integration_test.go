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
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/hadoop"
	"github.com/apache/iceberg-go/internal/recipe"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go/modules/compose"
)

type HadoopIntegrationSuite struct {
	suite.Suite

	ctx       context.Context
	cat       *hadoop.Catalog
	warehouse string
	stack     *compose.DockerCompose
}

func (s *HadoopIntegrationSuite) SetupSuite() {
	// Use the same warehouse path that Spark is configured with inside the
	// Docker container (see hadoop_validation.py and docker-compose.yml).
	// The docker-compose volume mount maps host /tmp/iceberg-hadoop-warehouse
	// to container /tmp/iceberg-hadoop-warehouse, so both Go (on host) and
	// Spark (in container) share the same directory.
	s.warehouse = "/tmp/iceberg-hadoop-warehouse"
	s.Require().NoError(os.MkdirAll(s.warehouse, 0o755))

	var err error
	s.stack, err = recipe.Start(s.T())
	s.Require().NoError(err)
}

func (s *HadoopIntegrationSuite) SetupTest() {
	s.ctx = context.Background()

	cat, err := hadoop.NewCatalog("hadoop_test", s.warehouse, nil)
	s.Require().NoError(err)
	s.cat = cat
}

func (s *HadoopIntegrationSuite) TearDownTest() {
	entries, _ := os.ReadDir(s.warehouse)
	for _, e := range entries {
		os.RemoveAll(filepath.Join(s.warehouse, e.Name()))
	}
}

func (s *HadoopIntegrationSuite) sparkSQL(sql string) string {
	s.T().Helper()

	output, err := recipe.ExecuteSpark(s.T(), "./hadoop_validation.py", "--sql", sql)
	s.Require().NoError(err)

	return output
}

// createFakeTable creates a minimal table directory structure owned by
// the runner process. This avoids root-ownership issues that arise when
// Spark creates tables via Docker (root in container).
func (s *HadoopIntegrationSuite) createFakeTable(ident table.Identifier) {
	s.T().Helper()

	tablePath := filepath.Join(append([]string{s.warehouse}, ident...)...)
	metaDir := filepath.Join(tablePath, "metadata")
	s.Require().NoError(os.MkdirAll(metaDir, 0o755))
	s.Require().NoError(os.WriteFile(
		filepath.Join(metaDir, "v1.metadata.json"), []byte("{}"), 0o644))
}

// TestListTablesSparkCreated creates tables in Spark and verifies that
// the Go Hadoop catalog can list them.
func (s *HadoopIntegrationSuite) TestListTablesSparkCreated() {
	s.sparkSQL("CREATE NAMESPACE IF NOT EXISTS hadoop_test.list_ns")
	s.sparkSQL("CREATE TABLE hadoop_test.list_ns.tbl_a (id INT, name STRING) USING iceberg")
	s.sparkSQL("CREATE TABLE hadoop_test.list_ns.tbl_b (id INT, name STRING) USING iceberg")

	var tables []table.Identifier
	for ident, err := range s.cat.ListTables(s.ctx, []string{"list_ns"}) {
		s.Require().NoError(err)
		tables = append(tables, ident)
	}

	s.Len(tables, 2)

	names := make([]string, len(tables))
	for i, t := range tables {
		names[i] = t[len(t)-1]
	}

	s.Contains(names, "tbl_a")
	s.Contains(names, "tbl_b")
}

// TestListTablesEmptyNamespace creates an empty namespace via Spark and
// verifies ListTables returns zero results.
func (s *HadoopIntegrationSuite) TestListTablesEmptyNamespace() {
	s.sparkSQL("CREATE NAMESPACE IF NOT EXISTS hadoop_test.empty_ns")

	var tables []table.Identifier
	for ident, err := range s.cat.ListTables(s.ctx, []string{"empty_ns"}) {
		s.Require().NoError(err)
		tables = append(tables, ident)
	}

	s.Empty(tables)
}

// TestListTablesNoSuchNamespace verifies that listing tables for a
// non-existent namespace returns ErrNoSuchNamespace.
func (s *HadoopIntegrationSuite) TestListTablesNoSuchNamespace() {
	for _, err := range s.cat.ListTables(s.ctx, []string{"does_not_exist"}) {
		s.ErrorIs(err, catalog.ErrNoSuchNamespace)

		break
	}
}

// TestDropTableExisting creates a table from Go (to avoid root-ownership
// issues with Spark-created dirs), drops it, and verifies removal.
func (s *HadoopIntegrationSuite) TestDropTableExisting() {
	err := s.cat.CreateNamespace(s.ctx, []string{"drop_ns"}, nil)
	s.Require().NoError(err)

	s.createFakeTable([]string{"drop_ns", "to_drop"})

	tablePath := filepath.Join(s.warehouse, "drop_ns", "to_drop")
	_, err = os.Stat(tablePath)
	s.Require().NoError(err, "table directory should exist before drop")

	err = s.cat.DropTable(s.ctx, []string{"drop_ns", "to_drop"})
	s.Require().NoError(err)

	_, err = os.Stat(tablePath)
	s.True(os.IsNotExist(err), "table directory should be removed after drop")
}

// TestDropTableNotExists verifies that dropping a non-existent table
// returns ErrNoSuchTable.
func (s *HadoopIntegrationSuite) TestDropTableNotExists() {
	err := s.cat.DropTable(s.ctx, []string{"no_ns", "no_tbl"})
	s.ErrorIs(err, catalog.ErrNoSuchTable)
}

// TestDropTableNamespacePreserved verifies that dropping a table does
// not remove the parent namespace directory.
func (s *HadoopIntegrationSuite) TestDropTableNamespacePreserved() {
	err := s.cat.CreateNamespace(s.ctx, []string{"preserve_ns"}, nil)
	s.Require().NoError(err)

	s.createFakeTable([]string{"preserve_ns", "tbl"})

	err = s.cat.DropTable(s.ctx, []string{"preserve_ns", "tbl"})
	s.Require().NoError(err)

	info, err := os.Stat(filepath.Join(s.warehouse, "preserve_ns"))
	s.Require().NoError(err)
	s.True(info.IsDir(), "namespace directory should still exist")
}

// TestRenameTableUnsupported verifies that RenameTable returns an error.
func (s *HadoopIntegrationSuite) TestRenameTableUnsupported() {
	_, err := s.cat.RenameTable(s.ctx, []string{"ns", "old"}, []string{"ns", "new"})
	s.Require().Error(err)
	s.True(strings.Contains(err.Error(), "not supported"))
}

// TestNamespaceRoundTrip creates a namespace in Go, creates a table in
// Spark under it, then lists from Go.
func (s *HadoopIntegrationSuite) TestNamespaceRoundTrip() {
	err := s.cat.CreateNamespace(s.ctx, []string{"round_trip"}, nil)
	s.Require().NoError(err)

	s.sparkSQL("CREATE TABLE hadoop_test.round_trip.rt_tbl (id INT, val STRING) USING iceberg")

	var tables []table.Identifier
	for ident, err := range s.cat.ListTables(s.ctx, []string{"round_trip"}) {
		s.Require().NoError(err)
		tables = append(tables, ident)
	}

	s.Len(tables, 1)
	s.Equal("rt_tbl", tables[0][len(tables[0])-1])
}

// TestDropTableThenListReflects verifies that after dropping, the table
// no longer appears in ListTables.
func (s *HadoopIntegrationSuite) TestDropTableThenListReflects() {
	err := s.cat.CreateNamespace(s.ctx, []string{"droplist_ns"}, nil)
	s.Require().NoError(err)

	s.createFakeTable([]string{"droplist_ns", "keep"})
	s.createFakeTable([]string{"droplist_ns", "remove"})

	err = s.cat.DropTable(s.ctx, []string{"droplist_ns", "remove"})
	s.Require().NoError(err)

	var tables []table.Identifier
	for ident, err := range s.cat.ListTables(s.ctx, []string{"droplist_ns"}) {
		s.Require().NoError(err)
		tables = append(tables, ident)
	}

	s.Len(tables, 1)
	s.Equal("keep", tables[0][len(tables[0])-1])
}

// TestGoCreateSparkDescribe creates a table from Go, then asks Spark to
// DESCRIBE it and verifies that Spark sees the expected columns.
func (s *HadoopIntegrationSuite) TestGoCreateSparkDescribe() {
	// Create namespace from Go so the directory is owned by the runner
	// process and Go can create table subdirectories inside it.
	err := s.cat.CreateNamespace(s.ctx, []string{"describe_ns"}, nil)
	s.Require().NoError(err)

	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false},
	)

	tbl, err := s.cat.CreateTable(s.ctx, []string{"describe_ns", "go_created_table"}, schema)
	s.Require().NoError(err)
	s.NotNil(tbl)

	output := s.sparkSQL("DESCRIBE TABLE hadoop_test.describe_ns.go_created_table")
	s.Contains(output, "id")
	s.Contains(output, "name")
}

// TestSparkCreateGoLoad creates a table from Spark, then loads it from
// Go and verifies schema and metadata are consistent.
func (s *HadoopIntegrationSuite) TestSparkCreateGoLoad() {
	// Spark creates both namespace and table — Go only reads here.
	s.sparkSQL("CREATE NAMESPACE IF NOT EXISTS hadoop_test.load_ns")
	s.sparkSQL("CREATE TABLE hadoop_test.load_ns.spark_created_table (age INT, city STRING) USING iceberg")

	tbl, err := s.cat.LoadTable(s.ctx, []string{"load_ns", "spark_created_table"})
	s.Require().NoError(err)
	s.NotNil(tbl)

	fields := tbl.Schema().Fields()
	fieldNames := make([]string, len(fields))
	for i, f := range fields {
		fieldNames[i] = f.Name
	}

	s.Contains(fieldNames, "age")
	s.Contains(fieldNames, "city")
}

// TestGoCreateSparkSelect creates a table from Go, then runs a SELECT
// from Spark to confirm the table is queryable (should return empty).
func (s *HadoopIntegrationSuite) TestGoCreateSparkSelect() {
	err := s.cat.CreateNamespace(s.ctx, []string{"select_ns"}, nil)
	s.Require().NoError(err)

	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "value", Type: iceberg.PrimitiveTypes.Int32, Required: true},
	)

	_, err = s.cat.CreateTable(s.ctx, []string{"select_ns", "go_select_table"}, schema)
	s.Require().NoError(err)

	output := s.sparkSQL("SELECT * FROM hadoop_test.select_ns.go_select_table")
	s.Contains(output, "value")
}

// TestGoCheckTableExistsForSparkTable verifies that CheckTableExists
// returns true for a table created by Spark.
func (s *HadoopIntegrationSuite) TestGoCheckTableExistsForSparkTable() {
	// Spark creates both namespace and table — Go only checks existence.
	s.sparkSQL("CREATE NAMESPACE IF NOT EXISTS hadoop_test.exists_ns")
	s.sparkSQL("CREATE TABLE hadoop_test.exists_ns.spark_exists_table (x INT) USING iceberg")

	exists, err := s.cat.CheckTableExists(s.ctx, []string{"exists_ns", "spark_exists_table"})
	s.Require().NoError(err)
	s.True(exists)

	exists, err = s.cat.CheckTableExists(s.ctx, []string{"exists_ns", "no_such_table"})
	s.Require().NoError(err)
	s.False(exists)
}

func TestHadoopIntegration(t *testing.T) {
	suite.Run(t, new(HadoopIntegrationSuite))
}
