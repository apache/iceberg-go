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
	"runtime"
	"strings"
	"testing"

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

func repoRoot() string {
	_, file, _, _ := runtime.Caller(0)

	return filepath.Join(filepath.Dir(file), "..", "..")
}

func (s *HadoopIntegrationSuite) SetupSuite() {
	// Create the warehouse directory before Docker starts so it is owned
	// by the runner user. If Docker creates it first via the volume mount,
	// it will be root-owned and Go won't be able to write into it.
	s.warehouse = filepath.Join(repoRoot(), "internal", "recipe", "hadoop-warehouse")
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

func TestHadoopIntegration(t *testing.T) {
	suite.Run(t, new(HadoopIntegrationSuite))
}
