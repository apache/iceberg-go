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
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog/hadoop"
	"github.com/apache/iceberg-go/internal/recipe"
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
