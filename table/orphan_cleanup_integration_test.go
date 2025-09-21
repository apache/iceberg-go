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

package table_test

import (
	"context"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/suite"
)

type OrphanCleanupIntegrationSuite struct {
	suite.Suite

	ctx      context.Context
	cat      *rest.Catalog
	location string
}

const (
	TestNamespace     = "orphan_test_ns"
	TestTableName     = "orphan_test_table"
	TestTableLocation = "s3://warehouse/iceberg/orphan_test"
	OrphanFilePrefix  = "orphan_"
)

var tableSchemaSimple = iceberg.NewSchemaWithIdentifiers(1,
	[]int{1},
	iceberg.NestedField{
		ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true,
	},
	iceberg.NestedField{
		ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: true,
	},
	iceberg.NestedField{
		ID: 3, Name: "value", Type: iceberg.PrimitiveTypes.Float64, Required: false,
	},
)

func (s *OrphanCleanupIntegrationSuite) loadCatalog() *rest.Catalog {
	cat, err := catalog.Load(s.ctx, "local", iceberg.Properties{
		"type":               "rest",
		"uri":                "http://localhost:8181",
		io.S3Region:          "us-east-1",
		io.S3AccessKeyID:     "admin",
		io.S3SecretAccessKey: "password",
	})
	s.Require().NoError(err)
	s.Require().IsType(&rest.Catalog{}, cat)
	return cat.(*rest.Catalog)
}

func (s *OrphanCleanupIntegrationSuite) SetupSuite() {
	s.ctx = context.Background()
	s.cat = s.loadCatalog()
	s.location = TestTableLocation

	// Ensure namespace exists
	s.ensureNamespace()
}

func (s *OrphanCleanupIntegrationSuite) TearDownSuite() {
	if exists, err := s.cat.CheckTableExists(s.ctx, catalog.ToIdentifier(TestNamespace, TestTableName)); err == nil && exists {
		s.NoError(s.cat.DropTable(s.ctx, catalog.ToIdentifier(TestNamespace, TestTableName)))
	}

	if exists, err := s.cat.CheckNamespaceExists(s.ctx, catalog.ToIdentifier(TestNamespace)); err == nil && exists {
		s.NoError(s.cat.DropNamespace(s.ctx, catalog.ToIdentifier(TestNamespace)))
	}
}

func (s *OrphanCleanupIntegrationSuite) SetupTest() {
	if exists, err := s.cat.CheckTableExists(s.ctx, catalog.ToIdentifier(TestNamespace, TestTableName)); err == nil && exists {
		s.Require().NoError(s.cat.DropTable(s.ctx, catalog.ToIdentifier(TestNamespace, TestTableName)))
	}
}

func (s *OrphanCleanupIntegrationSuite) ensureNamespace() {
	namespace := catalog.ToIdentifier(TestNamespace)
	exists, err := s.cat.CheckNamespaceExists(s.ctx, namespace)
	s.Require().NoError(err)

	if exists {
		for tbl, err := range s.cat.ListTables(s.ctx, namespace) {
			if err != nil {
				break
			}
			err := s.cat.DropTable(s.ctx, tbl)
			s.Require().NoError(err)
		}

		s.Require().NoError(s.cat.DropNamespace(s.ctx, namespace))
	}

	s.NoError(s.cat.CreateNamespace(s.ctx, namespace,
		iceberg.Properties{"description": "Namespace for orphan cleanup integration tests"}))
}

func (s *OrphanCleanupIntegrationSuite) createTableWithData() *table.Table {
	tbl, err := s.cat.CreateTable(s.ctx, catalog.ToIdentifier(TestNamespace, TestTableName), tableSchemaSimple,
		catalog.WithLocation(s.location))
	s.Require().NoError(err)
	s.Require().NotNil(tbl)

	// Create an arrow table with test data
	arrSchema, err := table.SchemaToArrowSchema(tableSchemaSimple, nil, false, false)
	s.Require().NoError(err)

	arrowTable, err := array.TableFromJSON(memory.DefaultAllocator, arrSchema,
		[]string{`[
		{"id": 1, "name": "alice", "value": 100.5},
		{"id": 2, "name": "bob", "value": 200.7},
		{"id": 3, "name": "charlie", "value": 300.9}
	]`})
	s.Require().NoError(err)
	defer arrowTable.Release()

	dataFile, err := url.JoinPath(s.location, "data", "test_data.parquet")
	s.Require().NoError(err)

	fs := s.mustFS(tbl)
	fw, err := fs.(io.WriteFileIO).Create(dataFile)
	s.Require().NoError(err)
	s.Require().NoError(pqarrow.WriteTable(arrowTable, fw, arrowTable.NumRows(),
		nil, pqarrow.DefaultWriterProps()))

	// Commit the data file to the table
	txn := tbl.NewTransaction()
	s.Require().NoError(txn.AddFiles(s.ctx, []string{dataFile}, nil, false))
	updatedTable, err := txn.Commit(s.ctx)
	s.Require().NoError(err)

	return updatedTable
}

func (s *OrphanCleanupIntegrationSuite) createOrphanFiles(tbl *table.Table, location string, fileNames []string) []string {
	fs := s.mustFS(tbl)
	var createdPaths []string

	for _, fileName := range fileNames {
		// Create orphan files in the data directory
		orphanPath, err := url.JoinPath(location, "data", OrphanFilePrefix+fileName)
		s.Require().NoError(err)

		// Write some fake contents to the orphan file
		fw, err := fs.(io.WriteFileIO).Create(orphanPath)
		s.Require().NoError(err)

		content := []byte("This is an orphan file: " + fileName)
		_, err = fw.Write(content)
		s.Require().NoError(err)
		s.Require().NoError(fw.Close())

		createdPaths = append(createdPaths, orphanPath)
	}

	return createdPaths
}

func (s *OrphanCleanupIntegrationSuite) mustFS(tbl *table.Table) io.IO {
	fs, err := tbl.FS(s.ctx)
	s.Require().NoError(err)
	return fs
}

func (s *OrphanCleanupIntegrationSuite) fileExists(fs io.IO, path string) bool {
	f, err := fs.Open(path)
	if err != nil {
		return false
	}
	err = f.Close()
	s.Require().NoError(err)
	return true
}

func (s *OrphanCleanupIntegrationSuite) TestOrphanCleanupDryRun() {
	// Create a table with real data
	tbl := s.createTableWithData()

	// Debug: Print locations
	s.T().Logf("Table location: %s", tbl.Location())
	s.T().Logf("Test location: %s", s.location)

	orphanFiles := s.createOrphanFiles(tbl, s.location, []string{"old1.parquet", "old2.parquet", "old3.parquet"})

	fs := s.mustFS(tbl)
	for _, orphanFile := range orphanFiles {
		s.True(s.fileExists(fs, orphanFile), "Orphan file should exist: %s", orphanFile)
	}

	// Run dry-run orphan cleanup (scan table root where both data and metadata exist)
	s.T().Logf("Scanning location: %s", tbl.Location())
	result, err := tbl.DeleteOrphanFiles(s.ctx,
		table.WithDryRun(true),
		table.WithLocation(tbl.Location()),     // Scan the table root location
		table.WithFilesOlderThan(-1*time.Hour), // Consider all files (including future timestamp)
	)

	s.Require().NoError(err)
	s.Require().NotNil(result)

	s.GreaterOrEqual(len(result.OrphanFileLocations), 3, "Should identify at least 3 orphan files")
	s.Empty(result.DeletedFiles, "Dry run should not delete any files")

	for _, orphanFile := range orphanFiles {
		s.True(s.fileExists(fs, orphanFile), "Orphan file should still exist after dry run: %s", orphanFile)
	}

	for _, orphanFile := range orphanFiles {
		found := false
		for _, identified := range result.OrphanFileLocations {
			if strings.Contains(identified, filepath.Base(orphanFile)) {
				found = true
				break
			}
		}
		s.True(found, "Created orphan file should be identified: %s", orphanFile)
	}

	for _, orphanFile := range orphanFiles {
		s.NoError(fs.Remove(orphanFile))
	}
}

func (s *OrphanCleanupIntegrationSuite) TestOrphanCleanupActualDeletion() {
	tbl := s.createTableWithData()

	// Create some orphan files that are old enough to be deleted
	orphanFiles := s.createOrphanFiles(tbl, s.location, []string{"delete1.parquet", "delete2.parquet"})

	// Wait a moment to ensure files are old enough
	time.Sleep(100 * time.Millisecond)

	// Verify orphan files exist
	fs := s.mustFS(tbl)
	for _, orphanFile := range orphanFiles {
		s.True(s.fileExists(fs, orphanFile), "Orphan file should exist: %s", orphanFile)
	}

	// Run actual orphan cleanup
	result, err := tbl.DeleteOrphanFiles(s.ctx,
		table.WithDryRun(false),
		table.WithFilesOlderThan(-1*time.Hour), // Consider all files
	)

	s.Require().NoError(err)
	s.Require().NotNil(result)

	s.GreaterOrEqual(len(result.OrphanFileLocations), 2, "Should identify at least 2 orphan files")
	s.GreaterOrEqual(len(result.DeletedFiles), 2, "Should delete at least 2 orphan files")

	for _, orphanFile := range orphanFiles {
		s.False(s.fileExists(fs, orphanFile), "Orphan file should be deleted: %s", orphanFile)
	}

	for manifest, err := range tbl.AllManifests(s.ctx) {
		s.Require().NoError(err)
		entries, err := manifest.FetchEntries(fs, false)
		s.Require().NoError(err)

		for _, entry := range entries {
			dataFile := entry.DataFile().FilePath()
			s.True(s.fileExists(fs, dataFile), "Data file should still exist: %s", dataFile)
		}
	}
}

func (s *OrphanCleanupIntegrationSuite) TestOrphanCleanupCustomLocation() {
	tbl := s.createTableWithData()

	customLocation, err := url.JoinPath(s.location, "custom_data")
	s.Require().NoError(err)
	orphanFiles := s.createOrphanFiles(tbl, customLocation, []string{"custom1.parquet", "custom2.parquet"})

	fs := s.mustFS(tbl)
	for _, orphanFile := range orphanFiles {
		s.True(s.fileExists(fs, orphanFile), "Orphan file should exist: %s", orphanFile)
	}

	// Wait to ensure files are old enough
	time.Sleep(100 * time.Millisecond)

	// Run orphan cleanup on table root (which includes the custom subdirectory)
	result, err := tbl.DeleteOrphanFiles(s.ctx,
		table.WithLocation(tbl.Location()), // Scan table root to find all subdirectories
		table.WithFilesOlderThan(-1*time.Hour),
		table.WithDryRun(false),
	)

	s.Require().NoError(err)
	s.Require().NotNil(result)

	s.GreaterOrEqual(len(result.DeletedFiles), 2, "Should delete at least 2 files from custom location")

	for _, orphanFile := range orphanFiles {
		s.False(s.fileExists(fs, orphanFile), "Custom location orphan file should be deleted: %s", orphanFile)
	}
}

func (s *OrphanCleanupIntegrationSuite) TestOrphanCleanupWithConcurrency() {
	// Create table with real data
	tbl := s.createTableWithData()

	// Create multiple orphan files for concurrent deletion testing
	orphanFiles := s.createOrphanFiles(tbl, s.location, []string{
		"concurrent1.parquet", "concurrent2.parquet", "concurrent3.parquet",
		"concurrent4.parquet", "concurrent5.parquet",
	})

	// Wait to ensure files are old enough
	time.Sleep(100 * time.Millisecond)

	// Test different concurrency levels
	testCases := []struct {
		name        string
		concurrency int
	}{
		{"sequential", 1},
		{"parallel_2", 2},
		{"parallel_4", 4},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			// Create fresh orphan files for this test case
			freshOrphanFiles := s.createOrphanFiles(tbl, s.location, []string{
				tc.name + "_1.parquet", tc.name + "_2.parquet", tc.name + "_3.parquet",
			})

			time.Sleep(100 * time.Millisecond)

			// Run orphan cleanup with specific concurrency
			result, err := tbl.DeleteOrphanFiles(s.ctx,
				table.WithMaxConcurrency(tc.concurrency),
				table.WithFilesOlderThan(-1*time.Hour),
				table.WithDryRun(false),
			)

			s.Require().NoError(err)
			s.Require().NotNil(result)

			// Verify files were deleted regardless of concurrency level
			s.GreaterOrEqual(len(result.DeletedFiles), 3, "Should delete at least 3 files with concurrency %d", tc.concurrency)

			fs := s.mustFS(tbl)
			for _, orphanFile := range freshOrphanFiles {
				s.False(s.fileExists(fs, orphanFile), "Orphan file should be deleted with concurrency %d: %s", tc.concurrency, orphanFile)
			}
		})
	}

	// Clean up original orphan files
	fs := s.mustFS(tbl)
	for _, orphanFile := range orphanFiles {
		fs.Remove(orphanFile) // Ignore errors, might already be deleted
	}
}

func (s *OrphanCleanupIntegrationSuite) TestOrphanCleanupCustomDeleteFunction() {
	tbl := s.createTableWithData()
	orphanFiles := s.createOrphanFiles(tbl, s.location, []string{"custom_delete.parquet"})

	// Wait to ensure files are old enough
	time.Sleep(100 * time.Millisecond)

	var deletedByCustomFunc []string
	customDeleteFunc := func(filePath string) error {
		deletedByCustomFunc = append(deletedByCustomFunc, filePath)
		fs := s.mustFS(tbl)
		return fs.Remove(filePath)
	}

	result, err := tbl.DeleteOrphanFiles(s.ctx,
		table.WithDeleteFunc(customDeleteFunc),
		table.WithFilesOlderThan(-1*time.Hour),
		table.WithDryRun(false),
		table.WithDryRun(false),
	)

	s.Require().NoError(err)
	s.Require().NotNil(result)

	s.GreaterOrEqual(len(deletedByCustomFunc), 1, "Custom delete function should be called")

	fs := s.mustFS(tbl)
	for _, orphanFile := range orphanFiles {
		s.False(s.fileExists(fs, orphanFile), "Orphan file should be deleted by custom function: %s", orphanFile)
	}

	for _, orphanFile := range orphanFiles {
		found := false
		for _, deletedFile := range deletedByCustomFunc {
			if strings.Contains(deletedFile, filepath.Base(orphanFile)) {
				found = true
				break
			}
		}
		s.True(found, "Orphan file should be processed by custom delete function: %s", orphanFile)
	}
}

func TestOrphanCleanupIntegration(t *testing.T) {
	suite.Run(t, new(OrphanCleanupIntegrationSuite))
}
