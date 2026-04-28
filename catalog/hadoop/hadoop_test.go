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

package hadoop

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/suite"
)

type HadoopCatalogTestSuite struct {
	suite.Suite
	warehouse string
	cat       *Catalog
}

func (s *HadoopCatalogTestSuite) SetupTest() {
	var err error
	s.warehouse, err = os.MkdirTemp("", "test_hadoop_*")
	s.Require().NoError(err)

	s.cat, err = NewCatalog("test", s.warehouse, nil)
	s.Require().NoError(err)
}

func (s *HadoopCatalogTestSuite) TearDownTest() {
	s.Require().NoError(os.RemoveAll(s.warehouse))
}

func TestHadoopCatalogTestSuite(t *testing.T) {
	suite.Run(t, new(HadoopCatalogTestSuite))
}

func (s *HadoopCatalogTestSuite) TestCatalogType() {
	s.Equal(catalog.Hadoop, s.cat.CatalogType())
}

func (s *HadoopCatalogTestSuite) TestRegistration() {
	cat, err := catalog.Load(context.Background(), "test", iceberg.Properties{
		"type":      "hadoop",
		"warehouse": s.warehouse,
	})
	s.Require().NoError(err)
	s.Equal(catalog.Hadoop, cat.CatalogType())
}

func (s *HadoopCatalogTestSuite) TestRegistrationMissingWarehouse() {
	_, err := catalog.Load(context.Background(), "test", iceberg.Properties{
		"type": "hadoop",
	})
	s.Require().Error(err)
	s.Contains(err.Error(), "warehouse")
}

func (s *HadoopCatalogTestSuite) TestNewCatalogTrimsTrailingSlash() {
	cat, err := NewCatalog("test", "/tmp/wh/", nil)
	s.Require().NoError(err)
	s.Equal("/tmp/wh", cat.warehouse)
}

func (s *HadoopCatalogTestSuite) TestNewCatalogStripsFilePrefix() {
	cat, err := NewCatalog("test", "file:///tmp/wh", nil)
	s.Require().NoError(err)
	s.Equal("/tmp/wh", cat.warehouse)

	cat, err = NewCatalog("test", "file:/tmp/wh", nil)
	s.Require().NoError(err)
	s.Equal("/tmp/wh", cat.warehouse)
}

func (s *HadoopCatalogTestSuite) TestNewCatalogRejectsNonFileScheme() {
	_, err := NewCatalog("test", "s3://bucket/path", nil)
	s.Require().Error(err)
	s.Contains(err.Error(), "unsupported warehouse scheme")

	_, err = NewCatalog("test", "hdfs://namenode/warehouse", nil)
	s.Require().Error(err)
	s.Contains(err.Error(), "unsupported warehouse scheme")
}

func (s *HadoopCatalogTestSuite) TestNamespaceToPathSingleLevel() {
	path := s.cat.namespaceToPath([]string{"db"})
	s.Equal(filepath.Join(s.warehouse, "db"), path)
}

func (s *HadoopCatalogTestSuite) TestNamespaceToPathNested() {
	path := s.cat.namespaceToPath([]string{"a", "b", "c"})
	s.Equal(filepath.Join(s.warehouse, "a", "b", "c"), path)
}

func (s *HadoopCatalogTestSuite) TestTableToPathSingleNamespace() {
	path := s.cat.tableToPath([]string{"ns", "tbl"})
	s.Equal(filepath.Join(s.warehouse, "ns", "tbl"), path)
}

func (s *HadoopCatalogTestSuite) TestTableToPathNestedNamespace() {
	path := s.cat.tableToPath([]string{"a", "b", "tbl"})
	s.Equal(filepath.Join(s.warehouse, "a", "b", "tbl"), path)
}

func (s *HadoopCatalogTestSuite) TestMetadataDir() {
	path := s.cat.metadataDir([]string{"ns", "tbl"})
	s.Equal(filepath.Join(s.warehouse, "ns", "tbl", "metadata"), path)
}

func (s *HadoopCatalogTestSuite) TestMetadataFilePath() {
	path := s.cat.metadataFilePath([]string{"ns", "tbl"}, 1)
	s.Equal(filepath.Join(s.warehouse, "ns", "tbl", "metadata", "v1.metadata.json"), path)

	path = s.cat.metadataFilePath([]string{"ns", "tbl"}, 42)
	s.Equal(filepath.Join(s.warehouse, "ns", "tbl", "metadata", "v42.metadata.json"), path)
}

func (s *HadoopCatalogTestSuite) TestVersionHintPath() {
	path := s.cat.versionHintPath([]string{"ns", "tbl"})
	s.Equal(filepath.Join(s.warehouse, "ns", "tbl", "metadata", "version-hint.text"), path)
}

func (s *HadoopCatalogTestSuite) TestDefaultTableLocation() {
	path := s.cat.defaultTableLocation([]string{"ns", "tbl"})
	s.Equal(filepath.Join(s.warehouse, "ns", "tbl"), path)
}

func (s *HadoopCatalogTestSuite) TestDefaultTableLocationNested() {
	path := s.cat.defaultTableLocation([]string{"a", "b", "tbl"})
	s.Equal(filepath.Join(s.warehouse, "a", "b", "tbl"), path)
}

func (s *HadoopCatalogTestSuite) TestVersionPatternMatches() {
	names := []string{
		"v1.metadata.json",
		"v42.metadata.json",
		"v100.metadata.json",
		"v1.gz.metadata.json",
		"v42.gz.metadata.json",
	}

	for _, name := range names {
		s.True(versionPattern.MatchString(name), "expected %s to match", name)
	}
}

func (s *HadoopCatalogTestSuite) TestVersionPatternRejects() {
	tests := []string{
		"00001-a1b2c3d4.metadata.json",
		"random.json",
		"v.metadata.json",
		"metadata.json",
		"v1.metadata.json.bak",
		"v-1.metadata.json",
	}

	for _, name := range tests {
		s.False(versionPattern.MatchString(name), "expected %s to not match", name)
	}
}

func (s *HadoopCatalogTestSuite) TestIsTableDirTrue() {
	tableDir := filepath.Join(s.warehouse, "ns", "tbl")
	metaDir := filepath.Join(tableDir, "metadata")
	s.Require().NoError(os.MkdirAll(metaDir, 0o755))
	s.Require().NoError(os.WriteFile(filepath.Join(metaDir, "v1.metadata.json"), nil, 0o644))

	s.True(isTableDir(tableDir))
}

func (s *HadoopCatalogTestSuite) TestIsTableDirFalseNoMetadataDir() {
	nsDir := filepath.Join(s.warehouse, "ns")
	s.Require().NoError(os.MkdirAll(nsDir, 0o755))

	s.False(isTableDir(nsDir))
}

func (s *HadoopCatalogTestSuite) TestIsTableDirFalseEmptyMetadataDir() {
	tableDir := filepath.Join(s.warehouse, "ns", "tbl")
	metaDir := filepath.Join(tableDir, "metadata")
	s.Require().NoError(os.MkdirAll(metaDir, 0o755))

	s.False(isTableDir(tableDir))
}

func (s *HadoopCatalogTestSuite) TestIsTableDirFalseNonMatchingFiles() {
	tableDir := filepath.Join(s.warehouse, "ns", "tbl")
	metaDir := filepath.Join(tableDir, "metadata")
	s.Require().NoError(os.MkdirAll(metaDir, 0o755))
	s.Require().NoError(os.WriteFile(filepath.Join(metaDir, "00001-uuid.metadata.json"), nil, 0o644))

	s.False(isTableDir(tableDir))
}

func (s *HadoopCatalogTestSuite) TestIsTableDirWithGzipMetadata() {
	tableDir := filepath.Join(s.warehouse, "ns", "tbl")
	metaDir := filepath.Join(tableDir, "metadata")
	s.Require().NoError(os.MkdirAll(metaDir, 0o755))
	s.Require().NoError(os.WriteFile(filepath.Join(metaDir, "v1.gz.metadata.json"), nil, 0o644))

	s.True(isTableDir(tableDir))
}

func (s *HadoopCatalogTestSuite) TestIsTableDirNonExistentPath() {
	s.False(isTableDir(filepath.Join(s.warehouse, "does", "not", "exist")))
}

func (s *HadoopCatalogTestSuite) createMetadataFile(ident table.Identifier, version int) {
	path := s.cat.metadataFilePath(ident, version)
	s.Require().NoError(os.MkdirAll(filepath.Dir(path), 0o755))
	s.Require().NoError(os.WriteFile(path, nil, 0o644))
}

func (s *HadoopCatalogTestSuite) TestReadWriteVersionHint() {
	ident := []string{"ns", "tbl"}
	s.Require().NoError(os.MkdirAll(s.cat.metadataDir(ident), 0o755))
	s.cat.writeVersionHint(ident, 42)
	s.Equal(42, s.cat.readVersionHint(ident))
}

func (s *HadoopCatalogTestSuite) TestReadVersionHintMissing() {
	s.Equal(0, s.cat.readVersionHint([]string{"ns", "tbl"}))
}

func (s *HadoopCatalogTestSuite) TestReadVersionHintCorrupt() {
	ident := []string{"ns", "tbl"}
	dir := s.cat.metadataDir(ident)
	s.Require().NoError(os.MkdirAll(dir, 0o755))
	s.Require().NoError(os.WriteFile(s.cat.versionHintPath(ident), []byte("garbage"), 0o644))
	s.Equal(0, s.cat.readVersionHint(ident))
}

func (s *HadoopCatalogTestSuite) TestWriteVersionHintCreatesFile() {
	ident := []string{"ns", "tbl"}
	s.Require().NoError(os.MkdirAll(s.cat.metadataDir(ident), 0o755))
	s.cat.writeVersionHint(ident, 7)
	data, err := os.ReadFile(s.cat.versionHintPath(ident))
	s.Require().NoError(err)
	s.Equal("7", string(data))
}

func (s *HadoopCatalogTestSuite) TestFindVersionFromHint() {
	ident := []string{"ns", "tbl"}
	s.createMetadataFile(ident, 1)
	s.cat.writeVersionHint(ident, 1)
	ver, err := s.cat.findVersion(ident)
	s.Require().NoError(err)
	s.Equal(1, ver)
}

func (s *HadoopCatalogTestSuite) TestFindVersionNoHint() {
	ident := []string{"ns", "tbl"}
	for i := 1; i <= 3; i++ {
		s.createMetadataFile(ident, i)
	}

	ver, err := s.cat.findVersion(ident)
	s.Require().NoError(err)
	s.Equal(3, ver)
}

func (s *HadoopCatalogTestSuite) TestFindVersionNoMetadata() {
	ident := []string{"ns", "tbl"}
	s.Require().NoError(os.MkdirAll(s.cat.metadataDir(ident), 0o755))
	_, err := s.cat.findVersion(ident)
	s.Require().Error(err)
	s.ErrorIs(err, catalog.ErrNoSuchTable)
}

func (s *HadoopCatalogTestSuite) TestFindVersionScanForward() {
	ident := []string{"ns", "tbl"}
	for i := 1; i <= 5; i++ {
		s.createMetadataFile(ident, i)
	}

	s.cat.writeVersionHint(ident, 2)
	ver, err := s.cat.findVersion(ident)
	s.Require().NoError(err)
	s.Equal(5, ver)
}

func (s *HadoopCatalogTestSuite) TestFindVersionNoMetadataDir() {
	_, err := s.cat.findVersion([]string{"ns", "tbl"})
	s.Require().Error(err)
	s.ErrorIs(err, catalog.ErrNoSuchTable)
}

func (s *HadoopCatalogTestSuite) TestFindVersionStaleHintPointsToDeletedVersion() {
	// hint=5, but only v1-v3 exist. Hint file points to a version whose
	// metadata was deleted. Should fall through to dir listing and find v3.
	ident := []string{"ns", "tbl"}
	for i := 1; i <= 3; i++ {
		s.createMetadataFile(ident, i)
	}

	s.cat.writeVersionHint(ident, 5)
	ver, err := s.cat.findVersion(ident)
	s.Require().NoError(err)
	s.Equal(3, ver)
}

func (s *HadoopCatalogTestSuite) TestReadVersionHintZeroValue() {
	ident := []string{"ns", "tbl"}
	s.Require().NoError(os.MkdirAll(s.cat.metadataDir(ident), 0o755))
	s.Require().NoError(os.WriteFile(s.cat.versionHintPath(ident), []byte("0"), 0o644))
	s.Equal(0, s.cat.readVersionHint(ident))
}

func (s *HadoopCatalogTestSuite) TestReadVersionHintWithWhitespace() {
	ident := []string{"ns", "tbl"}
	s.Require().NoError(os.MkdirAll(s.cat.metadataDir(ident), 0o755))
	s.Require().NoError(os.WriteFile(s.cat.versionHintPath(ident), []byte("  42\n"), 0o644))
	s.Equal(42, s.cat.readVersionHint(ident))
}

func (s *HadoopCatalogTestSuite) TestWriteVersionHintNoMetadataDir() {
	// metadata dir doesn't exist — writeVersionHint should not panic,
	// just log a warning silently.
	ident := []string{"nonexistent", "tbl"}
	s.NotPanics(func() {
		s.cat.writeVersionHint(ident, 1)
	})
}

func (s *HadoopCatalogTestSuite) TestFindVersionIgnoresTempFiles() {
	// metadata dir has both valid metadata files and orphaned temp files
	// from crashed commits. findVersion should ignore temp files.
	ident := []string{"ns", "tbl"}
	dir := s.cat.metadataDir(ident)
	s.Require().NoError(os.MkdirAll(dir, 0o755))
	s.createMetadataFile(ident, 1)
	s.createMetadataFile(ident, 2)
	s.Require().NoError(os.WriteFile(filepath.Join(dir, "abc123-def456.metadata.json"), nil, 0o644))
	s.Require().NoError(os.WriteFile(filepath.Join(dir, "abc123-version-hint.temp"), nil, 0o644))

	ver, err := s.cat.findVersion(ident)
	s.Require().NoError(err)
	s.Equal(2, ver)
}

func (s *HadoopCatalogTestSuite) TestFindVersionGzipOnlyWithHint() {
	// Table has only gzip metadata. Hint exists but metadataFilePath
	// generates non-gzip paths, so hint validation fails. Should fall
	// through to dir listing which handles gzip filenames correctly.
	ident := []string{"ns", "tbl"}
	dir := s.cat.metadataDir(ident)
	s.Require().NoError(os.MkdirAll(dir, 0o755))
	s.Require().NoError(os.WriteFile(filepath.Join(dir, "v1.gz.metadata.json"), nil, 0o644))
	s.Require().NoError(os.WriteFile(filepath.Join(dir, "v2.gz.metadata.json"), nil, 0o644))
	s.cat.writeVersionHint(ident, 2)

	ver, err := s.cat.findVersion(ident)
	s.Require().NoError(err)
	// Dir listing finds max=2, scanForward checks v3.metadata.json (doesn't exist) → returns 2
	s.Equal(2, ver)
}
