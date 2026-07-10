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
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	icebergio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type barrierRenameNoReplaceFS struct {
	icebergio.LocalFS

	targetNames map[string]struct{}
	release     chan struct{}

	mu    sync.Mutex
	calls int
}

func newBarrierRenameNoReplaceFS(targetNames ...string) *barrierRenameNoReplaceFS {
	targets := make(map[string]struct{}, len(targetNames))
	for _, target := range targetNames {
		targets[target] = struct{}{}
	}

	return &barrierRenameNoReplaceFS{
		targetNames: targets,
		release:     make(chan struct{}),
	}
}

func (f *barrierRenameNoReplaceFS) RenameNoReplace(oldpath, newpath string) error {
	if _, ok := f.targetNames[filepath.Base(newpath)]; !ok {
		return f.LocalFS.RenameNoReplace(oldpath, newpath)
	}

	f.mu.Lock()
	f.calls++
	if f.calls == 2 {
		close(f.release)
	}
	release := f.release
	f.mu.Unlock()

	select {
	case <-release:
	case <-time.After(5 * time.Second):
		return errors.New("timed out waiting for concurrent metadata publish")
	}

	return f.LocalFS.RenameNoReplace(oldpath, newpath)
}

type versionConflictOnCreateFS struct {
	icebergio.LocalFS

	conflictPath string

	once        sync.Once
	conflictErr error
}

func (f *versionConflictOnCreateFS) Create(name string) (icebergio.FileWriter, error) {
	w, err := f.LocalFS.Create(name)
	if err != nil {
		return nil, err
	}

	f.once.Do(func() {
		f.conflictErr = f.WriteFile(f.conflictPath, nil)
	})
	if f.conflictErr != nil {
		_ = w.Close()

		return nil, f.conflictErr
	}

	return w, nil
}

// a mock hadoop catalog filesystem to ensure that we
// can load arbitrary new filesystem implementations
// as long as they full the HadoopCatalogFS interface
type stubHadoopCatalogFS struct {
	icebergio.LocalFS
}

var _ HadoopCatalogFS = (*stubHadoopCatalogFS)(nil)

type stubIO struct{}

func (stubIO) Open(string) (icebergio.File, error) {
	return nil, fs.ErrNotExist
}

func (stubIO) Remove(string) error {
	return nil
}

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

func (s *HadoopCatalogTestSuite) registerScheme(factory icebergio.SchemeFactory) string {
	scheme := "hadoop-test-" + uuid.NewString()
	icebergio.Register(scheme, factory)
	s.T().Cleanup(func() {
		icebergio.Unregister(scheme)
	})

	return scheme
}

func (s *HadoopCatalogTestSuite) requireIsTableDir(path string) bool {
	isTable, err := isTableDir(s.cat.filesystem, s.cat.isLocal, path)
	s.Require().NoError(err)

	return isTable
}

func TestMetadataFileBetterThan(t *testing.T) {
	tests := []struct {
		name    string
		next    metadataFile
		current metadataFile
		want    bool
	}{
		{
			name: "empty current loses",
			next: metadataFile{
				location: "v1.metadata.json",
				version:  1,
			},
			want: true,
		},
		{
			name: "higher version wins",
			next: metadataFile{
				location: "v2.metadata.json",
				version:  2,
			},
			current: metadataFile{
				location: "v1.metadata.json",
				version:  1,
			},
			want: true,
		},
		{
			name: "lower version loses",
			next: metadataFile{
				location: "v1.metadata.json",
				version:  1,
			},
			current: metadataFile{
				location: "v2.metadata.json",
				version:  2,
			},
			want: false,
		},
		{
			name: "hadoop name wins same version",
			next: metadataFile{
				location:   "v1.metadata.json",
				version:    1,
				hadoopName: true,
			},
			current: metadataFile{
				location: "00001-11111111-1111-1111-1111-111111111111.metadata.json",
				version:  1,
			},
			want: true,
		},
		{
			name: "uuid name loses same version against hadoop name",
			next: metadataFile{
				location: "00001-11111111-1111-1111-1111-111111111111.metadata.json",
				version:  1,
			},
			current: metadataFile{
				location:   "v1.metadata.json",
				version:    1,
				hadoopName: true,
			},
			want: false,
		},
		{
			name: "uncompressed wins same version and style",
			next: metadataFile{
				location:   "v1.metadata.json",
				version:    1,
				hadoopName: true,
			},
			current: metadataFile{
				location:   "v1.gz.metadata.json",
				version:    1,
				hadoopName: true,
				compressed: true,
			},
			want: true,
		},
		{
			name: "same file does not beat itself",
			next: metadataFile{
				location:   "v1.metadata.json",
				version:    1,
				hadoopName: true,
			},
			current: metadataFile{
				location:   "v1.metadata.json",
				version:    1,
				hadoopName: true,
			},
			want: false,
		},
		{
			name: "naming style wins before compression",
			next: metadataFile{
				location:   "v1.gz.metadata.json",
				version:    1,
				hadoopName: true,
				compressed: true,
			},
			current: metadataFile{
				location:   "00001-11111111-1111-1111-1111-111111111111.metadata.json",
				version:    1,
				compressed: false,
			},
			want: true,
		},
		{
			name: "lexicographic fallback",
			next: metadataFile{
				location: "b.metadata.json",
				version:  1,
			},
			current: metadataFile{
				location: "a.metadata.json",
				version:  1,
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, tt.next.betterThan(tt.current))
		})
	}
}

type failingWalkFS struct {
	HadoopCatalogFS
	err error
}

func (f failingWalkFS) WalkDir(string, fs.WalkDirFunc) error {
	return f.err
}

type stagedWalkDirFS struct {
	HadoopCatalogFS
	targetPath string
	stage      func() error

	mu     sync.Mutex
	calls  int
	staged bool
}

func (f *stagedWalkDirFS) WalkDir(path string, fn fs.WalkDirFunc) error {
	if path == f.targetPath {
		stageNow := false
		f.mu.Lock()
		f.calls++
		if f.calls == 2 && !f.staged {
			f.staged = true
			stageNow = true
		}
		f.mu.Unlock()

		if stageNow {
			if err := f.stage(); err != nil {
				return err
			}
		}
	}

	return f.HadoopCatalogFS.WalkDir(path, fn)
}

type selectiveFailingWalkFS struct {
	HadoopCatalogFS
	failPath string
	err      error
}

func (f selectiveFailingWalkFS) WalkDir(path string, fn fs.WalkDirFunc) error {
	if path == f.failPath {
		return f.err
	}

	return f.HadoopCatalogFS.WalkDir(path, fn)
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

func (s *HadoopCatalogTestSuite) TestNewCatalogRequiresOptInForRemoteSchemes() {
	_, err := NewCatalog("test", "s3://bucket/path", nil)
	s.Require().Error(err)
	s.Contains(err.Error(), "`allow-unsafe-commits` must be set to true")

	_, err = NewCatalog("test", "hdfs://namenode/warehouse", nil)
	s.Require().Error(err)
	s.Contains(err.Error(), "`allow-unsafe-commits` must be set to true")

	loadFSCalled := false
	scheme := s.registerScheme(func(context.Context, *url.URL, map[string]string) (icebergio.IO, error) {
		loadFSCalled = true

		return &stubHadoopCatalogFS{}, nil
	})

	_, err = NewCatalog("test", scheme+"://bucket/path", nil)
	s.Require().Error(err)
	s.Contains(err.Error(), "`allow-unsafe-commits` must be set to true")
	s.False(loadFSCalled)
}

func (s *HadoopCatalogTestSuite) TestNewCatalogRemoteHappyPathWithUnsafeCommits() {
	scheme := s.registerScheme(func(_ context.Context, parsed *url.URL, props map[string]string) (icebergio.IO, error) {
		s.Equal("bucket", parsed.Host)
		s.Equal("/wh/", parsed.Path)
		s.Equal("true", props["allow-unsafe-commits"])

		return &stubHadoopCatalogFS{}, nil
	})

	cat, err := NewCatalog("test", scheme+"://bucket/wh/", iceberg.Properties{
		"allow-unsafe-commits": "true",
	})
	s.Require().NoError(err)
	s.False(cat.isLocal)
	s.IsType(&stubHadoopCatalogFS{}, cat.filesystem)

	ident := table.Identifier{"ns", "tbl"}
	s.Equal(scheme+"://bucket/wh/ns", cat.namespaceToPath(ident[:1]))
	s.Equal(scheme+"://bucket/wh/ns/tbl", cat.tableToPath(ident))
	s.Equal(scheme+"://bucket/wh/ns/tbl/metadata", cat.metadataDir(ident))
	metaPath, err := cat.metadataFilePathForCompression(ident, 1, table.MetadataCompressionCodecNone)
	s.Require().NoError(err)
	s.Equal(scheme+"://bucket/wh/ns/tbl/metadata/v1.metadata.json", metaPath)
	s.Equal(scheme+"://bucket/wh/ns/tbl/metadata/version-hint.text", cat.versionHintPath(ident))
	s.Equal(scheme+"://bucket/wh/ns/tbl", cat.defaultTableLocation(ident))
}

func (s *HadoopCatalogTestSuite) TestNewCatalogRemoteRequiresHadoopCatalogFS() {
	scheme := s.registerScheme(func(context.Context, *url.URL, map[string]string) (icebergio.IO, error) {
		return stubIO{}, nil
	})

	_, err := NewCatalog("test", scheme+"://bucket/wh", iceberg.Properties{
		"allow-unsafe-commits": "true",
	})
	s.Require().Error(err)
	s.Contains(err.Error(), "does not implement HadoopCatalogFS")
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

func (s *HadoopCatalogTestSuite) TestMetadataFilePathForCompression() {
	ident := []string{"ns", "tbl"}

	path, err := s.cat.metadataFilePathForCompression(ident, 1, table.MetadataCompressionCodecNone)
	s.Require().NoError(err)
	s.Equal(filepath.Join(s.warehouse, "ns", "tbl", "metadata", "v1.metadata.json"), path)

	path, err = s.cat.metadataFilePathForCompression(ident, 1, table.MetadataCompressionCodecGzip)
	s.Require().NoError(err)
	s.Equal(filepath.Join(s.warehouse, "ns", "tbl", "metadata", "v1.gz.metadata.json"), path)

	path, err = s.cat.metadataFilePathForCompression(ident, 42, table.MetadataCompressionCodecGzip)
	s.Require().NoError(err)
	s.Equal(filepath.Join(s.warehouse, "ns", "tbl", "metadata", "v42.gz.metadata.json"), path)

	for _, codec := range []string{table.MetadataCompressionCodecZstd, "brotli"} {
		_, err = s.cat.metadataFilePathForCompression(ident, 1, codec)
		s.Require().Error(err)
		s.Contains(err.Error(), "unsupported write metadata compression codec")
	}
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
		"v1.zstd.metadata.json",
	}

	for _, name := range tests {
		s.False(versionPattern.MatchString(name), "expected %s to not match", name)
	}
}

func (s *HadoopCatalogTestSuite) TestMetadataFileFromNameAcceptsZeroUUIDVersion() {
	file, ok := metadataFileFromName(
		"/tmp/00000-a1b2c3d4-e5f6-7890-abcd-ef1234567890.metadata.json",
		"00000-a1b2c3d4-e5f6-7890-abcd-ef1234567890.metadata.json",
	)
	s.True(ok)
	s.Equal(0, file.version)
	s.Equal("/tmp/00000-a1b2c3d4-e5f6-7890-abcd-ef1234567890.metadata.json", file.location)
}

func (s *HadoopCatalogTestSuite) TestMetadataFileFromNameRejectsOverflowVersion() {
	_, ok := metadataFileFromName(
		"/tmp/v99999999999999999999.metadata.json",
		"v99999999999999999999.metadata.json",
	)
	s.False(ok)
}

func (s *HadoopCatalogTestSuite) TestIsTableDirTrue() {
	tableDir := filepath.Join(s.warehouse, "ns", "tbl")
	metaDir := filepath.Join(tableDir, "metadata")
	s.Require().NoError(os.MkdirAll(metaDir, 0o755))
	s.Require().NoError(os.WriteFile(filepath.Join(metaDir, "v1.metadata.json"), nil, 0o644))

	s.True(s.requireIsTableDir(tableDir))
}

func (s *HadoopCatalogTestSuite) TestIsTableDirFalseNoMetadataDir() {
	nsDir := filepath.Join(s.warehouse, "ns")
	s.Require().NoError(os.MkdirAll(nsDir, 0o755))

	s.False(s.requireIsTableDir(nsDir))
}

func (s *HadoopCatalogTestSuite) TestIsTableDirFalseEmptyMetadataDir() {
	tableDir := filepath.Join(s.warehouse, "ns", "tbl")
	metaDir := filepath.Join(tableDir, "metadata")
	s.Require().NoError(os.MkdirAll(metaDir, 0o755))

	s.False(s.requireIsTableDir(tableDir))
}

func (s *HadoopCatalogTestSuite) TestIsTableDirTrueUUIDMetadata() {
	tableDir := filepath.Join(s.warehouse, "ns", "tbl")
	metaDir := filepath.Join(tableDir, "metadata")
	s.Require().NoError(os.MkdirAll(metaDir, 0o755))
	s.Require().NoError(os.WriteFile(filepath.Join(metaDir, "00001-a1b2c3d4-e5f6-7890-abcd-ef1234567890.metadata.json"), nil, 0o644))

	s.True(s.requireIsTableDir(tableDir))
}

func (s *HadoopCatalogTestSuite) TestIsTableDirTrueUUIDMetadataVersionZero() {
	tableDir := filepath.Join(s.warehouse, "ns", "tbl")
	metaDir := filepath.Join(tableDir, "metadata")
	s.Require().NoError(os.MkdirAll(metaDir, 0o755))
	s.Require().NoError(os.WriteFile(filepath.Join(metaDir, "00000-a1b2c3d4-e5f6-7890-abcd-ef1234567890.metadata.json"), nil, 0o644))

	s.True(s.requireIsTableDir(tableDir))
}

func (s *HadoopCatalogTestSuite) TestIsTableDirWithGzipMetadata() {
	tableDir := filepath.Join(s.warehouse, "ns", "tbl")
	metaDir := filepath.Join(tableDir, "metadata")
	s.Require().NoError(os.MkdirAll(metaDir, 0o755))
	s.Require().NoError(os.WriteFile(filepath.Join(metaDir, "v1.gz.metadata.json"), nil, 0o644))

	s.True(s.requireIsTableDir(tableDir))
}

func (s *HadoopCatalogTestSuite) TestIsTableDirIgnoresZstdMetadata() {
	tableDir := filepath.Join(s.warehouse, "ns", "tbl")
	metaDir := filepath.Join(tableDir, "metadata")
	s.Require().NoError(os.MkdirAll(metaDir, 0o755))
	s.Require().NoError(os.WriteFile(filepath.Join(metaDir, "v1.zstd.metadata.json"), nil, 0o644))

	s.False(s.requireIsTableDir(tableDir))
}

func (s *HadoopCatalogTestSuite) TestIsTableDirNonExistentPath() {
	s.False(s.requireIsTableDir(filepath.Join(s.warehouse, "does", "not", "exist")))
}

func (s *HadoopCatalogTestSuite) createMetadataFile(ident table.Identifier, version int) {
	path, err := s.cat.metadataFilePathForCompression(ident, version, table.MetadataCompressionCodecNone)
	s.Require().NoError(err)
	s.Require().NoError(os.MkdirAll(filepath.Dir(path), 0o755))
	s.Require().NoError(os.WriteFile(path, nil, 0o644))
}

func (s *HadoopCatalogTestSuite) replaceMetadataWithGzip(ident table.Identifier, version int) string {
	plainPath, err := s.cat.metadataFilePathForCompression(ident, version, table.MetadataCompressionCodecNone)
	s.Require().NoError(err)
	data, err := os.ReadFile(plainPath)
	s.Require().NoError(err)

	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	_, err = zw.Write(data)
	s.Require().NoError(err)
	s.Require().NoError(zw.Close())

	gzPath := filepath.Join(s.cat.metadataDir(ident), fmt.Sprintf("v%d.gz.metadata.json", version))
	s.Require().NoError(os.WriteFile(gzPath, buf.Bytes(), 0o644))
	s.Require().NoError(os.Remove(plainPath))

	return gzPath
}

func (s *HadoopCatalogTestSuite) replaceMetadataWithUUIDName(ident table.Identifier, version int) string {
	return s.replaceMetadataWithUUIDSequence(ident, version, version)
}

func (s *HadoopCatalogTestSuite) replaceMetadataWithUUIDSequence(ident table.Identifier, version, sequence int) string {
	plainPath, err := s.cat.metadataFilePathForCompression(ident, version, table.MetadataCompressionCodecNone)
	s.Require().NoError(err)
	uuidPath := filepath.Join(
		s.cat.metadataDir(ident),
		fmt.Sprintf("%05d-a1b2c3d4-e5f6-7890-abcd-ef1234567890.metadata.json", sequence),
	)
	s.Require().NoError(os.Rename(plainPath, uuidPath))

	return uuidPath
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

func (s *HadoopCatalogTestSuite) TestFindMetadataLocationHintGapKeepsLatest() {
	ident := []string{"ns", "tbl"}
	for _, version := range []int{1, 2, 5} {
		s.createMetadataFile(ident, version)
	}

	s.cat.writeVersionHint(ident, 1)
	location, version, err := s.cat.findMetadataLocation(ident)
	s.Require().NoError(err)
	s.Equal(5, version)
	plainPath, err := s.cat.metadataFilePathForCompression(ident, 5, table.MetadataCompressionCodecNone)
	s.Require().NoError(err)
	s.Equal(plainPath, location)
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
	// Table has only gzip metadata. Discovery should recognize
	// gzip-compressed metadata files.
	ident := []string{"ns", "tbl"}
	dir := s.cat.metadataDir(ident)
	s.Require().NoError(os.MkdirAll(dir, 0o755))
	s.Require().NoError(os.WriteFile(filepath.Join(dir, "v1.gz.metadata.json"), nil, 0o644))
	s.Require().NoError(os.WriteFile(filepath.Join(dir, "v2.gz.metadata.json"), nil, 0o644))
	s.cat.writeVersionHint(ident, 1)

	path, ver, err := s.cat.findMetadataLocation(ident)
	s.Require().NoError(err)
	s.Equal(2, ver)
	s.Equal(filepath.Join(dir, "v2.gz.metadata.json"), path)
}

func (s *HadoopCatalogTestSuite) TestFindVersionMixedGzipAndPlain() {
	// v1 and v2 are plain, v3 is gzip-compressed. Discovery should choose
	// the latest recognized metadata file across both formats.
	ident := []string{"ns", "tbl"}
	dir := s.cat.metadataDir(ident)
	s.Require().NoError(os.MkdirAll(dir, 0o755))
	s.createMetadataFile(ident, 1)
	s.createMetadataFile(ident, 2)
	s.Require().NoError(os.WriteFile(filepath.Join(dir, "v3.gz.metadata.json"), nil, 0o644))
	s.cat.writeVersionHint(ident, 1)

	path, ver, err := s.cat.findMetadataLocation(ident)
	s.Require().NoError(err)
	s.Equal(3, ver)
	s.Equal(filepath.Join(dir, "v3.gz.metadata.json"), path)
}

func (s *HadoopCatalogTestSuite) TestFindMetadataLocationUsesCanonicalPathForDuplicateVersion() {
	ident := []string{"ns", "tbl"}
	dir := s.cat.metadataDir(ident)
	s.Require().NoError(os.MkdirAll(dir, 0o755))
	s.createMetadataFile(ident, 1)
	s.createMetadataFile(ident, 2)
	s.createMetadataFile(ident, 3)
	s.Require().NoError(os.WriteFile(filepath.Join(dir, "v3.gz.metadata.json"), nil, 0o644))

	path, ver, err := s.cat.findMetadataLocation(ident)
	s.Require().NoError(err)
	s.Equal(3, ver)
	s.Equal(filepath.Join(dir, "v3.metadata.json"), path)
}

// CreateNamespace tests

func (s *HadoopCatalogTestSuite) TestCreateNamespace() {
	err := s.cat.CreateNamespace(context.Background(), []string{"ns"}, nil)
	s.Require().NoError(err)

	info, err := os.Stat(filepath.Join(s.warehouse, "ns"))
	s.Require().NoError(err)
	s.True(info.IsDir())
}

func (s *HadoopCatalogTestSuite) TestCreateNamespaceAlreadyExists() {
	s.Require().NoError(os.Mkdir(filepath.Join(s.warehouse, "ns"), 0o755))

	err := s.cat.CreateNamespace(context.Background(), []string{"ns"}, nil)
	s.ErrorIs(err, catalog.ErrNamespaceAlreadyExists)
}

func (s *HadoopCatalogTestSuite) TestCreateNamespaceNested() {
	// Parent namespaces must exist first with atomic os.Mkdir.
	s.Require().NoError(os.Mkdir(filepath.Join(s.warehouse, "a"), 0o755))
	s.Require().NoError(os.Mkdir(filepath.Join(s.warehouse, "a", "b"), 0o755))

	err := s.cat.CreateNamespace(context.Background(), []string{"a", "b", "c"}, nil)
	s.Require().NoError(err)

	info, err := os.Stat(filepath.Join(s.warehouse, "a", "b", "c"))
	s.Require().NoError(err)
	s.True(info.IsDir())
}

func (s *HadoopCatalogTestSuite) TestCreateNamespaceNestedParentMissing() {
	// Creating a nested namespace without parent should fail.
	err := s.cat.CreateNamespace(context.Background(), []string{"a", "b", "c"}, nil)
	s.Require().Error(err)
	s.ErrorIs(err, catalog.ErrNoSuchNamespace)
}

func (s *HadoopCatalogTestSuite) TestCreateNamespaceWithProperties() {
	err := s.cat.CreateNamespace(context.Background(), []string{"ns"}, iceberg.Properties{"key": "val"})
	s.Require().Error(err)
	s.Contains(err.Error(), "properties are not supported")
}

func (s *HadoopCatalogTestSuite) TestCreateNamespaceNilProperties() {
	err := s.cat.CreateNamespace(context.Background(), []string{"ns"}, nil)
	s.Require().NoError(err)

	info, err := os.Stat(filepath.Join(s.warehouse, "ns"))
	s.Require().NoError(err)
	s.True(info.IsDir())
}

// DropNamespace tests

func (s *HadoopCatalogTestSuite) TestDropNamespace() {
	nsDir := filepath.Join(s.warehouse, "ns")
	s.Require().NoError(os.Mkdir(nsDir, 0o755))

	err := s.cat.DropNamespace(context.Background(), []string{"ns"})
	s.Require().NoError(err)

	_, err = os.Stat(nsDir)
	s.True(os.IsNotExist(err))
}

// statFailingFS guards against reintroducing a pre-walk Stat in DropNamespace
// (see issue #1273). It embeds LocalFS so every other operation behaves
// normally, but fails and counts any Stat call so the test can assert that
// DropNamespace never stats the namespace before walking it.
type statFailingFS struct {
	icebergio.LocalFS
	statCalls int
}

func (f *statFailingFS) Stat(string) (fs.FileInfo, error) {
	f.statCalls++

	return nil, errors.New("stat should not be called")
}

func (s *HadoopCatalogTestSuite) TestDropNamespaceDoesNotPreStat() {
	nsDir := filepath.Join(s.warehouse, "ns")
	s.Require().NoError(os.Mkdir(nsDir, 0o755))

	fsys := &statFailingFS{}
	s.cat.filesystem = fsys

	err := s.cat.DropNamespace(context.Background(), []string{"ns"})
	s.Require().NoError(err)
	s.Zero(fsys.statCalls)

	_, err = os.Stat(nsDir)
	s.True(os.IsNotExist(err))
}

func (s *HadoopCatalogTestSuite) TestDropNamespaceNotExists() {
	err := s.cat.DropNamespace(context.Background(), []string{"nope"})
	s.ErrorIs(err, catalog.ErrNoSuchNamespace)
}

func (s *HadoopCatalogTestSuite) TestDropNamespaceNotEmptyWithTable() {
	nsDir := filepath.Join(s.warehouse, "ns")
	metaDir := filepath.Join(nsDir, "tbl", "metadata")
	s.Require().NoError(os.MkdirAll(metaDir, 0o755))
	s.Require().NoError(os.WriteFile(filepath.Join(metaDir, "v1.metadata.json"), nil, 0o644))

	err := s.cat.DropNamespace(context.Background(), []string{"ns"})
	s.ErrorIs(err, catalog.ErrNamespaceNotEmpty)
}

func (s *HadoopCatalogTestSuite) TestDropNamespaceNotEmptyWithChildNamespace() {
	nsDir := filepath.Join(s.warehouse, "ns")
	s.Require().NoError(os.MkdirAll(filepath.Join(nsDir, "child_ns"), 0o755))

	err := s.cat.DropNamespace(context.Background(), []string{"ns"})
	s.ErrorIs(err, catalog.ErrNamespaceNotEmpty)
}

func (s *HadoopCatalogTestSuite) TestDropNameSpaceFileInsteadofDir() {
	filePath := filepath.Join(s.warehouse, "not_a_dir")
	s.Require().NoError(os.WriteFile(filePath, nil, 0o644))
	err := s.cat.DropNamespace(context.Background(), []string{"not_a_dir"})
	s.ErrorIs(err, catalog.ErrNoSuchNamespace)
	info, err := os.Stat(filePath)
	s.NoError(err)
	s.False(info.IsDir())
}

// CheckNamespaceExists tests

func (s *HadoopCatalogTestSuite) TestCheckNamespaceExistsTrue() {
	s.Require().NoError(os.Mkdir(filepath.Join(s.warehouse, "ns"), 0o755))

	exists, err := s.cat.CheckNamespaceExists(context.Background(), []string{"ns"})
	s.Require().NoError(err)
	s.True(exists)
}

func (s *HadoopCatalogTestSuite) TestCheckNamespaceExistsFalse() {
	exists, err := s.cat.CheckNamespaceExists(context.Background(), []string{"nope"})
	s.Require().NoError(err)
	s.False(exists)
}

// ListNamespaces tests

func (s *HadoopCatalogTestSuite) TestListNamespacesRoot() {
	s.Require().NoError(os.Mkdir(filepath.Join(s.warehouse, "ns1"), 0o755))
	s.Require().NoError(os.Mkdir(filepath.Join(s.warehouse, "ns2"), 0o755))

	namespaces, err := s.cat.ListNamespaces(context.Background(), nil)
	s.Require().NoError(err)
	s.Len(namespaces, 2)
	s.Contains(namespaces, table.Identifier{"ns1"})
	s.Contains(namespaces, table.Identifier{"ns2"})
}

func (s *HadoopCatalogTestSuite) TestListNamespacesEmpty() {
	namespaces, err := s.cat.ListNamespaces(context.Background(), nil)
	s.Require().NoError(err)
	s.Empty(namespaces)
	s.NotNil(namespaces)
}

func (s *HadoopCatalogTestSuite) TestListNamespacesNested() {
	parentDir := filepath.Join(s.warehouse, "a")
	s.Require().NoError(os.MkdirAll(filepath.Join(parentDir, "child1"), 0o755))
	s.Require().NoError(os.MkdirAll(filepath.Join(parentDir, "child2"), 0o755))

	namespaces, err := s.cat.ListNamespaces(context.Background(), []string{"a"})
	s.Require().NoError(err)
	s.Len(namespaces, 2)
	s.Contains(namespaces, table.Identifier{"child1"})
	s.Contains(namespaces, table.Identifier{"child2"})
}

func (s *HadoopCatalogTestSuite) TestListNamespacesParentNotExists() {
	_, err := s.cat.ListNamespaces(context.Background(), []string{"nope"})
	s.ErrorIs(err, catalog.ErrNoSuchNamespace)
}

func (s *HadoopCatalogTestSuite) TestListNamespacesParentIsFile() {
	filePath := filepath.Join(s.warehouse, "a_file")
	s.Require().NoError(os.WriteFile(filePath, nil, 0o644))

	_, err := s.cat.ListNamespaces(context.Background(), []string{"a_file"})
	s.ErrorIs(err, catalog.ErrNoSuchNamespace)
}

// LoadNamespaceProperties tests

func (s *HadoopCatalogTestSuite) TestLoadNamespaceProperties() {
	nsDir := filepath.Join(s.warehouse, "ns")
	s.Require().NoError(os.Mkdir(nsDir, 0o755))

	props, err := s.cat.LoadNamespaceProperties(context.Background(), []string{"ns"})
	s.Require().NoError(err)
	s.Equal(iceberg.Properties{"location": "file://" + nsDir}, props)
}

func (s *HadoopCatalogTestSuite) TestLoadNamespacePropertiesNotExists() {
	_, err := s.cat.LoadNamespaceProperties(context.Background(), []string{"nope"})
	s.ErrorIs(err, catalog.ErrNoSuchNamespace)
}

func (s *HadoopCatalogTestSuite) TestLoadNamespacePropertiesFileNotDir() {
	filePath := filepath.Join(s.warehouse, "not_a_ns")
	s.Require().NoError(os.WriteFile(filePath, nil, 0o644))

	_, err := s.cat.LoadNamespaceProperties(context.Background(), []string{"not_a_ns"})
	s.ErrorIs(err, catalog.ErrNoSuchNamespace)
}

// UpdateNamespaceProperties test

func (s *HadoopCatalogTestSuite) TestUpdateNamespacePropertiesUnsupported() {
	_, err := s.cat.UpdateNamespaceProperties(context.Background(), []string{"ns"}, nil, nil)
	s.Require().Error(err)
	s.Contains(err.Error(), "not yet implemented")
}

func (s *HadoopCatalogTestSuite) TestDropNamespaceWithRegularFilesOnly() {
	// A namespace dir containing only regular files (no subdirectories)
	// should still return ErrNamespaceNotEmpty.
	nsDir := filepath.Join(s.warehouse, "ns")
	s.Require().NoError(os.Mkdir(nsDir, 0o755))
	s.Require().NoError(os.WriteFile(filepath.Join(nsDir, "stray_file.txt"), nil, 0o644))

	err := s.cat.DropNamespace(context.Background(), []string{"ns"})
	s.ErrorIs(err, catalog.ErrNamespaceNotEmpty)
}

func (s *HadoopCatalogTestSuite) TestCheckNamespaceExistsFileNotDir() {
	// A file at the namespace path should return false (not a directory).
	filePath := filepath.Join(s.warehouse, "not_a_dir")
	s.Require().NoError(os.WriteFile(filePath, nil, 0o644))

	exists, err := s.cat.CheckNamespaceExists(context.Background(), []string{"not_a_dir"})
	s.Require().NoError(err)
	s.False(exists)
}

func (s *HadoopCatalogTestSuite) TestCreateNamespaceEmptyIdentifier() {
	err := s.cat.CreateNamespace(context.Background(), []string{}, nil)
	s.Require().Error(err)
	s.ErrorIs(err, catalog.ErrInvalidIdentifier)
	s.NotErrorIs(err, catalog.ErrNoSuchNamespace)
}

func (s *HadoopCatalogTestSuite) TestDropNamespaceEmptyIdentifier() {
	err := s.cat.DropNamespace(context.Background(), []string{})
	s.Require().Error(err)
	s.ErrorIs(err, catalog.ErrInvalidIdentifier)
	s.NotErrorIs(err, catalog.ErrNoSuchNamespace)
}

func (s *HadoopCatalogTestSuite) TestLoadNamespacePropertiesNested() {
	// Verify nested namespace returns the correct absolute path.
	nsDir := filepath.Join(s.warehouse, "a", "b", "c")
	s.Require().NoError(os.MkdirAll(nsDir, 0o755))

	props, err := s.cat.LoadNamespaceProperties(context.Background(), []string{"a", "b", "c"})
	s.Require().NoError(err)
	s.Equal("file://"+nsDir, props["location"])
}

func (s *HadoopCatalogTestSuite) TestListNamespacesMixedContent() {
	// Directory with tables, namespaces, and regular files.
	// Only non-table directories should appear.
	s.Require().NoError(os.Mkdir(filepath.Join(s.warehouse, "child_ns"), 0o755))

	// Create a table dir
	tableDir := filepath.Join(s.warehouse, "my_table")
	metaDir := filepath.Join(tableDir, "metadata")
	s.Require().NoError(os.MkdirAll(metaDir, 0o755))
	s.Require().NoError(os.WriteFile(filepath.Join(metaDir, "v1.metadata.json"), nil, 0o644))

	// Regular file
	s.Require().NoError(os.WriteFile(filepath.Join(s.warehouse, "README.txt"), nil, 0o644))

	namespaces, err := s.cat.ListNamespaces(context.Background(), nil)
	s.Require().NoError(err)
	s.Len(namespaces, 1)
	s.Equal(table.Identifier{"child_ns"}, namespaces[0])
}

// Path validation tests

func (s *HadoopCatalogTestSuite) TestCreateNamespaceRejectsDotDot() {
	err := s.cat.CreateNamespace(context.Background(), []string{"a", ".."}, nil)
	s.Require().Error(err)
	s.ErrorIs(err, catalog.ErrInvalidIdentifier)
	s.NotErrorIs(err, catalog.ErrNoSuchNamespace)
}

func (s *HadoopCatalogTestSuite) TestCreateNamespaceRejectsDot() {
	err := s.cat.CreateNamespace(context.Background(), []string{"."}, nil)
	s.Require().Error(err)
	s.ErrorIs(err, catalog.ErrInvalidIdentifier)
	s.NotErrorIs(err, catalog.ErrNoSuchNamespace)
}

func (s *HadoopCatalogTestSuite) TestCreateNamespaceRejectsEmptyComponent() {
	err := s.cat.CreateNamespace(context.Background(), []string{"a", "", "b"}, nil)
	s.Require().Error(err)
	s.ErrorIs(err, catalog.ErrInvalidIdentifier)
	s.NotErrorIs(err, catalog.ErrNoSuchNamespace)
}

func (s *HadoopCatalogTestSuite) TestCreateNamespaceRejectsPathSeparator() {
	err := s.cat.CreateNamespace(context.Background(), []string{"a/b"}, nil)
	s.Require().Error(err)
	s.ErrorIs(err, catalog.ErrInvalidIdentifier)
	s.NotErrorIs(err, catalog.ErrNoSuchNamespace)
}

func (s *HadoopCatalogTestSuite) TestDropNamespaceRejectsDotDot() {
	err := s.cat.DropNamespace(context.Background(), []string{".."})
	s.Require().Error(err)
	s.ErrorIs(err, catalog.ErrInvalidIdentifier)
	s.NotErrorIs(err, catalog.ErrNoSuchNamespace)
}

func (s *HadoopCatalogTestSuite) TestListNamespacesRejectsInvalidParent() {
	_, err := s.cat.ListNamespaces(context.Background(), []string{"a", ".."})
	s.Require().Error(err)
	s.ErrorIs(err, catalog.ErrInvalidIdentifier)
	s.NotErrorIs(err, catalog.ErrNoSuchNamespace)
}

func (s *HadoopCatalogTestSuite) TestLoadNamespacePropertiesRejectsInvalid() {
	_, err := s.cat.LoadNamespaceProperties(context.Background(), []string{".."})
	s.Require().Error(err)
	s.ErrorIs(err, catalog.ErrInvalidIdentifier)
	s.NotErrorIs(err, catalog.ErrNoSuchNamespace)
}

func (s *HadoopCatalogTestSuite) TestCheckNamespaceExistsRejectsInvalid() {
	_, err := s.cat.CheckNamespaceExists(context.Background(), []string{"a/b"})
	s.Require().Error(err)
	s.Require().ErrorIs(err, catalog.ErrInvalidIdentifier)
	s.NotErrorIs(err, catalog.ErrNoSuchNamespace)
}

func (s *HadoopCatalogTestSuite) TestListTablesRejectsInvalidIdentifiers() {
	tests := []struct {
		name  string
		ident table.Identifier
	}{
		{"empty component", table.Identifier{"a", "", "b"}},
		{"dot component", table.Identifier{"."}},
		{"dot dot component", table.Identifier{".."}},
		{"slash component", table.Identifier{"a/b"}},
		{"backslash component", table.Identifier{`a\b`}},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			for _, err := range s.cat.ListTables(context.Background(), tt.ident) {
				s.Require().Error(err)
				s.ErrorIs(err, catalog.ErrInvalidIdentifier)
				s.NotErrorIs(err, catalog.ErrNoSuchNamespace)

				break
			}
		})
	}
}

// isTableDir interop tests

func (s *HadoopCatalogTestSuite) TestIsTableDirFalseVersionHintOnly() {
	tableDir := filepath.Join(s.warehouse, "ns", "tbl")
	metaDir := filepath.Join(tableDir, "metadata")
	s.Require().NoError(os.MkdirAll(metaDir, 0o755))
	s.Require().NoError(os.WriteFile(filepath.Join(metaDir, "version-hint.text"), []byte("1"), 0o644))

	s.False(s.requireIsTableDir(tableDir))
}

func (s *HadoopCatalogTestSuite) TestIsTableDirTrueUUIDMetadataGzip() {
	tableDir := filepath.Join(s.warehouse, "ns", "tbl")
	metaDir := filepath.Join(tableDir, "metadata")
	s.Require().NoError(os.MkdirAll(metaDir, 0o755))
	s.Require().NoError(os.WriteFile(filepath.Join(metaDir, "00001-a1b2c3d4-e5f6-7890-abcd-ef1234567890.gz.metadata.json"), nil, 0o644))

	s.True(s.requireIsTableDir(tableDir))
}

func (s *HadoopCatalogTestSuite) TestIsTableDirFalseNonMatchingFiles() {
	tableDir := filepath.Join(s.warehouse, "ns", "tbl")
	metaDir := filepath.Join(tableDir, "metadata")
	s.Require().NoError(os.MkdirAll(metaDir, 0o755))
	s.Require().NoError(os.WriteFile(filepath.Join(metaDir, "random.json"), nil, 0o644))

	s.False(s.requireIsTableDir(tableDir))
}

// Helper to create a fake table directory with a metadata file.
func (s *HadoopCatalogTestSuite) createFakeTable(ident table.Identifier) {
	metaDir := filepath.Join(s.cat.tableToPath(ident), "metadata")
	s.Require().NoError(os.MkdirAll(metaDir, 0o755))
	s.Require().NoError(os.WriteFile(filepath.Join(metaDir, "v1.metadata.json"), []byte("{}"), 0o644))
}

// ListTables tests

func (s *HadoopCatalogTestSuite) TestListTablesEmpty() {
	ctx := context.Background()
	s.Require().NoError(os.Mkdir(filepath.Join(s.warehouse, "ns"), 0o755))

	var tables []table.Identifier
	for ident, err := range s.cat.ListTables(ctx, []string{"ns"}) {
		s.Require().NoError(err)
		tables = append(tables, ident)
	}

	s.Empty(tables)
}

func (s *HadoopCatalogTestSuite) TestListTablesWithTables() {
	ctx := context.Background()
	s.Require().NoError(os.Mkdir(filepath.Join(s.warehouse, "ns"), 0o755))

	s.createFakeTable([]string{"ns", "tbl1"})
	s.createFakeTable([]string{"ns", "tbl2"})

	var tables []table.Identifier
	for ident, err := range s.cat.ListTables(ctx, []string{"ns"}) {
		s.Require().NoError(err)
		tables = append(tables, ident)
	}

	s.Len(tables, 2)
	s.Contains(tables, table.Identifier{"ns", "tbl1"})
	s.Contains(tables, table.Identifier{"ns", "tbl2"})
}

func (s *HadoopCatalogTestSuite) TestListTablesNoNamespace() {
	ctx := context.Background()

	for _, err := range s.cat.ListTables(ctx, []string{"nope"}) {
		s.ErrorIs(err, catalog.ErrNoSuchNamespace)

		break
	}
}

func (s *HadoopCatalogTestSuite) TestListTablesEmptyIdentifier() {
	ctx := context.Background()

	for _, err := range s.cat.ListTables(ctx, []string{}) {
		s.Require().Error(err)
		s.Contains(err.Error(), "must not be empty")

		break
	}
}

func (s *HadoopCatalogTestSuite) TestListTablesMixedContent() {
	// Namespace with tables, child namespaces, and regular files.
	// Only table dirs should be returned.
	ctx := context.Background()
	nsDir := filepath.Join(s.warehouse, "ns")
	s.Require().NoError(os.Mkdir(nsDir, 0o755))

	s.createFakeTable([]string{"ns", "tbl1"})
	s.Require().NoError(os.Mkdir(filepath.Join(nsDir, "child_ns"), 0o755))
	s.Require().NoError(os.WriteFile(filepath.Join(nsDir, "stray_file.txt"), nil, 0o644))

	var tables []table.Identifier
	for ident, err := range s.cat.ListTables(ctx, []string{"ns"}) {
		s.Require().NoError(err)
		tables = append(tables, ident)
	}

	s.Len(tables, 1)
	s.Equal(table.Identifier{"ns", "tbl1"}, tables[0])
}

func (s *HadoopCatalogTestSuite) TestListTablesSkipsUnreadableMetadataDir() {
	ctx := context.Background()
	s.Require().NoError(os.Mkdir(filepath.Join(s.warehouse, "ns"), 0o755))

	s.createFakeTable([]string{"ns", "visible"})
	s.createFakeTable([]string{"ns", "hidden"})

	originalFS := s.cat.filesystem
	s.cat.filesystem = selectiveFailingWalkFS{
		HadoopCatalogFS: originalFS,
		failPath:        s.cat.metadataDir([]string{"ns", "hidden"}),
		err:             fs.ErrPermission,
	}
	defer func() {
		s.cat.filesystem = originalFS
	}()

	var tables []table.Identifier
	for ident, err := range s.cat.ListTables(ctx, []string{"ns"}) {
		s.Require().NoError(err)
		tables = append(tables, ident)
	}

	s.Equal([]table.Identifier{{"ns", "visible"}}, tables)
}

func (s *HadoopCatalogTestSuite) TestListTablesNestedNamespace() {
	ctx := context.Background()
	s.Require().NoError(os.MkdirAll(filepath.Join(s.warehouse, "a", "b"), 0o755))

	s.createFakeTable([]string{"a", "b", "tbl1"})

	var tables []table.Identifier
	for ident, err := range s.cat.ListTables(ctx, []string{"a", "b"}) {
		s.Require().NoError(err)
		tables = append(tables, ident)
	}

	s.Len(tables, 1)
	s.Equal(table.Identifier{"a", "b", "tbl1"}, tables[0])
}

func (s *HadoopCatalogTestSuite) TestListTablesDoesNotYieldTablesInChildNamespace() {
	// When a namespace is passed into ListTables, only tables in that direct
	// namespace should be yielded. Not the additionaltables in the children namespaces.
	ctx := context.Background()
	err := s.cat.CreateNamespace(ctx, []string{"ns"}, nil)
	s.Require().NoError(err)
	s.createFakeTable([]string{"ns", "tbl1"})
	err = s.cat.CreateNamespace(ctx, []string{"ns", "child_ns"}, nil)
	s.Require().NoError(err)
	s.createFakeTable([]string{"ns", "child_ns", "tbl2"})
	var tables []table.Identifier
	for ident, err := range s.cat.ListTables(ctx, []string{"ns"}) {
		s.Require().NoError(err)
		tables = append(tables, ident)
	}
	s.Len(tables, 1)
	s.Equal(table.Identifier{"ns", "tbl1"}, tables[0])
}

// DropTable tests

func (s *HadoopCatalogTestSuite) TestDropTable() {
	ctx := context.Background()
	s.Require().NoError(os.Mkdir(filepath.Join(s.warehouse, "ns"), 0o755))
	s.createFakeTable([]string{"ns", "tbl"})

	err := s.cat.DropTable(ctx, []string{"ns", "tbl"})
	s.Require().NoError(err)

	// Verify the table directory is completely removed.
	_, statErr := os.Stat(filepath.Join(s.warehouse, "ns", "tbl"))
	s.True(os.IsNotExist(statErr))
}

func (s *HadoopCatalogTestSuite) TestDropTableNotExists() {
	err := s.cat.DropTable(context.Background(), []string{"ns", "tbl"})
	s.ErrorIs(err, catalog.ErrNoSuchTable)
}

func (s *HadoopCatalogTestSuite) TestDropTableVerifyCleanup() {
	ctx := context.Background()
	s.Require().NoError(os.Mkdir(filepath.Join(s.warehouse, "ns"), 0o755))
	s.createFakeTable([]string{"ns", "tbl"})

	// Add a data file to confirm everything is purged.
	dataDir := filepath.Join(s.warehouse, "ns", "tbl", "data")
	s.Require().NoError(os.MkdirAll(dataDir, 0o755))
	s.Require().NoError(os.WriteFile(filepath.Join(dataDir, "00000-0-0-00000.parquet"), []byte("data"), 0o644))

	err := s.cat.DropTable(ctx, []string{"ns", "tbl"})
	s.Require().NoError(err)

	_, statErr := os.Stat(filepath.Join(s.warehouse, "ns", "tbl"))
	s.True(os.IsNotExist(statErr))
}

func (s *HadoopCatalogTestSuite) TestPurgeTable() {
	ctx := context.Background()
	ident := table.Identifier{"ns", "tbl"}
	s.Require().NoError(os.Mkdir(filepath.Join(s.warehouse, "ns"), 0o755))

	tbl, err := s.cat.CreateTable(ctx, ident, s.testSchema())
	s.Require().NoError(err)

	externalDataPath := filepath.Join(s.warehouse, "external", "data.parquet")
	s.Require().NoError(os.MkdirAll(filepath.Dir(externalDataPath), 0o755))
	s.Require().NoError(os.WriteFile(externalDataPath, []byte("data"), 0o644))

	dataFileBuilder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec,
		iceberg.EntryContentData,
		externalDataPath,
		iceberg.ParquetFile,
		nil,
		nil,
		nil,
		1,
		4,
	)
	s.Require().NoError(err)

	tx := tbl.NewTransaction()
	s.Require().NoError(tx.AddDataFiles(ctx, []iceberg.DataFile{dataFileBuilder.Build()}, nil))
	_, err = tx.Commit(ctx)
	s.Require().NoError(err)

	s.Require().NoError(s.cat.PurgeTable(ctx, ident))

	_, statErr := os.Stat(filepath.Join(s.warehouse, "ns", "tbl"))
	s.True(os.IsNotExist(statErr))
	_, statErr = os.Stat(externalDataPath)
	s.True(os.IsNotExist(statErr))
	exists, err := s.cat.CheckTableExists(ctx, ident)
	s.Require().NoError(err)
	s.False(exists)
}

func (s *HadoopCatalogTestSuite) TestDropTableShortIdentifier() {
	err := s.cat.DropTable(context.Background(), []string{"tbl"})
	s.Require().Error(err)
	s.ErrorIs(err, catalog.ErrInvalidIdentifier)
	s.NotErrorIs(err, catalog.ErrNoSuchTable)
	s.Contains(err.Error(), "at least a namespace and table name")
}

// CreateTable tests

func (s *HadoopCatalogTestSuite) testSchema() *iceberg.Schema {
	return iceberg.NewSchema(
		1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
}

func (s *HadoopCatalogTestSuite) TestCreateTable() {
	ctx := context.Background()
	s.Require().NoError(os.Mkdir(filepath.Join(s.warehouse, "ns"), 0o755))

	tbl, err := s.cat.CreateTable(ctx, []string{"ns", "tbl"}, s.testSchema())
	s.Require().NoError(err)
	s.NotNil(tbl)

	// Verify metadata directory and files exist
	metaDir := filepath.Join(s.warehouse, "ns", "tbl", "metadata")
	s.DirExists(metaDir)
	s.FileExists(filepath.Join(metaDir, "v1.metadata.json"))

	// Verify version hint
	data, err := os.ReadFile(filepath.Join(metaDir, "version-hint.text"))
	s.Require().NoError(err)
	s.Equal("1", string(data))

	// Verify metadata
	s.Equal(filepath.Join(metaDir, "v1.metadata.json"), tbl.MetadataLocation())
	s.Equal(filepath.Join(s.warehouse, "ns", "tbl"), tbl.Location())
}

func (s *HadoopCatalogTestSuite) TestCreateTableAndLoad() {
	ctx := context.Background()
	s.Require().NoError(os.Mkdir(filepath.Join(s.warehouse, "ns"), 0o755))

	schema := s.testSchema()
	created, err := s.cat.CreateTable(ctx, []string{"ns", "tbl"}, schema)
	s.Require().NoError(err)

	loaded, err := s.cat.LoadTable(ctx, []string{"ns", "tbl"})
	s.Require().NoError(err)

	s.Equal(created.Metadata().TableUUID(), loaded.Metadata().TableUUID())
	s.Equal(created.Location(), loaded.Location())
	s.Equal(len(created.Schema().Fields()), len(loaded.Schema().Fields()))
}

func (s *HadoopCatalogTestSuite) TestCreateTableGzipMetadata() {
	ctx := context.Background()
	s.Require().NoError(os.Mkdir(filepath.Join(s.warehouse, "ns"), 0o755))

	props := iceberg.Properties{
		table.MetadataCompressionKey: table.MetadataCompressionCodecGzip,
	}

	tbl, err := s.cat.CreateTable(ctx, []string{"ns", "tbl"}, s.testSchema(),
		catalog.WithProperties(props))
	s.Require().NoError(err)

	metaPath := filepath.Join(s.warehouse, "ns", "tbl", "metadata", "v1.gz.metadata.json")
	s.FileExists(metaPath)
	s.Equal(metaPath, tbl.MetadataLocation())

	loaded, err := s.cat.LoadTable(ctx, []string{"ns", "tbl"})
	s.Require().NoError(err)
	s.Equal(metaPath, loaded.MetadataLocation())
	s.Equal(tbl.Metadata().TableUUID(), loaded.Metadata().TableUUID())
}

func (s *HadoopCatalogTestSuite) TestCreateTableRejectsZstdMetadata() {
	ctx := context.Background()
	s.Require().NoError(os.Mkdir(filepath.Join(s.warehouse, "ns"), 0o755))

	props := iceberg.Properties{
		table.MetadataCompressionKey: table.MetadataCompressionCodecZstd,
	}

	_, err := s.cat.CreateTable(ctx, []string{"ns", "tbl"}, s.testSchema(),
		catalog.WithProperties(props))
	s.Require().Error(err)
	s.Contains(err.Error(), "unsupported write metadata compression codec")
}

func (s *HadoopCatalogTestSuite) TestCreateTableCustomLocation() {
	ctx := context.Background()
	s.Require().NoError(os.Mkdir(filepath.Join(s.warehouse, "ns"), 0o755))

	_, err := s.cat.CreateTable(ctx, []string{"ns", "tbl"}, s.testSchema(),
		catalog.WithLocation("/some/other/path"))
	s.Require().Error(err)
	s.Contains(err.Error(), "custom table locations are not supported")
}

func (s *HadoopCatalogTestSuite) TestCreateTableSameLocationAllowed() {
	ctx := context.Background()
	s.Require().NoError(os.Mkdir(filepath.Join(s.warehouse, "ns"), 0o755))

	loc := filepath.Join(s.warehouse, "ns", "tbl")
	tbl, err := s.cat.CreateTable(ctx, []string{"ns", "tbl"}, s.testSchema(),
		catalog.WithLocation(loc))
	s.Require().NoError(err)
	s.Equal(loc, tbl.Location())
}

func (s *HadoopCatalogTestSuite) TestCreateTableNoNamespace() {
	ctx := context.Background()

	_, err := s.cat.CreateTable(ctx, []string{"nonexistent", "tbl"}, s.testSchema())
	s.ErrorIs(err, catalog.ErrNoSuchNamespace)
}

func (s *HadoopCatalogTestSuite) TestCreateTableAlreadyExists() {
	ctx := context.Background()
	s.Require().NoError(os.Mkdir(filepath.Join(s.warehouse, "ns"), 0o755))

	_, err := s.cat.CreateTable(ctx, []string{"ns", "tbl"}, s.testSchema())
	s.Require().NoError(err)

	_, err = s.cat.CreateTable(ctx, []string{"ns", "tbl"}, s.testSchema())
	s.ErrorIs(err, catalog.ErrTableAlreadyExists)
}

func (s *HadoopCatalogTestSuite) TestCreateTableConcurrentMetadataPublishConflict() {
	ctx := context.Background()
	s.Require().NoError(os.Mkdir(filepath.Join(s.warehouse, "ns"), 0o755))
	ident := []string{"ns", "tbl"}
	s.cat.filesystem = newBarrierRenameNoReplaceFS(filepath.Base(s.cat.metadataVersionClaimPath(ident, 1)))

	type createResult struct {
		metaLoc string
		err     error
	}

	results := make(chan createResult, 2)
	var wg sync.WaitGroup

	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()

			tbl, err := s.cat.CreateTable(ctx, ident, s.testSchema())
			result := createResult{err: err}
			if err == nil {
				result.metaLoc = tbl.MetadataLocation()
			}

			results <- result
		}()
	}

	wg.Wait()
	close(results)

	successes := 0
	conflicts := 0
	for result := range results {
		if result.err == nil {
			successes++
			s.Contains(result.metaLoc, "v1.metadata.json")

			continue
		}

		conflicts++
		s.ErrorIs(result.err, catalog.ErrTableAlreadyExists)
	}

	s.Equal(1, successes)
	s.Equal(1, conflicts)
	metaPath, err := s.cat.metadataFilePathForCompression(ident, 1, table.MetadataCompressionCodecNone)
	s.Require().NoError(err)
	s.FileExists(metaPath)
	s.NoFileExists(s.cat.metadataVersionClaimPath(ident, 1))
}

func (s *HadoopCatalogTestSuite) TestCreateTableConcurrentMixedCodecVersionClaim() {
	ctx := context.Background()
	s.Require().NoError(os.Mkdir(filepath.Join(s.warehouse, "ns"), 0o755))
	ident := []string{"ns", "tbl"}
	s.cat.filesystem = newBarrierRenameNoReplaceFS(filepath.Base(s.cat.metadataVersionClaimPath(ident, 1)))

	type createResult struct {
		metaLoc string
		err     error
	}

	results := make(chan createResult, 2)
	var wg sync.WaitGroup
	for _, props := range []iceberg.Properties{
		nil,
		{table.MetadataCompressionKey: table.MetadataCompressionCodecGzip},
	} {
		wg.Add(1)
		go func() {
			defer wg.Done()

			opts := []catalog.CreateTableOpt(nil)
			if props != nil {
				opts = append(opts, catalog.WithProperties(props))
			}

			tbl, err := s.cat.CreateTable(ctx, ident, s.testSchema(), opts...)
			result := createResult{err: err}
			if err == nil {
				result.metaLoc = tbl.MetadataLocation()
			}

			results <- result
		}()
	}

	wg.Wait()
	close(results)

	successes := 0
	conflicts := 0
	var successLoc string
	for result := range results {
		if result.err == nil {
			successes++
			successLoc = result.metaLoc
			s.Contains([]string{"v1.metadata.json", "v1.gz.metadata.json"}, filepath.Base(result.metaLoc))

			continue
		}

		conflicts++
		s.ErrorIs(result.err, catalog.ErrTableAlreadyExists)
	}

	s.Equal(1, successes)
	s.Equal(1, conflicts)

	plainPath, err := s.cat.metadataFilePathForCompression(ident, 1, table.MetadataCompressionCodecNone)
	s.Require().NoError(err)
	gzipPath, err := s.cat.metadataFilePathForCompression(ident, 1, table.MetadataCompressionCodecGzip)
	s.Require().NoError(err)

	existing := 0
	for _, path := range []string{plainPath, gzipPath} {
		if _, err := os.Stat(path); err == nil {
			existing++
		} else {
			s.ErrorIs(err, fs.ErrNotExist)
		}
	}

	s.Equal(1, existing)
	s.NoFileExists(s.cat.metadataVersionClaimPath(ident, 1))

	loaded, err := s.cat.LoadTable(ctx, ident)
	s.Require().NoError(err)
	s.Equal(successLoc, loaded.MetadataLocation())
}

func (s *HadoopCatalogTestSuite) TestCreateTableRecoversFromStaleVersionClaim() {
	ctx := context.Background()
	s.Require().NoError(os.Mkdir(filepath.Join(s.warehouse, "ns"), 0o755))
	ident := []string{"ns", "tbl"}

	claimPath := s.cat.metadataVersionClaimPath(ident, 1)
	s.Require().NoError(os.MkdirAll(filepath.Dir(claimPath), 0o755))
	s.Require().NoError(os.WriteFile(claimPath, []byte("stale claim"), 0o644))
	stale := time.Now().Add(-2 * metadataClaimStaleAfter)
	s.Require().NoError(os.Chtimes(claimPath, stale, stale))

	tbl, err := s.cat.CreateTable(ctx, ident, s.testSchema())
	s.Require().NoError(err)

	metaPath, err := s.cat.metadataFilePathForCompression(ident, 1, table.MetadataCompressionCodecNone)
	s.Require().NoError(err)
	s.Equal(metaPath, tbl.MetadataLocation())
	s.FileExists(metaPath)
	s.NoFileExists(claimPath)
}

func (s *HadoopCatalogTestSuite) TestCommitTableRecoversFromStaleVersionClaim() {
	ctx := context.Background()
	tbl := s.createTestTable("ns", "tbl")
	ident := []string{"ns", "tbl"}

	claimPath := s.cat.metadataVersionClaimPath(ident, 2)
	s.Require().NoError(os.MkdirAll(filepath.Dir(claimPath), 0o755))
	s.Require().NoError(os.WriteFile(claimPath, []byte("stale claim"), 0o644))
	stale := time.Now().Add(-2 * metadataClaimStaleAfter)
	s.Require().NoError(os.Chtimes(claimPath, stale, stale))

	meta, metaLoc, err := s.cat.CommitTable(
		ctx, ident,
		[]table.Requirement{
			table.AssertTableUUID(tbl.Metadata().TableUUID()),
		},
		[]table.Update{
			table.NewSetPropertiesUpdate(iceberg.Properties{"test.key": "value"}),
		},
	)
	s.Require().NoError(err)
	s.Equal("value", meta.Properties()["test.key"])

	s.Contains(metaLoc, "v2.metadata.json")
	s.FileExists(metaLoc)
	s.NoFileExists(claimPath)

	loaded, err := s.cat.LoadTable(ctx, ident)
	s.Require().NoError(err)
	s.Equal(metaLoc, loaded.MetadataLocation())
}

func (s *HadoopCatalogTestSuite) TestCommitMetadataFileFailsClosedOnMetadataScanError() {
	ident := []string{"ns", "tbl"}
	metaDir := s.cat.metadataDir(ident)
	s.Require().NoError(os.MkdirAll(metaDir, 0o755))

	tempPath := filepath.Join(metaDir, uuid.NewString()+".metadata.json")
	s.Require().NoError(os.WriteFile(tempPath, []byte("{}"), 0o644))

	metaPath, err := s.cat.metadataFilePathForCompression(ident, 1, table.MetadataCompressionCodecNone)
	s.Require().NoError(err)

	originalFS := s.cat.filesystem
	s.cat.filesystem = selectiveFailingWalkFS{
		HadoopCatalogFS: originalFS,
		failPath:        metaDir,
		err:             fs.ErrPermission,
	}
	defer func() {
		s.cat.filesystem = originalFS
	}()

	err = s.cat.commitMetadataFile(ident, 1, tempPath, metaPath, table.ErrCommitFailed)
	s.Require().Error(err)
	s.Contains(err.Error(), "failed to inspect metadata directory for version 1")
	s.ErrorIs(err, fs.ErrPermission)
	s.NotErrorIs(err, table.ErrCommitFailed)
	s.NoFileExists(metaPath)
	s.NoFileExists(s.cat.metadataVersionClaimPath(ident, 1))
}

func (s *HadoopCatalogTestSuite) TestCreateTableWithPartitionSpec() {
	ctx := context.Background()
	s.Require().NoError(os.Mkdir(filepath.Join(s.warehouse, "ns"), 0o755))

	spec := iceberg.NewPartitionSpec(
		iceberg.PartitionField{
			SourceIDs: []int{1},
			FieldID:   1000,
			Name:      "id_bucket",
			Transform: iceberg.BucketTransform{NumBuckets: 16},
		},
	)

	tbl, err := s.cat.CreateTable(ctx, []string{"ns", "tbl"}, s.testSchema(),
		catalog.WithPartitionSpec(&spec))
	s.Require().NoError(err)
	s.False(tbl.Spec().IsUnpartitioned())
}

func (s *HadoopCatalogTestSuite) TestCreateTableWithSortOrder() {
	ctx := context.Background()
	s.Require().NoError(os.Mkdir(filepath.Join(s.warehouse, "ns"), 0o755))

	sortOrder, err := table.NewSortOrder(1, []table.SortField{
		{
			SourceIDs: []int{1},
			Transform: iceberg.IdentityTransform{},
			Direction: table.SortASC,
			NullOrder: table.NullsFirst,
		},
	})
	s.Require().NoError(err)

	tbl, err := s.cat.CreateTable(ctx, []string{"ns", "tbl"}, s.testSchema(),
		catalog.WithSortOrder(sortOrder))
	s.Require().NoError(err)
	s.Greater(tbl.SortOrder().Len(), 0)
}

func (s *HadoopCatalogTestSuite) TestCreateTableWithProperties() {
	ctx := context.Background()
	s.Require().NoError(os.Mkdir(filepath.Join(s.warehouse, "ns"), 0o755))

	props := iceberg.Properties{
		"custom.key": "custom.value",
	}

	tbl, err := s.cat.CreateTable(ctx, []string{"ns", "tbl"}, s.testSchema(),
		catalog.WithProperties(props))
	s.Require().NoError(err)
	s.Equal("custom.value", tbl.Properties()["custom.key"])
}

func (s *HadoopCatalogTestSuite) TestCreateTableShortIdentifier() {
	ctx := context.Background()

	_, err := s.cat.CreateTable(ctx, []string{"tbl"}, s.testSchema())
	s.Require().Error(err)
	s.ErrorIs(err, catalog.ErrInvalidIdentifier)
	s.NotErrorIs(err, catalog.ErrNoSuchTable)
	s.Contains(err.Error(), "at least a namespace and table name")
}

func (s *HadoopCatalogTestSuite) tableIdentifierErrorOperations(ctx context.Context) []struct {
	name string
	run  func(table.Identifier) error
} {
	return []struct {
		name string
		run  func(table.Identifier) error
	}{
		{
			name: "CreateTable",
			run: func(ident table.Identifier) error {
				_, err := s.cat.CreateTable(ctx, ident, s.testSchema())

				return err
			},
		},
		{
			name: "LoadTable",
			run: func(ident table.Identifier) error {
				_, err := s.cat.LoadTable(ctx, ident)

				return err
			},
		},
		{
			name: "CommitTable",
			run: func(ident table.Identifier) error {
				_, _, err := s.cat.CommitTable(ctx, ident, nil, nil)

				return err
			},
		},
		{
			name: "DropTable",
			run: func(ident table.Identifier) error {
				return s.cat.DropTable(ctx, ident)
			},
		},
		{
			name: "PurgeTable",
			run: func(ident table.Identifier) error {
				return s.cat.PurgeTable(ctx, ident)
			},
		},
	}
}

func (s *HadoopCatalogTestSuite) TestTableOperationsRejectInvalidIdentifiers() {
	ctx := context.Background()
	tests := []struct {
		name  string
		ident table.Identifier
	}{
		{"empty namespace component", table.Identifier{"", "tbl"}},
		{"empty table name", table.Identifier{"ns", ""}},
		{"dot namespace component", table.Identifier{".", "tbl"}},
		{"dot table name", table.Identifier{"ns", "."}},
		{"dot dot namespace component", table.Identifier{"..", "tbl"}},
		{"dot dot table name", table.Identifier{"ns", ".."}},
		{"slash namespace component", table.Identifier{"a/b", "tbl"}},
		{"slash table name", table.Identifier{"ns", "a/b"}},
		{"backslash namespace component", table.Identifier{`a\b`, "tbl"}},
		{"backslash table name", table.Identifier{"ns", `a\b`}},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			for _, op := range s.tableIdentifierErrorOperations(ctx) {
				s.Run(op.name, func() {
					err := op.run(tt.ident)
					s.Require().Error(err)
					s.ErrorIs(err, catalog.ErrInvalidIdentifier)
					s.NotErrorIs(err, catalog.ErrNoSuchTable)
				})
			}
		})
	}
}

func (s *HadoopCatalogTestSuite) TestTableOperationsRejectParentTraversalIdentifier() {
	ctx := context.Background()
	outside, err := os.MkdirTemp(filepath.Dir(s.warehouse), "outside_table_")
	s.Require().NoError(err)
	defer os.RemoveAll(outside)

	outsideTable := filepath.Join(outside, "tbl")
	metaDir := filepath.Join(outsideTable, "metadata")
	marker := filepath.Join(outsideTable, "sentinel.txt")
	s.Require().NoError(os.MkdirAll(metaDir, 0o755))
	s.Require().NoError(os.WriteFile(filepath.Join(metaDir, "v1.metadata.json"), []byte("{}"), 0o644))
	s.Require().NoError(os.WriteFile(marker, []byte("keep"), 0o644))

	ident := table.Identifier{"..", filepath.Base(outside), "tbl"}
	for _, op := range s.tableIdentifierErrorOperations(ctx) {
		s.Run(op.name, func() {
			err := op.run(ident)
			s.Require().Error(err)
			s.ErrorIs(err, catalog.ErrInvalidIdentifier)
			s.NotErrorIs(err, catalog.ErrNoSuchTable)
			s.FileExists(marker)
			s.DirExists(metaDir)
		})
	}
}

func (s *HadoopCatalogTestSuite) TestDropTableNamespacePreserved() {
	// Dropping a table should not affect the parent namespace directory.
	ctx := context.Background()
	s.Require().NoError(os.Mkdir(filepath.Join(s.warehouse, "ns"), 0o755))
	s.createFakeTable([]string{"ns", "tbl"})

	s.Require().NoError(s.cat.DropTable(ctx, []string{"ns", "tbl"}))

	// Namespace dir should still exist.
	info, err := os.Stat(filepath.Join(s.warehouse, "ns"))
	s.Require().NoError(err)
	s.True(info.IsDir())
}

// RenameTable tests

func (s *HadoopCatalogTestSuite) TestRenameTableUnsupported() {
	_, err := s.cat.RenameTable(context.Background(), []string{"ns", "old"}, []string{"ns", "new"})
	s.Require().Error(err)
	s.Contains(err.Error(), "not supported")
}

func (s *HadoopCatalogTestSuite) TestListTablesNamespaceIsFile() {
	ctx := context.Background()
	s.Require().NoError(os.WriteFile(filepath.Join(s.warehouse, "not_a_ns"), nil, 0o644))

	for _, err := range s.cat.ListTables(ctx, []string{"not_a_ns"}) {
		s.ErrorIs(err, catalog.ErrNoSuchNamespace)

		break
	}
}

func (s *HadoopCatalogTestSuite) TestListTablesIdentifierIsolation() {
	// Verify that yielded identifiers don't alias the namespace slice.
	ctx := context.Background()
	s.Require().NoError(os.Mkdir(filepath.Join(s.warehouse, "ns"), 0o755))
	s.createFakeTable([]string{"ns", "tbl1"})
	s.createFakeTable([]string{"ns", "tbl2"})

	var tables []table.Identifier
	for ident, err := range s.cat.ListTables(ctx, []string{"ns"}) {
		s.Require().NoError(err)
		tables = append(tables, ident)
	}

	s.Len(tables, 2)
	// Each identifier should be independent — no aliasing.
	s.NotEqual(tables[0][len(tables[0])-1], tables[1][len(tables[1])-1])
}

// LoadTable tests

func (s *HadoopCatalogTestSuite) TestLoadTableNotExists() {
	ctx := context.Background()

	_, err := s.cat.LoadTable(ctx, []string{"ns", "tbl"})
	s.Require().Error(err)
	s.ErrorIs(err, catalog.ErrNoSuchTable)
}

func (s *HadoopCatalogTestSuite) TestLoadTablePropagatesMetadataReadError() {
	ctx := context.Background()
	walkErr := errors.New("metadata walk failed")
	originalFS := s.cat.filesystem
	s.cat.filesystem = failingWalkFS{HadoopCatalogFS: originalFS, err: walkErr}
	defer func() {
		s.cat.filesystem = originalFS
	}()

	_, err := s.cat.LoadTable(ctx, []string{"ns", "tbl"})
	s.Require().Error(err)
	s.ErrorIs(err, walkErr)
	s.NotErrorIs(err, catalog.ErrNoSuchTable)
}

func (s *HadoopCatalogTestSuite) TestLoadTableStaleHint() {
	ctx := context.Background()
	s.Require().NoError(os.Mkdir(filepath.Join(s.warehouse, "ns"), 0o755))

	// Create table (version 1)
	_, err := s.cat.CreateTable(ctx, []string{"ns", "tbl"}, s.testSchema())
	s.Require().NoError(err)

	// Manually set the hint to a stale value
	ident := []string{"ns", "tbl"}
	s.cat.writeVersionHint(ident, 99)

	// LoadTable should still succeed by falling back to dir listing
	tbl, err := s.cat.LoadTable(ctx, ident)
	s.Require().NoError(err)
	s.NotNil(tbl)
}

func (s *HadoopCatalogTestSuite) TestLoadTableGzipMetadata() {
	ctx := context.Background()
	ident := []string{"ns", "tbl"}
	s.Require().NoError(os.Mkdir(filepath.Join(s.warehouse, "ns"), 0o755))

	created, err := s.cat.CreateTable(ctx, ident, s.testSchema())
	s.Require().NoError(err)
	gzPath := s.replaceMetadataWithGzip(ident, 1)

	loaded, err := s.cat.LoadTable(ctx, ident)
	s.Require().NoError(err)
	s.Equal(created.Metadata().TableUUID(), loaded.Metadata().TableUUID())
	s.Equal(gzPath, loaded.MetadataLocation())

	exists, err := s.cat.CheckTableExists(ctx, ident)
	s.Require().NoError(err)
	s.True(exists)
}

func (s *HadoopCatalogTestSuite) TestLoadTableUUIDMetadata() {
	ctx := context.Background()
	ident := []string{"ns", "tbl"}
	s.Require().NoError(os.Mkdir(filepath.Join(s.warehouse, "ns"), 0o755))

	created, err := s.cat.CreateTable(ctx, ident, s.testSchema())
	s.Require().NoError(err)
	uuidPath := s.replaceMetadataWithUUIDName(ident, 1)

	loaded, err := s.cat.LoadTable(ctx, ident)
	s.Require().NoError(err)
	s.Equal(created.Metadata().TableUUID(), loaded.Metadata().TableUUID())
	s.Equal(uuidPath, loaded.MetadataLocation())

	exists, err := s.cat.CheckTableExists(ctx, ident)
	s.Require().NoError(err)
	s.True(exists)
}

func (s *HadoopCatalogTestSuite) TestLoadTableUUIDMetadataVersionZero() {
	ctx := context.Background()
	ident := []string{"ns", "tbl"}
	s.Require().NoError(os.Mkdir(filepath.Join(s.warehouse, "ns"), 0o755))

	created, err := s.cat.CreateTable(ctx, ident, s.testSchema())
	s.Require().NoError(err)
	uuidPath := s.replaceMetadataWithUUIDSequence(ident, 1, 0)

	loaded, err := s.cat.LoadTable(ctx, ident)
	s.Require().NoError(err)
	s.Equal(created.Metadata().TableUUID(), loaded.Metadata().TableUUID())
	s.Equal(uuidPath, loaded.MetadataLocation())

	exists, err := s.cat.CheckTableExists(ctx, ident)
	s.Require().NoError(err)
	s.True(exists)
}

func (s *HadoopCatalogTestSuite) TestVersionHintWithoutMetadataIsNotLoadableTable() {
	ctx := context.Background()
	ident := []string{"ns", "tbl"}
	metaDir := s.cat.metadataDir(ident)
	s.Require().NoError(os.MkdirAll(metaDir, 0o755))
	s.cat.writeVersionHint(ident, 1)

	exists, err := s.cat.CheckTableExists(ctx, ident)
	s.Require().NoError(err)
	s.False(exists)

	var listed []table.Identifier
	for tblIdent, err := range s.cat.ListTables(ctx, []string{"ns"}) {
		s.Require().NoError(err)
		listed = append(listed, tblIdent)
	}
	s.Empty(listed)

	_, err = s.cat.LoadTable(ctx, ident)
	s.Require().Error(err)
	s.ErrorIs(err, catalog.ErrNoSuchTable)
}

func (s *HadoopCatalogTestSuite) TestLoadTableShortIdentifier() {
	ctx := context.Background()

	_, err := s.cat.LoadTable(ctx, []string{"tbl"})
	s.Require().Error(err)
	s.ErrorIs(err, catalog.ErrInvalidIdentifier)
	s.NotErrorIs(err, catalog.ErrNoSuchTable)
	s.Contains(err.Error(), "at least a namespace and table name")
}

// CheckTableExists tests

func (s *HadoopCatalogTestSuite) TestCheckTableExistsTrue() {
	ctx := context.Background()
	s.Require().NoError(os.Mkdir(filepath.Join(s.warehouse, "ns"), 0o755))

	_, err := s.cat.CreateTable(ctx, []string{"ns", "tbl"}, s.testSchema())
	s.Require().NoError(err)

	exists, err := s.cat.CheckTableExists(ctx, []string{"ns", "tbl"})
	s.Require().NoError(err)
	s.True(exists)
}

func (s *HadoopCatalogTestSuite) TestCheckTableExistsFalse() {
	exists, err := s.cat.CheckTableExists(context.Background(), []string{"ns", "tbl"})
	s.Require().NoError(err)
	s.False(exists)
}

func (s *HadoopCatalogTestSuite) TestCheckTableExistsPropagatesMetadataReadError() {
	walkErr := errors.New("metadata walk failed")
	originalFS := s.cat.filesystem
	s.cat.filesystem = failingWalkFS{HadoopCatalogFS: originalFS, err: walkErr}
	defer func() {
		s.cat.filesystem = originalFS
	}()

	exists, err := s.cat.CheckTableExists(context.Background(), []string{"ns", "tbl"})
	s.False(exists)
	s.Require().Error(err)
	s.ErrorIs(err, walkErr)
	s.NotErrorIs(err, catalog.ErrNoSuchTable)
}

func (s *HadoopCatalogTestSuite) TestCheckTableExistsShortIdentifier() {
	exists, err := s.cat.CheckTableExists(context.Background(), []string{"tbl"})
	s.Require().Error(err)
	s.Require().ErrorIs(err, catalog.ErrInvalidIdentifier)
	s.NotErrorIs(err, catalog.ErrNoSuchTable)
	s.False(exists)
}

func (s *HadoopCatalogTestSuite) TestCheckTableExistsInvalidIdentifierFalse() {
	tests := []struct {
		name  string
		ident table.Identifier
	}{
		{"empty namespace component", table.Identifier{"", "tbl"}},
		{"empty table name", table.Identifier{"ns", ""}},
		{"dot namespace component", table.Identifier{".", "tbl"}},
		{"dot table name", table.Identifier{"ns", "."}},
		{"dot dot namespace component", table.Identifier{"..", "tbl"}},
		{"dot dot table name", table.Identifier{"ns", ".."}},
		{"slash namespace component", table.Identifier{"a/b", "tbl"}},
		{"slash table name", table.Identifier{"ns", "a/b"}},
		{"backslash namespace component", table.Identifier{`a\b`, "tbl"}},
		{"backslash table name", table.Identifier{"ns", `a\b`}},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			exists, err := s.cat.CheckTableExists(context.Background(), tt.ident)
			s.Require().Error(err)
			s.Require().ErrorIs(err, catalog.ErrInvalidIdentifier)
			s.NotErrorIs(err, catalog.ErrNoSuchTable)
			s.False(exists)
		})
	}
}

func (s *HadoopCatalogTestSuite) TestCreateTableNestedNamespace() {
	ctx := context.Background()
	s.Require().NoError(os.MkdirAll(filepath.Join(s.warehouse, "a", "b"), 0o755))

	tbl, err := s.cat.CreateTable(ctx, []string{"a", "b", "tbl"}, s.testSchema())
	s.Require().NoError(err)
	s.Equal(filepath.Join(s.warehouse, "a", "b", "tbl"), tbl.Location())

	// Verify round-trip
	loaded, err := s.cat.LoadTable(ctx, []string{"a", "b", "tbl"})
	s.Require().NoError(err)
	s.Equal(tbl.Metadata().TableUUID(), loaded.Metadata().TableUUID())
}

func (s *HadoopCatalogTestSuite) TestCreateTableMetadataFormatVersion() {
	ctx := context.Background()
	s.Require().NoError(os.Mkdir(filepath.Join(s.warehouse, "ns"), 0o755))

	tbl, err := s.cat.CreateTable(ctx, []string{"ns", "tbl"}, s.testSchema())
	s.Require().NoError(err)

	// Default format version should be V2
	s.Equal(2, tbl.Metadata().Version())
}

// CommitTable tests

func (s *HadoopCatalogTestSuite) createTestTable(ns, name string) *table.Table {
	ctx := context.Background()
	s.Require().NoError(os.MkdirAll(filepath.Join(s.warehouse, ns), 0o755))

	tbl, err := s.cat.CreateTable(ctx, []string{ns, name}, s.testSchema())
	s.Require().NoError(err)

	return tbl
}

func (s *HadoopCatalogTestSuite) TestCommitTableSingleUpdate() {
	ctx := context.Background()
	tbl := s.createTestTable("ns", "tbl")

	meta, metaLoc, err := s.cat.CommitTable(
		ctx, []string{"ns", "tbl"},
		[]table.Requirement{
			table.AssertTableUUID(tbl.Metadata().TableUUID()),
		},
		[]table.Update{
			table.NewSetPropertiesUpdate(iceberg.Properties{"test.key": "test.value"}),
		},
	)
	s.Require().NoError(err)

	// Version should have incremented to 2.
	s.Contains(metaLoc, "v2.metadata.json")
	s.Equal("test.value", meta.Properties()["test.key"])

	// File should exist on disk.
	s.FileExists(metaLoc)
}

func (s *HadoopCatalogTestSuite) TestCommitTableGzipMetadata() {
	ctx := context.Background()
	s.Require().NoError(os.Mkdir(filepath.Join(s.warehouse, "ns"), 0o755))

	props := iceberg.Properties{
		table.MetadataCompressionKey: table.MetadataCompressionCodecGzip,
	}

	tbl, err := s.cat.CreateTable(ctx, []string{"ns", "tbl"}, s.testSchema(),
		catalog.WithProperties(props))
	s.Require().NoError(err)

	_, metaLoc, err := s.cat.CommitTable(
		ctx, []string{"ns", "tbl"},
		[]table.Requirement{
			table.AssertTableUUID(tbl.Metadata().TableUUID()),
		},
		[]table.Update{
			table.NewSetPropertiesUpdate(iceberg.Properties{"test.key": "test.value"}),
		},
	)
	s.Require().NoError(err)
	s.Equal(filepath.Join(s.warehouse, "ns", "tbl", "metadata", "v2.gz.metadata.json"), metaLoc)
	s.FileExists(metaLoc)

	loaded, err := s.cat.LoadTable(ctx, []string{"ns", "tbl"})
	s.Require().NoError(err)
	s.Equal(metaLoc, loaded.MetadataLocation())
	s.Equal("test.value", loaded.Properties()["test.key"])
}

func (s *HadoopCatalogTestSuite) TestCommitTableDetectsCrossCodecVersionConflict() {
	ctx := context.Background()
	tbl := s.createTestTable("ns", "tbl")
	ident := []string{"ns", "tbl"}
	conflictPath := filepath.Join(s.cat.metadataDir(ident), "v2.gz.metadata.json")
	s.cat.filesystem = &versionConflictOnCreateFS{conflictPath: conflictPath}

	_, _, err := s.cat.CommitTable(
		ctx, ident,
		[]table.Requirement{
			table.AssertTableUUID(tbl.Metadata().TableUUID()),
		},
		[]table.Update{
			table.NewSetPropertiesUpdate(iceberg.Properties{"test.key": "test.value"}),
		},
	)
	s.Require().ErrorIs(err, table.ErrCommitFailed)
	s.Contains(err.Error(), conflictPath)
}

func (s *HadoopCatalogTestSuite) TestCommitTableRejectsZstdMetadata() {
	ctx := context.Background()
	s.Require().NoError(os.Mkdir(filepath.Join(s.warehouse, "ns"), 0o755))

	tbl, err := s.cat.CreateTable(ctx, []string{"ns", "tbl"}, s.testSchema())
	s.Require().NoError(err)

	_, _, err = s.cat.CommitTable(
		ctx, []string{"ns", "tbl"},
		[]table.Requirement{
			table.AssertTableUUID(tbl.Metadata().TableUUID()),
		},
		[]table.Update{
			table.NewSetPropertiesUpdate(iceberg.Properties{
				table.MetadataCompressionKey: table.MetadataCompressionCodecZstd,
			}),
		},
	)
	s.Require().Error(err)
	s.Contains(err.Error(), "unsupported write metadata compression codec")
}

func (s *HadoopCatalogTestSuite) TestCommitTableMultipleSequential() {
	ctx := context.Background()
	tbl := s.createTestTable("ns", "tbl")
	ident := []string{"ns", "tbl"}

	for i := 0; i < 3; i++ {
		loaded, err := s.cat.LoadTable(ctx, ident)
		s.Require().NoError(err)

		_, metaLoc, err := s.cat.CommitTable(
			ctx, ident,
			[]table.Requirement{
				table.AssertTableUUID(loaded.Metadata().TableUUID()),
			},
			[]table.Update{
				table.NewSetPropertiesUpdate(iceberg.Properties{
					fmt.Sprintf("iter.%d", i): "val",
				}),
			},
		)
		s.Require().NoError(err)
		s.Contains(metaLoc, fmt.Sprintf("v%d.metadata.json", i+2))
	}

	// Final load should have all properties.
	final, err := s.cat.LoadTable(ctx, ident)
	s.Require().NoError(err)
	s.Equal("val", final.Properties()["iter.0"])
	s.Equal("val", final.Properties()["iter.1"])
	s.Equal("val", final.Properties()["iter.2"])

	// Version hint should reflect the latest version.
	_ = tbl // suppress unused
	s.Equal(4, s.cat.readVersionHint(ident))
}

func (s *HadoopCatalogTestSuite) TestCommitTableNoChanges() {
	ctx := context.Background()
	tbl := s.createTestTable("ns", "tbl")

	meta, metaLoc, err := s.cat.CommitTable(
		ctx, []string{"ns", "tbl"},
		[]table.Requirement{
			table.AssertTableUUID(tbl.Metadata().TableUUID()),
		},
		nil, // no updates
	)
	s.Require().NoError(err)

	// Even with no updates, UpdateTableMetadata produces a new metadata
	// object (different timestamp), so a new version is written.
	s.Contains(metaLoc, "v2.metadata.json")
	s.Equal(tbl.Metadata().TableUUID(), meta.TableUUID())
}

func (s *HadoopCatalogTestSuite) TestCommitTableConflictDetection() {
	ctx := context.Background()
	s.createTestTable("ns", "tbl")
	ident := []string{"ns", "tbl"}

	for i := 2; i <= 5; i++ {
		_, metaLoc, err := s.cat.CommitTable(
			ctx, ident,
			nil,
			[]table.Update{
				table.NewSetPropertiesUpdate(iceberg.Properties{
					fmt.Sprintf("v%d", i): "committed",
				}),
			},
		)
		s.Require().NoError(err)
		s.Contains(metaLoc, fmt.Sprintf("v%d.metadata.json", i))
		expectedPath, pathErr := s.cat.metadataFilePathForCompression(ident, i, table.MetadataCompressionCodecNone)
		s.Require().NoError(pathErr)
		s.FileExists(expectedPath)
	}
}

func (s *HadoopCatalogTestSuite) TestCommitTableConcurrentMetadataPublishConflict() {
	ctx := context.Background()
	s.createTestTable("ns", "tbl")
	ident := []string{"ns", "tbl"}
	s.cat.filesystem = newBarrierRenameNoReplaceFS(filepath.Base(s.cat.metadataVersionClaimPath(ident, 2)))

	type commitResult struct {
		metaLoc string
		err     error
	}

	results := make(chan commitResult, 2)
	var wg sync.WaitGroup

	for i := range 2 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			_, metaLoc, err := s.cat.CommitTable(
				ctx, ident,
				nil,
				[]table.Update{
					table.NewSetPropertiesUpdate(iceberg.Properties{
						fmt.Sprintf("writer.%d", i): "committed",
					}),
				},
			)
			results <- commitResult{metaLoc: metaLoc, err: err}
		}(i)
	}

	wg.Wait()
	close(results)

	successes := 0
	conflicts := 0
	for result := range results {
		if result.err == nil {
			successes++
			s.Contains(result.metaLoc, "v2.metadata.json")

			continue
		}

		conflicts++
		s.ErrorIs(result.err, table.ErrCommitFailed)
	}

	s.Equal(1, successes)
	s.Equal(1, conflicts)
	metaPath, err := s.cat.metadataFilePathForCompression(ident, 2, table.MetadataCompressionCodecNone)
	s.Require().NoError(err)
	s.FileExists(metaPath)
	s.Equal(2, s.cat.readVersionHint(ident))
	s.NoFileExists(s.cat.metadataVersionClaimPath(ident, 2))
}

func (s *HadoopCatalogTestSuite) TestCommitTableConcurrentMixedCodecVersionClaim() {
	ctx := context.Background()
	s.createTestTable("ns", "tbl")
	ident := []string{"ns", "tbl"}
	s.cat.filesystem = newBarrierRenameNoReplaceFS(filepath.Base(s.cat.metadataVersionClaimPath(ident, 2)))

	type commitResult struct {
		metaLoc string
		err     error
	}

	results := make(chan commitResult, 2)
	var wg sync.WaitGroup
	for _, props := range []iceberg.Properties{
		{"writer.default": "committed"},
		{
			"writer.gzip":                "committed",
			table.MetadataCompressionKey: table.MetadataCompressionCodecGzip,
		},
	} {
		wg.Add(1)
		go func() {
			defer wg.Done()

			_, metaLoc, err := s.cat.CommitTable(
				ctx, ident,
				nil,
				[]table.Update{table.NewSetPropertiesUpdate(props)},
			)
			results <- commitResult{metaLoc: metaLoc, err: err}
		}()
	}

	wg.Wait()
	close(results)

	successes := 0
	conflicts := 0
	var successLoc string
	for result := range results {
		if result.err == nil {
			successes++
			successLoc = result.metaLoc
			s.Contains([]string{"v2.metadata.json", "v2.gz.metadata.json"}, filepath.Base(result.metaLoc))

			continue
		}

		conflicts++
		s.ErrorIs(result.err, table.ErrCommitFailed)
	}

	s.Equal(1, successes)
	s.Equal(1, conflicts)

	plainPath, err := s.cat.metadataFilePathForCompression(ident, 2, table.MetadataCompressionCodecNone)
	s.Require().NoError(err)
	gzipPath, err := s.cat.metadataFilePathForCompression(ident, 2, table.MetadataCompressionCodecGzip)
	s.Require().NoError(err)

	existing := 0
	for _, path := range []string{plainPath, gzipPath} {
		if _, err := os.Stat(path); err == nil {
			existing++
		} else {
			s.ErrorIs(err, fs.ErrNotExist)
		}
	}

	s.Equal(1, existing)
	s.Equal(2, s.cat.readVersionHint(ident))
	s.NoFileExists(s.cat.metadataVersionClaimPath(ident, 2))

	loaded, err := s.cat.LoadTable(ctx, ident)
	s.Require().NoError(err)
	s.Equal(successLoc, loaded.MetadataLocation())
}

func (s *HadoopCatalogTestSuite) TestCommitTableConflictDetectionRecognizesConcurrentNextVersionMetadata() {
	ctx := context.Background()

	tests := []struct {
		name     string
		table    string
		filename string
	}{
		{
			name:     "uuid metadata",
			table:    "uuid_conflict",
			filename: "00002-a1b2c3d4-e5f6-7890-abcd-ef1234567890.metadata.json",
		},
		{
			name:     "gzip metadata",
			table:    "gzip_conflict",
			filename: "v2.gz.metadata.json",
		},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			s.createTestTable("ns", tt.table)
			ident := []string{"ns", tt.table}
			metaDir := s.cat.metadataDir(ident)
			conflictPath := filepath.Join(metaDir, tt.filename)

			originalFS := s.cat.filesystem
			s.cat.filesystem = &stagedWalkDirFS{
				HadoopCatalogFS: originalFS,
				targetPath:      metaDir,
				stage: func() error {
					return os.WriteFile(conflictPath, nil, 0o644)
				},
			}
			defer func() {
				s.cat.filesystem = originalFS
			}()

			_, _, err := s.cat.CommitTable(
				ctx, ident,
				nil,
				[]table.Update{
					table.NewSetPropertiesUpdate(iceberg.Properties{
						"writer": tt.name,
					}),
				},
			)
			s.Require().Error(err)
			s.Contains(err.Error(), "already exists")
			s.FileExists(conflictPath)
			plainPath, err := s.cat.metadataFilePathForCompression(ident, 2, table.MetadataCompressionCodecNone)
			s.Require().NoError(err)
			s.NoFileExists(plainPath)
		})
	}
}

func (s *HadoopCatalogTestSuite) TestCommitTableRequirementFailure() {
	ctx := context.Background()
	s.createTestTable("ns", "tbl")

	wrongUUID := uuid.New()
	_, _, err := s.cat.CommitTable(
		ctx, []string{"ns", "tbl"},
		[]table.Requirement{
			table.AssertTableUUID(wrongUUID),
		},
		[]table.Update{
			table.NewSetPropertiesUpdate(iceberg.Properties{"k": "v"}),
		},
	)
	s.Require().Error(err)
	s.Contains(err.Error(), "UUID")
}

func (s *HadoopCatalogTestSuite) TestCommitTableLocationChange() {
	ctx := context.Background()
	s.createTestTable("ns", "tbl")

	_, _, err := s.cat.CommitTable(
		ctx, []string{"ns", "tbl"},
		nil,
		[]table.Update{
			table.NewSetLocationUpdate("/some/other/location"),
		},
	)
	s.Require().Error(err)
	s.Contains(err.Error(), "location cannot be changed")
}

func (s *HadoopCatalogTestSuite) TestCommitTableWriteMetadataLocation() {
	ctx := context.Background()
	s.createTestTable("ns", "tbl")

	_, _, err := s.cat.CommitTable(
		ctx, []string{"ns", "tbl"},
		nil,
		[]table.Update{
			table.NewSetPropertiesUpdate(iceberg.Properties{
				"write.metadata.location": "/custom/metadata/path",
			}),
		},
	)
	s.Require().Error(err)
	s.Contains(err.Error(), "write.metadata.location")
}

func (s *HadoopCatalogTestSuite) TestCommitTableNoOrphanedTempFiles() {
	ctx := context.Background()
	s.createTestTable("ns", "tbl")
	ident := []string{"ns", "tbl"}

	// Do several commits and verify no temp files are left behind.
	for i := 0; i < 3; i++ {
		_, _, err := s.cat.CommitTable(
			ctx, ident,
			nil,
			[]table.Update{
				table.NewSetPropertiesUpdate(iceberg.Properties{
					fmt.Sprintf("iter.%d", i): "val",
				}),
			},
		)
		s.Require().NoError(err)
	}

	metaDir := s.cat.metadataDir(ident)
	entries, err := os.ReadDir(metaDir)
	s.Require().NoError(err)

	for _, e := range entries {
		if !e.IsDir() && !versionPattern.MatchString(e.Name()) && e.Name() != "version-hint.text" {
			s.Failf("orphaned temp file", "found orphaned file: %s", e.Name())
		}
	}
}

func (s *HadoopCatalogTestSuite) TestCommitTableVersionHintUpdated() {
	ctx := context.Background()
	s.createTestTable("ns", "tbl")
	ident := []string{"ns", "tbl"}

	_, _, err := s.cat.CommitTable(
		ctx, ident,
		nil,
		[]table.Update{
			table.NewSetPropertiesUpdate(iceberg.Properties{"k": "v"}),
		},
	)
	s.Require().NoError(err)

	s.Equal(2, s.cat.readVersionHint(ident))
}

func (s *HadoopCatalogTestSuite) TestCommitTableCreateViaCommit() {
	ctx := context.Background()
	s.Require().NoError(os.Mkdir(filepath.Join(s.warehouse, "ns"), 0o755))
	ident := []string{"ns", "tbl"}

	loc := s.cat.defaultTableLocation(ident)
	meta, metaLoc, err := s.cat.CommitTable(
		ctx, ident,
		[]table.Requirement{
			table.AssertCreate(),
		},
		[]table.Update{
			table.NewAssignUUIDUpdate(uuid.New()),
			table.NewUpgradeFormatVersionUpdate(2),
			table.NewAddSchemaUpdate(s.testSchema()),
			table.NewSetCurrentSchemaUpdate(-1),
			table.NewSetLocationUpdate(loc),
		},
	)
	s.Require().NoError(err)
	s.Contains(metaLoc, "v1.metadata.json")
	s.Equal(loc, meta.Location())

	// Should be loadable.
	loaded, err := s.cat.LoadTable(ctx, ident)
	s.Require().NoError(err)
	s.Equal(meta.TableUUID(), loaded.Metadata().TableUUID())
}

func (s *HadoopCatalogTestSuite) TestCommitTableShortIdentifier() {
	_, _, err := s.cat.CommitTable(context.Background(), []string{"tbl"}, nil, nil)
	s.Require().Error(err)
	s.ErrorIs(err, catalog.ErrInvalidIdentifier)
	s.NotErrorIs(err, catalog.ErrNoSuchTable)
	s.Contains(err.Error(), "at least a namespace and table name")
}

func (s *HadoopCatalogTestSuite) TestJoinPathLocal() {
	tests := []struct {
		name  string
		base  string
		parts []string
		want  string
	}{
		{
			name:  "base only",
			base:  "/tmp/wh/",
			parts: nil,
			want:  filepath.Clean("/tmp/wh/"),
		},
		{
			name:  "namespace and table",
			base:  "/tmp/wh",
			parts: []string{"ns", "tbl"},
			want:  filepath.Join("/tmp/wh", "ns", "tbl"),
		},
		{
			name:  "cleans local path components",
			base:  "/tmp/wh",
			parts: []string{"ns", ".", "tbl", "metadata"},
			want:  filepath.Join("/tmp/wh", "ns", "tbl", "metadata"),
		},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			s.Equal(tt.want, joinPath(true, tt.base, tt.parts...))
		})
	}
}

func (s *HadoopCatalogTestSuite) TestJoinPathRemotePreservesSchemeAuthority() {
	cases := []struct {
		name  string
		base  string
		parts []string
		want  string
	}{
		{
			name:  "base only trims trailing slash",
			base:  "s3://bucket/wh/",
			parts: nil,
			want:  "s3://bucket/wh",
		},
		{
			name:  "namespace and table",
			base:  "s3://bucket/wh/",
			parts: []string{"ns", "tbl"},
			want:  "s3://bucket/wh/ns/tbl",
		},
		{
			name:  "metadata file",
			base:  "s3://bucket/wh",
			parts: []string{"ns", "tbl", "metadata", "v1.metadata.json"},
			want:  "s3://bucket/wh/ns/tbl/metadata/v1.metadata.json",
		},
		{
			name:  "cleans remote path components after authority",
			base:  "s3://bucket/wh/",
			parts: []string{"ns", ".", "tbl", "metadata"},
			want:  "s3://bucket/wh/ns/tbl/metadata",
		},
		{
			name:  "bucket root",
			base:  "s3://bucket/",
			parts: []string{"ns"},
			want:  "s3://bucket/ns",
		},
	}

	for _, tt := range cases {
		s.Run(tt.name, func() {
			s.Equal(tt.want, joinPath(false, tt.base, tt.parts...))
		})
	}
}
