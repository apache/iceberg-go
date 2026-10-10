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

package table_test

import (
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLocationProviderLocalPaths(t *testing.T) {
	for _, objectStore := range []string{"false", "true"} {
		for _, partitionPaths := range []string{"false", "true"} {
			for _, name := range []string{"my table", "literal%20table", "100% complete", "table#fragment", "table?query"} {
				t.Run(objectStore+"/"+partitionPaths+"/"+name, func(t *testing.T) {
					if strings.Contains(name, "?") && os.PathSeparator == '\\' {
						t.Skip("question marks are not valid in Windows filenames")
					}
					root := filepath.Join(t.TempDir(), name)
					for _, custom := range []bool{false, true} {
						props := iceberg.Properties{
							table.ObjectStoreEnabledKey:               objectStore,
							table.WriteObjectStorePartitionedPathsKey: partitionPaths,
						}
						dataDir, metadataDir := filepath.Join(root, "data"), filepath.Join(root, "metadata")
						if custom {
							dataDir, metadataDir = filepath.Join(root, "custom data"), filepath.Join(root, "custom metadata")
							props[table.WriteDataPathKey] = dataDir
							props[table.WriteMetadataPathKey] = metadataDir
						}
						provider, err := table.LoadLocationProvider(root, props)
						require.NoError(t, err)
						metadataFile, err := provider.NewTableMetadataFileLocation(1)
						require.NoError(t, err)
						for _, file := range []struct{ location, dir string }{
							{provider.NewDataLocation("data.parquet"), dataDir},
							{provider.NewDataLocation("partition=value/data.parquet"), dataDir},
							{provider.NewMetadataLocation("manifest.avro"), metadataDir},
							{metadataFile, metadataDir},
						} {
							require.True(t, strings.HasPrefix(file.location, file.dir+string(os.PathSeparator)), "%q is outside %q", file.location, file.dir)
							writer, err := (io.LocalFS{}).Create(file.location)
							require.NoError(t, err)
							require.NoError(t, writer.Close())
							_, err = os.Stat(file.location)
							require.NoError(t, err)
						}
					}
				})
			}
		}
	}
}

func TestLocationProviderEscapedURIs(t *testing.T) {
	for _, scheme := range []string{"file", "s3", "gs", "az"} {
		t.Run(scheme, func(t *testing.T) {
			location := (&url.URL{Scheme: scheme, Host: "bucket", Path: "/my table/literal%20#?"}).String()
			provider, err := table.LoadLocationProvider(location, nil)
			require.NoError(t, err)
			assert.Equal(t, location+"/data/data.parquet", provider.NewDataLocation("data.parquet"))
			assert.Equal(t, location+"/metadata/manifest.avro", provider.NewMetadataLocation("manifest.avro"))
		})
	}
}

func TestLocationProviderRelativePaths(t *testing.T) {
	for _, location := range []string{"my table", "100% complete", "literal%20table", "table#fragment", "table?query", "dir/table:part%20"} {
		t.Run(location, func(t *testing.T) {
			provider, err := table.LoadLocationProvider(location, nil)
			require.NoError(t, err)
			assert.Equal(t, filepath.Join(location, "data", "data.parquet"), provider.NewDataLocation("data.parquet"))
			assert.Equal(t, filepath.Join(location, "metadata", "manifest.avro"), provider.NewMetadataLocation("manifest.avro"))
		})
	}
}

func TestLocationProviderMetadataDefaultLocation(t *testing.T) {
	provider, err := table.LoadLocationProvider("table_location", nil)
	require.NoError(t, err)
	assert.Equal(t, "table_location/metadata/manifest.avro", provider.NewMetadataLocation("manifest.avro"))
}

func TestLocationProviderMetadataLocationCustomPath(t *testing.T) {
	provider, err := table.LoadLocationProvider("table_location",
		iceberg.Properties{table.WriteMetadataPathKey: "s3://table-location/custom/path"})
	require.NoError(t, err)

	assert.Equal(t, "s3://table-location/custom/path/metadata.json", provider.NewMetadataLocation("metadata.json"))
}

func TestLocationProviderMetadataLocationTrailingSlash(t *testing.T) {
	provider, err := table.LoadLocationProvider("table_location",
		iceberg.Properties{table.WriteMetadataPathKey: "s3://table-location/custom/path/"})
	require.NoError(t, err)

	assert.Equal(t, "s3://table-location/custom/path/metadata.json", provider.NewMetadataLocation("metadata.json"))
}

func TestLocationProviderMetadataFileLocation(t *testing.T) {
	uuid.SetRand(strings.NewReader("0123456789abcdefghijkl"))
	defer uuid.SetRand(nil)

	provider, err := table.LoadLocationProvider("table_location", nil)
	require.NoError(t, err)

	loc, err := provider.NewTableMetadataFileLocation(1)
	require.NoError(t, err)
	assert.Equal(t, "table_location/metadata/00001-30313233-3435-4637-b839-616263646566.metadata.json", loc)
}

func TestLocationProviderMetadataFileLocationCustomPath(t *testing.T) {
	uuid.SetRand(strings.NewReader("0123456789abcdefghijkl"))
	defer uuid.SetRand(nil)

	provider, err := table.LoadLocationProvider("table_location",
		iceberg.Properties{table.WriteMetadataPathKey: "s3://table-location/custom/path/"})
	require.NoError(t, err)

	loc, err := provider.NewTableMetadataFileLocation(1)
	require.NoError(t, err)
	assert.Equal(t, "s3://table-location/custom/path/00001-30313233-3435-4637-b839-616263646566.metadata.json", loc)
}

func TestObjectStoreLocationProvider(t *testing.T) {
	provider, err := table.LoadLocationProvider("table_location",
		iceberg.Properties{table.ObjectStoreEnabledKey: "true"})
	require.NoError(t, err)

	assert.Equal(t, "table_location/data/0101/0110/1001/10110010/a", provider.NewDataLocation("a"))
	assert.Equal(t, "table_location/data/1110/0111/1110/00000011/b", provider.NewDataLocation("b"))
	assert.Equal(t, "table_location/data/0010/1101/0110/01011111/c", provider.NewDataLocation("c"))
	assert.Equal(t, "table_location/data/1001/0001/0100/01110011/d", provider.NewDataLocation("d"))
	assert.Equal(t, "table_location/data/0000/0000/0000/00000000/part-00164929.parquet",
		provider.NewDataLocation("part-00164929.parquet"))
}

// TestObjectStoreLocationProviderPartitionedPathsDisabled tests that when partitioned paths are disabled,
// the final "/" is still replaced with "-" even for un-partitioned files. This matches the behavior of
// the Java implementation.
func TestObjectStoreLocationProviderPartitionedPathsDisabled(t *testing.T) {
	provider, err := table.LoadLocationProvider("table_location",
		iceberg.Properties{table.ObjectStoreEnabledKey: "true", table.WriteObjectStorePartitionedPathsKey: "false"})
	require.NoError(t, err)

	assert.Equal(t, "table_location/data/0101/0110/1001/10110010-a", provider.NewDataLocation("a"))
	assert.Equal(t, "table_location/data/1110/0111/1110/00000011-b", provider.NewDataLocation("b"))
	assert.Equal(t, "table_location/data/0010/1101/0110/01011111-c", provider.NewDataLocation("c"))
	assert.Equal(t, "table_location/data/1001/0001/0100/01110011-d", provider.NewDataLocation("d"))
	assert.Equal(t, "table_location/data/0110/1010/0011/11101000-test.parquet", provider.NewDataLocation("test.parquet"))
}
