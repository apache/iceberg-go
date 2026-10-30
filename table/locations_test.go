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
	"context"
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
					root := filepath.ToSlash(filepath.Join(t.TempDir(), name))
					for _, custom := range []bool{false, true} {
						props := iceberg.Properties{
							table.ObjectStoreEnabledKey:               objectStore,
							table.WriteObjectStorePartitionedPathsKey: partitionPaths,
						}
						dataDir, metadataDir := root+"/data", root+"/metadata"
						if custom {
							dataDir, metadataDir = root+"/custom data", root+"/custom metadata"
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
							require.True(t, strings.HasPrefix(file.location, file.dir+"/"), "%q is outside %q", file.location, file.dir)
							require.NotContains(t, file.location, `\`)
							writer, err := (io.LocalFS{}).Create(context.Background(), file.location)
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

func TestLocationProviderLiteralPaths(t *testing.T) {
	for _, tt := range []struct{ location, data, metadata string }{
		{"my table", "my table/data/a", "my table/metadata/manifest.avro"},
		{"100% complete", "100% complete/data/a", "100% complete/metadata/manifest.avro"},
		{"literal%20table", "literal%20table/data/a", "literal%20table/metadata/manifest.avro"},
		{"table#fragment", "table#fragment/data/a", "table#fragment/metadata/manifest.avro"},
		{"table?query", "table?query/data/a", "table?query/metadata/manifest.avro"},
		{"db:2024", "db:2024/data/a", "db:2024/metadata/manifest.avro"},
		{"my table:v2", "my table:v2/data/a", "my table:v2/metadata/manifest.avro"},
		{"dir/table:part%20", "dir/table:part%20/data/a", "dir/table:part%20/metadata/manifest.avro"},
		{"s3://bucket/my table", "s3://bucket/my table/data/a", "s3://bucket/my table/metadata/manifest.avro"},
		{"s3://bucket/100% complete", "s3://bucket/100% complete/data/a", "s3://bucket/100% complete/metadata/manifest.avro"},
		{"s3://bucket/literal%20#?", "s3://bucket/literal%20#?/data/a", "s3://bucket/literal%20#?/metadata/manifest.avro"},
		{"gs://bucket/my table", "gs://bucket/my table/data/a", "gs://bucket/my table/metadata/manifest.avro"},
		{"az://bucket/my table", "az://bucket/my table/data/a", "az://bucket/my table/metadata/manifest.avro"},
		{"file:///tmp/my table", "file:///tmp/my table/data/a", "file:///tmp/my table/metadata/manifest.avro"},
		{"s3://bucket/a//./b/../table///", "s3://bucket/a//./b/../table/data/a", "s3://bucket/a//./b/../table/metadata/manifest.avro"},
		{"dir//./other/../table/", "dir//./other/../table/data/a", "dir//./other/../table/metadata/manifest.avro"},
		{"/", "/data/a", "/metadata/manifest.avro"},
		{"C:/my table", "C:/my table/data/a", "C:/my table/metadata/manifest.avro"},
		{`C:\my table`, `C:\my table/data/a`, `C:\my table/metadata/manifest.avro`},
		{`s3://bucket/literal\name`, `s3://bucket/literal\name/data/a`, `s3://bucket/literal\name/metadata/manifest.avro`},
	} {
		t.Run(tt.location, func(t *testing.T) {
			provider, err := table.LoadLocationProvider(tt.location, nil)
			require.NoError(t, err)
			assert.Equal(t, tt.data, provider.NewDataLocation("a"))
			assert.Equal(t, tt.metadata, provider.NewMetadataLocation("manifest.avro"))
		})
	}
}

func TestLocationProviderLiteralCustomPaths(t *testing.T) {
	for _, tt := range []struct{ objectStore, partitionPaths, data string }{
		{"false", "false", "s3://bucket/my data%20#?/a"},
		{"false", "true", "s3://bucket/my data%20#?/a"},
		{"true", "false", "s3://bucket/my data%20#?/0101/0110/1001/10110010-a"},
		{"true", "true", "s3://bucket/my data%20#?/0101/0110/1001/10110010/a"},
	} {
		t.Run(tt.objectStore+"/"+tt.partitionPaths, func(t *testing.T) {
			provider, err := table.LoadLocationProvider("s3://bucket/my table", iceberg.Properties{
				table.ObjectStoreEnabledKey:               tt.objectStore,
				table.WriteObjectStorePartitionedPathsKey: tt.partitionPaths,
				table.WriteDataPathKey:                    "s3://bucket/my data%20#?///",
				table.WriteMetadataPathKey:                "gs://bucket/100% metadata/./../here/",
			})
			require.NoError(t, err)
			assert.Equal(t, tt.data, provider.NewDataLocation("a"))
			assert.Equal(t, "s3://bucket/my data%20#?/partition=a b%20#?/file.parquet",
				provider.NewDataLocation("partition=a b%20#?/file.parquet"))
			assert.Equal(t, "gs://bucket/100% metadata/./../here/manifest #?%.avro",
				provider.NewMetadataLocation("manifest #?%.avro"))
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
