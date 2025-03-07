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
	"strings"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
