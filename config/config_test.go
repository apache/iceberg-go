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

package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testArgs = []struct {
	file     []byte
	catName  string
	expected *CatalogConfig
}{
	// config file does not exist; empty name falls back to built-in "default"
	{nil, "", nil},
	// config does not have requested catalog
	{[]byte(`
catalog:
  custom-catalog:
    type: rest
    uri: http://localhost:8181/
    output: text
    credential: client-id:client-secret
    warehouse: catalog_name
`), "default", nil},
	// default catalog
	{
		[]byte(`
catalog:
  default:
    type: rest
    uri: http://localhost:8181/
    output: text
    credential: client-id:client-secret
    warehouse: catalog_name
`), "default",
		&CatalogConfig{
			CatalogType: "rest",
			URI:         "http://localhost:8181/",
			Output:      "text",
			Credential:  "client-id:client-secret",
			Warehouse:   "catalog_name",
		},
	},
	// custom catalog
	{
		[]byte(`
catalog:
  custom-catalog:
    type: rest
    uri: http://localhost:8181/
    output: text
    credential: client-id:client-secret
    warehouse: catalog_name
`), "custom-catalog",
		&CatalogConfig{
			CatalogType: "rest",
			URI:         "http://localhost:8181/",
			Output:      "text",
			Credential:  "client-id:client-secret",
			Warehouse:   "catalog_name",
		},
	},
	// include rest options
	{
		[]byte(`
catalog:
  default:
    type: rest
    warehouse: "arn:aws:s3tables:us-east-1:account-id:bucket/bucketname"
    uri: "https://s3tables.us-east-1.amazonaws.com/iceberg"
    rest:
      sigv4-enabled: true
      signing-name: "s3tables"
      signing-region: "us-east-1"
`), "default",
		&CatalogConfig{
			CatalogType: "rest",
			URI:         "https://s3tables.us-east-1.amazonaws.com/iceberg",
			Warehouse:   "arn:aws:s3tables:us-east-1:account-id:bucket/bucketname",
			RestOptions: &RestOptions{
				SigV4Enabled:  true,
				SigningName:   "s3tables",
				SigningRegion: "us-east-1",
			},
		},
	},
	// glue catalog with aws-profile
	{
		[]byte(`
catalog:
  default:
    type: glue
    aws-profile: my-aws-profile
`), "default",
		&CatalogConfig{
			CatalogType: "glue",
			AwsProfile:  "my-aws-profile",
		},
	},
	// empty name resolves via the file's default-catalog field
	{
		[]byte(`
default-catalog: custom-catalog
catalog:
  custom-catalog:
    type: rest
    uri: http://localhost:8181/
    warehouse: my_wh
`), "",
		&CatalogConfig{
			CatalogType: "rest",
			URI:         "http://localhost:8181/",
			Warehouse:   "my_wh",
		},
	},
	// explicit name takes precedence over the file's default-catalog
	{
		[]byte(`
default-catalog: custom-catalog
catalog:
  custom-catalog:
    type: rest
    uri: http://localhost:8181/
  other:
    type: rest
    uri: http://other:8181/
`), "other",
		&CatalogConfig{
			CatalogType: "rest",
			URI:         "http://other:8181/",
		},
	},
}

func TestParseConfig(t *testing.T) {
	for _, tt := range testArgs {
		actual, err := ParseConfig(tt.file, tt.catName)
		require.NoError(t, err)
		assert.Equal(t, tt.expected, actual)
	}
}

func TestParseConfigMalformed(t *testing.T) {
	_, err := ParseConfig([]byte(":\t[broken yaml"), "default")
	assert.Error(t, err)
}
