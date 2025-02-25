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
)

var testArgs = []struct {
	file     []byte
	catName  string
	expected *CatalogConfig
}{
	// config file does not exist
	{nil, "default", nil},
	// config does not have default catalog
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
}

func TestParseConfig(t *testing.T) {
	for _, tt := range testArgs {
		actual := ParseConfig([]byte(tt.file), tt.catName)

		assert.Equal(t, tt.expected, actual)
	}
}
