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

package udf

import (
	"testing"

	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUDF(t *testing.T) {
	meta := parseFixture(t, "udf-metadata-scalar.json")
	ident := table.Identifier{"accounting", "add_one"}
	location := "s3://bucket/functions/add_one/metadata/00001-x.metadata.json"

	fn := New(ident, meta, location)

	assert.Equal(t, ident, fn.Identifier())
	assert.Equal(t, location, fn.MetadataLocation())
	assert.True(t, meta.Equals(fn.Metadata()))
	assert.Len(t, fn.Definitions(), 2)
	assert.Empty(t, fn.Properties())
	assert.Empty(t, fn.Location())

	same := New(table.Identifier{"accounting", "add_one"}, parseFixture(t, "udf-metadata-scalar.json"), location)
	assert.True(t, fn.Equals(*same))

	differentIdent := New(table.Identifier{"accounting", "other"}, meta, location)
	assert.False(t, fn.Equals(*differentIdent))

	differentLocation := New(ident, meta, "s3://bucket/elsewhere")
	assert.False(t, fn.Equals(*differentLocation))

	differentMetadata := New(ident, parseFixture(t, "udf-metadata-table.json"), location)
	assert.False(t, fn.Equals(*differentMetadata))

	require.NotNil(t, fn.Metadata())
}
