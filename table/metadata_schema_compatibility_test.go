// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package table

import (
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewMetadata_DuplicateValues(t *testing.T) {
	names := []string{"foo", "bar", "foo"}

	fields := make([]iceberg.NestedField, len(names))
	for i, name := range names {
		fields[i] = iceberg.NestedField{
			ID:   i + 1,
			Name: name,
			Type: iceberg.PrimitiveTypes.String,
		}
	}
	icebergSchema := iceberg.NewSchema(1, fields...)

	_, err := NewMetadata(
		icebergSchema,
		&iceberg.PartitionSpec{},
		UnsortedSortOrder,
		"",
		iceberg.Properties{},
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid schema: error encountered during schema visitor")
	assert.Contains(t, err.Error(), "multiple fields for name foo")
}
