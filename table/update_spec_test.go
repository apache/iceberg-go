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
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"testing"
)

var testSchema = iceberg.NewSchema(1,
	iceberg.NestedField{ID: 1, Name: "id", Required: true, Type: iceberg.PrimitiveTypes.Int64},
	iceberg.NestedField{ID: 2, Name: "name", Required: true, Type: iceberg.PrimitiveTypes.String},
	iceberg.NestedField{ID: 3, Name: "ts", Required: false, Type: iceberg.PrimitiveTypes.Timestamp},
	iceberg.NestedField{ID: 4, Name: "address", Required: false, Type: &iceberg.StructType{
		FieldList: []iceberg.NestedField{
			{ID: 5, Name: "street", Type: iceberg.PrimitiveTypes.String, Required: true},
			{ID: 6, Name: "city", Type: iceberg.PrimitiveTypes.String, Required: true},
			{ID: 7, Name: "zip_code", Type: iceberg.PrimitiveTypes.Int64, Required: false},
		},
	}},
)

var partitionSpec = iceberg.NewPartitionSpec(
	iceberg.PartitionField{
		SourceID:  1,
		FieldID:   iceberg.InitialPartitionSpecID,
		Name:      "id_identity",
		Transform: iceberg.IdentityTransform{},
	})

var testMetadataNonPartitioned, _ = table.NewMetadata(testSchema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, "", nil)
var testMetadataPartitioned, _ = table.NewMetadata(testSchema, &partitionSpec, table.UnsortedSortOrder, "", nil)

var testNonPartitionedTable = table.New([]string{"non_partitioned"}, testMetadataNonPartitioned, "", nil, nil)
var testPartitionedTable = table.New([]string{"partitioned"}, testMetadataPartitioned, "", nil, nil)

func TestUpdateSpecAddField(t *testing.T) {
	var txn *table.Transaction

	t.Run("add partition fields", func(t *testing.T) {
		txn = testPartitionedTable.NewTransaction()
		updates := table.NewUpdateSpec(txn, false)
		updates, err := updates.AddField("ts", iceberg.YearTransform{}, "year_transform")
		assert.NoError(t, err)
		assert.NotNil(t, updates)

		updates, err = updates.AddField("address.Zip_cOdE", iceberg.BucketTransform{NumBuckets: 5}, "zipcode_bucket")
		assert.NoError(t, err)
		assert.NotNil(t, updates)

		newSpec := updates.Apply()
		assert.NotNil(t, newSpec)
		assert.Equal(t, 1, newSpec.ID())
		assert.Equal(t, 1002, newSpec.LastAssignedFieldID())
		assert.Equal(t, 3, newSpec.NumFields())
		assert.Equal(t, "id_identity", newSpec.FieldsBySourceID(1)[0].Name)

		addedField := newSpec.FieldsBySourceID(3)[0]
		assert.Equal(t, 3, addedField.SourceID)
		assert.Equal(t, 1001, addedField.FieldID)
		assert.Equal(t, "year_transform", addedField.Name)
		assert.Equal(t, iceberg.YearTransform{}, addedField.Transform)

		addedField = newSpec.FieldsBySourceID(7)[0]
		assert.Equal(t, 7, addedField.SourceID)
		assert.Equal(t, 1002, addedField.FieldID)
		assert.Equal(t, "zipcode_bucket", addedField.Name)
		assert.Equal(t, iceberg.BucketTransform{NumBuckets: 5}, addedField.Transform)
	})
}
