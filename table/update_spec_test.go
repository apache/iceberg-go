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
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
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
	},
	iceberg.PartitionField{
		SourceID:  5,
		FieldID:   iceberg.InitialPartitionSpecID + 1,
		Name:      "street_void",
		Transform: iceberg.VoidTransform{},
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
		assert.Equal(t, 1003, newSpec.LastAssignedFieldID())
		assert.Equal(t, 4, newSpec.NumFields())
		assert.Equal(t, "id_identity", newSpec.FieldsBySourceID(1)[0].Name)
		assert.Equal(t, "street_void", newSpec.FieldsBySourceID(5)[0].Name)

		addedField := newSpec.FieldsBySourceID(3)[0]
		assert.Equal(t, 3, addedField.SourceID)
		assert.Equal(t, 1002, addedField.FieldID)
		assert.Equal(t, "year_transform", addedField.Name)
		assert.Equal(t, iceberg.YearTransform{}, addedField.Transform)

		addedField = newSpec.FieldsBySourceID(7)[0]
		assert.Equal(t, 7, addedField.SourceID)
		assert.Equal(t, 1003, addedField.FieldID)
		assert.Equal(t, "zipcode_bucket", addedField.Name)
		assert.Equal(t, iceberg.BucketTransform{NumBuckets: 5}, addedField.Transform)
	})

	t.Run("add partition field case sensitive", func(t *testing.T) {
		txn = testNonPartitionedTable.NewTransaction()
		updates := table.NewUpdateSpec(txn, true)
		updates, err := updates.AddField("NaMe", iceberg.VoidTransform{}, "name_void")
		assert.ErrorContains(t, err, "invalid schema: could not bind reference")
		assert.Nil(t, updates)
	})

	t.Run("add invalid partition transform field", func(t *testing.T) {
		txn = testNonPartitionedTable.NewTransaction()
		updates := table.NewUpdateSpec(txn, true)
		updates, err := updates.AddField("name", iceberg.YearTransform{}, "name_year")
		assert.ErrorContains(t, err, "year cannot transform string values from name")
		assert.Nil(t, updates)
	})

	t.Run("add duplicate partition field", func(t *testing.T) {
		txn = testPartitionedTable.NewTransaction()
		updates := table.NewUpdateSpec(txn, true)
		updates, err := updates.AddField("id", iceberg.IdentityTransform{}, "id_transform")
		assert.ErrorContains(t, err, "duplicate partition field")
		assert.Nil(t, updates)
	})

	t.Run("add already added partition field", func(t *testing.T) {
		txn = testPartitionedTable.NewTransaction()
		updates := table.NewUpdateSpec(txn, true)

		updates, err := updates.AddField("ts", iceberg.YearTransform{}, "year_transform_1")
		assert.NoError(t, err)
		assert.NotNil(t, updates)
		updates, err = updates.AddField("ts", iceberg.YearTransform{}, "year_transform_2")
		assert.ErrorContains(t, err, "already added partition")
		assert.Nil(t, updates)
	})

	t.Run("add duplicate partition field name", func(t *testing.T) {
		txn = testPartitionedTable.NewTransaction()
		updates := table.NewUpdateSpec(txn, true)

		updates, err := updates.AddField("ts", iceberg.YearTransform{}, "year_transform_1")
		assert.NoError(t, err)
		assert.NotNil(t, updates)
		updates, err = updates.AddField("ts", iceberg.MonthTransform{}, "year_transform_1")
		assert.ErrorContains(t, err, "already added partition field with name")
		assert.Nil(t, updates)
	})

	t.Run("add conflicted time transform partition field", func(t *testing.T) {
		txn = testNonPartitionedTable.NewTransaction()
		updates := table.NewUpdateSpec(txn, true)

		updates, err := updates.AddField("ts", iceberg.YearTransform{}, "ts_year")
		assert.NoError(t, err)
		assert.NotNil(t, updates)

		updates, err = updates.AddField("ts", iceberg.MonthTransform{}, "ts_month")
		assert.ErrorContains(t, err, "cannot add time partition field")
		assert.Nil(t, updates)
	})

	t.Run("add duplicate partition field", func(t *testing.T) {
		txn = testPartitionedTable.NewTransaction()
		updates := table.NewUpdateSpec(txn, true)

		updates, err := updates.AddField("ts", iceberg.YearTransform{}, "id_identity")
		assert.ErrorContains(t, err, "cannot add duplicate partition field name")
		assert.Nil(t, updates)
	})

	t.Run("add duplicate partition field name with void transform", func(t *testing.T) {
		txn = testPartitionedTable.NewTransaction()
		updates := table.NewUpdateSpec(txn, true)

		updates, err := updates.AddField("ts", iceberg.VoidTransform{}, "street_void")
		assert.NoError(t, err)
		assert.NotNil(t, updates)

		newSpec := updates.Apply()
		assert.NotNil(t, newSpec)
		assert.Equal(t, "street_void_1001", newSpec.FieldsBySourceID(5)[0].Name)
	})
}
