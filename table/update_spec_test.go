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
		FieldID:   iceberg.PartitionDataIDStart,
		Name:      "id_identity",
		Transform: iceberg.IdentityTransform{},
	},
	iceberg.PartitionField{
		SourceID:  5,
		FieldID:   iceberg.PartitionDataIDStart + 1,
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
		specUpdate := table.NewUpdateSpec(txn, false)

		updates, reqs, err := specUpdate.
			AddField("ts", iceberg.YearTransform{}, "year_transform").
			AddField("address.Zip_cOdE", iceberg.BucketTransform{NumBuckets: 5}, "zipcode_bucket").
			BuildUpdates()
		assert.NoError(t, err)
		assert.NotNil(t, updates)
		assert.NotNil(t, reqs)

		newSpec, err := specUpdate.Apply()
		assert.NoError(t, err)
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
		_, _, err := updates.
			AddField("NaMe", iceberg.VoidTransform{}, "name_void").
			BuildUpdates()
		assert.Error(t, err)
		assert.ErrorContains(t, err, "invalid schema: could not bind reference")
	})

	t.Run("add invalid partition transform field", func(t *testing.T) {
		txn = testNonPartitionedTable.NewTransaction()
		specUpdate := table.NewUpdateSpec(txn, true)
		updates, reqs, err := specUpdate.
			AddField("name", iceberg.YearTransform{}, "name_year").
			BuildUpdates()
		assert.Error(t, err)
		assert.ErrorContains(t, err, "year cannot transform string values from name")
		assert.Nil(t, updates)
		assert.Nil(t, reqs)
	})

	t.Run("add duplicate partition field", func(t *testing.T) {
		txn = testPartitionedTable.NewTransaction()
		specUpdate := table.NewUpdateSpec(txn, true)
		updates, reqs, err := specUpdate.
			AddField("id", iceberg.IdentityTransform{}, "id_transform").
			BuildUpdates()
		assert.Error(t, err)
		assert.ErrorContains(t, err, "duplicate partition field")
		assert.Nil(t, updates)
		assert.Nil(t, reqs)
	})

	t.Run("add already added partition field", func(t *testing.T) {
		txn = testPartitionedTable.NewTransaction()
		updateSpec := table.NewUpdateSpec(txn, true)

		updates, reqs, err := updateSpec.
			AddField("ts", iceberg.YearTransform{}, "year_transform_1").
			AddField("ts", iceberg.YearTransform{}, "year_transform_2").
			BuildUpdates()
		assert.Error(t, err)
		assert.ErrorContains(t, err, "already added partition")
		assert.Nil(t, updates)
		assert.Nil(t, reqs)
	})

	t.Run("add duplicate partition field name", func(t *testing.T) {
		txn = testPartitionedTable.NewTransaction()
		specUpdate := table.NewUpdateSpec(txn, true)

		updates, reqs, err := specUpdate.
			AddField("ts", iceberg.YearTransform{}, "year_transform_1").
			AddField("ts", iceberg.MonthTransform{}, "year_transform_1").
			BuildUpdates()

		assert.Error(t, err)
		assert.ErrorContains(t, err, "already added partition field with name")
		assert.Nil(t, updates)
		assert.Nil(t, reqs)
	})

	t.Run("add conflicted time transform partition field", func(t *testing.T) {
		txn = testNonPartitionedTable.NewTransaction()
		updateSpec := table.NewUpdateSpec(txn, true)

		updates, reqs, err := updateSpec.
			AddField("ts", iceberg.YearTransform{}, "ts_year").
			AddField("ts", iceberg.MonthTransform{}, "ts_month").
			BuildUpdates()

		assert.Error(t, err)
		assert.ErrorContains(t, err, "cannot add time partition field")
		assert.Nil(t, updates)
		assert.Nil(t, reqs)
	})

	t.Run("add duplicate partition field", func(t *testing.T) {
		txn = testPartitionedTable.NewTransaction()
		updateSpec := table.NewUpdateSpec(txn, true)

		updates, reqs, err := updateSpec.
			AddField("ts", iceberg.YearTransform{}, "id_identity").
			BuildUpdates()
		assert.Error(t, err)
		assert.ErrorContains(t, err, "cannot add duplicate partition field name")
		assert.Nil(t, updates)
		assert.Nil(t, reqs)
	})

	t.Run("add duplicate partition field name with void transform", func(t *testing.T) {
		txn = testPartitionedTable.NewTransaction()
		specUpdate := table.NewUpdateSpec(txn, true)

		updates, reqs, err := specUpdate.
			AddField("ts", iceberg.VoidTransform{}, "street_void").
			BuildUpdates()
		assert.NoError(t, err)
		assert.NotNil(t, updates)
		assert.NotNil(t, reqs)

		newSpec, err := specUpdate.Apply()
		assert.NoError(t, err)
		assert.NotNil(t, newSpec)
		assert.Equal(t, "street_void_1001", newSpec.FieldsBySourceID(5)[0].Name)
	})
}

func TestUpdateSpecAddIdentityField(t *testing.T) {
	var txn *table.Transaction

	t.Run("add identity partition fields", func(t *testing.T) {
		txn = testPartitionedTable.NewTransaction()
		specUpdate := table.NewUpdateSpec(txn, false)
		updates, reqs, err := specUpdate.
			AddIdentity("ts").
			AddIdentity("name").
			BuildUpdates()
		assert.NoError(t, err)
		assert.NotNil(t, updates)
		assert.NotNil(t, reqs)

		newSpec, err := specUpdate.Apply()
		assert.NoError(t, err)
		assert.NotNil(t, newSpec)
		assert.Equal(t, 1, newSpec.ID())
		assert.Equal(t, 1003, newSpec.LastAssignedFieldID())
		assert.Equal(t, 4, newSpec.NumFields())
		assert.Equal(t, "id_identity", newSpec.FieldsBySourceID(1)[0].Name)
		assert.Equal(t, "street_void", newSpec.FieldsBySourceID(5)[0].Name)

		addedField := newSpec.FieldsBySourceID(3)[0]
		assert.Equal(t, 3, addedField.SourceID)
		assert.Equal(t, 1002, addedField.FieldID)
		assert.Equal(t, "ts", addedField.Name)
		assert.Equal(t, iceberg.IdentityTransform{}, addedField.Transform)

		addedField = newSpec.FieldsBySourceID(2)[0]
		assert.Equal(t, 2, addedField.SourceID)
		assert.Equal(t, 1003, addedField.FieldID)
		assert.Equal(t, "name", addedField.Name)
		assert.Equal(t, iceberg.IdentityTransform{}, addedField.Transform)
	})
}

func TestUpdateSpecRenameField(t *testing.T) {
	var txn *table.Transaction

	t.Run("rename partition fields", func(t *testing.T) {
		txn = testPartitionedTable.NewTransaction()
		updateSpec := table.NewUpdateSpec(txn, false)
		updates, reqs, err := updateSpec.
			RenameField("id_identity", "new_id_identity").
			RenameField("street_void", "new_street_void").
			BuildUpdates()
		assert.NoError(t, err)
		assert.NotNil(t, updates)
		assert.NotNil(t, reqs)

		newSpec, err := updateSpec.Apply()
		assert.NoError(t, err)
		assert.NotNil(t, newSpec)
		assert.Equal(t, 1, newSpec.ID())
		assert.Equal(t, "new_id_identity", newSpec.FieldsBySourceID(1)[0].Name)
		assert.Equal(t, "new_street_void", newSpec.FieldsBySourceID(5)[0].Name)
	})

	t.Run("rename recently added partition", func(t *testing.T) {
		txn = testPartitionedTable.NewTransaction()
		updateSpec := table.NewUpdateSpec(txn, false)

		updates, reqs, err := updateSpec.
			AddField("ts", iceberg.YearTransform{}, "year_transform").
			RenameField("year_transform", "new_year_transform").
			BuildUpdates()
		assert.Error(t, err)
		assert.ErrorContains(t, err, "cannot rename recently added partitions")
		assert.Nil(t, updates)
		assert.Nil(t, reqs)
	})

	t.Run("rename a partition field that doesn't exist", func(t *testing.T) {
		txn = testPartitionedTable.NewTransaction()
		updateSpec := table.NewUpdateSpec(txn, false)

		updates, reqs, err := updateSpec.
			RenameField("non_exist_field", "new_non_exist_field").
			BuildUpdates()
		assert.Error(t, err)
		assert.ErrorContains(t, err, "cannot find partition field")
		assert.Nil(t, updates)
		assert.Nil(t, reqs)
	})

	t.Run("rename a partition field deleted in the same transaction", func(t *testing.T) {
		txn = testPartitionedTable.NewTransaction()
		updateSpec := table.NewUpdateSpec(txn, false)

		updates, reqs, err := updateSpec.
			RemoveField("id_identity").
			RenameField("id_identity", "new_id_identity").
			BuildUpdates()
		assert.Error(t, err)
		assert.ErrorContains(t, err, "cannot delete and rename partition field")
		assert.Nil(t, updates)
		assert.Nil(t, reqs)
	})
}

func TestUpdateSpecRemoveField(t *testing.T) {
	var txn *table.Transaction

	t.Run("remove existing partition fields", func(t *testing.T) {
		txn = testPartitionedTable.NewTransaction()
		updateSpec := table.NewUpdateSpec(txn, false)

		updates, reqs, err := updateSpec.
			RemoveField("street_void").
			RemoveField("id_identity").
			BuildUpdates()
		assert.NoError(t, err)
		assert.NotNil(t, updates)
		assert.NotNil(t, reqs)

		newSpec, err := updateSpec.Apply()
		assert.NoError(t, err)
		assert.NotNil(t, newSpec)
		assert.Equal(t, 1, newSpec.ID())
		assert.Equal(t, 999, newSpec.LastAssignedFieldID())
		assert.Equal(t, 0, newSpec.NumFields())
		assert.Equal(t, true, newSpec.IsUnpartitioned())
	})

	t.Run("remove newly added partition field", func(t *testing.T) {
		txn = testPartitionedTable.NewTransaction()
		updateSpec := table.NewUpdateSpec(txn, false)

		updates, reqs, err := updateSpec.
			AddField("ts", iceberg.YearTransform{}, "year_transform").
			RemoveField("year_transform").
			BuildUpdates()
		assert.Error(t, err)
		assert.ErrorContains(t, err, "cannot remove newly added field")
		assert.Nil(t, updates)
		assert.Nil(t, reqs)
	})

	t.Run("remove renamed partition field", func(t *testing.T) {
		txn = testPartitionedTable.NewTransaction()
		updateSpec := table.NewUpdateSpec(txn, false)

		updates, reqs, err := updateSpec.
			RenameField("id_identity", "new_id_identity").
			RemoveField("id_identity").
			BuildUpdates()
		assert.Error(t, err)
		assert.ErrorContains(t, err, "cannot rename and delete field")
		assert.Nil(t, updates)
		assert.Nil(t, reqs)
	})

	t.Run("remove partition field that doesn't exist", func(t *testing.T) {
		txn = testPartitionedTable.NewTransaction()
		updateSpec := table.NewUpdateSpec(txn, false)

		updates, reqs, err := updateSpec.
			RemoveField("non_exist_field").
			BuildUpdates()
		assert.Error(t, err)
		assert.ErrorContains(t, err, "cannot find partition field")
		assert.Nil(t, updates)
		assert.Nil(t, reqs)
	})
}

func TestUpdateSpecBuildChanges(t *testing.T) {
	var txn *table.Transaction

	t.Run("build changes on added partition fields", func(t *testing.T) {
		txn = testNonPartitionedTable.NewTransaction()
		specUpdate := table.NewUpdateSpec(txn, false)

		updates, reqs, err := specUpdate.
			AddField("ts", iceberg.YearTransform{}, "year_transform").
			AddField("address.zip_code", iceberg.BucketTransform{NumBuckets: 5}, "zipcode_bucket").
			BuildUpdates()
		assert.NoError(t, err)
		assert.NotNil(t, updates)
		assert.NotNil(t, reqs)

		assert.Equal(t, 2, len(updates))
		assert.Equal(t, 1, len(reqs))

		assert.Equal(t, table.UpdateAddSpec, updates[0].Action())
		assert.Equal(t, table.UpdateSetDefaultSpec, updates[1].Action())
		assert.Equal(t, "assert-last-assigned-partition-id", reqs[0].GetType())
	})

	t.Run("build changes on removed partition field", func(t *testing.T) {
		txn = testPartitionedTable.NewTransaction()
		specUpdate := table.NewUpdateSpec(txn, false)

		updates, reqs, err := specUpdate.
			RemoveField("street_void").
			BuildUpdates()
		assert.NoError(t, err)
		assert.NotNil(t, updates)
		assert.NotNil(t, reqs)

		assert.Equal(t, 2, len(updates))
		assert.Equal(t, 1, len(reqs))

		assert.Equal(t, table.UpdateAddSpec, updates[0].Action())
		assert.Equal(t, table.UpdateSetDefaultSpec, updates[1].Action())
		assert.Equal(t, "assert-last-assigned-partition-id", reqs[0].GetType())
	})

	t.Run("build changes on renamed partition field", func(t *testing.T) {
		txn = testPartitionedTable.NewTransaction()
		specUpdate := table.NewUpdateSpec(txn, false)

		updates, reqs, err := specUpdate.
			RenameField("street_void", "new_street_void").
			BuildUpdates()

		assert.NoError(t, err)
		assert.Equal(t, 2, len(updates))
		assert.Equal(t, 1, len(reqs))

		assert.Equal(t, table.UpdateAddSpec, updates[0].Action())
		assert.Equal(t, table.UpdateSetDefaultSpec, updates[1].Action())
		assert.Equal(t, "assert-last-assigned-partition-id", reqs[0].GetType())
	})
}

func TestUpdateSpecCommit(t *testing.T) {
	var txn *table.Transaction

	t.Run("test commit apply changes on transaction", func(t *testing.T) {
		txn = testPartitionedTable.NewTransaction()

		specUpdate := table.NewUpdateSpec(txn, false)

		err := specUpdate.
			AddField("address.city", iceberg.TruncateTransform{Width: 3}, "").
			AddIdentity("address.zip_code").
			RenameField("street_void", "new_street_void").
			RemoveField("id_identity").
			Commit()
		assert.NoError(t, err)

		stagedTbl, err := txn.StagedTable()
		assert.NoError(t, err)

		currSpec := stagedTbl.Spec()
		assert.NotNil(t, currSpec)
		assert.Equal(t, 1, currSpec.ID())
		assert.Equal(t, 1003, currSpec.LastAssignedFieldID())
		assert.Equal(t, 3, currSpec.NumFields())
		assert.Equal(t, "new_street_void", currSpec.FieldsBySourceID(5)[0].Name)
		assert.Equal(t, []iceberg.PartitionField(nil), currSpec.FieldsBySourceID(1))

		addedField := currSpec.FieldsBySourceID(6)[0]
		assert.Equal(t, 6, addedField.SourceID)
		assert.Equal(t, 1002, addedField.FieldID)
		assert.Equal(t, "address.city_trunc_3", addedField.Name)
		assert.Equal(t, iceberg.TruncateTransform{Width: 3}, addedField.Transform)

		addedIdentity := currSpec.FieldsBySourceID(7)[0]
		assert.Equal(t, 7, addedIdentity.SourceID)
		assert.Equal(t, 1003, addedIdentity.FieldID)
		assert.Equal(t, "address.zip_code", addedIdentity.Name)
		assert.Equal(t, iceberg.IdentityTransform{}, addedIdentity.Transform)
	})

	t.Run("test commit with build errors", func(t *testing.T) {
		txn = testPartitionedTable.NewTransaction()

		specUpdate := table.NewUpdateSpec(txn, false)

		err := specUpdate.
			AddField("id", iceberg.IdentityTransform{}, "id_transform").
			Commit()
		assert.Error(t, err)
		assert.ErrorContains(t, err, "duplicate partition field")
	})

	t.Run("test commit with empty updates", func(t *testing.T) {
		txn = testPartitionedTable.NewTransaction()

		specUpdate := table.NewUpdateSpec(txn, false)

		err := specUpdate.Commit()
		assert.Nil(t, err)
	})
}
