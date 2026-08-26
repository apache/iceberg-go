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
	"bytes"
	"encoding/json"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		SourceIDs: []int{1},
		FieldID:   iceberg.PartitionDataIDStart,
		Name:      "id_identity",
		Transform: iceberg.IdentityTransform{},
	},
	iceberg.PartitionField{
		SourceIDs: []int{5},
		FieldID:   iceberg.PartitionDataIDStart + 1,
		Name:      "street_void",
		Transform: iceberg.VoidTransform{},
	})

var testMetadataNonPartitioned, _ = table.NewMetadata(testSchema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, "", nil)

var testMetadataPartitioned, _ = table.NewMetadata(testSchema, &partitionSpec, table.UnsortedSortOrder, "", nil)

var testNonPartitionedTable = table.New([]string{"non_partitioned"}, testMetadataNonPartitioned, "", nil, nil)

var testPartitionedTable = table.New([]string{"partitioned"}, testMetadataPartitioned, "", nil, nil)

func TestNewUpdateSpecRejectsUnknownTransformInBaseSpec(t *testing.T) {
	unknown, err := iceberg.ParseTransform("custom_transform[42]")
	require.NoError(t, err)

	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1},
		FieldID:   iceberg.PartitionDataIDStart,
		Name:      "id_custom",
		Transform: unknown,
	})
	meta, err := table.NewMetadata(testSchema, &spec, table.UnsortedSortOrder, "", nil)
	require.NoError(t, err)
	tbl := table.New([]string{"unknown_spec"}, meta, "", nil, nil)

	_, _, err = table.NewUpdateSpec(tbl.NewTransaction(), false).
		AddField("name", iceberg.IdentityTransform{}, "name_identity").
		BuildUpdates()
	require.ErrorIs(t, err, iceberg.ErrInvalidTransform)
	require.ErrorContains(t, err, "custom_transform[42]")
}

func TestNewUpdateSpecWithNilTransactionReturnsError(t *testing.T) {
	err := table.NewUpdateSpec(nil, false).Commit()
	require.ErrorIs(t, err, table.ErrInvalidMetadata)
	assert.ErrorContains(t, err, "transaction is nil")
}

func TestUpdateSpecPreservesSourceLessVoidTombstone(t *testing.T) {
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "id",
		Transform: iceberg.VoidTransform{},
	})
	meta, err := table.NewMetadata(testSchema, &spec, table.UnsortedSortOrder, "s3://bucket/test/location", nil)
	require.NoError(t, err)

	metadataJSON, err := json.Marshal(meta)
	require.NoError(t, err)
	source := []byte(`"source-id":1,`)
	metadataJSON = bytes.ReplaceAll(metadataJSON, source, nil)
	require.NotContains(t, string(metadataJSON), string(source))
	meta, err = table.ParseMetadataBytes(metadataJSON)
	require.NoError(t, err)
	tbl := table.New([]string{"source_less_void"}, meta, "", nil, nil)

	update := table.NewUpdateSpec(tbl.NewTransaction(), false).
		AddField("name", iceberg.IdentityTransform{}, "name_identity")
	_, _, err = update.BuildUpdates()
	require.NoError(t, err)
	newSpec, err := update.Apply()
	require.NoError(t, err)
	require.Equal(t, 2, newSpec.NumFields())
	assert.Equal(t, []int{0}, newSpec.Field(0).SourceIDs)
	assert.Equal(t, 1000, newSpec.Field(0).FieldID)
	assert.Equal(t, "id", newSpec.Field(0).Name)
	assert.Equal(t, iceberg.VoidTransform{}, newSpec.Field(0).Transform)
	assert.Equal(t, 2, newSpec.Field(1).SourceID())
	assert.Equal(t, "name_identity", newSpec.Field(1).Name)
}

func TestUpdateSpecRejectsTimeTransformConflictWithExistingField(t *testing.T) {
	baseSpec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{3},
		FieldID:   iceberg.PartitionDataIDStart,
		Name:      "ts_hour",
		Transform: iceberg.HourTransform{},
	})
	meta, err := table.NewMetadata(testSchema, &baseSpec, table.UnsortedSortOrder, "", nil)
	require.NoError(t, err)
	tbl := table.New([]string{"hour_partitioned"}, meta, "", nil, nil)

	updates, reqs, err := table.NewUpdateSpec(tbl.NewTransaction(), false).
		AddField("ts", iceberg.DayTransform{}, "ts_day").
		BuildUpdates()

	require.ErrorIs(t, err, iceberg.ErrInvalidPartitionSpec)
	assert.ErrorContains(t, err, "redundant partition field")
	assert.Nil(t, updates)
	assert.Nil(t, reqs)
}

func TestUpdateSpecAllowsReplacingTimeTransformOnSameSource(t *testing.T) {
	newYearPartitionedTable := func() *table.Table {
		baseSpec := iceberg.NewPartitionSpec(iceberg.PartitionField{
			SourceIDs: []int{3},
			FieldID:   iceberg.PartitionDataIDStart,
			Name:      "ts_year",
			Transform: iceberg.YearTransform{},
		})
		meta, err := table.NewMetadata(testSchema, &baseSpec, table.UnsortedSortOrder, "", nil)
		require.NoError(t, err)

		return table.New([]string{"year_partitioned"}, meta, "", nil, nil)
	}

	assertReplaced := func(t *testing.T, specUpdate *table.UpdateSpec) {
		t.Helper()
		updates, reqs, err := specUpdate.BuildUpdates()
		require.NoError(t, err)
		assert.NotNil(t, updates)
		assert.NotNil(t, reqs)

		newSpec, err := specUpdate.Apply()
		require.NoError(t, err)
		require.Equal(t, 1, newSpec.NumFields())
		assert.Equal(t, "ts_month", newSpec.Field(0).Name)
		assert.Equal(t, iceberg.MonthTransform{}, newSpec.Field(0).Transform)
		assert.Equal(t, 3, newSpec.Field(0).SourceID())
	}

	t.Run("remove then add", func(t *testing.T) {
		tbl := newYearPartitionedTable()
		assertReplaced(t, table.NewUpdateSpec(tbl.NewTransaction(), false).
			RemoveField("ts_year").
			AddField("ts", iceberg.MonthTransform{}, "ts_month"))
	})

	t.Run("add then remove", func(t *testing.T) {
		tbl := newYearPartitionedTable()
		assertReplaced(t, table.NewUpdateSpec(tbl.NewTransaction(), false).
			AddField("ts", iceberg.MonthTransform{}, "ts_month").
			RemoveField("ts_year"))
	})
}

func TestUpdateSpecReplacesTimeTransformOverV1VoidTombstone(t *testing.T) {
	baseSpec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{3},
		FieldID:   iceberg.PartitionDataIDStart,
		Name:      "ts_year",
		Transform: iceberg.YearTransform{},
	})
	meta, err := table.NewMetadata(testSchema, &baseSpec, table.UnsortedSortOrder, "",
		iceberg.Properties{"format-version": "1"})
	require.NoError(t, err)
	require.Equal(t, 1, meta.Version())
	tbl := table.New([]string{"v1_year_partitioned"}, meta, "", nil, nil)

	specUpdate := table.NewUpdateSpec(tbl.NewTransaction(), false).
		RemoveField("ts_year").
		AddField("ts", iceberg.MonthTransform{}, "ts_month")
	_, _, err = specUpdate.BuildUpdates()
	require.NoError(t, err)

	newSpec, err := specUpdate.Apply()
	require.NoError(t, err)
	require.Equal(t, 2, newSpec.NumFields())
	assert.Equal(t, "ts_year", newSpec.Field(0).Name)
	assert.Equal(t, iceberg.VoidTransform{}, newSpec.Field(0).Transform)
	assert.Equal(t, 3, newSpec.Field(0).SourceID())
	assert.Equal(t, "ts_month", newSpec.Field(1).Name)
	assert.Equal(t, iceberg.MonthTransform{}, newSpec.Field(1).Transform)
	assert.Equal(t, 3, newSpec.Field(1).SourceID())
}

// Iceberg-Java writes this metadata today, so loading it has to keep working: the
// redundancy rule is a write-side rule.
// Metadata carrying day(ts) and hour(ts) on one column, the shape both Iceberg-Java and
// iceberg-go before at some point could write.
const timeOverlapMetadataJSON = `{
	"format-version": 2,
	"table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
	"location": "s3://bucket/table",
	"last-sequence-number": 0,
	"last-updated-ms": 1602638573874,
	"last-column-id": 3,
	"current-schema-id": 0,
	"schemas": [{"type":"struct","schema-id":0,"fields":[
		{"id":1,"name":"id","required":true,"type":"long"},
		{"id":3,"name":"ts","required":false,"type":"timestamp"}]}],
	"default-spec-id": 0,
	"partition-specs": [{"spec-id":0,"fields":[
		{"source-id":3,"field-id":1000,"name":"ts_day","transform":"day"},
		{"source-id":3,"field-id":1001,"name":"ts_hour","transform":"hour"}]}],
	"last-partition-id": 1001,
	"default-sort-order-id": 0,
	"sort-orders": [{"order-id":0,"fields":[]}],
	"properties": {},
	"current-snapshot-id": -1,
	"snapshots": [],
	"snapshot-log": [],
	"metadata-log": []
}`

func newTimeOverlapTable(t *testing.T) *table.Table {
	t.Helper()
	meta, err := table.ParseMetadataString(timeOverlapMetadataJSON)
	require.NoError(t, err)

	return table.New([]string{"time_overlap"}, meta, "", nil, nil)
}

func TestParseMetadataAllowsTimeGranularityOverlapInSpec(t *testing.T) {
	tbl := newTimeOverlapTable(t)

	spec := tbl.Metadata().PartitionSpec()
	require.Equal(t, 2, spec.NumFields())
	assert.Equal(t, iceberg.DayTransform{}, spec.Field(0).Transform)
	assert.Equal(t, iceberg.HourTransform{}, spec.Field(1).Transform)
}

// Inherited redundancy is checked on the spec the update produces, not on what
// the update touches, so it blocks unrelated evolution until it is cleaned up.
// Deliberate: rewriting the spec unchanged would carry the redundancy forward.
func TestUpdateSpecOnInheritedTimeOverlap(t *testing.T) {
	t.Run("unrelated add is blocked", func(t *testing.T) {
		tbl := newTimeOverlapTable(t)
		_, _, err := table.NewUpdateSpec(tbl.NewTransaction(), false).
			AddField("id", iceberg.BucketTransform{NumBuckets: 8}, "id_bucket").
			BuildUpdates()
		require.ErrorIs(t, err, iceberg.ErrInvalidPartitionSpec)
		assert.ErrorContains(t, err, "ts_hour (hour) conflicts with ts_day (day)")
		assert.ErrorContains(t, err, "remove one of them in this update")
	})

	t.Run("unrelated rename is blocked", func(t *testing.T) {
		tbl := newTimeOverlapTable(t)
		_, _, err := table.NewUpdateSpec(tbl.NewTransaction(), false).
			RenameField("ts_day", "ts_daily").
			BuildUpdates()
		require.ErrorIs(t, err, iceberg.ErrInvalidPartitionSpec)
	})

	// The escape hatch: dropping the redundant field is always allowed, and may
	// be staged alongside the change that was blocked.
	t.Run("removing the redundant field unblocks the rest", func(t *testing.T) {
		tbl := newTimeOverlapTable(t)
		specUpdate := table.NewUpdateSpec(tbl.NewTransaction(), false).
			RemoveField("ts_day").
			AddField("id", iceberg.BucketTransform{NumBuckets: 8}, "id_bucket")
		_, _, err := specUpdate.BuildUpdates()
		require.NoError(t, err)

		newSpec, err := specUpdate.Apply()
		require.NoError(t, err)
		require.Equal(t, 2, newSpec.NumFields())
		assert.Equal(t, "ts_hour", newSpec.Field(0).Name)
		assert.Equal(t, "id_bucket", newSpec.Field(1).Name)
	})
}

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
		assert.Equal(t, 3, addedField.SourceID())
		assert.Equal(t, 1002, addedField.FieldID)
		assert.Equal(t, "year_transform", addedField.Name)
		assert.Equal(t, iceberg.YearTransform{}, addedField.Transform)

		addedField = newSpec.FieldsBySourceID(7)[0]
		assert.Equal(t, 7, addedField.SourceID())
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

	for _, tt := range []struct {
		name      string
		transform iceberg.Transform
	}{
		{"bucket", iceberg.BucketTransform{NumBuckets: 0}},
		{"truncate", iceberg.TruncateTransform{Width: 0}},
	} {
		t.Run("reject invalid "+tt.name+" transform parameters", func(t *testing.T) {
			specUpdate := table.NewUpdateSpec(testNonPartitionedTable.NewTransaction(), true)
			updates, reqs, err := specUpdate.
				AddField("id", tt.transform, "invalid_transform").
				BuildUpdates()

			require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
			assert.ErrorContains(t, err, "invalid partition transform")
			assert.Nil(t, updates)
			assert.Nil(t, reqs)
		})
	}

	t.Run("add unknown transform field", func(t *testing.T) {
		unknown, err := iceberg.ParseTransform("custom_transform[42]")
		require.NoError(t, err)

		txn = testPartitionedTable.NewTransaction()
		updates, reqs, err := table.NewUpdateSpec(txn, true).
			AddField("id", unknown, "id_custom").
			BuildUpdates()
		require.ErrorIs(t, err, iceberg.ErrInvalidTransform)
		assert.ErrorContains(t, err, "custom_transform[42]")
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

		// Added-vs-added, and it reports the same sentinel and wording as the
		// existing-vs-added case
		require.ErrorIs(t, err, iceberg.ErrInvalidPartitionSpec)
		assert.ErrorContains(t, err, "redundant partition field")
		assert.ErrorContains(t, err, "ts_month (month) conflicts with ts_year (year)")
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

	t.Run("reject geometry source for identity partition transform", func(t *testing.T) {
		geoSchema := iceberg.NewSchema(1,
			iceberg.NestedField{ID: 1, Name: "id", Required: true, Type: iceberg.PrimitiveTypes.Int64},
			iceberg.NestedField{ID: 2, Name: "geom", Required: false, Type: iceberg.GeometryType{}},
		)
		metadata, err := table.NewMetadata(geoSchema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, "", iceberg.Properties{
			table.PropertyFormatVersion: "3",
		})
		assert.NoError(t, err)

		tbl := table.New([]string{"geo_geometry"}, metadata, "", nil, nil)
		specUpdate := table.NewUpdateSpec(tbl.NewTransaction(), true)

		updates, reqs, err := specUpdate.
			AddField("geom", iceberg.IdentityTransform{}, "geom_identity").
			BuildUpdates()
		assert.Error(t, err)
		assert.Nil(t, updates)
		assert.Nil(t, reqs)
	})

	t.Run("reject geography source for identity partition transform", func(t *testing.T) {
		geog, err := iceberg.GeographyTypeOf("srid:4269", "karney")
		assert.NoError(t, err)

		geoSchema := iceberg.NewSchema(1,
			iceberg.NestedField{ID: 1, Name: "id", Required: true, Type: iceberg.PrimitiveTypes.Int64},
			iceberg.NestedField{ID: 2, Name: "geog", Required: false, Type: geog},
		)
		metadata, err := table.NewMetadata(geoSchema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, "", iceberg.Properties{
			table.PropertyFormatVersion: "3",
		})
		assert.NoError(t, err)

		tbl := table.New([]string{"geo_geography"}, metadata, "", nil, nil)
		specUpdate := table.NewUpdateSpec(tbl.NewTransaction(), true)

		updates, reqs, err := specUpdate.
			AddField("geog", iceberg.IdentityTransform{}, "geog_identity").
			BuildUpdates()
		assert.Error(t, err)
		assert.ErrorContains(t, err, "cannot transform")
		assert.ErrorContains(t, err, "geog")
		assert.Nil(t, updates)
		assert.Nil(t, reqs)
	})
}

func TestUpdateSpecReadsStagedTransactionMetadata(t *testing.T) {
	t.Run("end-to-end: partition by column added earlier in the same transaction", func(t *testing.T) {
		txn := testNonPartitionedTable.NewTransaction()

		require.NoError(t, txn.UpdateSchema(false, false).
			AddColumn([]string{"new_col"}, iceberg.PrimitiveTypes.String, "", false, nil).
			Commit())

		require.NoError(t, txn.UpdateSpec(false).
			AddField("new_col", iceberg.IdentityTransform{}, "new_col_identity").
			Commit())

		stagedTbl, err := txn.StagedTable()
		require.NoError(t, err)

		// Resolve the new column's id from the staged schema rather than
		// hard-coding it, so the test does not silently break if the fixture
		// schema changes.
		newCol, ok := stagedTbl.Schema().FindFieldByName("new_col")
		require.True(t, ok)

		spec := stagedTbl.Spec()
		added := spec.FieldsBySourceID(newCol.ID)
		require.Len(t, added, 1)
		assert.Equal(t, "new_col_identity", added[0].Name)
		assert.Equal(t, iceberg.IdentityTransform{}, added[0].Transform)
		assert.Equal(t, iceberg.PartitionDataIDStart, added[0].FieldID)
	})

	t.Run("auto-generated partition name resolves against the staged schema", func(t *testing.T) {
		txn := testNonPartitionedTable.NewTransaction()

		require.NoError(t, txn.UpdateSchema(false, false).
			AddColumn([]string{"new_col"}, iceberg.PrimitiveTypes.String, "", false, nil).
			Commit())

		// An empty target name forces GeneratePartitionFieldName, which must
		// resolve the source column against the staged schema.
		specUpdate := txn.UpdateSpec(false)
		_, _, err := specUpdate.
			AddField("new_col", iceberg.IdentityTransform{}, "").
			BuildUpdates()
		require.NoError(t, err)

		stagedTbl, err := txn.StagedTable()
		require.NoError(t, err)
		newCol, ok := stagedTbl.Schema().FindFieldByName("new_col")
		require.True(t, ok)

		newSpec, err := specUpdate.Apply()
		require.NoError(t, err)
		added := newSpec.FieldsBySourceID(newCol.ID)
		require.Len(t, added, 1)
		assert.Equal(t, "new_col", added[0].Name)
	})

	t.Run("partition by renamed column staged in the same transaction", func(t *testing.T) {
		txn := testNonPartitionedTable.NewTransaction()

		// Rename an existing column, staging it into the transaction.
		require.NoError(t, txn.UpdateSchema(false, false).
			RenameColumn([]string{"name"}, "full_name").
			Commit())

		// The old name must no longer bind against the staged schema.
		_, _, err := txn.UpdateSpec(false).
			AddField("name", iceberg.IdentityTransform{}, "name_identity").
			BuildUpdates()
		require.Error(t, err)
		assert.ErrorContains(t, err, "could not bind reference")

		// The new name must bind and produce a partition field on the same source column id.
		specUpdate := txn.UpdateSpec(false)
		_, _, err = specUpdate.
			AddField("full_name", iceberg.IdentityTransform{}, "full_name_identity").
			BuildUpdates()
		require.NoError(t, err)

		newSpec, err := specUpdate.Apply()
		require.NoError(t, err)
		added := newSpec.FieldsBySourceID(2)
		require.Len(t, added, 1)
		assert.Equal(t, "full_name_identity", added[0].Name)
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
		assert.Equal(t, 3, addedField.SourceID())
		assert.Equal(t, 1002, addedField.FieldID)
		assert.Equal(t, "ts", addedField.Name)
		assert.Equal(t, iceberg.IdentityTransform{}, addedField.Transform)

		addedField = newSpec.FieldsBySourceID(2)[0]
		assert.Equal(t, 2, addedField.SourceID())
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
		assert.Equal(t, 6, addedField.SourceID())
		assert.Equal(t, 1002, addedField.FieldID)
		assert.Equal(t, "address.city_trunc_3", addedField.Name)
		assert.Equal(t, iceberg.TruncateTransform{Width: 3}, addedField.Transform)

		addedIdentity := currSpec.FieldsBySourceID(7)[0]
		assert.Equal(t, 7, addedIdentity.SourceID())
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

func newPartitionedTableWithVersion(t *testing.T, version string) *table.Table {
	t.Helper()
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1},
		FieldID:   iceberg.PartitionDataIDStart,
		Name:      "id_identity",
		Transform: iceberg.IdentityTransform{},
	})
	metadata, err := table.NewMetadata(testSchema, &spec, table.UnsortedSortOrder, "", iceberg.Properties{
		table.PropertyFormatVersion: version,
	})
	require.NoError(t, err)

	return table.New([]string{"partitioned_v" + version}, metadata, "", nil, nil)
}

// TestUpdateSpecReuseSameUpdate covers the rewriteDeleteAndAddField path: a
// field removed and re-added within the *same* update must keep its permanent
// field ID (undo-delete plus optional rename), regardless of the requested
// name.
func TestUpdateSpecReuseSameUpdate(t *testing.T) {
	t.Run("re-add without a name keeps the current field ID and name", func(t *testing.T) {
		specUpdate := table.NewUpdateSpec(testPartitionedTable.NewTransaction(), false)
		_, _, err := specUpdate.
			RemoveField("id_identity").
			AddField("id", iceberg.IdentityTransform{}, "").
			BuildUpdates()
		require.NoError(t, err)

		newSpec, err := specUpdate.Apply()
		require.NoError(t, err)

		reAdded := newSpec.FieldsBySourceID(1)
		require.Len(t, reAdded, 1)
		assert.Equal(t, iceberg.PartitionDataIDStart, reAdded[0].FieldID)
		assert.Equal(t, "id_identity", reAdded[0].Name)
		assert.Equal(t, iceberg.IdentityTransform{}, reAdded[0].Transform)

		// The untouched field must remain unchanged, and no new ID may be
		// allocated on the reuse path.
		untouched := newSpec.FieldsBySourceID(5)
		require.Len(t, untouched, 1)
		assert.Equal(t, iceberg.PartitionDataIDStart+1, untouched[0].FieldID)
		assert.Equal(t, "street_void", untouched[0].Name)
		assert.Equal(t, iceberg.PartitionDataIDStart+1, newSpec.LastAssignedFieldID())
	})

	t.Run("re-add with the matching name keeps the current field ID", func(t *testing.T) {
		specUpdate := table.NewUpdateSpec(testPartitionedTable.NewTransaction(), false)
		_, _, err := specUpdate.
			RemoveField("id_identity").
			AddField("id", iceberg.IdentityTransform{}, "id_identity").
			BuildUpdates()
		require.NoError(t, err)

		newSpec, err := specUpdate.Apply()
		require.NoError(t, err)

		reAdded := newSpec.FieldsBySourceID(1)
		require.Len(t, reAdded, 1)
		assert.Equal(t, iceberg.PartitionDataIDStart, reAdded[0].FieldID)
		assert.Equal(t, "id_identity", reAdded[0].Name)
		assert.Equal(t, iceberg.IdentityTransform{}, reAdded[0].Transform)
		assert.Equal(t, iceberg.PartitionDataIDStart+1, newSpec.LastAssignedFieldID())
	})

	t.Run("re-add with a different name keeps the ID and renames the field", func(t *testing.T) {
		specUpdate := table.NewUpdateSpec(testPartitionedTable.NewTransaction(), false)
		_, _, err := specUpdate.
			RemoveField("id_identity").
			AddField("id", iceberg.IdentityTransform{}, "id_renamed").
			BuildUpdates()
		require.NoError(t, err)

		newSpec, err := specUpdate.Apply()
		require.NoError(t, err)

		reAdded := newSpec.FieldsBySourceID(1)
		require.Len(t, reAdded, 1)
		assert.Equal(t, iceberg.PartitionDataIDStart, reAdded[0].FieldID)
		assert.Equal(t, "id_renamed", reAdded[0].Name)
		assert.Equal(t, iceberg.IdentityTransform{}, reAdded[0].Transform)
		assert.Equal(t, iceberg.PartitionDataIDStart+1, newSpec.LastAssignedFieldID())
	})

	t.Run("reuse does not bump the field-ID counter for later additions", func(t *testing.T) {
		// A field added *after* a reuse must get the next sequential ID; if the
		// reuse path wrongly bumped the counter, this add would skip an ID.
		specUpdate := table.NewUpdateSpec(testPartitionedTable.NewTransaction(), false)
		_, _, err := specUpdate.
			RemoveField("id_identity").
			AddField("id", iceberg.IdentityTransform{}, "").                      // reuse -> 1000
			AddField("address.zip_code", iceberg.IdentityTransform{}, "zip_new"). // new -> 1002
			BuildUpdates()
		require.NoError(t, err)

		newSpec, err := specUpdate.Apply()
		require.NoError(t, err)

		reused := newSpec.FieldsBySourceID(1)
		require.Len(t, reused, 1)
		assert.Equal(t, iceberg.PartitionDataIDStart, reused[0].FieldID)

		added := newSpec.FieldsBySourceID(7)
		require.Len(t, added, 1)
		assert.Equal(t, iceberg.PartitionDataIDStart+2, added[0].FieldID)
		assert.Equal(t, iceberg.PartitionDataIDStart+2, newSpec.LastAssignedFieldID())
	})
}

// TestUpdateSpecReuseHistoricalFieldID covers the partitionField historical
// lookup: a field removed in an *earlier committed* update lives only in the
// table's historical partition specs, and re-adding it on the same source +
// transform must recycle its permanent field ID on format v2+ tables.
func TestUpdateSpecReuseHistoricalFieldID(t *testing.T) {
	// removeAndCommit removes the named field and returns the resulting staged
	// table, whose current spec no longer contains that field but whose history still does.
	removeAndCommit := func(t *testing.T, tbl *table.Table, name string) *table.StagedTable {
		t.Helper()
		txn := tbl.NewTransaction()
		require.NoError(t, txn.UpdateSpec(false).RemoveField(name).Commit())
		staged, err := txn.StagedTable()
		require.NoError(t, err)

		return staged
	}

	t.Run("re-add without a name reuses the historical field ID (end-to-end)", func(t *testing.T) {
		staged := removeAndCommit(t, testPartitionedTable, "id_identity")
		removed := staged.Spec()
		require.Empty(t, removed.FieldsBySourceID(1), "field should be gone from the current spec")

		txn2 := staged.NewTransaction()
		require.NoError(t, txn2.UpdateSpec(false).AddField("id", iceberg.IdentityTransform{}, "").Commit())
		staged2, err := txn2.StagedTable()
		require.NoError(t, err)

		reAddedSpec := staged2.Spec()
		reAdded := reAddedSpec.FieldsBySourceID(1)
		require.Len(t, reAdded, 1)
		assert.Equal(t, iceberg.PartitionDataIDStart, reAdded[0].FieldID)
		assert.Equal(t, "id_identity", reAdded[0].Name)
		assert.Equal(t, iceberg.IdentityTransform{}, reAdded[0].Transform)
		// The reuse must not advance the table's last-assigned partition ID.
		require.NotNil(t, staged2.Metadata().LastPartitionSpecID())
		assert.Equal(t, iceberg.PartitionDataIDStart+1, *staged2.Metadata().LastPartitionSpecID())
	})

	t.Run("re-add with the matching name reuses the historical field ID", func(t *testing.T) {
		staged := removeAndCommit(t, testPartitionedTable, "id_identity")

		txn2 := staged.NewTransaction()
		require.NoError(t, txn2.UpdateSpec(false).AddField("id", iceberg.IdentityTransform{}, "id_identity").Commit())
		staged2, err := txn2.StagedTable()
		require.NoError(t, err)

		reAddedSpec := staged2.Spec()
		reAdded := reAddedSpec.FieldsBySourceID(1)
		require.Len(t, reAdded, 1)
		assert.Equal(t, iceberg.PartitionDataIDStart, reAdded[0].FieldID)
		assert.Equal(t, "id_identity", reAdded[0].Name)
		assert.Equal(t, iceberg.IdentityTransform{}, reAdded[0].Transform)
	})

	t.Run("re-add with a different name after a committed removal allocates a new ID", func(t *testing.T) {
		// Unlike the same-update case, once the removal is committed the field
		// only lives in history, so a different-name re-add is a brand-new
		// field and must receive a fresh ID.
		staged := removeAndCommit(t, testPartitionedTable, "id_identity")

		txn2 := staged.NewTransaction()
		require.NoError(t, txn2.UpdateSpec(false).AddField("id", iceberg.IdentityTransform{}, "id_renamed").Commit())
		staged2, err := txn2.StagedTable()
		require.NoError(t, err)

		reAddedSpec := staged2.Spec()
		reAdded := reAddedSpec.FieldsBySourceID(1)
		require.Len(t, reAdded, 1)
		assert.Equal(t, iceberg.PartitionDataIDStart+2, reAdded[0].FieldID)
		assert.Equal(t, "id_renamed", reAdded[0].Name)
	})

	t.Run("reuse applies to format version 3 tables", func(t *testing.T) {
		// Guards the Version() >= 2 boundary: v3 must reuse historical IDs too.
		staged := removeAndCommit(t, newPartitionedTableWithVersion(t, "3"), "id_identity")

		txn2 := staged.NewTransaction()
		require.NoError(t, txn2.UpdateSpec(false).AddField("id", iceberg.IdentityTransform{}, "").Commit())
		staged2, err := txn2.StagedTable()
		require.NoError(t, err)

		reAddedSpec := staged2.Spec()
		reAdded := reAddedSpec.FieldsBySourceID(1)
		require.Len(t, reAdded, 1)
		assert.Equal(t, iceberg.PartitionDataIDStart, reAdded[0].FieldID)
		assert.Equal(t, "id_identity", reAdded[0].Name)
	})

	t.Run("v1 tables do not reuse historical field IDs", func(t *testing.T) {
		// v1 lacks the permanent field-ID identity contract, so the historical
		// lookup is skipped: the removed field remains as a void tombstone and
		// the re-add gets a fresh, sequential ID with an auto-generated name.
		staged := removeAndCommit(t, newPartitionedTableWithVersion(t, "1"), "id_identity")

		txn2 := staged.NewTransaction()
		require.NoError(t, txn2.UpdateSpec(false).AddField("id", iceberg.IdentityTransform{}, "").Commit())
		staged2, err := txn2.StagedTable()
		require.NoError(t, err)

		reAddedSpec := staged2.Spec()
		reAdded := reAddedSpec.FieldsBySourceID(1)
		require.Len(t, reAdded, 2) // void tombstone + freshly-added identity field
		var identityField *iceberg.PartitionField
		for i := range reAdded {
			if _, isIdentity := reAdded[i].Transform.(iceberg.IdentityTransform); isIdentity {
				identityField = &reAdded[i]
			}
		}
		require.NotNil(t, identityField)
		assert.Equal(t, iceberg.PartitionDataIDStart+1, identityField.FieldID, "v1 must allocate a fresh ID, not reuse 1000")
		assert.Equal(t, "id", identityField.Name)
	})

	t.Run("parameterized transform mismatch is not reused", func(t *testing.T) {
		bucketSpec := iceberg.NewPartitionSpec(iceberg.PartitionField{
			SourceIDs: []int{1},
			FieldID:   iceberg.PartitionDataIDStart,
			Name:      "id_bucket",
			Transform: iceberg.BucketTransform{NumBuckets: 16},
		})
		metadata, err := table.NewMetadata(testSchema, &bucketSpec, table.UnsortedSortOrder, "", nil)
		require.NoError(t, err)
		bucketTable := table.New([]string{"bucketed"}, metadata, "", nil, nil)

		staged := removeAndCommit(t, bucketTable, "id_bucket")

		// bucket[8] must NOT match the historical bucket[16]; a fresh ID is used.
		txnMismatch := staged.NewTransaction()
		require.NoError(t, txnMismatch.UpdateSpec(false).AddField("id", iceberg.BucketTransform{NumBuckets: 8}, "").Commit())
		mismatchStaged, err := txnMismatch.StagedTable()
		require.NoError(t, err)
		mismatchSpec := mismatchStaged.Spec()
		mismatch := mismatchSpec.FieldsBySourceID(1)
		require.Len(t, mismatch, 1)
		assert.Equal(t, iceberg.PartitionDataIDStart+1, mismatch[0].FieldID)
		assert.Equal(t, iceberg.BucketTransform{NumBuckets: 8}, mismatch[0].Transform)

		// bucket[16] with the same parameter DOES match and reuses 1000.
		txnMatch := staged.NewTransaction()
		require.NoError(t, txnMatch.UpdateSpec(false).AddField("id", iceberg.BucketTransform{NumBuckets: 16}, "").Commit())
		matchStaged, err := txnMatch.StagedTable()
		require.NoError(t, err)
		matchSpec := matchStaged.Spec()
		match := matchSpec.FieldsBySourceID(1)
		require.Len(t, match, 1)
		assert.Equal(t, iceberg.PartitionDataIDStart, match[0].FieldID)
		assert.Equal(t, iceberg.BucketTransform{NumBuckets: 16}, match[0].Transform)
	})

	t.Run("lowest-spec-ID historical match wins for an unnamed re-add", func(t *testing.T) {
		// History contains two fields with the same source + transform but
		// different IDs/names (id 1000 "id_identity" in spec 0, id 1001 "id_v2"
		// added later). An unnamed re-add must deterministically resurrect the
		// lowest-spec-ID match (id 1000 "id_identity").
		staged := removeAndCommit(t, testPartitionedTable, "id_identity")

		txnAdd := staged.NewTransaction()
		require.NoError(t, txnAdd.UpdateSpec(false).AddField("id", iceberg.IdentityTransform{}, "id_v2").Commit())
		stagedAdd, err := txnAdd.StagedTable()
		require.NoError(t, err)
		addedSpec := stagedAdd.Spec()
		added := addedSpec.FieldsBySourceID(1)
		require.Len(t, added, 1)
		require.Equal(t, iceberg.PartitionDataIDStart+2, added[0].FieldID) // fresh ID 1002

		txnRemove := stagedAdd.NewTransaction()
		require.NoError(t, txnRemove.UpdateSpec(false).RemoveField("id_v2").Commit())
		stagedRemove, err := txnRemove.StagedTable()
		require.NoError(t, err)

		txnReAdd := stagedRemove.NewTransaction()
		require.NoError(t, txnReAdd.UpdateSpec(false).AddField("id", iceberg.IdentityTransform{}, "").Commit())
		stagedReAdd, err := txnReAdd.StagedTable()
		require.NoError(t, err)

		reAddedSpec := stagedReAdd.Spec()
		reAdded := reAddedSpec.FieldsBySourceID(1)
		require.Len(t, reAdded, 1)
		assert.Equal(t, iceberg.PartitionDataIDStart, reAdded[0].FieldID)
		assert.Equal(t, "id_identity", reAdded[0].Name)
	})

	t.Run("unnamed re-add returns the stored historical name after a source-column rename", func(t *testing.T) {
		// Remove the field, rename its source column, then re-add unnamed. The
		// reuse must return the *stored* historical partition-field name
		// ("id_identity"), not a name freshly generated from the renamed column
		// ("identifier"). The source ID is stable across the column rename, so
		// the historical match still applies and the ID is recycled.
		staged := removeAndCommit(t, testPartitionedTable, "id_identity")

		txnRename := staged.NewTransaction()
		require.NoError(t, txnRename.UpdateSchema(false, false).RenameColumn([]string{"id"}, "identifier").Commit())
		stagedRename, err := txnRename.StagedTable()
		require.NoError(t, err)

		txnReAdd := stagedRename.NewTransaction()
		require.NoError(t, txnReAdd.UpdateSpec(false).AddField("identifier", iceberg.IdentityTransform{}, "").Commit())
		stagedReAdd, err := txnReAdd.StagedTable()
		require.NoError(t, err)

		reAddedSpec := stagedReAdd.Spec()
		reAdded := reAddedSpec.FieldsBySourceID(1)
		require.Len(t, reAdded, 1)
		assert.Equal(t, iceberg.PartitionDataIDStart, reAdded[0].FieldID)
		assert.Equal(t, "id_identity", reAdded[0].Name)
		assert.Equal(t, iceberg.IdentityTransform{}, reAdded[0].Transform)
	})
}
