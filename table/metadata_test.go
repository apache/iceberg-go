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
	"encoding/json"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const ExampleTableMetadataV2 = `{
    "format-version": 2,
    "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
    "location": "s3://bucket/test/location",
    "last-sequence-number": 34,
    "last-updated-ms": 1602638573590,
    "last-column-id": 3,
    "current-schema-id": 1,
    "schemas": [
        {"type": "struct", "schema-id": 0, "fields": [{"id": 1, "name": "x", "required": true, "type": "long"}]},
        {
            "type": "struct",
            "schema-id": 1,
            "identifier-field-ids": [1, 2],
            "fields": [
                {"id": 1, "name": "x", "required": true, "type": "long"},
                {"id": 2, "name": "y", "required": true, "type": "long", "doc": "comment"},
                {"id": 3, "name": "z", "required": true, "type": "long"}
            ]
        }
    ],
    "default-spec-id": 0,
    "partition-specs": [{"spec-id": 0, "fields": [{"name": "x", "transform": "identity", "source-id": 1, "field-id": 1000}]}],
    "last-partition-id": 1000,
    "default-sort-order-id": 3,
    "sort-orders": [
        {
            "order-id": 3,
            "fields": [
                {"transform": "identity", "source-id": 2, "direction": "asc", "null-order": "nulls-first"},
                {"transform": "bucket[4]", "source-id": 3, "direction": "desc", "null-order": "nulls-last"}
            ]
        }
    ],
    "properties": {"read.split.target.size": "134217728"},
    "current-snapshot-id": 3055729675574597004,
    "snapshots": [
        {
            "snapshot-id": 3051729675574597004,
            "timestamp-ms": 1515100955770,
            "sequence-number": 0,
            "summary": {"operation": "append"},
            "manifest-list": "s3://a/b/1.avro"
        },
        {
            "snapshot-id": 3055729675574597004,
            "parent-snapshot-id": 3051729675574597004,
            "timestamp-ms": 1555100955770,
            "sequence-number": 1,
            "summary": {"operation": "append"},
            "manifest-list": "s3://a/b/2.avro",
            "schema-id": 1
        }
    ],
    "snapshot-log": [
        {"snapshot-id": 3051729675574597004, "timestamp-ms": 1515100955770},
        {"snapshot-id": 3055729675574597004, "timestamp-ms": 1555100955770}
    ],
    "metadata-log": [{"metadata-file": "s3://bucket/.../v1.json", "timestamp-ms": 1515100}],
    "refs": {"test": {"snapshot-id": 3051729675574597004, "type": "tag", "max-ref-age-ms": 10000000}}
}`

const ExampleTableMetadataV1 = `{
	"format-version": 1,
	"table-uuid": "d20125c8-7284-442c-9aea-15fee620737c",
	"location": "s3://bucket/test/location",
	"last-updated-ms": 1602638573874,
	"last-column-id": 3,
	"schema": {
		"type": "struct",
		"fields": [
			{"id": 1, "name": "x", "required": true, "type": "long"},
			{"id": 2, "name": "y", "required": true, "type": "long", "doc": "comment"},
			{"id": 3, "name": "z", "required": true, "type": "long"}
		]
	},
	"partition-spec": [{"name": "x", "transform": "identity", "source-id": 1, "field-id": 1000}],
	"properties": {},
	"current-snapshot-id": -1,
	"snapshots": [{"snapshot-id": 1925, "timestamp-ms": 1602638573822}]
}`

func TestMetadataV1Parsing(t *testing.T) {
	meta, err := table.ParseMetadataBytes([]byte(ExampleTableMetadataV1))
	require.NoError(t, err)
	require.NotNil(t, meta)

	assert.IsType(t, (*table.MetadataV1)(nil), meta)
	assert.Equal(t, 1, meta.Version())

	data := meta.(*table.MetadataV1)
	assert.Equal(t, uuid.MustParse("d20125c8-7284-442c-9aea-15fee620737c"), meta.TableUUID())
	assert.Equal(t, "s3://bucket/test/location", meta.Location())
	assert.Equal(t, int64(1602638573874), meta.LastUpdatedMillis())
	assert.Equal(t, 3, meta.LastColumnID())

	expected := iceberg.NewSchema(
		0,
		iceberg.NestedField{ID: 1, Name: "x", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "y", Type: iceberg.PrimitiveTypes.Int64, Required: true, Doc: "comment"},
		iceberg.NestedField{ID: 3, Name: "z", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	assert.Equal(t, []*iceberg.Schema{expected}, meta.Schemas())
	assert.Zero(t, data.SchemaList[0].ID)
	assert.True(t, meta.CurrentSchema().Equals(expected))
	assert.Equal(t, []iceberg.PartitionSpec{
		iceberg.NewPartitionSpec(iceberg.PartitionField{
			SourceID: 1, FieldID: 1000, Transform: iceberg.IdentityTransform{}, Name: "x",
		}),
	}, meta.PartitionSpecs())

	assert.Equal(t, iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceID: 1, FieldID: 1000, Transform: iceberg.IdentityTransform{}, Name: "x",
	}), meta.PartitionSpec())

	assert.Equal(t, 0, meta.DefaultPartitionSpec())
	assert.Equal(t, 1000, *meta.LastPartitionSpecID())
	assert.Nil(t, data.CurrentSnapshotID)
	assert.Nil(t, meta.CurrentSnapshot())
	assert.Len(t, meta.Snapshots(), 1)
	assert.NotNil(t, meta.SnapshotByID(1925))
	assert.Nil(t, meta.SnapshotByID(0))
	assert.Nil(t, meta.SnapshotByName("foo"))
	assert.Zero(t, data.DefaultSortOrderID)
	assert.Equal(t, table.UnsortedSortOrder, meta.SortOrder())
}

func TestMetadataV2Parsing(t *testing.T) {
	meta, err := table.ParseMetadataBytes([]byte(ExampleTableMetadataV2))
	require.NoError(t, err)
	require.NotNil(t, meta)

	assert.IsType(t, (*table.MetadataV2)(nil), meta)
	assert.Equal(t, 2, meta.Version())

	data := meta.(*table.MetadataV2)
	assert.Equal(t, uuid.MustParse("9c12d441-03fe-4693-9a96-a0705ddf69c1"), data.UUID)
	assert.Equal(t, "s3://bucket/test/location", data.Location())
	assert.Equal(t, 34, data.LastSequenceNumber)
	assert.Equal(t, int64(1602638573590), data.LastUpdatedMS)
	assert.Equal(t, 3, data.LastColumnId)
	assert.Equal(t, 0, data.SchemaList[0].ID)
	assert.Equal(t, 1, data.CurrentSchemaID)
	assert.Equal(t, 0, data.Specs[0].ID())
	assert.Equal(t, 0, data.DefaultSpecID)
	assert.Equal(t, 1000, *data.LastPartitionID)
	assert.EqualValues(t, "134217728", data.Props["read.split.target.size"])
	assert.EqualValues(t, 3055729675574597004, *data.CurrentSnapshotID)
	assert.EqualValues(t, 3051729675574597004, data.SnapshotList[0].SnapshotID)
	assert.Equal(t, int64(1515100955770), data.SnapshotLog[0].TimestampMs)
	assert.Equal(t, 3, data.SortOrderList[0].OrderID)
	assert.Equal(t, 3, data.DefaultSortOrderID)

	assert.Len(t, meta.Snapshots(), 2)
	assert.Equal(t, data.SnapshotList[1], *meta.CurrentSnapshot())
	assert.Equal(t, data.SnapshotList[0], *meta.SnapshotByName("test"))
	assert.EqualValues(t, "134217728", meta.Properties()["read.split.target.size"])
}

func TestParsingCorrectTypes(t *testing.T) {
	var meta table.MetadataV2
	require.NoError(t, json.Unmarshal([]byte(ExampleTableMetadataV2), &meta))

	assert.IsType(t, &iceberg.Schema{}, meta.SchemaList[0])
	assert.IsType(t, iceberg.NestedField{}, meta.SchemaList[0].Field(0))
	assert.IsType(t, iceberg.PrimitiveTypes.Int64, meta.SchemaList[0].Field(0).Type)
}

func TestSerializeMetadataV1(t *testing.T) {
	var meta table.MetadataV1
	require.NoError(t, json.Unmarshal([]byte(ExampleTableMetadataV1), &meta))

	data, err := json.Marshal(&meta)
	require.NoError(t, err)

	assert.JSONEq(t, `{"location": "s3://bucket/test/location", "table-uuid": "d20125c8-7284-442c-9aea-15fee620737c", "last-updated-ms": 1602638573874, "last-column-id": 3, "schemas": [{"type": "struct", "fields": [{"id": 1, "name": "x", "type": "long", "required": true}, {"id": 2, "name": "y", "type": "long", "required": true, "doc": "comment"}, {"id": 3, "name": "z", "type": "long", "required": true}], "schema-id": 0, "identifier-field-ids": []}], "current-schema-id": 0, "partition-specs": [{"spec-id": 0, "fields": [{"source-id": 1, "field-id": 1000, "transform": "identity", "name": "x"}]}], "default-spec-id": 0, "last-partition-id": 1000, "snapshots": [{"snapshot-id": 1925, "sequence-number": 0, "timestamp-ms": 1602638573822}], "sort-orders": [{"order-id": 0, "fields": []}], "format-version": 1, "schema": {"type": "struct", "fields": [{"id": 1, "name": "x", "type": "long", "required": true}, {"id": 2, "name": "y", "type": "long", "required": true, "doc": "comment"}, {"id": 3, "name": "z", "type": "long", "required": true}], "schema-id": 0, "identifier-field-ids": []}, "partition-spec": [{"name": "x", "transform": "identity", "source-id": 1, "field-id": 1000}]}`,
		string(data))
}

func TestSerializeMetadataV2(t *testing.T) {
	var meta table.MetadataV2
	require.NoError(t, json.Unmarshal([]byte(ExampleTableMetadataV2), &meta))

	data, err := json.Marshal(&meta)
	require.NoError(t, err)

	assert.JSONEq(t, `{"location": "s3://bucket/test/location", "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1", "last-updated-ms": 1602638573590, "last-column-id": 3, "schemas": [{"type": "struct", "fields": [{"id": 1, "name": "x", "type": "long", "required": true}], "schema-id": 0, "identifier-field-ids": []}, {"type": "struct", "fields": [{"id": 1, "name": "x", "type": "long", "required": true}, {"id": 2, "name": "y", "type": "long", "required": true, "doc": "comment"}, {"id": 3, "name": "z", "type": "long", "required": true}], "schema-id": 1, "identifier-field-ids": [1, 2]}], "current-schema-id": 1, "partition-specs": [{"spec-id": 0, "fields": [{"source-id": 1, "field-id": 1000, "transform": "identity", "name": "x"}]}], "default-spec-id": 0, "last-partition-id": 1000, "properties": {"read.split.target.size": "134217728"}, "current-snapshot-id": 3055729675574597004, "snapshots": [{"snapshot-id": 3051729675574597004, "sequence-number": 0, "timestamp-ms": 1515100955770, "manifest-list": "s3://a/b/1.avro", "summary": {"operation": "append"}}, {"snapshot-id": 3055729675574597004, "parent-snapshot-id": 3051729675574597004, "sequence-number": 1, "timestamp-ms": 1555100955770, "manifest-list": "s3://a/b/2.avro", "summary": {"operation": "append"}, "schema-id": 1}], "snapshot-log": [{"snapshot-id": 3051729675574597004, "timestamp-ms": 1515100955770}, {"snapshot-id": 3055729675574597004, "timestamp-ms": 1555100955770}], "metadata-log": [{"metadata-file": "s3://bucket/.../v1.json", "timestamp-ms": 1515100}], "sort-orders": [{"order-id": 3, "fields": [{"source-id": 2, "transform": "identity", "direction": "asc", "null-order": "nulls-first"}, {"source-id": 3, "transform": "bucket[4]", "direction": "desc", "null-order": "nulls-last"}]}], "default-sort-order-id": 3, "refs": {"test": {"snapshot-id": 3051729675574597004, "type": "tag", "max-ref-age-ms": 10000000}, "main": {"snapshot-id": 3055729675574597004, "type": "branch"}}, "format-version": 2, "last-sequence-number": 34}`,
		string(data))
}

func TestInvalidFormatVersion(t *testing.T) {
	metadataInvalidFormat := `{
        "format-version": -1,
        "table-uuid": "d20125c8-7284-442c-9aea-15fee620737c",
        "location": "s3://bucket/test/location",
        "last-updated-ms": 1602638573874,
        "last-column-id": 3,
        "schema": {
            "type": "struct",
            "fields": [
                {"id": 1, "name": "x", "required": true, "type": "long"},
                {"id": 2, "name": "y", "required": true, "type": "long", "doc": "comment"},
                {"id": 3, "name": "z", "required": true, "type": "long"}
            ]
        },
        "partition-spec": [{"name": "x", "transform": "identity", "source-id": 1, "field-id": 1000}],
        "properties": {},
        "current-snapshot-id": -1,
        "snapshots": []
    }`

	_, err := table.ParseMetadataBytes([]byte(metadataInvalidFormat))
	assert.Error(t, err)
	assert.ErrorIs(t, err, table.ErrInvalidMetadataFormatVersion)
}

func TestCurrentSchemaNotFound(t *testing.T) {
	schemaNotFound := `{
        "format-version": 2,
        "table-uuid": "d20125c8-7284-442c-9aea-15fee620737c",
        "location": "s3://bucket/test/location",
        "last-updated-ms": 1602638573874,
        "last-column-id": 3,
        "schemas": [
            {"type": "struct", "schema-id": 0, "fields": [{"id": 1, "name": "x", "required": true, "type": "long"}]},
            {
                "type": "struct",
                "schema-id": 1,
                "identifier-field-ids": [1, 2],
                "fields": [
                    {"id": 1, "name": "x", "required": true, "type": "long"},
                    {"id": 2, "name": "y", "required": true, "type": "long", "doc": "comment"},
                    {"id": 3, "name": "z", "required": true, "type": "long"}
                ]
            }
        ],
        "current-schema-id": 2,
        "default-spec-id": 0,
        "partition-specs": [{"spec-id": 0, "fields": [{"name": "x", "transform": "identity", "source-id": 1, "field-id": 1000}]}],
        "last-partition-id": 1000,
        "default-sort-order-id": 0,
        "properties": {},
        "current-snapshot-id": -1,
        "snapshots": []
    }`

	_, err := table.ParseMetadataBytes([]byte(schemaNotFound))
	assert.Error(t, err)
	assert.ErrorIs(t, err, table.ErrInvalidMetadata)
	assert.ErrorContains(t, err, "current-schema-id 2 can't be found in any schema")
}

func TestSortOrderNotFound(t *testing.T) {
	metadataSortOrderNotFound := `{
        "format-version": 2,
        "table-uuid": "d20125c8-7284-442c-9aea-15fee620737c",
        "location": "s3://bucket/test/location",
        "last-updated-ms": 1602638573874,
        "last-column-id": 3,
        "schemas": [
            {
                "type": "struct",
                "schema-id": 0,
                "identifier-field-ids": [1, 2],
                "fields": [
                    {"id": 1, "name": "x", "required": true, "type": "long"},
                    {"id": 2, "name": "y", "required": true, "type": "long", "doc": "comment"},
                    {"id": 3, "name": "z", "required": true, "type": "long"}
                ]
            }
        ],
        "default-sort-order-id": 4,
        "sort-orders": [
            {
                "order-id": 3,
                "fields": [
                    {"transform": "identity", "source-id": 2, "direction": "asc", "null-order": "nulls-first"},
                    {"transform": "bucket[4]", "source-id": 3, "direction": "desc", "null-order": "nulls-last"}
                ]
            }
        ],
        "current-schema-id": 0,
        "default-spec-id": 0,
        "partition-specs": [{"spec-id": 0, "fields": [{"name": "x", "transform": "identity", "source-id": 1, "field-id": 1000}]}],
        "last-partition-id": 1000,
        "properties": {},
        "current-snapshot-id": -1,
        "snapshots": []
    }`

	_, err := table.ParseMetadataBytes([]byte(metadataSortOrderNotFound))
	assert.Error(t, err)
	assert.ErrorIs(t, err, table.ErrInvalidMetadata)
	assert.ErrorContains(t, err, "default-sort-order-id 4 can't be found in [3: [\n2 asc nulls-first\nbucket[4](3) desc nulls-last\n]]")
}

func TestSortOrderUnsorted(t *testing.T) {
	sortOrderUnsorted := `{
        "format-version": 2,
        "table-uuid": "d20125c8-7284-442c-9aea-15fee620737c",
        "location": "s3://bucket/test/location",
        "last-updated-ms": 1602638573874,
        "last-column-id": 3,
        "schemas": [
            {
                "type": "struct",
                "schema-id": 0,
                "identifier-field-ids": [1, 2],
                "fields": [
                    {"id": 1, "name": "x", "required": true, "type": "long"},
                    {"id": 2, "name": "y", "required": true, "type": "long", "doc": "comment"},
                    {"id": 3, "name": "z", "required": true, "type": "long"}
                ]
            }
        ],
        "default-sort-order-id": 0,
        "sort-orders": [],
        "current-schema-id": 0,
        "default-spec-id": 0,
        "partition-specs": [{"spec-id": 0, "fields": [{"name": "x", "transform": "identity", "source-id": 1, "field-id": 1000}]}],
        "last-partition-id": 1000,
        "properties": {},
        "current-snapshot-id": -1,
        "snapshots": []
    }`

	var meta table.MetadataV2
	require.NoError(t, json.Unmarshal([]byte(sortOrderUnsorted), &meta))

	assert.Equal(t, table.UnsortedSortOrderID, meta.DefaultSortOrderID)
	assert.Len(t, meta.SortOrderList, 0)
}

func TestInvalidPartitionSpecID(t *testing.T) {
	invalidSpecID := `{
        "format-version": 2,
        "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
        "location": "s3://bucket/test/location",
        "last-sequence-number": 34,
        "last-updated-ms": 1602638573590,
        "last-column-id": 3,
        "current-schema-id": 1,
        "schemas": [
            {"type": "struct", "schema-id": 0, "fields": [{"id": 1, "name": "x", "required": true, "type": "long"}]},
            {
                "type": "struct",
                "schema-id": 1,
                "identifier-field-ids": [1, 2],
                "fields": [
                    {"id": 1, "name": "x", "required": true, "type": "long"},
                    {"id": 2, "name": "y", "required": true, "type": "long", "doc": "comment"},
                    {"id": 3, "name": "z", "required": true, "type": "long"}
                ]
            }
        ],
        "sort-orders": [],
        "default-sort-order-id": 0,
        "default-spec-id": 1,
        "partition-specs": [{"spec-id": 0, "fields": [{"name": "x", "transform": "identity", "source-id": 1, "field-id": 1000}]}],
        "last-partition-id": 1000
    }`

	var meta table.MetadataV2
	err := json.Unmarshal([]byte(invalidSpecID), &meta)
	assert.ErrorIs(t, err, table.ErrInvalidMetadata)
	assert.ErrorContains(t, err, "default-spec-id 1 can't be found")
}

func TestV2RefCreation(t *testing.T) {
	var meta table.MetadataV2
	require.NoError(t, json.Unmarshal([]byte(ExampleTableMetadataV2), &meta))

	maxRefAge := int64(10000000)
	assert.Equal(t, map[string]table.SnapshotRef{
		"main": {
			SnapshotID:      3055729675574597004,
			SnapshotRefType: table.BranchRef,
		},
		"test": {
			SnapshotID:      3051729675574597004,
			SnapshotRefType: table.TagRef,
			MaxRefAgeMs:     &maxRefAge,
		},
	}, meta.Refs)
}

func TestV1WriteMetadataToV2(t *testing.T) {
	// https://iceberg.apache.org/spec/#version-2
	//
	// Table metadata JSON:
	//     - last-sequence-number was added and is required; default to 0 when reading v1 metadata
	//     - table-uuid is now required
	//     - current-schema-id is now required
	//     - schemas is now required
	//     - partition-specs is now required
	//     - default-spec-id is now required
	//     - last-partition-id is now required
	//     - sort-orders is now required
	//     - default-sort-order-id is now required
	//     - schema is no longer required and should be omitted; use schemas and current-schema-id instead
	//     - partition-spec is no longer required and should be omitted; use partition-specs and default-spec-id instead

	minimalV1Example := `{
		"format-version": 1,
		"location": "s3://bucket/test/location",
		"last-updated-ms": 1062638573874,
		"last-column-id": 3,
		"schema": {
			"type": "struct",
			"fields": [
				{"id": 1, "name": "x", "required": true, "type": "long"},
				{"id": 2, "name": "y", "required": true, "type": "long", "doc": "comment"},
				{"id": 3, "name": "z", "required": true, "type": "long"}
			]
		},
		"partition-spec": [{"name": "x", "transform": "identity", "source-id": 1, "field-id": 1000}],
		"properties": {},
		"current-snapshot-id": -1,
		"snapshots": [{"snapshot-id": 1925, "timestamp-ms": 1602638573822}]
	}`

	meta, err := table.ParseMetadataString(minimalV1Example)
	require.NoError(t, err)
	assert.IsType(t, (*table.MetadataV1)(nil), meta)

	metaV2 := meta.(*table.MetadataV1).ToV2()
	metaV2Json, err := json.Marshal(metaV2)
	require.NoError(t, err)

	rawData := make(map[string]any)
	require.NoError(t, json.Unmarshal(metaV2Json, &rawData))

	assert.EqualValues(t, 0, rawData["last-sequence-number"])
	assert.NotEmpty(t, rawData["table-uuid"])
	assert.EqualValues(t, 0, rawData["current-schema-id"])
	assert.Equal(t, []any{map[string]any{
		"fields": []any{
			map[string]any{"id": float64(1), "name": "x", "required": true, "type": "long"},
			map[string]any{"id": float64(2), "name": "y", "required": true, "type": "long", "doc": "comment"},
			map[string]any{"id": float64(3), "name": "z", "required": true, "type": "long"},
		},
		"identifier-field-ids": []any{},
		"schema-id":            float64(0),
		"type":                 "struct",
	}}, rawData["schemas"])
	assert.Equal(t, []any{map[string]any{
		"spec-id": float64(0),
		"fields": []any{map[string]any{
			"name": "x", "transform": "identity",
			"source-id": float64(1), "field-id": float64(1000),
		}},
	}}, rawData["partition-specs"])

	assert.Zero(t, rawData["default-spec-id"])
	assert.EqualValues(t, 1000, rawData["last-partition-id"])
	assert.Zero(t, rawData["default-sort-order-id"])
	assert.Equal(t, []any{map[string]any{"order-id": float64(0), "fields": []any{}}}, rawData["sort-orders"])
	assert.NotContains(t, rawData, "schema")
	assert.NotContains(t, rawData, "partition-spec")
}

func TestV1_MetadataBuilder(t *testing.T) {
	const exampleV1Metadata = `{
	"format-version": 1,
	"table-uuid": "d20125c8-7284-442c-9aea-15fee620737c",
	"location": "s3://bucket/test/location",
	"last-updated-ms": 1602638573874,
	"last-column-id": 3,
	"schema": {
		"type": "struct",
		"fields": [
			{"id": 1, "name": "x", "required": true, "type": "long"},
			{"id": 2, "name": "y", "required": true, "type": "long", "doc": "comment"},
			{"id": 3, "name": "z", "required": true, "type": "long"}
		]
	},
    "partition-specs": [{"spec-id": 0, "fields": [{"name": "x", "transform": "identity", "source-id": 1, "field-id": 1000}]}],
	"current-snapshot-id": -1,
	"snapshots": [{"snapshot-id": 1925, "timestamp-ms": 1602638573822}]
}`
	meta, err := table.ParseMetadataBytes([]byte(exampleV1Metadata))
	require.NoError(t, err)
	require.NotNil(t, meta)

	schema := iceberg.NewSchema(
		0,
		iceberg.NestedField{ID: 1, Name: "x", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "y", Type: iceberg.PrimitiveTypes.Int64, Required: true, Doc: "comment"},
		iceberg.NestedField{ID: 3, Name: "z", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	md := table.NewMetadataV1Builder(
		"s3://bucket/test/location",
		schema,
		1602638573874,
		3,
	).
		WithTableUUID(uuid.MustParse("d20125c8-7284-442c-9aea-15fee620737c")).
		WithPartitionSpecs([]iceberg.PartitionSpec{
			iceberg.NewPartitionSpec(iceberg.PartitionField{
				SourceID: 1, FieldID: 1000, Transform: iceberg.IdentityTransform{}, Name: "x",
			})}).
		WithCurrentSnapshotID(-1).
		WithSnapshots([]table.Snapshot{
			{SnapshotID: 1925, TimestampMs: 1602638573822},
		}).Build()

	b, err := json.Marshal(md)
	require.NoError(t, err)

	var built table.MetadataV1
	json.Unmarshal(b, &built)

	require.Equal(t, meta, &built)
}
