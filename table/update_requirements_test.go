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

const (
	reqTestSnapshotID   int64 = 3051729675574597004
	reqTestCurrentSnap  int64 = 3055729675574597004
	reqTestLastColumnID       = 3
	reqTestSchemaID           = 1
	reqTestLastPartID         = 1000
	reqTestSpecID             = 0
	reqTestSortOrderID        = 3
)

var reqTestUUID = uuid.MustParse("9c12d441-03fe-4693-9a96-a0705ddf69c1")

// requirementsTestMetadata is ExampleTableMetadataV2 plus a non-main branch,
// so branch-sensitive updates have something to assert.
func requirementsTestMetadata(t *testing.T) table.Metadata {
	t.Helper()

	js := strings.Replace(table.ExampleTableMetadataV2,
		`"refs": {`,
		`"refs": {"dev": {"snapshot-id": 3051729675574597004, "type": "branch"}, `, 1)
	meta, err := table.ParseMetadataString(js)
	require.NoError(t, err)

	return meta
}

func newSchemaForRequirements() *iceberg.Schema {
	return iceberg.NewSchema(2,
		iceberg.NestedField{ID: 1, Name: "x", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 4, Name: "w", Type: iceberg.PrimitiveTypes.String},
	)
}

func TestRequirementsForUpdateTableNilBase(t *testing.T) {
	_, err := table.RequirementsForUpdateTable(nil, nil)
	assert.ErrorIs(t, err, iceberg.ErrInvalidArgument)

	_, err = table.RequirementsForReplaceTable(nil, nil)
	assert.ErrorIs(t, err, iceberg.ErrInvalidArgument)
}

func TestRequirementsForCreateTable(t *testing.T) {
	assert.Equal(t, []table.Requirement{table.AssertCreate()}, table.RequirementsForCreateTable(nil))

	// With no base, schema/spec/order updates add nothing beyond assert-create.
	spec := iceberg.NewPartitionSpec()
	order := table.UnsortedSortOrder
	reqs := table.RequirementsForCreateTable([]table.Update{
		table.NewAssignUUIDUpdate(reqTestUUID),
		table.NewAddSchemaUpdate(newSchemaForRequirements()),
		table.NewSetCurrentSchemaUpdate(-1),
		table.NewAddPartitionSpecUpdate(&spec, true),
		table.NewSetDefaultSpecUpdate(-1),
		table.NewAddSortOrderUpdate(&order),
		table.NewSetDefaultSortOrderUpdate(-1),
		table.NewSetSnapshotRefUpdate(table.MainBranch, 1, table.BranchRef, 0, 0, 0),
	})
	assert.Equal(t, []table.Requirement{table.AssertCreate()}, reqs)
}

func TestRequirementsForUpdateTable(t *testing.T) {
	meta := requirementsTestMetadata(t)
	uuidReq := table.AssertTableUUID(reqTestUUID)
	spec := iceberg.NewPartitionSpec()
	order := table.UnsortedSortOrder

	tests := []struct {
		name    string
		updates []table.Update
		want    []table.Requirement
	}{
		{
			name: "empty",
			want: []table.Requirement{uuidReq},
		},
		{
			name: "updates without requirements",
			updates: []table.Update{
				table.NewAssignUUIDUpdate(reqTestUUID),
				table.NewUpgradeFormatVersionUpdate(3),
				table.NewAddSortOrderUpdate(&order),
				table.NewSetPropertiesUpdate(iceberg.Properties{"a": "b"}),
				table.NewRemovePropertiesUpdate([]string{"a"}),
				table.NewSetLocationUpdate("s3://bucket/other"),
				table.NewRemoveSnapshotRefUpdate("dev"),
			},
			want: []table.Requirement{uuidReq},
		},
		{
			name: "add-schema is asserted once",
			updates: []table.Update{
				table.NewAddSchemaUpdate(newSchemaForRequirements()),
				table.NewAddSchemaUpdate(newSchemaForRequirements()),
			},
			want: []table.Requirement{uuidReq, table.AssertLastAssignedFieldID(reqTestLastColumnID)},
		},
		{
			name: "set-current-schema is asserted once",
			updates: []table.Update{
				table.NewSetCurrentSchemaUpdate(0),
				table.NewSetCurrentSchemaUpdate(1),
			},
			want: []table.Requirement{uuidReq, table.AssertCurrentSchemaID(reqTestSchemaID)},
		},
		{
			name: "add-spec is asserted once",
			updates: []table.Update{
				table.NewAddPartitionSpecUpdate(&spec, false),
				table.NewAddPartitionSpecUpdate(&spec, false),
			},
			want: []table.Requirement{uuidReq, table.AssertLastAssignedPartitionID(reqTestLastPartID)},
		},
		{
			name: "set-default-spec is asserted once",
			updates: []table.Update{
				table.NewSetDefaultSpecUpdate(-1),
				table.NewSetDefaultSpecUpdate(0),
			},
			want: []table.Requirement{uuidReq, table.AssertDefaultSpecID(reqTestSpecID)},
		},
		{
			name: "set-default-sort-order is asserted once",
			updates: []table.Update{
				table.NewSetDefaultSortOrderUpdate(-1),
				table.NewSetDefaultSortOrderUpdate(0),
			},
			want: []table.Requirement{uuidReq, table.AssertDefaultSortOrderID(reqTestSortOrderID)},
		},
		{
			name: "remove-partition-specs asserts default spec and non-main branches",
			updates: []table.Update{
				table.NewRemoveSpecUpdate([]int{1}),
				table.NewRemoveSpecUpdate([]int{2}),
			},
			want: []table.Requirement{
				uuidReq,
				table.AssertDefaultSpecID(reqTestSpecID),
				table.AssertRefSnapshotID("dev", new(reqTestSnapshotID)),
			},
		},
		{
			name: "remove-schemas asserts current schema and non-main branches",
			updates: []table.Update{
				table.NewRemoveSchemasUpdate([]int{0}),
			},
			want: []table.Requirement{
				uuidReq,
				table.AssertCurrentSchemaID(reqTestSchemaID),
				table.AssertRefSnapshotID("dev", new(reqTestSnapshotID)),
			},
		},
		{
			name: "set-snapshot-ref asserts the base snapshot, or absence for a new ref",
			updates: []table.Update{
				table.NewSetSnapshotRefUpdate(table.MainBranch, 1, table.BranchRef, 0, 0, 0),
				table.NewSetSnapshotRefUpdate(table.MainBranch, 2, table.BranchRef, 0, 0, 0),
				table.NewSetSnapshotRefUpdate("new", 1, table.BranchRef, 0, 0, 0),
			},
			want: []table.Requirement{
				uuidReq,
				table.AssertRefSnapshotID(table.MainBranch, new(reqTestCurrentSnap)),
				table.AssertRefSnapshotID("new", nil),
			},
		},
		{
			name: "a branch asserted by set-snapshot-ref is not asserted again",
			updates: []table.Update{
				table.NewSetSnapshotRefUpdate("dev", 1, table.BranchRef, 0, 0, 0),
				table.NewRemoveSchemasUpdate([]int{0}),
			},
			want: []table.Requirement{
				uuidReq,
				table.AssertRefSnapshotID("dev", new(reqTestSnapshotID)),
				table.AssertCurrentSchemaID(reqTestSchemaID),
			},
		},
		{
			name: "schema and partition evolution",
			updates: []table.Update{
				table.NewAddSchemaUpdate(newSchemaForRequirements()),
				table.NewSetCurrentSchemaUpdate(-1),
				table.NewAddPartitionSpecUpdate(&spec, false),
				table.NewSetDefaultSpecUpdate(-1),
				table.NewAddSortOrderUpdate(&order),
				table.NewSetDefaultSortOrderUpdate(-1),
			},
			want: []table.Requirement{
				uuidReq,
				table.AssertLastAssignedFieldID(reqTestLastColumnID),
				table.AssertCurrentSchemaID(reqTestSchemaID),
				table.AssertLastAssignedPartitionID(reqTestLastPartID),
				table.AssertDefaultSpecID(reqTestSpecID),
				table.AssertDefaultSortOrderID(reqTestSortOrderID),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := table.RequirementsForUpdateTable(meta, tt.updates)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)

			for _, r := range got {
				assert.NoError(t, r.Validate(meta), "%s must hold against its own base", r.GetType())
			}
		})
	}
}

func TestRequirementsForReplaceTable(t *testing.T) {
	meta := requirementsTestMetadata(t)
	spec := iceberg.NewPartitionSpec()
	order := table.UnsortedSortOrder

	// A replace only guards ID assignment; it overwrites the current schema,
	// default spec, sort order and refs.
	got, err := table.RequirementsForReplaceTable(meta, []table.Update{
		table.NewAddSchemaUpdate(newSchemaForRequirements()),
		table.NewSetCurrentSchemaUpdate(-1),
		table.NewAddPartitionSpecUpdate(&spec, false),
		table.NewSetDefaultSpecUpdate(-1),
		table.NewAddSortOrderUpdate(&order),
		table.NewSetDefaultSortOrderUpdate(-1),
		table.NewSetSnapshotRefUpdate(table.MainBranch, 1, table.BranchRef, 0, 0, 0),
		table.NewRemoveSpecUpdate([]int{1}),
		table.NewRemoveSchemasUpdate([]int{0}),
	})
	require.NoError(t, err)
	assert.Equal(t, []table.Requirement{
		table.AssertTableUUID(reqTestUUID),
		table.AssertLastAssignedFieldID(reqTestLastColumnID),
		table.AssertLastAssignedPartitionID(reqTestLastPartID),
	}, got)
}

type noLastPartitionIDMetadata struct{ table.Metadata }

func (noLastPartitionIDMetadata) LastPartitionSpecID() *int { return nil }

func TestRequirementsForUpdateTableMissingLastPartitionID(t *testing.T) {
	meta := noLastPartitionIDMetadata{requirementsTestMetadata(t)}
	spec := iceberg.NewPartitionSpec()

	got, err := table.RequirementsForUpdateTable(meta, []table.Update{
		table.NewAddPartitionSpecUpdate(&spec, false),
	})
	require.NoError(t, err)
	assert.Equal(t, []table.Requirement{
		table.AssertTableUUID(reqTestUUID),
		table.AssertLastAssignedPartitionID(iceberg.PartitionDataIDStart - 1),
	}, got)
}

// Each requirement must fail once a concurrent commit changes the state it
// guards, so a stale commit is rejected instead of overwriting that change.
func TestRequirementsForUpdateTableConcurrentChange(t *testing.T) {
	meta := requirementsTestMetadata(t)

	tests := []struct {
		name       string
		update     table.Update
		concurrent func(*table.MetadataBuilder) error
	}{
		{
			name:   "add-schema",
			update: table.NewAddSchemaUpdate(newSchemaForRequirements()),
			concurrent: func(b *table.MetadataBuilder) error {
				return b.AddSchema(iceberg.NewSchema(2,
					iceberg.NestedField{ID: 1, Name: "x", Type: iceberg.PrimitiveTypes.Int64, Required: true},
					iceberg.NestedField{ID: 4, Name: "v", Type: iceberg.PrimitiveTypes.Int32},
				))
			},
		},
		{
			name:   "set-current-schema",
			update: table.NewSetCurrentSchemaUpdate(0),
			concurrent: func(b *table.MetadataBuilder) error {
				if err := b.AddSchema(iceberg.NewSchema(2,
					iceberg.NestedField{ID: 1, Name: "x", Type: iceberg.PrimitiveTypes.Int64, Required: true},
					iceberg.NestedField{ID: 2, Name: "y", Type: iceberg.PrimitiveTypes.Int64, Required: true},
					iceberg.NestedField{ID: 3, Name: "z", Type: iceberg.PrimitiveTypes.Int64, Required: true},
					iceberg.NestedField{ID: 4, Name: "v", Type: iceberg.PrimitiveTypes.Int32},
				)); err != nil {
					return err
				}

				return b.SetCurrentSchemaID(-1)
			},
		},
		{
			name:   "add-spec",
			update: table.NewAddPartitionSpecUpdate(new(iceberg.NewPartitionSpec()), false),
			concurrent: func(b *table.MetadataBuilder) error {
				spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
					SourceIDs: []int{2}, FieldID: 1001, Name: "y", Transform: iceberg.IdentityTransform{},
				})

				return b.AddPartitionSpec(&spec, false)
			},
		},
		{
			name:   "set-default-spec",
			update: table.NewSetDefaultSpecUpdate(-1),
			concurrent: func(b *table.MetadataBuilder) error {
				spec := iceberg.NewPartitionSpec()
				if err := b.AddPartitionSpec(&spec, false); err != nil {
					return err
				}

				return b.SetDefaultSpecID(-1)
			},
		},
		{
			name:   "set-default-sort-order",
			update: table.NewSetDefaultSortOrderUpdate(-1),
			concurrent: func(b *table.MetadataBuilder) error {
				order := table.UnsortedSortOrder
				if err := b.AddSortOrder(&order); err != nil {
					return err
				}

				return b.SetDefaultSortOrderID(-1)
			},
		},
		{
			name:   "set-snapshot-ref",
			update: table.NewSetSnapshotRefUpdate(table.MainBranch, reqTestSnapshotID, table.BranchRef, 0, 0, 0),
			concurrent: func(b *table.MetadataBuilder) error {
				return b.SetSnapshotRef(table.MainBranch, reqTestSnapshotID, table.BranchRef)
			},
		},
		{
			name:   "remove-schemas with a moved branch",
			update: table.NewRemoveSchemasUpdate([]int{0}),
			concurrent: func(b *table.MetadataBuilder) error {
				return b.SetSnapshotRef("dev", reqTestCurrentSnap, table.BranchRef)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reqs, err := table.RequirementsForUpdateTable(meta, []table.Update{tt.update})
			require.NoError(t, err)

			b, err := table.MetadataBuilderFromBase(meta, "")
			require.NoError(t, err)
			require.NoError(t, tt.concurrent(b))
			changed, err := b.Build()
			require.NoError(t, err)

			// The table UUID is unchanged, so the failure must come from the
			// last requirement, which guards the state the concurrent commit changed.
			require.Greater(t, len(reqs), 1)
			assert.NoError(t, reqs[0].Validate(changed))
			last := reqs[len(reqs)-1]
			assert.Error(t, last.Validate(changed), "%s should reject the concurrent change", last.GetType())
		})
	}
}
