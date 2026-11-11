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

package view

import (
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func viewWithVersionHistory(t *testing.T) *View {
	t.Helper()

	schema := iceberg.NewSchema(0, iceberg.NestedField{
		ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true,
	})
	first := &Version{
		VersionID: 1, SchemaID: 0, TimestampMS: 500,
		Summary: VersionSummary{"operation": "create"},
		Representations: []Representation{NewRepresentation(Representation{
			SQL: "SELECT 1", Dialect: "spark",
		})},
		DefaultNamespace: []string{"db"},
	}
	second := first.Clone()
	second.VersionID = 2
	second.TimestampMS = 1500
	second.Summary["operation"] = "replace"
	second.Representations[0] = NewRepresentation(Representation{
		SQL: "SELECT 2", Dialect: "spark",
	})

	id := uuid.MustParse("fa6506c3-7681-40c8-86dc-e36561f83385")
	md := &metadata{
		FormatVersionValue:    1,
		UUID:                  &id,
		Loc:                   "s3://warehouse/db/view",
		CurrentVersionIDValue: 2,
		SchemaList:            []*iceberg.Schema{schema},
		VersionList:           []*Version{first, second},
		VersionLogList: []VersionLogEntry{
			{TimestampMS: 1000, VersionID: 1},
			{TimestampMS: 2000, VersionID: 2},
			{TimestampMS: 3000, VersionID: 1}, // rollback
			{TimestampMS: 4000, VersionID: 2},
		},
	}
	md.init()
	require.NoError(t, md.validate())

	return New([]string{"db", "view"}, md, "s3://warehouse/db/view/metadata.json")
}

func TestVersionAsOf(t *testing.T) {
	vw := viewWithVersionHistory(t)

	tests := []struct {
		name      string
		timestamp int64
		wantID    int64
	}{
		{name: "before history", timestamp: 999},
		{name: "at first", timestamp: 1000, wantID: 1},
		{name: "between changes", timestamp: 1500, wantID: 1},
		{name: "at second", timestamp: 2000, wantID: 2},
		{name: "rolled back", timestamp: 3000, wantID: 1},
		{name: "after rollback", timestamp: 3500, wantID: 1},
		{name: "at fourth", timestamp: 4000, wantID: 2},
		{name: "after last", timestamp: 9000, wantID: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			version := vw.VersionAsOf(tt.timestamp)
			if tt.wantID == 0 {
				require.Nil(t, version)
			} else {
				require.NotNil(t, version)
				require.Equal(t, tt.wantID, version.VersionID)
			}
		})
	}
}

func TestVersionAsOfEmptyAndNegativeHistory(t *testing.T) {
	vw := viewWithVersionHistory(t)
	md := vw.Metadata().(*metadata)
	md.VersionLogList = nil
	require.Nil(t, vw.VersionAsOf(1000))

	md.VersionLogList = []VersionLogEntry{{TimestampMS: -2000, VersionID: 1}}
	md.CurrentVersionIDValue = 1
	require.NoError(t, md.validate())
	require.Nil(t, vw.VersionAsOf(-2001))
	require.EqualValues(t, 1, vw.VersionAsOf(-2000).VersionID)
	require.EqualValues(t, 1, vw.VersionAsOf(-1000).VersionID)
}

func TestVersionAsOfOutOfOrderTimestamps(t *testing.T) {
	vw := viewWithVersionHistory(t)
	md := vw.Metadata().(*metadata)
	md.VersionLogList = []VersionLogEntry{
		{TimestampMS: 1000, VersionID: 1},
		{TimestampMS: 3000, VersionID: 2},
		{TimestampMS: 2000, VersionID: 1},
	}
	md.CurrentVersionIDValue = 1
	require.NoError(t, md.validate())

	// The last eligible log entry wins despite its older timestamp.
	require.EqualValues(t, 1, vw.VersionAsOf(2500).VersionID)
	require.EqualValues(t, 1, vw.VersionAsOf(3000).VersionID)
	require.EqualValues(t, 1, vw.VersionAsOf(3500).VersionID)
	require.Equal(t, vw.CurrentVersion().VersionID, vw.VersionAsOf(3500).VersionID)
}

func TestVersionByIDAndHistoryAreDefensive(t *testing.T) {
	vw := viewWithVersionHistory(t)

	require.Nil(t, vw.VersionByID(3))
	first := vw.VersionByID(1)
	require.NotNil(t, first)
	first.Summary["operation"] = "changed"
	first.Representations[0] = NewRepresentation(Representation{
		SQL: "SELECT 999", Dialect: "spark",
	})
	first.DefaultNamespace[0] = "changed"
	require.Equal(t, "create", vw.VersionByID(1).Summary["operation"])
	require.Equal(t, "SELECT 1", vw.VersionByID(1).Representations[0].SQL)
	require.Equal(t, "db", vw.VersionByID(1).DefaultNamespace[0])

	history := vw.History()
	require.Len(t, history, 4)
	history[0].VersionID = 99
	require.EqualValues(t, 1, vw.History()[0].VersionID)

	asOf := vw.VersionAsOf(3000)
	asOf.Summary["operation"] = "changed"
	require.Equal(t, "create", vw.VersionAsOf(3000).Summary["operation"])
}

func TestVersionAsOfTruncatedHistory(t *testing.T) {
	vw := viewWithVersionHistory(t)
	md := vw.Metadata().(*metadata)

	md.VersionLogList = md.VersionLogList[1:]
	require.Nil(t, vw.VersionAsOf(1500))
	require.EqualValues(t, 2, vw.VersionAsOf(2000).VersionID)

	// If a log entry refers to a version that has been expired, do not
	// incorrectly return an older or newer version in its place.
	md.VersionList = md.VersionList[1:]
	md.init() // Rebuild indexes after changing the metadata test fixture.
	require.Nil(t, vw.VersionAsOf(3000))
	require.EqualValues(t, 2, vw.VersionAsOf(4000).VersionID)
}

func TestVersionAsOfRepeatedTimestamps(t *testing.T) {
	vw := viewWithVersionHistory(t)
	md := vw.Metadata().(*metadata)

	third := md.VersionList[1].Clone()
	third.VersionID = 3
	third.Representations[0].SQL = "SELECT 3"
	md.VersionList = append(md.VersionList, third)
	md.CurrentVersionIDValue = 3
	md.VersionLogList = []VersionLogEntry{
		{TimestampMS: 1000, VersionID: 1},
		{TimestampMS: 2000, VersionID: 2},
		{TimestampMS: 2000, VersionID: 3},
	}
	md.init()
	require.NoError(t, md.validate())

	// Later changes in the log win when timestamps tie.
	require.EqualValues(t, 1, vw.VersionAsOf(1999).VersionID)
	require.EqualValues(t, 3, vw.VersionAsOf(2000).VersionID)
	require.EqualValues(t, 3, vw.VersionAsOf(2001).VersionID)
	require.Equal(t, vw.CurrentVersion().VersionID, vw.VersionAsOf(2001).VersionID)
}

func TestVersionAsOfBuilderRollback(t *testing.T) {
	vw := viewWithVersionHistory(t)
	builder, err := MetadataBuilderFromBase(vw.Metadata())
	require.NoError(t, err)

	result, err := builder.SetCurrentVersionID(1).Build()
	require.NoError(t, err)

	rolledBack := New(vw.Identifier(), result.Metadata, vw.MetadataLocation())
	history := rolledBack.History()
	require.Len(t, history, len(vw.History())+1)
	rollback := history[len(history)-1]
	require.EqualValues(t, 1, rollback.VersionID)
	require.Greater(t, rollback.TimestampMS, history[len(history)-2].TimestampMS)
	require.Equal(t, rollback.VersionID, rolledBack.CurrentVersion().VersionID)

	require.EqualValues(t, 2, vw.CurrentVersion().VersionID)
	require.EqualValues(t, 2, rolledBack.VersionAsOf(rollback.TimestampMS-1).VersionID)
	require.EqualValues(t, 1, rolledBack.VersionAsOf(rollback.TimestampMS).VersionID)
	require.EqualValues(t, 1, rolledBack.VersionAsOf(rollback.TimestampMS+1).VersionID)
}

func TestVersionAsOfAfterHistoryExpiration(t *testing.T) {
	vw := viewWithVersionHistory(t)
	builder, err := MetadataBuilderFromBase(vw.Metadata())
	require.NoError(t, err)

	// Match Java's history normalization: after version 1 expires, all
	// entries before its last occurrence are removed, even if version 2
	// was active at some earlier point.
	result, err := builder.SetProperties(iceberg.Properties{
		VersionHistorySizeKey: "1",
	}).Build()
	require.NoError(t, err)

	retained := New(vw.Identifier(), result.Metadata, vw.MetadataLocation())
	require.Equal(t, []VersionLogEntry{
		{TimestampMS: 4000, VersionID: 2},
	}, retained.History())
	require.Nil(t, retained.VersionByID(1))
	require.Nil(t, retained.VersionAsOf(3999))
	require.EqualValues(t, 2, retained.VersionAsOf(4000).VersionID)
}

func TestVersionAsOfBuilderCreatedHistory(t *testing.T) {
	first, err := NewMetadataBuilder().
		SetLoc("s3://warehouse/db/view").
		SetCurrentVersion(newTestVersionWithSQL(1, 0, "SELECT 1", WithTimestampMS(1000)), newTestSchema(0)).
		Build()
	require.NoError(t, err)

	builder, err := MetadataBuilderFromBase(first.Metadata)
	require.NoError(t, err)
	second, err := builder.
		SetCurrentVersion(newTestVersionWithSQL(2, 0, "SELECT 2", WithTimestampMS(2000)), newTestSchema(0)).
		Build()
	require.NoError(t, err)

	builder, err = MetadataBuilderFromBase(second.Metadata)
	require.NoError(t, err)
	rolledBack, err := builder.SetCurrentVersionID(1).Build()
	require.NoError(t, err)

	vw := New([]string{"db", "view"}, rolledBack.Metadata, "s3://warehouse/db/view/metadata.json")
	history := vw.History()
	require.Len(t, history, 3)
	require.Equal(t, []VersionLogEntry{
		{TimestampMS: 1000, VersionID: 1},
		{TimestampMS: 2000, VersionID: 2},
	}, history[:2])
	require.EqualValues(t, 1, history[2].VersionID)
	require.Greater(t, history[2].TimestampMS, int64(2000))

	require.Nil(t, vw.VersionAsOf(999))
	require.EqualValues(t, 1, vw.VersionAsOf(1000).VersionID)
	require.EqualValues(t, 2, vw.VersionAsOf(2000).VersionID)
	require.EqualValues(t, 2, vw.VersionAsOf(history[2].TimestampMS-1).VersionID)
	require.EqualValues(t, 1, vw.VersionAsOf(history[2].TimestampMS).VersionID)
	require.EqualValues(t, 1, vw.VersionAsOf(history[2].TimestampMS+1).VersionID)
	require.Equal(t, vw.CurrentVersion().VersionID, vw.VersionAsOf(history[2].TimestampMS).VersionID)
}
