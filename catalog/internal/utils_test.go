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

package internal

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestUpdateAndStageTableValidatesRequirementsForMissingTable(t *testing.T) {
	ctx := context.Background()
	tableLocation := filepath.Join(t.TempDir(), "table")
	tableUUID := uuid.New()
	schema := iceberg.NewSchema(1, iceberg.NestedField{
		ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true,
	})
	updates := []table.Update{
		table.NewAssignUUIDUpdate(tableUUID),
		table.NewUpgradeFormatVersionUpdate(2),
		table.NewAddSchemaUpdate(schema),
		table.NewSetCurrentSchemaUpdate(-1),
		table.NewSetLocationUpdate(tableLocation),
	}

	staged, err := UpdateAndStageTable(
		ctx,
		nil,
		nil,
		table.Identifier{"db", "tbl"},
		[]table.Requirement{table.AssertTableUUID(tableUUID)},
		updates,
		nil,
	)

	require.ErrorContains(t, err, "current table metadata does not exist")
	require.Nil(t, staged)
	_, statErr := os.Stat(tableLocation)
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestUpdateAndStageTableAllowsCreateRequirementForMissingTable(t *testing.T) {
	ctx := context.Background()
	tableLocation := filepath.Join(t.TempDir(), "table")
	tableUUID := uuid.New()
	schema := iceberg.NewSchema(1, iceberg.NestedField{
		ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true,
	})

	staged, err := UpdateAndStageTable(
		ctx,
		nil,
		nil,
		table.Identifier{"db", "tbl"},
		[]table.Requirement{table.AssertCreate()},
		[]table.Update{
			table.NewAssignUUIDUpdate(tableUUID),
			table.NewUpgradeFormatVersionUpdate(2),
			table.NewAddSchemaUpdate(schema),
			table.NewSetCurrentSchemaUpdate(-1),
			table.NewSetLocationUpdate(tableLocation),
		},
		nil,
	)

	require.NoError(t, err)
	require.NotNil(t, staged)
	require.Equal(t, tableUUID, staged.Metadata().TableUUID())
	require.Equal(t, tableLocation, staged.Location())
}
