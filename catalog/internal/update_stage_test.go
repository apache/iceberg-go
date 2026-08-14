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
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

func TestUpdateAndStageTableRejectsInvalidMetadataLocation(t *testing.T) {
	metadata, err := table.NewMetadata(
		iceberg.NewSchema(0),
		nil,
		table.UnsortedSortOrder,
		"file:///tmp/table",
		nil,
	)
	require.NoError(t, err)

	current := table.New(
		[]string{"db", "tbl"},
		metadata,
		"file:///tmp/table/metadata/current.metadata.json",
		nil,
		nil,
	)

	staged, err := UpdateAndStageTable(
		context.Background(),
		nil,
		current,
		current.Identifier(),
		nil,
		nil,
		nil,
	)
	require.Error(t, err)
	require.Nil(t, staged)
	require.Contains(t, err.Error(), "invalid metadata location")
}

func TestUpdateAndStageTableRejectsEmptyMetadataLocationForExistingTable(t *testing.T) {
	metadata, err := table.NewMetadata(
		iceberg.NewSchema(0),
		nil,
		table.UnsortedSortOrder,
		"file:///tmp/table",
		nil,
	)
	require.NoError(t, err)

	current := table.New(
		[]string{"db", "tbl"},
		metadata,
		"",
		nil,
		nil,
	)

	staged, err := UpdateAndStageTable(
		context.Background(),
		nil,
		current,
		current.Identifier(),
		nil,
		nil,
		nil,
	)
	require.Error(t, err)
	require.Nil(t, staged)
	require.Contains(t, err.Error(), "invalid metadata location")
}

func TestParseMetadataVersionAcceptsLegacyMetadataLocation(t *testing.T) {
	require.Equal(t, 1, ParseMetadataVersion("file:///tmp/table/metadata/v1.metadata.json"))
	require.Equal(t, 2, ParseMetadataVersion("file:///tmp/table/metadata/v2.gz.metadata.json"))
}

func TestUpdateAndStageTableUsesVersionZeroForCreate(t *testing.T) {
	staged, err := UpdateAndStageTable(
		context.Background(),
		nil,
		nil,
		[]string{"db", "tbl"},
		nil,
		[]table.Update{table.NewSetLocationUpdate("file:///tmp/table")},
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, 0, ParseMetadataVersion(staged.MetadataLocation()))
}
