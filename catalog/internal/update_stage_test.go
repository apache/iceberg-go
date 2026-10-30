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
	"errors"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
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

// failingRequirement fails with err, standing in for a requirement whose
// failure is not a concurrent change, such as a tag used as a transaction
// target.
type failingRequirement struct{ err error }

func (r failingRequirement) Validate(table.Metadata) error { return r.err }

func (r failingRequirement) GetType() string { return "failing" }

func TestValidateRequirementsClassifiesFailures(t *testing.T) {
	metadata, err := table.NewMetadata(iceberg.NewSchema(0), nil, table.UnsortedSortOrder, "file:///tmp/table", nil)
	require.NoError(t, err)
	ident := table.Identifier{"db", "tbl"}

	tests := []struct {
		name    string
		current table.Metadata
		req     table.Requirement
		want    error
	}{
		{"stale requirement on existing table", metadata, table.AssertCurrentSchemaID(99), table.ErrCommitFailed},
		{"create onto existing table", metadata, table.AssertCreate(), table.ErrCommitFailed},
		{"requirement on missing table", nil, table.AssertTableUUID(metadata.TableUUID()), catalog.ErrNoSuchTable},
		{"satisfied requirement", metadata, table.AssertTableUUID(metadata.TableUUID()), nil},
		{"create of missing table", nil, table.AssertCreate(), nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateRequirements(ident, tt.current, []table.Requirement{tt.req})
			if tt.want == nil {
				require.NoError(t, err)

				return
			}
			require.ErrorIs(t, err, tt.want)
		})
	}
}

// A requirement failure that is not a concurrent change must not gain
// ErrCommitFailed, or a retry loop would treat it as one.
func TestValidateRequirementsReturnsRequirementErrorUnchanged(t *testing.T) {
	metadata, err := table.NewMetadata(iceberg.NewSchema(0), nil, table.UnsortedSortOrder, "file:///tmp/table", nil)
	require.NoError(t, err)
	errTag := errors.New("requirement failed: ref \"t\" is a tag")

	err = ValidateRequirements(table.Identifier{"db", "tbl"}, metadata, []table.Requirement{failingRequirement{errTag}})
	require.Same(t, errTag, err)
}
