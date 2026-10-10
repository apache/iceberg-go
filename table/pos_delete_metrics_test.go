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
	"context"
	"maps"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

// TestPositionDeleteFilesKeepFullFilePathBounds pins #2141: v2 position delete
// files keep exact file_path bounds and pos bounds whatever the table metrics
// settings are, as Java's MetricsConfig.forPositionDelete does. Data file
// paths are far longer than 16 characters, so the table-defaults case would
// truncate them under the default truncate(16) mode, and the override case
// asks for no file_path or pos metrics at all.
func TestPositionDeleteFilesKeepFullFilePathBounds(t *testing.T) {
	identity := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{2}, FieldID: 1000, Transform: iceberg.IdentityTransform{}, Name: "data",
	})

	specs := []struct {
		name string
		spec *iceberg.PartitionSpec
	}{
		{"unpartitioned", iceberg.UnpartitionedSpec},
		{"partitioned", &identity},
	}

	metricsConfigs := []struct {
		name  string
		props iceberg.Properties
	}{
		{"table defaults", nil},
		{"file_path and pos none", iceberg.Properties{
			table.MetricsModeColumnConfPrefix + ".file_path": "none",
			table.MetricsModeColumnConfPrefix + ".pos":       "none",
		}},
	}

	filePathField, ok := iceberg.PositionalDeleteSchema.FindFieldByName("file_path")
	require.True(t, ok)
	posField, ok := iceberg.PositionalDeleteSchema.FindFieldByName("pos")
	require.True(t, ok)

	for _, tc := range specs {
		for _, mc := range metricsConfigs {
			t.Run(tc.name+"/"+mc.name, func(t *testing.T) {
				ctx := context.Background()

				location := filepath.ToSlash(t.TempDir())
				props := iceberg.Properties{
					table.PropertyFormatVersion: "2",
					table.WriteDeleteModeKey:    table.WriteModeMergeOnRead,
				}
				maps.Copy(props, mc.props)
				schema := iceberg.NewSchema(0,
					iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
					iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: true},
				)
				meta, err := table.NewMetadata(schema, tc.spec, table.UnsortedSortOrder, location, props)
				require.NoError(t, err)

				metaLoc := location + "/metadata/v1.metadata.json"
				fsF := func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }
				cat := &concurrentTestCatalog{metadata: meta, location: metaLoc, fsF: fsF}
				tbl := table.New(table.Identifier{"db", "pos_delete_metrics"}, meta, metaLoc, fsF, cat)

				arrowSchema := arrow.NewSchema([]arrow.Field{
					{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
					{Name: "data", Type: arrow.BinaryTypes.String, Nullable: false},
				}, nil)
				data, err := array.TableFromJSON(memory.DefaultAllocator, arrowSchema, []string{
					`[{"id":1,"data":"a"},{"id":2,"data":"a"},{"id":3,"data":"b"},{"id":4,"data":"b"}]`,
				})
				require.NoError(t, err)
				defer data.Release()

				rdr := array.NewTableReader(data, -1)
				defer rdr.Release()

				tbl, err = tbl.Append(ctx, rdr, nil)
				require.NoError(t, err)
				tbl, err = tbl.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(2)), nil)
				require.NoError(t, err)

				tasks, err := tbl.Scan().PlanFiles(ctx)
				require.NoError(t, err)

				deleteFiles := 0
				for _, task := range tasks {
					dataPath := task.File.FilePath()
					require.Greater(t, len(dataPath), 16)
					for _, df := range task.DeleteFiles {
						require.Equal(t, iceberg.EntryContentPosDeletes, df.ContentType())
						deleteFiles++

						require.Equal(t, []byte(dataPath), df.LowerBoundValues()[filePathField.ID])
						require.Equal(t, []byte(dataPath), df.UpperBoundValues()[filePathField.ID])
						require.NotEmpty(t, df.LowerBoundValues()[posField.ID])
						require.NotEmpty(t, df.UpperBoundValues()[posField.ID])
					}
				}
				require.Equal(t, 1, deleteFiles)
			})
		}
	}
}
