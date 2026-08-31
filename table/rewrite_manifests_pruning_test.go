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

package table

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/require"
)

// TestRewriteManifestsClusterByPrunesManifests keeps the read-side benefit of
// clustering under ordinary test coverage. Size-only output contains the
// queried partition value in every bin; clustered output can reject the bins
// for the other partition values without opening their entries.
func TestRewriteManifestsClusterByPrunesManifests(t *testing.T) {
	const (
		inputCount   = 64
		clusterCount = 8
		targetValue  = int32(7)
	)

	spec := partitionedSpec()
	schema := simpleSchema()
	mem := newMemIO(1<<30, errLimitedWrite)
	meta, err := NewMetadata(schema, &spec, UnsortedSortOrder, "table-location", nil)
	require.NoError(t, err)
	tbl := New(Identifier{"db", "pruning"}, meta, "metadata.json",
		func(context.Context) (iceio.IO, error) { return mem, nil }, nil)
	prod := newRewriteManifestsProducer(tbl.NewTransaction(), mem, iceberg.Properties{}, rewriteManifestsCfg{})

	inputSnapshotID := int64(1)
	inputSequenceNumber := int64(1)
	manifests := make([]iceberg.ManifestFile, 0, inputCount)
	for i := range inputCount {
		partitionValue := int32((i / 8) % (clusterCount - 1))
		if partitionValue >= targetValue {
			partitionValue++
		}
		if i%8 == 0 {
			partitionValue = targetValue
		}

		builder, err := iceberg.NewDataFileBuilder(
			spec, iceberg.EntryContentData, fmt.Sprintf("file://data-%d.parquet", i),
			iceberg.ParquetFile, map[int]any{1000: partitionValue}, nil, nil, 1, 100,
		)
		require.NoError(t, err)
		entry := iceberg.NewManifestEntry(
			iceberg.EntryStatusADDED,
			&inputSnapshotID,
			&inputSequenceNumber,
			nil,
			builder.Build(),
		)

		path := fmt.Sprintf("table-location/metadata/input-%d.avro", i)
		var buf bytes.Buffer
		manifest, err := iceberg.WriteManifest(path, &buf, 2, spec, schema, inputSnapshotID,
			[]iceberg.ManifestEntry{entry})
		require.NoError(t, err)
		require.NoError(t, mem.WriteFile(path, buf.Bytes()))
		manifests = append(manifests, manifest)
	}

	var maxLength int64
	for _, manifest := range manifests {
		maxLength = max(maxLength, manifest.Length())
	}
	newManager := func(cluster bool) []iceberg.ManifestFile {
		mgr := manifestMergeManager{
			targetSizeBytes:  8 * maxLength,
			mergeEnabled:     true,
			mergeConcurrency: 1,
			snap:             prod,
		}
		if cluster {
			mgr.clusterBy = func(df iceberg.DataFile) any { return df.Partition()[1000] }
		}

		merged, mergeErr := mgr.mergeManifests(manifests)
		require.NoError(t, mergeErr)

		return merged
	}

	sizeOnly := newManager(false)
	clustered := newManager(true)
	scan := tbl.Scan(WithRowFilter(iceberg.EqualTo(iceberg.Reference("id"), targetValue)))
	sizeOnlySelected, err := scan.filterManifestsWithSchema(sizeOnly, schema, &scanMetricsAccumulator{})
	require.NoError(t, err)
	clusteredSelected, err := scan.filterManifestsWithSchema(clustered, schema, &scanMetricsAccumulator{})
	require.NoError(t, err)

	require.Len(t, sizeOnlySelected, len(sizeOnly),
		"size-only output should keep the queried partition in every manifest")
	require.Less(t, len(clusteredSelected), len(clustered),
		"clustered output should prune manifests for unrelated partition values")
}
