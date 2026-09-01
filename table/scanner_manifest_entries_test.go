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
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
)

func TestFlattenClassifiedManifestEntriesPreservesManifestOrder(t *testing.T) {
	snapshotID := int64(1)
	dataOne := iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapshotID, nil, nil, &mockDataFile{
		path:        "data-1.parquet",
		contentType: iceberg.EntryContentData,
	})
	dataTwo := iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapshotID, nil, nil, &mockDataFile{
		path:        "data-2.parquet",
		contentType: iceberg.EntryContentData,
	})
	positionalDelete := iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapshotID, nil, nil, &mockDataFile{
		path:        "pos-delete.parquet",
		contentType: iceberg.EntryContentPosDeletes,
	})
	equalityDelete := iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapshotID, nil, nil, &mockDataFile{
		path:        "eq-delete.parquet",
		contentType: iceberg.EntryContentEqDeletes,
	})
	dv := iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapshotID, nil, nil, &mockDataFile{
		path:        "deletion-vector.puffin",
		contentType: iceberg.EntryContentPosDeletes,
		format:      iceberg.PuffinFile,
	})

	got := flattenClassifiedManifestEntries([]classifiedManifestEntries{
		{
			dataEntries:           []iceberg.ManifestEntry{dataOne},
			equalityDeleteEntries: []iceberg.ManifestEntry{equalityDelete},
			dvEntries:             []iceberg.ManifestEntry{dv},
		},
		{},
		{dataEntries: []iceberg.ManifestEntry{dataTwo}, positionalDeleteEntries: []iceberg.ManifestEntry{positionalDelete}},
	})

	assert.Equal(t, []iceberg.ManifestEntry{dataOne, dataTwo}, got.dataEntries)
	assert.Equal(t, []iceberg.ManifestEntry{positionalDelete}, got.positionalDeleteEntries)
	assert.Equal(t, []iceberg.ManifestEntry{equalityDelete}, got.equalityDeleteEntries)
	assert.Equal(t, []iceberg.ManifestEntry{dv}, got.dvEntries)
	assert.Equal(t, len(got.dataEntries), cap(got.dataEntries))
	assert.Equal(t, len(got.positionalDeleteEntries), cap(got.positionalDeleteEntries))
	assert.Equal(t, len(got.equalityDeleteEntries), cap(got.equalityDeleteEntries))
	assert.Equal(t, len(got.dvEntries), cap(got.dvEntries))
}

func TestFlattenClassifiedManifestEntriesEmpty(t *testing.T) {
	got := flattenClassifiedManifestEntries(nil)

	assert.NotNil(t, got.dataEntries)
	assert.NotNil(t, got.positionalDeleteEntries)
	assert.NotNil(t, got.equalityDeleteEntries)
	assert.NotNil(t, got.dvEntries)
	assert.Empty(t, got.dataEntries)
	assert.Empty(t, got.positionalDeleteEntries)
	assert.Empty(t, got.equalityDeleteEntries)
	assert.Empty(t, got.dvEntries)
}
