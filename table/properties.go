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
	"math"

	"github.com/apache/iceberg-go/table/internal"
)

const (
	WriteDataPathKey                        = "write.data.path"
	WriteMetadataPathKey                    = "write.metadata.path"
	WriteObjectStorePartitionedPathsKey     = "write.object-storage.partitioned-paths"
	WriteObjectStorePartitionedPathsDefault = true
	ObjectStoreEnabledKey                   = "write.object-storage.enabled"
	ObjectStoreEnabledDefault               = false

	DefaultNameMappingKey = "schema.name-mapping.default"

	MetricsModeColumnConfPrefix    = "write.metadata.metrics.column"
	DefaultWriteMetricsModeKey     = "write.metadata.metrics.default"
	DefaultWriteMetricsModeDefault = "truncate(16)"

	ParquetRowGroupSizeBytesKey              = internal.ParquetRowGroupSizeBytesKey
	ParquetRowGroupSizeBytesDefault          = internal.ParquetRowGroupSizeBytesDefault
	ParquetRowGroupLimitKey                  = internal.ParquetRowGroupLimitKey
	ParquetRowGroupLimitDefault              = internal.ParquetRowGroupLimitDefault
	ParquetPageSizeBytesKey                  = internal.ParquetPageSizeBytesKey
	ParquetPageSizeBytesDefault              = internal.ParquetPageSizeBytesDefault
	ParquetPageRowLimitKey                   = internal.ParquetPageRowLimitKey
	ParquetPageRowLimitDefault               = internal.ParquetPageRowLimitDefault
	ParquetDictSizeBytesKey                  = internal.ParquetDictSizeBytesKey
	ParquetDictSizeBytesDefault              = internal.ParquetDictSizeBytesDefault
	ParquetCompressionKey                    = internal.ParquetCompressionKey
	ParquetCompressionDefault                = internal.ParquetCompressionDefault
	ParquetCompressionLevelKey               = internal.ParquetCompressionLevelKey
	ParquetCompressionLevelDefault           = internal.ParquetCompressionLevelDefault
	ParquetBloomFilterMaxBytesKey            = internal.ParquetBloomFilterMaxBytesKey
	ParquetBloomFilterMaxBytesDefault        = internal.ParquetBloomFilterMaxBytesDefault
	ParquetBloomFilterColumnEnabledKeyPrefix = internal.ParquetBloomFilterColumnEnabledKeyPrefix

	ManifestMergeEnabledKey     = "commit.manifest-merge.enabled"
	ManifestMergeEnabledDefault = false

	ManifestTargetSizeBytesKey     = "commit.manifest.target-size-bytes"
	ManifestTargetSizeBytesDefault = 8 * 1024 * 1024 // 8 MB

	ManifestMinMergeCountKey     = "commit.manifest.min-count-to-merge"
	ManifestMinMergeCountDefault = 100

	WritePartitionSummaryLimitKey     = "write.summary.partition-limit"
	WritePartitionSummaryLimitDefault = 0

	MetadataDeleteAfterCommitEnabledKey     = "write.metadata.delete-after-commit.enabled"
	MetadataDeleteAfterCommitEnabledDefault = false

	MetadataPreviousVersionsMaxKey     = "write.metadata.previous-versions-max"
	MetadataPreviousVersionsMaxDefault = 100

	WriteTargetFileSizeBytesKey     = "write.target-file-size-bytes"
	WriteTargetFileSizeBytesDefault = 512 * 1024 * 1024 // 512 MB

	MinSnapshotsToKeepKey     = "min-snapshots-to-keep"
	MinSnapshotsToKeepDefault = math.MaxInt

	MaxSnapshotAgeMsKey     = "max-snapshot-age-ms"
	MaxSnapshotAgeMsDefault = math.MaxInt

	MaxRefAgeMsKey     = "max-ref-age-ms"
	MaxRefAgeMsDefault = math.MaxInt
)
