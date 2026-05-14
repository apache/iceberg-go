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
	WriteMetadataLocationKey                = "write.metadata.location"
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
	ParquetPageVersionKey                    = internal.ParquetPageVersionKey
	ParquetPageVersionDefault                = internal.ParquetPageVersionDefault
	ParquetCompressionKey                    = internal.ParquetCompressionKey
	ParquetCompressionDefault                = internal.ParquetCompressionDefault
	ParquetCompressionLevelKey               = internal.ParquetCompressionLevelKey
	ParquetCompressionLevelDefault           = internal.ParquetCompressionLevelDefault
	ParquetBloomFilterMaxBytesKey            = internal.ParquetBloomFilterMaxBytesKey
	ParquetBloomFilterMaxBytesDefault        = internal.ParquetBloomFilterMaxBytesDefault
	ParquetBloomFilterColumnEnabledKeyPrefix = internal.ParquetBloomFilterColumnEnabledKeyPrefix
	ParquetRootRepetitionKey                 = internal.ParquetRootRepetitionKey
	ParquetRootRepetitionDefault             = internal.ParquetRootRepetitionDefault

	ParquetBatchSizeKey     = internal.ParquetBatchSizeKey
	ParquetBatchSizeDefault = internal.ParquetBatchSizeDefault

	ManifestMergeEnabledKey     = "commit.manifest-merge.enabled"
	ManifestMergeEnabledDefault = false

	ManifestTargetSizeBytesKey     = "commit.manifest.target-size-bytes"
	ManifestTargetSizeBytesDefault = 8 * 1024 * 1024 // 8 MB

	ManifestMinMergeCountKey     = "commit.manifest.min-count-to-merge"
	ManifestMinMergeCountDefault = 100

	WritePartitionSummaryLimitKey     = "write.summary.partition-limit"
	WritePartitionSummaryLimitDefault = 0

	WriteDeleteModeKey     = "write.delete.mode"
	WriteDeleteModeDefault = WriteModeCopyOnWrite

	MetadataDeleteAfterCommitEnabledKey     = "write.metadata.delete-after-commit.enabled"
	MetadataDeleteAfterCommitEnabledDefault = false

	MetadataPreviousVersionsMaxKey     = "write.metadata.previous-versions-max"
	MetadataPreviousVersionsMaxDefault = 100

	MetadataCompressionKey     = "write.metadata.compression-codec"
	MetadataCompressionDefault = "none"

	WriteFormatDefaultKey     = "write.format.default"
	WriteFormatDefaultDefault = "parquet"

	WriteTargetFileSizeBytesKey     = "write.target-file-size-bytes"
	WriteTargetFileSizeBytesDefault = 512 * 1024 * 1024 // 512 MB

	MinSnapshotsToKeepKey     = "min-snapshots-to-keep"
	MinSnapshotsToKeepDefault = math.MaxInt

	MaxSnapshotAgeMsKey     = "max-snapshot-age-ms"
	MaxSnapshotAgeMsDefault = math.MaxInt

	MaxRefAgeMsKey     = "max-ref-age-ms"
	MaxRefAgeMsDefault = math.MaxInt

	// CommitNumRetriesKey is the number of commit retry attempts before
	// giving up on ErrCommitFailed from the catalog.
	//
	// The default is 0 (no retries) until refresh-and-replay lands; a
	// retry loop that reuses the original updates/requirements will
	// fail deterministically on genuine OCC conflicts and only slow
	// down the final error. Callers that observe transient catalog
	// flakiness (dropped connections, brief 409 during leader
	// election) can raise this to recover.
	CommitNumRetriesKey     = "commit.retry.num-retries"
	CommitNumRetriesDefault = 0

	// CommitMinRetryWaitMsKey is the initial wait time in milliseconds
	// for exponential backoff between commit retry attempts. Default: 100ms.
	CommitMinRetryWaitMsKey     = "commit.retry.min-wait-ms"
	CommitMinRetryWaitMsDefault = 100

	// CommitMaxRetryWaitMsKey is the maximum wait time in milliseconds
	// between commit retry attempts. Default: 60s.
	CommitMaxRetryWaitMsKey     = "commit.retry.max-wait-ms"
	CommitMaxRetryWaitMsDefault = 60 * 1000

	// CommitTotalRetryTimeoutMsKey bounds the total time spent across all
	// retry attempts. Default: 30 minutes.
	CommitTotalRetryTimeoutMsKey     = "commit.retry.total-timeout-ms"
	CommitTotalRetryTimeoutMsDefault = 30 * 60 * 1000
)

// Reserved properties
const (
	PropertyFormatVersion            = "format-version"
	PropertyUuid                     = "uuid"
	PropertySnapshotCount            = "snapshot-count"
	PropertyCurrentSnapshotId        = "current-snapshot-id"
	PropertyCurrentSnapshotSummary   = "current-snapshot-summary"
	PropertyCurrentSnapshotTimestamp = "current-snapshot-timestamp"
	PropertyCurrentSchema            = "current-schema"
	PropertyDefaultPartitionSpec     = "default-partition-spec"
	PropertyDefaultSortOrder         = "default-sort-order"
)

var ReservedProperties = [9]string{
	PropertyFormatVersion,
	PropertyUuid,
	PropertySnapshotCount,
	PropertyCurrentSnapshotId,
	PropertyCurrentSnapshotSummary,
	PropertyCurrentSnapshotTimestamp,
	PropertyCurrentSchema,
	PropertyDefaultPartitionSpec,
	PropertyDefaultSortOrder,
}

// Metadata compression codecs
const (
	MetadataCompressionCodecNone = "none"
	MetadataCompressionCodecGzip = "gzip"
	MetadataCompressionCodecZstd = "zstd"
)

// Write modes
const (
	WriteModeCopyOnWrite = "copy-on-write"
	WriteModeMergeOnRead = "merge-on-read"
)
