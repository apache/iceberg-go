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
	"fmt"

	"github.com/apache/iceberg-go"
)

// SetStatistics registers a previously written Puffin statistics file with
// the file's snapshot. A file for the same snapshot replaces the previous
// reference. This only stages a metadata update; Commit publishes it, and
// the caller is responsible for writing a valid Puffin file beforehand.
func (t *Transaction) SetStatistics(stats StatisticsFile) error {
	if _, err := t.txnMeta(); err != nil {
		return err
	}
	if stats.StatisticsPath == "" || stats.FileSizeInBytes <= 0 ||
		stats.FileFooterSizeInBytes <= 0 || stats.FileFooterSizeInBytes > stats.FileSizeInBytes {
		return fmt.Errorf("%w: statistics path must be nonempty, and file and footer sizes must be positive with footer size at most file size",
			iceberg.ErrInvalidArgument)
	}
	if stats.BlobMetadata == nil {
		return fmt.Errorf("%w: statistics blob-metadata must be a list", iceberg.ErrInvalidArgument)
	}
	for idx, blob := range stats.BlobMetadata {
		if blob.Type == "" || blob.Fields == nil {
			return fmt.Errorf("%w: statistics blob %d requires a type and fields list",
				iceberg.ErrInvalidArgument, idx)
		}
	}
	// A transaction may be committed after the caller has reused its buffers.
	// Keep the staged update independent of the caller's maps and slices.
	registered := cloneStatisticsFiles([]StatisticsFile{stats})[0]

	return t.apply([]Update{NewSetStatisticsUpdate(registered)}, nil)
}

// RemoveStatistics removes the statistics file reference for snapshotID. An
// absent reference is not an error. The physical Puffin file is not deleted.
func (t *Transaction) RemoveStatistics(snapshotID int64) error {
	if _, err := t.txnMeta(); err != nil {
		return err
	}

	return t.apply([]Update{NewRemoveStatisticsUpdate(snapshotID)}, nil)
}
