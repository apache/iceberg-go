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
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
)

// DeleteFiles returns the live equality-delete, position-delete, and
// deletion-vector files in the current snapshot. It reports file metadata and
// does not open or scan the delete-file contents.
func (i InspectTable) DeleteFiles(ctx context.Context) (array.RecordReader, error) {
	partitionType := inspectPartitionType(i.tbl.metadata)
	schema := DeleteFilesSchema(partitionType)
	arrowSchema, err := SchemaToArrowSchema(schema, nil, true, false)
	if err != nil {
		return nil, fmt.Errorf("inspect delete files: build arrow schema: %w", err)
	}

	rr, err := i.manifestEntryReader(ctx, arrowSchema, true,
		func(manifest iceberg.ManifestFile) bool {
			return manifest.ManifestContent() == iceberg.ManifestContentDeletes
		},
		func(bldr *array.RecordBuilder, entry iceberg.ManifestEntry) error {
			return appendContentFileRecord(bldr, partitionType, entry.DataFile())
		})
	if err != nil {
		return nil, fmt.Errorf("inspect delete files: %w", err)
	}

	return rr, nil
}

// DeleteFilesSchema returns the content-file schema used by the delete_files
// metadata table. It is shared with DataFilesSchema so both tables expose the
// same partition-evolution and nested metric semantics.
func DeleteFilesSchema(partitionType *iceberg.StructType) *iceberg.Schema {
	return DataFilesSchema(partitionType)
}
