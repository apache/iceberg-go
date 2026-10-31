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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package table

import (
	"context"
	"fmt"
	"maps"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
	"github.com/google/uuid"
)

// dynamicPartitionKeys normalizes all partition values before comparing them.
// A manifest decoder may represent the same logical day as an int32 while the
// writer uses iceberg.Date. The shared canonical encoder handles both forms,
// decimals, bytes, UUIDs, and nulls without lossy string formatting.
func dynamicPartitionKeys(files []iceberg.DataFile) (map[string]struct{}, error) {
	keys := make(map[string]struct{}, len(files))
	for _, file := range files {
		key, err := canonicalPartitionKey(file.SpecID(), dataFilePartition(file))
		if err != nil {
			return nil, fmt.Errorf("dynamic partition overwrite file %q: %w", file.FilePath(), err)
		}
		keys[key] = struct{}{}
	}

	return keys, nil
}

func (t *Transaction) dynamicOverwriteSpec() (*iceberg.PartitionSpec, error) {
	meta, err := t.txnMeta()
	if err != nil {
		return nil, err
	}

	if meta.formatVersion >= 3 {
		// Row lineage in v3 needs dedicated first-row-ID reassignment when
		// replacing partitions. Do not use rewrite semantics to bypass it.
		return nil, fmt.Errorf("%w: dynamic partition overwrite does not support format v3 row lineage", ErrInvalidOperation)
	}

	spec, err := meta.CurrentSpec()
	if err != nil {
		return nil, err
	}
	if spec == nil || spec.NumFields() == 0 {
		return nil, fmt.Errorf("%w: dynamic partition overwrite requires a partitioned table; use Overwrite for unpartitioned tables", ErrInvalidOperation)
	}
	if err := checkNoUnknownTransform(spec); err != nil {
		return nil, err
	}

	return spec, nil
}

// DynamicPartitionOverwriteDataFiles atomically replaces all live data files
// in the partitions represented by filesToAdd. Other partitions are unchanged.
//
// The added files must use the table's current partition spec. Existing live
// data files must use that spec as well: overlapping partitions from an older
// spec cannot be identified by comparing tuples from different transforms.
// Rather than silently leave stale rows, this operation rejects mixed-spec
// tables. It supports identity and transformed partition fields alike.
//
// The operation pins the target branch head and default table shape. A
// concurrent snapshot commit or partition evolution fails instead of silently
// leaving newly appended files in a replaced partition. This also means the
// method should run on a fresh transaction, before other staged snapshot or
// schema/spec changes.
//
// An empty input is a no-op. Unpartitioned tables and format-v3 tables are
// rejected. All files must be pre-written and have valid DataFile metadata,
// just as with AddDataFiles. Uncommitted files remain the caller's responsibility
// if staging or the eventual transaction commit fails.
func (t *Transaction) DynamicPartitionOverwriteDataFiles(
	ctx context.Context, filesToAdd []iceberg.DataFile, snapshotProps iceberg.Properties,
) error {
	spec, err := t.dynamicOverwriteSpec()
	if err != nil {
		return err
	}
	if len(filesToAdd) == 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	if _, err := t.validateDataFilesToAdd(filesToAdd, "DynamicPartitionOverwriteDataFiles", true); err != nil {
		return err
	}
	for _, df := range filesToAdd {
		if int(df.SpecID()) != spec.ID() {
			return fmt.Errorf("%w: dynamic partition overwrite file %q uses spec %d, current spec is %d",
				ErrInvalidOperation, df.FilePath(), df.SpecID(), spec.ID())
		}
	}

	fs, err := t.tbl.fsF(ctx)
	if err != nil {
		return err
	}

	keys, err := dynamicPartitionKeys(filesToAdd)
	if err != nil {
		return err
	}

	var toDelete []iceberg.DataFile
	validationPlan := newPartitionValidationPlan(spec)
	if snapshot := t.planningSnapshot(t.meta); snapshot != nil {
		for df, err := range snapshot.dataFiles(fs, nil) {
			if err != nil {
				return err
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if df.ContentType() != iceberg.EntryContentData {
				continue
			}
			if int(df.SpecID()) != spec.ID() {
				return fmt.Errorf("%w: dynamic partition overwrite cannot replace data spanning partition spec %d (current %d)",
					ErrInvalidOperation, df.SpecID(), spec.ID())
			}
			if err := validationPlan.validate(df); err != nil {
				return fmt.Errorf("%w: invalid live partition in file %q: %w",
					ErrInvalidOperation, df.FilePath(), err)
			}
			key, err := canonicalPartitionKey(df.SpecID(), dataFilePartition(df))
			if err != nil {
				return fmt.Errorf("dynamic partition overwrite live file %q: %w", df.FilePath(), err)
			}
			if _, touched := keys[key]; touched {
				toDelete = append(toDelete, df)
			}
		}
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	// A ref fence is important even if no files matched: an append to
	// touched partition between planning and commit must not survive.
	if err := t.AssertDefaultShape(); err != nil {
		return err
	}
	if err := t.AssertRefSnapshotID(""); err != nil {
		return err
	}

	props := make(iceberg.Properties, len(snapshotProps)+1)
	maps.Copy(props, snapshotProps)
	props["replace-partitions"] = "true"

	return t.ReplaceDataFiles(ctx, toDelete, filesToAdd, props)
}

// DynamicPartitionOverwrite writes Arrow records to data files and replaces
// only the partitions represented by the written records. In particular,
// an empty reader never clears existing partitions.
func (t *Transaction) DynamicPartitionOverwrite(
	ctx context.Context, rdr array.RecordReader, snapshotProps iceberg.Properties,
) error {
	if rdr == nil {
		return fmt.Errorf("%w: nil record reader for dynamic partition overwrite", ErrInvalidOperation)
	}
	if _, err := t.dynamicOverwriteSpec(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	meta, err := t.txnMeta()
	if err != nil {
		return err
	}
	fs, err := t.tbl.fsF(ctx)
	if err != nil {
		return err
	}
	wfs, err := requireWriteFileIO(fs)
	if err != nil {
		return err
	}

	writeUUID := uuid.New()
	itr := recordsToDataFiles(ctx, t.tbl.Location(), meta, recordWritingArgs{
		sc:        rdr.Schema(),
		itr:       array.IterFromReader(rdr),
		fs:        wfs,
		writeUUID: &writeUUID,
	})

	var written []iceberg.DataFile
	for df, err := range itr {
		if err != nil {
			return err
		}
		written = append(written, df)
	}

	return t.DynamicPartitionOverwriteDataFiles(ctx, written, snapshotProps)
}

// DynamicPartitionOverwriteTable is the Arrow-table variant of
// DynamicPartitionOverwrite.
func (t *Transaction) DynamicPartitionOverwriteTable(
	ctx context.Context, tbl arrow.Table, batchSize int64, snapshotProps iceberg.Properties,
) error {
	if tbl == nil {
		return fmt.Errorf("%w: nil Arrow table for dynamic partition overwrite", ErrInvalidOperation)
	}
	rdr := array.NewTableReader(tbl, batchSize)
	defer rdr.Release()

	return t.DynamicPartitionOverwrite(ctx, rdr, snapshotProps)
}

// DynamicPartitionOverwrite commits a dynamic overwrite in one transaction.
func (t Table) DynamicPartitionOverwrite(
	ctx context.Context, rdr array.RecordReader, snapshotProps iceberg.Properties,
) (*Table, error) {
	txn := t.NewTransaction()
	if err := txn.DynamicPartitionOverwrite(ctx, rdr, snapshotProps); err != nil {
		return nil, err
	}

	return txn.Commit(ctx)
}

// DynamicPartitionOverwriteTable is the Arrow-table convenience wrapper.
func (t Table) DynamicPartitionOverwriteTable(
	ctx context.Context, tbl arrow.Table, batchSize int64, snapshotProps iceberg.Properties,
) (*Table, error) {
	txn := t.NewTransaction()
	if err := txn.DynamicPartitionOverwriteTable(ctx, tbl, batchSize, snapshotProps); err != nil {
		return nil, err
	}

	return txn.Commit(ctx)
}
