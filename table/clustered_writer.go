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
	"errors"
	"fmt"
	"iter"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/iceberg-go"
)

// clusteredPartitionedWrite writes records to partitioned data files,
// keeping at most one partition writer open at a time. When the
// partition changes, the current writer is closed before opening a
// new one. This is the memory-efficient write path for pre-clustered
// input (e.g. compaction reads where each source file belongs to a
// single partition).
//
// The input must be strictly clustered by partition: once a partition's
// writer has been closed, encountering further records for that
// partition is treated as a violation of the clustering assumption and
// returns an error. Use the fanout writer if the input is not
// clustered.
func clusteredPartitionedWrite(
	ctx context.Context,
	spec iceberg.PartitionSpec,
	schema *iceberg.Schema,
	factory *writerFactory,
	records iter.Seq2[arrow.RecordBatch, error],
) iter.Seq2[iceberg.DataFile, error] {
	outputCh := make(chan iceberg.DataFile, 1)
	errCh := make(chan error, 1)

	go func() {
		defer close(outputCh)
		defer factory.stopCount()

		var (
			currentKey          string
			currentWriter       *RollingDataWriter
			completedPartitions = make(map[string]struct{})
		)

		closeCurrentWriter := func() error {
			if currentWriter == nil {
				return nil
			}
			w := currentWriter
			currentWriter = nil
			completedPartitions[currentKey] = struct{}{}
			close(w.recordCh)
			w.wg.Wait()

			// stream's deferred close(errorCh) runs before its
			// deferred wg.Done, so by the time Wait returns the
			// channel is closed; this read never blocks and yields
			// either the buffered error or nil.
			return <-w.errorCh
		}

		fail := func(err error) {
			errCh <- errors.Join(err, closeCurrentWriter())
			close(errCh)
		}

		takeFn := partitionBatchByKey(ctx)

		for rec, err := range records {
			if err != nil {
				fail(err)

				return
			}

			partitions, err := getRecordPartitions(spec, schema, rec)
			if err != nil {
				fail(err)

				return
			}

			for _, part := range partitions {
				select {
				case <-ctx.Done():
					fail(context.Cause(ctx))

					return
				default:
				}

				subBatch, err := takeFn(rec, part.rows)
				if err != nil {
					fail(err)

					return
				}

				partitionPath := spec.PartitionToPath(part.partitionRec, schema)
				if currentWriter == nil || partitionPath != currentKey {
					if err := closeCurrentWriter(); err != nil {
						subBatch.Release()
						errCh <- err
						close(errCh)

						return
					}
					if _, seen := completedPartitions[partitionPath]; seen {
						subBatch.Release()
						fail(fmt.Errorf("clustered write: incoming records violate the clustering assumption; "+
							"partition %q has records arriving after its writer was already closed", partitionPath))

						return
					}
					currentWriter = factory.newRollingDataWriter(
						ctx, nil, partitionPath, part.partitionValues, outputCh)
					currentKey = partitionPath
				}

				addErr := currentWriter.Add(subBatch)
				subBatch.Release()
				if addErr != nil {
					fail(addErr)

					return
				}
			}
		}

		if err := closeCurrentWriter(); err != nil {
			errCh <- err
		}
		close(errCh)
	}()

	return func(yield func(iceberg.DataFile, error) bool) {
		defer func() {
			for range outputCh {
			}
		}()

		for df := range outputCh {
			if !yield(df, nil) {
				return
			}
		}

		if err := <-errCh; err != nil {
			yield(nil, err)
		}
	}
}
