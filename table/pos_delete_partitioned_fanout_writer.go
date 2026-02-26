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
	"iter"
	"maps"
	"slices"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
	"golang.org/x/sync/errgroup"
)

// positionDeletePartitionedFanoutWriter distributes Arrow position delete records across multiple partitions based on
// a partition specification, writing data to separate delete files for each partition using
// a fanout pattern with configurable parallelism.
type positionDeletePartitionedFanoutWriter struct {
	partitionContextByFilePath map[string]partitionContext
	metadata                   Metadata
	schema                     *iceberg.Schema
	itr                        iter.Seq2[arrow.RecordBatch, error]
	writerFactory              *writerFactory
	concurrentDataFileWriter   *concurrentDataFileWriter
}

// newPositionDeletePartitionedFanoutWriter creates a new PartitionedFanoutWriter with the specified
// partition specification, schema, and record iterator.
func newPositionDeletePartitionedFanoutWriter(metadata Metadata, concurrentWriter *concurrentDataFileWriter, partitionContextByFilePath map[string]partitionContext, itr iter.Seq2[arrow.RecordBatch, error], writerFactory *writerFactory) *positionDeletePartitionedFanoutWriter {
	return &positionDeletePartitionedFanoutWriter{
		partitionContextByFilePath: partitionContextByFilePath,
		metadata:                   metadata,
		schema:                     iceberg.PositionalDeleteSchema,
		itr:                        itr,
		writerFactory:              writerFactory,
		concurrentDataFileWriter:   concurrentWriter,
	}
}

// Write writes the Arrow records to the specified location using a fanout pattern with
// the specified number of workers. The returned iterator yields the data files written
// by the fanout process.
func (p *positionDeletePartitionedFanoutWriter) Write(ctx context.Context, workers int) iter.Seq2[iceberg.DataFile, error] {
	inputRecordsCh := make(chan arrow.RecordBatch, workers)
	outputDataFilesCh := make(chan iceberg.DataFile, workers)

	fanoutWorkers, ctx := errgroup.WithContext(ctx)
	startRecordFeeder(ctx, p.itr, fanoutWorkers, inputRecordsCh)

	for range workers {
		fanoutWorkers.Go(func() error {
			return p.fanout(ctx, inputRecordsCh, outputDataFilesCh)
		})
	}

	return p.yieldDataFiles(fanoutWorkers, outputDataFilesCh)
}

func (p *positionDeletePartitionedFanoutWriter) fanout(ctx context.Context, inputRecordsCh <-chan arrow.RecordBatch, dataFilesChannel chan<- iceberg.DataFile) error {
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)

		case record, ok := <-inputRecordsCh:
			if !ok {
				return nil
			}

			err := p.processBatch(ctx, record, dataFilesChannel)
			if err != nil {
				return err
			}
		}
	}
}

func (p *positionDeletePartitionedFanoutWriter) processBatch(ctx context.Context, batch arrow.RecordBatch, dataFilesChannel chan<- iceberg.DataFile) (err error) {
	defer batch.Release()

	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	default:
	}

	if batch.NumRows() == 0 {
		return
	}

	columns := batch.Columns()
	filePathArray := columns[0].(*array.String)
	filePath := filePathArray.ValueStr(0)
	partitionContext, ok := p.partitionContextByFilePath[filePath]
	if !ok {
		return fmt.Errorf("unexpected missing partition context for path %s", filePath)
	}

	partitionPath, err := p.partitionPath(partitionContext)
	if err != nil {
		return err
	}
	rollingDataWriter, err := p.writerFactory.getOrCreateRollingDataWriter(ctx, p.concurrentDataFileWriter, partitionPath, partitionContext.partitionData, dataFilesChannel)
	if err != nil {
		return err
	}

	err = rollingDataWriter.Add(batch)
	if err != nil {
		return err
	}

	return nil
}

func (p *positionDeletePartitionedFanoutWriter) partitionPath(partitionContext partitionContext) (string, error) {
	data := partitionRecord(slices.Collect(maps.Values(partitionContext.partitionData)))
	spec := p.metadata.PartitionSpecByID(int(partitionContext.specID))
	if spec == nil {
		return "", fmt.Errorf("unexpected missing partition spec in metadata for spec id %d", partitionContext.specID)
	}

	return spec.PartitionToPath(data, p.schema), nil
}

func (p *positionDeletePartitionedFanoutWriter) yieldDataFiles(fanoutWorkers *errgroup.Group, outputDataFilesCh chan iceberg.DataFile) iter.Seq2[iceberg.DataFile, error] {
	return yieldDataFiles(p.writerFactory, fanoutWorkers, outputDataFilesCh)
}
