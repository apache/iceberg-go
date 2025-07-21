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
	"iter"
	"runtime"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/iceberg-go"
	"golang.org/x/sync/errgroup"
)

type PartitionedFanoutWriter struct {
	partitionSpec  iceberg.PartitionSpec
	schema         *iceberg.Schema
	rootLocation   string
	targetFileSize int64
	args           recordWritingArgs
	meta           *MetadataBuilder
	writers        WriterFactory
}

func NewPartitionedFanoutWriter(partitionSpec iceberg.PartitionSpec, schema *iceberg.Schema, rootLocation string, targetFileSize int64, args recordWritingArgs, meta *MetadataBuilder) *PartitionedFanoutWriter {
	return &PartitionedFanoutWriter{
		partitionSpec:  partitionSpec,
		schema:         schema,
		rootLocation:   rootLocation,
		targetFileSize: targetFileSize,
		args:           args,
		meta:           meta,
		writers:        NewWriterFactory(rootLocation, args, meta, targetFileSize),
	}
}

func (p *PartitionedFanoutWriter) partitionPath(data partitionRecord) string {
	return p.partitionSpec.PartitionToPath(data, p.schema)
}

func getWorkerCount(ctx context.Context) int {
	if workers, ok := ctx.Value("fanoutWorkers").(int); ok {
		return workers
	}
	return runtime.NumCPU()
}

func (p *PartitionedFanoutWriter) write(ctx context.Context) iter.Seq2[iceberg.DataFile, error] {
	numWorkers := getWorkerCount(ctx)

	inputRecordsCh := make(chan arrow.Record, numWorkers)
	outputDataFilesCh := make(chan iceberg.DataFile, numWorkers)

	fanoutWorkers, ctx := errgroup.WithContext(ctx)
	p.startRecordFeeder(ctx, fanoutWorkers, inputRecordsCh)

	for range numWorkers {
		fanoutWorkers.Go(func() error {
			return p.fanout(ctx, inputRecordsCh, outputDataFilesCh)
		})
	}

	return p.yieldDataFiles(ctx, fanoutWorkers, outputDataFilesCh)
}

func (p *PartitionedFanoutWriter) startRecordFeeder(ctx context.Context, fanoutWorkers *errgroup.Group, inputRecordsCh chan<- arrow.Record) {
	fanoutWorkers.Go(func() error {
		defer close(inputRecordsCh)
		for record, err := range p.args.itr {
			if err != nil {
				return err
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case inputRecordsCh <- record:
			}
		}
		return nil
	})
}

func (p *PartitionedFanoutWriter) fanout(ctx context.Context, inputRecordsCh <-chan arrow.Record, dataFilesChannel chan<- iceberg.DataFile) error {
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case record, ok := <-inputRecordsCh:
			if !ok {
				return nil
			}
			partitionMap := p.getPartitionMap(record)

			for partition, rowIndices := range partitionMap {
				select {
				case <-ctx.Done():
					return context.Cause(ctx)
				default:
				}

				partitionRecord, err := partitionBatchByKey(ctx)(record, rowIndices)
				if err != nil {
					return err
				}

				rollingDataWriter, err := p.writers.getOrCreateRollingDataWriter(partition)
				if err != nil {
					return err
				}

				rollingDataWriter.Add(ctx, partitionRecord, dataFilesChannel)
			}
		}
	}
}

func (p *PartitionedFanoutWriter) yieldDataFiles(ctx context.Context, fanoutWorkers *errgroup.Group, outputDataFilesCh chan iceberg.DataFile) iter.Seq2[iceberg.DataFile, error] {
	return iter.Seq2[iceberg.DataFile, error](func(yield func(iceberg.DataFile, error) bool) {
		done := make(chan struct{})
		var waitErr error

		go func() {
			defer close(done)
			waitErr = fanoutWorkers.Wait()
			p.writers.closeAll(ctx, outputDataFilesCh)
			close(outputDataFilesCh)
		}()

		for {
			select {
			case <-ctx.Done():
				<-done
				return
			case dataFile, ok := <-outputDataFilesCh:
				if !ok {
					if waitErr != nil {
						_ = yield(nil, waitErr)
					}
					return
				}
				if !yield(dataFile, nil) {
					return
				}
			}
		}
	})
}

func (p *PartitionedFanoutWriter) getPartitionMap(record arrow.Record) map[string][]int64 {
	partitionMap := make(map[string][]int64)
	partitionFields := p.partitionSpec.PartitionType(p.schema).FieldList

	for row := range record.NumRows() {
		partitionRec := make(partitionRecord, len(partitionFields))

		for i, field := range partitionFields {
			colIdx := record.Schema().FieldIndices(field.Name)[0]
			col := record.Column(colIdx)
			partitionRec[i] = col.GetOneForMarshal(int(row))
		}
		partitionKey := p.partitionPath(partitionRec)
		partitionMap[partitionKey] = append(partitionMap[partitionKey], row)
	}

	return partitionMap
}

type partitionBatchFn func(arrow.Record, []int64) (arrow.Record, error)

func partitionBatchByKey(ctx context.Context) partitionBatchFn {
	mem := compute.GetAllocator(ctx)

	return func(record arrow.Record, rowIndices []int64) (arrow.Record, error) {
		bldr := array.NewInt64Builder(mem)
		bldr.AppendValues(rowIndices, nil)
		rowIndicesArr := bldr.NewInt64Array()

		partitionedRecord, err := compute.Take(
			ctx,
			compute.TakeOptions{BoundsCheck: true},
			compute.NewDatumWithoutOwning(record),
			compute.NewDatumWithoutOwning(rowIndicesArr),
		)
		if err != nil {
			return nil, err
		}

		return partitionedRecord.(*compute.RecordDatum).Value, nil
	}
}
