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
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/iceberg-go"
	"golang.org/x/sync/errgroup"
)

type fanoutArgs struct {
	partitionSpec  iceberg.PartitionSpec
	args           recordWritingArgs
	meta           *MetadataBuilder
	rootLocation   string
	targetFileSize int64
}

var rollingDataWriters = make(map[string]*RollingDataWriter)

func FanoutWriter(ctx context.Context, rootLocation string, args recordWritingArgs, meta *MetadataBuilder, targetFileSize int64) iter.Seq2[iceberg.DataFile, error] {
	fanoutArgs := fanoutArgs{
		partitionSpec:  meta.CurrentSpec(),
		meta:           meta,
		rootLocation:   rootLocation,
		targetFileSize: targetFileSize,
		args:           args,
	}

	workers, ok := ctx.Value("fanoutWorkers").(int)
	if !ok {
		workers = runtime.NumCPU()
	}

	fanoutWriters := errgroup.Group{}
	inputRecordsCh := make(chan arrow.Record, workers)
	outputDataFilesCh := make(chan iceberg.DataFile, workers)

	go func() {
		defer close(inputRecordsCh)
		for record, err := range args.itr {
			if err != nil {
				panic(err)
			}
			inputRecordsCh <- record
		}
	}()

	for range workers {
		fanoutWriters.Go(func() error {
			return fanout(ctx, fanoutArgs, inputRecordsCh, outputDataFilesCh)
		})
	}

	go func() {
		fanoutWriters.Wait()
		close(outputDataFilesCh)
	}()

	if err := fanoutWriters.Wait(); err != nil {
		panic(err)
	}

	partitionedDataFiles := iter.Seq2[iceberg.DataFile, error](func(yield func(iceberg.DataFile, error) bool) {
		for dataFile := range outputDataFilesCh {
			if !yield(dataFile, nil) {
				break
			}
		}
	})

	return partitionedDataFiles
}

func fanout(ctx context.Context, fanoutArgs fanoutArgs, inputRecordsCh <-chan arrow.Record, dataFilesChannel chan<- iceberg.DataFile) error {
	for record := range inputRecordsCh {
		partitionMap := getPartitionMap(record, fanoutArgs.partitionSpec)
		for partition, rowIndices := range partitionMap {
			partitionRecord, err := partitionBatchByKey(ctx)(record, rowIndices)
			if err != nil {
				return err
			}

			rollingDataWriter, ok := rollingDataWriters[partition]
			if !ok {
				rollingDataWriter = NewRollingDataWriter(partition, fanoutArgs)
				rollingDataWriters[partition] = rollingDataWriter
			}

			rollingDataWriter.Add(ctx, partitionRecord, dataFilesChannel)
		}
	}

	return nil
}

func getPartitionMap(record arrow.Record, partitionSpec iceberg.PartitionSpec) map[string][]int64 {
	partitionFields := partitionSpec.Fields()
	recordSchema := record.Schema()

	partitionFieldIndexes := make([]int, 0, partitionSpec.NumFields())
	for field := range partitionFields {
		index := recordSchema.FieldIndices(field.Name)[0]
		partitionFieldIndexes = append(partitionFieldIndexes, index)
	}

	partitionMap := make(map[string][]int64)

	for rowIdx := range record.NumRows() {
		var partitionKey []string
		for colIdx, fieldIdx := range partitionFieldIndexes {
			partitionField := partitionSpec.Field(colIdx)
			partitionValue := iceberg.StringLiteral(record.Column(fieldIdx).ValueStr(int(rowIdx)))
			transform := partitionField.Transform.Apply(iceberg.Optional[iceberg.Literal]{Val: partitionValue})
			partitionKey = append(partitionKey, transform.Val.String())
		}

		partition := strings.Join(partitionKey, "/")
		partitionMap[partition] = append(partitionMap[partition], int64(rowIdx))
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
