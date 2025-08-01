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

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/iceberg-go"
	"golang.org/x/sync/errgroup"
)

type PartitionedFanoutWriter struct {
	partitionSpec iceberg.PartitionSpec
	schema        *iceberg.Schema
	itr           iter.Seq2[arrow.Record, error]
	writers       *WriterFactory
}

type PartitionInfo struct {
	rows            []int64
	partitionValues map[int]any
}

func NewPartitionedFanoutWriter(partitionSpec iceberg.PartitionSpec, schema *iceberg.Schema, itr iter.Seq2[arrow.Record, error]) *PartitionedFanoutWriter {
	return &PartitionedFanoutWriter{
		partitionSpec: partitionSpec,
		schema:        schema,
		itr:           itr,
	}
}

func (p *PartitionedFanoutWriter) partitionPath(data partitionRecord) string {
	return p.partitionSpec.PartitionToPath(data, p.schema)
}

func (p *PartitionedFanoutWriter) Write(ctx context.Context, workers int) iter.Seq2[iceberg.DataFile, error] {
	inputRecordsCh := make(chan arrow.Record, workers)
	outputDataFilesCh := make(chan iceberg.DataFile, workers)

	fanoutWorkers, ctx := errgroup.WithContext(ctx)
	if err := p.startRecordFeeder(ctx, fanoutWorkers, inputRecordsCh); err != nil {
		return func(yield func(iceberg.DataFile, error) bool) {
			yield(nil, fmt.Errorf("failed to start record feeder: %w", err))
		}
	}

	for i := 0; i < workers; i++ {
		fanoutWorkers.Go(func() error {
			return p.fanout(ctx, inputRecordsCh, outputDataFilesCh)
		})
	}

	return p.yieldDataFiles(ctx, fanoutWorkers, outputDataFilesCh)
}

func (p *PartitionedFanoutWriter) startRecordFeeder(ctx context.Context, fanoutWorkers *errgroup.Group, inputRecordsCh chan<- arrow.Record) error {
	fanoutWorkers.Go(func() error {
		defer close(inputRecordsCh)

		for record, err := range p.itr {
			if err != nil {
				return err
			}

			select {
			case <-ctx.Done():
				record.Release()
				if err := context.Cause(ctx); err != nil {
					return err
				}
				return nil
			case inputRecordsCh <- record:
			}
		}
		return nil
	})

	return nil
}

func (p *PartitionedFanoutWriter) fanout(ctx context.Context, inputRecordsCh <-chan arrow.Record, dataFilesChannel chan<- iceberg.DataFile) error {
	for {
		select {
		case <-ctx.Done():
			if err := context.Cause(ctx); err != nil {
				return err
			}
			return nil
		case record, ok := <-inputRecordsCh:
			if !ok {
				return nil
			}
			defer record.Release()

			partitionMap, err := p.getPartitionMap(record)
			if err != nil {
				return err
			}

			for partition, val := range partitionMap {
				select {
				case <-ctx.Done():
					if err := context.Cause(ctx); err != nil {
						return err
					}
					return nil
				default:
				}

				partitionRecord, err := partitionBatchByKey(ctx)(record, val.rows)
				if err != nil {
					return err
				}

				rollingDataWriter, err := p.writers.getOrCreateRollingDataWriter(partition, val.partitionValues)
				if err != nil {
					return err
				}

				err = rollingDataWriter.Add(ctx, partitionRecord, dataFilesChannel)
				if err != nil {
					return err
				}
			}
		}
	}
}

func (p *PartitionedFanoutWriter) yieldDataFiles(ctx context.Context, fanoutWorkers *errgroup.Group, outputDataFilesCh chan iceberg.DataFile) iter.Seq2[iceberg.DataFile, error] {
	return iter.Seq2[iceberg.DataFile, error](func(yield func(iceberg.DataFile, error) bool) {
		var dataFiles []iceberg.DataFile

		done := make(chan struct{})
		bufferDone := make(chan struct{})

		var waitErr, closeErr error

		go func() {
			defer close(bufferDone)
			for dataFile := range outputDataFilesCh {
				dataFiles = append(dataFiles, dataFile)
			}
		}()

		go func() {
			defer close(done)
			defer close(outputDataFilesCh)
			waitErr = fanoutWorkers.Wait()
			closeErr = p.writers.closeAll(ctx, outputDataFilesCh)
		}()

		<-done
		<-bufferDone

		if waitErr != nil {
			_ = yield(nil, waitErr)
			return
		}

		if closeErr != nil {
			_ = yield(nil, closeErr)
			return
		}

		for _, dataFile := range dataFiles {
			if !yield(dataFile, nil) {
				return
			}
		}
	})
}

func (p *PartitionedFanoutWriter) getPartitionMap(record arrow.Record) (map[string]PartitionInfo, error) {
	partitionMap := make(map[string]PartitionInfo)
	partitionFields := p.partitionSpec.PartitionType(p.schema).FieldList

	for row := range record.NumRows() {
		partitionRec := make(partitionRecord, len(partitionFields))
		partitionValues := make(map[int]any)

		for i := range partitionFields {
			sourceField := p.partitionSpec.Field(i)
			colName, _ := p.schema.FindColumnName(sourceField.SourceID)
			colIdx := record.Schema().FieldIndices(colName)[0]
			col := record.Column(colIdx)

			val, err := getArrowValueAsIcebergLiteral(col, int(row))
			if err != nil {
				return nil, fmt.Errorf("failed to get arrow value as iceberg literal: %w", err)
			}

			transformedLiteral := sourceField.Transform.Apply(iceberg.Optional[iceberg.Literal]{
				Valid: val != nil,
				Val:   val,
			})

			if transformedLiteral.Valid {
				partitionRec[i] = transformedLiteral.Val.Any()
				partitionValues[sourceField.FieldID] = transformedLiteral.Val.Any()
			}
		}
		partitionKey := p.partitionPath(partitionRec)
		partVal := partitionMap[partitionKey]
		partVal.rows = append(partitionMap[partitionKey].rows, row)
		partVal.partitionValues = partitionValues
		partitionMap[partitionKey] = partVal
	}

	return partitionMap, nil
}

type partitionBatchFn func(arrow.Record, []int64) (arrow.Record, error)

func partitionBatchByKey(ctx context.Context) partitionBatchFn {
	mem := compute.GetAllocator(ctx)

	return func(record arrow.Record, rowIndices []int64) (arrow.Record, error) {
		bldr := array.NewInt64Builder(mem)
		defer bldr.Release()

		bldr.AppendValues(rowIndices, nil)
		rowIndicesArr := bldr.NewInt64Array()
		defer rowIndicesArr.Release()

		partitionedRecord, err := compute.Take(
			ctx,
			*compute.DefaultTakeOptions(),
			compute.NewDatumWithoutOwning(record),
			compute.NewDatumWithoutOwning(rowIndicesArr),
		)
		if err != nil {
			return nil, err
		}

		return partitionedRecord.(*compute.RecordDatum).Value, nil
	}
}

func getArrowValueAsIcebergLiteral(column arrow.Array, row int) (iceberg.Literal, error) {
	if column.IsNull(row) {
		return nil, nil
	}

	switch arr := column.(type) {
	case *array.Date32:
		return iceberg.NewLiteral(iceberg.Date(arr.Value(row))), nil
	case *array.Time64:
		return iceberg.NewLiteral(iceberg.Time(arr.Value(row))), nil
	case *array.Timestamp:
		return iceberg.NewLiteral(iceberg.Timestamp(arr.Value(row))), nil
	case *array.Decimal128:
		val := arr.Value(row)
		dec := iceberg.Decimal{
			Val:   val,
			Scale: int(arr.DataType().(*arrow.Decimal128Type).Scale),
		}
		return iceberg.NewLiteral(dec), nil
	default:
		val := column.GetOneForMarshal(row)
		switch v := val.(type) {
		case bool:
			return iceberg.NewLiteral(v), nil
		case int32:
			return iceberg.NewLiteral(v), nil
		case int64:
			return iceberg.NewLiteral(v), nil
		case float32:
			return iceberg.NewLiteral(v), nil
		case float64:
			return iceberg.NewLiteral(v), nil
		case string:
			return iceberg.NewLiteral(v), nil
		case []byte:
			return iceberg.NewLiteral(v), nil
		default:
			return nil, fmt.Errorf("unsupported value type: %T", v)
		}
	}
}
