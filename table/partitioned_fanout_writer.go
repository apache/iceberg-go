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
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/iceberg-go"
	"golang.org/x/sync/errgroup"
)

// PartitionedFanoutWriter distributes Arrow records across multiple partitions based on
// a partition specification, writing data to separate files for each partition using
// a fanout pattern with configurable parallelism.
type partitionedFanoutWriter struct {
	partitionSpec iceberg.PartitionSpec
	schema        *iceberg.Schema
	itr           iter.Seq2[arrow.RecordBatch, error]
	writers       *writerFactory
}

// PartitionInfo holds the row indices and partition values for a specific partition,
// used during the fanout process to group rows by their partition key.
type partitionInfo struct {
	rows            []int64
	partitionValues map[int]any
	partitionRec    partitionRecord // The actual partition values for generating the path
}

// NewPartitionedFanoutWriter creates a new PartitionedFanoutWriter with the specified
// partition specification, schema, and record iterator.
func newPartitionedFanoutWriter(partitionSpec iceberg.PartitionSpec, schema *iceberg.Schema, itr iter.Seq2[arrow.RecordBatch, error]) *partitionedFanoutWriter {
	return &partitionedFanoutWriter{
		partitionSpec: partitionSpec,
		schema:        schema,
		itr:           itr,
	}
}

func (p *partitionedFanoutWriter) partitionPath(data partitionRecord) string {
	return p.partitionSpec.PartitionToPath(data, p.schema)
}

// Write writes the Arrow records to the specified location using a fanout pattern with
// the specified number of workers. The returned iterator yields the data files written
// by the fanout process.
func (p *partitionedFanoutWriter) Write(ctx context.Context, workers int) iter.Seq2[iceberg.DataFile, error] {
	inputRecordsCh := make(chan arrow.RecordBatch, workers)
	outputDataFilesCh := make(chan iceberg.DataFile, workers)

	fanoutWorkers, ctx := errgroup.WithContext(ctx)
	p.startRecordFeeder(ctx, fanoutWorkers, inputRecordsCh)

	for range workers {
		fanoutWorkers.Go(func() error {
			return p.fanout(ctx, inputRecordsCh, outputDataFilesCh)
		})
	}

	return p.yieldDataFiles(fanoutWorkers, outputDataFilesCh)
}

func (p *partitionedFanoutWriter) startRecordFeeder(ctx context.Context, fanoutWorkers *errgroup.Group, inputRecordsCh chan<- arrow.RecordBatch) {
	fanoutWorkers.Go(func() error {
		defer close(inputRecordsCh)

		for record, err := range p.itr {
			if err != nil {
				return err
			}

			record.Retain()
			select {
			case <-ctx.Done():
				record.Release()

				return context.Cause(ctx)
			case inputRecordsCh <- record:
			}
		}

		return nil
	})
}

func (p *partitionedFanoutWriter) fanout(ctx context.Context, inputRecordsCh <-chan arrow.RecordBatch, dataFilesChannel chan<- iceberg.DataFile) error {
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)

		case record, ok := <-inputRecordsCh:
			if !ok {
				return nil
			}
			defer record.Release()

			partitions, err := p.getPartitions(record)
			if err != nil {
				return err
			}

			for _, val := range partitions {
				select {
				case <-ctx.Done():
					return context.Cause(ctx)
				default:
				}

				partitionRecord, err := partitionBatchByKey(ctx)(record, val.rows)
				if err != nil {
					return err
				}

				partitionPath := p.partitionPath(val.partitionRec)
				rollingDataWriter, err := p.writers.getOrCreateRollingDataWriter(ctx, partitionPath, val.partitionValues, dataFilesChannel)
				if err != nil {
					return err
				}

				err = rollingDataWriter.Add(partitionRecord)
				if err != nil {
					return err
				}
			}
		}
	}
}

func (p *partitionedFanoutWriter) yieldDataFiles(fanoutWorkers *errgroup.Group, outputDataFilesCh chan iceberg.DataFile) iter.Seq2[iceberg.DataFile, error] {
	// Use a channel to safely communicate the error from the goroutine
	// to avoid a data race between writing err in the goroutine and reading it in the iterator.
	errCh := make(chan error, 1)
	go func() {
		defer close(outputDataFilesCh)
		err := fanoutWorkers.Wait()
		err = errors.Join(err, p.writers.closeAll())
		errCh <- err
		close(errCh)
	}()

	return func(yield func(iceberg.DataFile, error) bool) {
		defer func() {
			for range outputDataFilesCh {
			}
		}()

		// Yield data files as they arrive - no error yet since goroutine is still running
		for f := range outputDataFilesCh {
			if !yield(f, nil) {
				return
			}
		}

		// Channel is closed, now safe to read the error
		if err := <-errCh; err != nil {
			yield(nil, err)
		}
	}
}

func (p *partitionedFanoutWriter) getPartitions(record arrow.RecordBatch) ([]*partitionInfo, error) {
	partitionMap := newPartitionMapNode()
	partitionFields := p.partitionSpec.PartitionType(p.schema).FieldList
	partitionRec := make(partitionRecord, len(partitionFields))

	partitionColumns := make([]arrow.Array, len(partitionFields))
	partitionFieldsInfo := make([]struct {
		sourceField *iceberg.PartitionField
		fieldID     int
	}, len(partitionFields))

	for i := range partitionFields {
		sourceField := p.partitionSpec.Field(i)
		colName, _ := p.schema.FindColumnName(sourceField.SourceID)
		colIdx := record.Schema().FieldIndices(colName)[0]
		partitionColumns[i] = record.Column(colIdx)
		partitionFieldsInfo[i] = struct {
			sourceField *iceberg.PartitionField
			fieldID     int
		}{&sourceField, sourceField.FieldID}
	}

	for row := range record.NumRows() {
		for i := range partitionFields {
			col := partitionColumns[i]
			if !col.IsNull(int(row)) {
				sourceField := partitionFieldsInfo[i].sourceField
				val, err := getArrowValueAsIcebergLiteral(col, int(row))
				if err != nil {
					return nil, fmt.Errorf("failed to get arrow values as iceberg literal: %w", err)
				}

				transformedLiteral := sourceField.Transform.Apply(iceberg.Optional[iceberg.Literal]{Valid: true, Val: val})
				if transformedLiteral.Valid {
					partitionRec[i] = transformedLiteral.Val.Any()
				} else {
					partitionRec[i] = nil
				}
			} else {
				partitionRec[i] = nil
			}
		}

		// Get or create partition info for this partition key
		partVal := partitionMap.getOrCreate(partitionRec, partitionFieldsInfo)
		partVal.rows = append(partVal.rows, row)
	}

	return partitionMap.collectPartitions(), nil
}

// partitionMapNode represents a simple tree structure for storing partitionInfo.
//
// Each key is the partition value at that level of the tree, and the key hierarchy
// is in the order of the partition spec.
// The value is either a *partitionMapNode or a *partitionInfo.
type partitionMapNode struct {
	children  map[any]any
	leafCount int
}

func newPartitionMapNode() *partitionMapNode {
	return &partitionMapNode{
		children:  make(map[any]any),
		leafCount: 0,
	}
}

// getOrCreate navigates the tree and returns the partitionInfo for the given partition key,
// creating nodes along the way if they don't exist
func (n *partitionMapNode) getOrCreate(partitionRec partitionRecord, fieldInfo []struct {
	sourceField *iceberg.PartitionField
	fieldID     int
},
) *partitionInfo {
	// Navigate through all but the last partition field
	node := n
	for _, part := range partitionRec[:len(partitionRec)-1] {
		val, ok := node.children[part]
		if !ok {
			newNode := newPartitionMapNode()
			node.children[part] = newNode
			node = newNode
		} else {
			node = val.(*partitionMapNode)
		}
	}

	// Last level stores the actual partitionInfo
	lastKey := partitionRec[len(partitionRec)-1]
	partVal, ok := node.children[lastKey].(*partitionInfo)
	if ok {
		return partVal
	}

	// First time seeing this partition - create partitionValues map
	partitionValues := make(map[int]any, len(partitionRec))

	// Copy partitionRec values so they don't get overwritten
	partRecCopy := make(partitionRecord, len(partitionRec))
	for i := range partitionRec {
		partitionValues[fieldInfo[i].fieldID] = partitionRec[i]
		partRecCopy[i] = partitionRec[i]
	}

	partVal = &partitionInfo{
		rows:            make([]int64, 0, 128), // modest starting capacity
		partitionValues: partitionValues,
		partitionRec:    partRecCopy,
	}
	node.children[lastKey] = partVal
	node.leafCount++

	return partVal
}

// collectPartitions recursively collects all partitionInfo into a slice
func (n *partitionMapNode) collectPartitions() []*partitionInfo {
	result := make([]*partitionInfo, 0, n.leafCount)

	for _, v := range n.children {
		switch node := v.(type) {
		case *partitionInfo:
			result = append(result, node)
		case *partitionMapNode:
			// Recursively collect from child nodes
			result = append(result, node.collectPartitions()...)
		}
	}

	return result
}

type partitionBatchFn func(arrow.RecordBatch, []int64) (arrow.RecordBatch, error)

func partitionBatchByKey(ctx context.Context) partitionBatchFn {
	mem := compute.GetAllocator(ctx)

	return func(record arrow.RecordBatch, rowIndices []int64) (arrow.RecordBatch, error) {
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
	case *array.Decimal32:
		val := arr.Value(row)
		dec := iceberg.Decimal{
			Val:   decimal128.FromU64(uint64(val)),
			Scale: int(arr.DataType().(*arrow.Decimal32Type).Scale),
		}

		return iceberg.NewLiteral(dec), nil
	case *array.Decimal64:
		val := arr.Value(row)
		dec := iceberg.Decimal{
			Val:   decimal128.FromU64(uint64(val)),
			Scale: int(arr.DataType().(*arrow.Decimal64Type).Scale),
		}

		return iceberg.NewLiteral(dec), nil
	case *array.Decimal128:
		val := arr.Value(row)
		dec := iceberg.Decimal{
			Val:   val,
			Scale: int(arr.DataType().(*arrow.Decimal128Type).Scale),
		}

		return iceberg.NewLiteral(dec), nil
	case *extensions.UUIDArray:

		return iceberg.NewLiteral(arr.Value(row)), nil

	case *array.String:

		return iceberg.NewLiteral(arr.Value(row)), nil
	case *array.Int64:

		return iceberg.NewLiteral(arr.Value(row)), nil
	case *array.Int32:

		return iceberg.NewLiteral(arr.Value(row)), nil
	case *array.Int16:

		return iceberg.NewLiteral(int32(arr.Value(row))), nil
	case *array.Int8:

		return iceberg.NewLiteral(int32(arr.Value(row))), nil
	case *array.Uint64:

		return iceberg.NewLiteral(int64(arr.Value(row))), nil
	case *array.Uint32:

		return iceberg.NewLiteral(int32(arr.Value(row))), nil
	case *array.Uint16:

		return iceberg.NewLiteral(int32(arr.Value(row))), nil
	case *array.Uint8:

		return iceberg.NewLiteral(int32(arr.Value(row))), nil
	case *array.Float32:

		return iceberg.NewLiteral(arr.Value(row)), nil
	case *array.Float64:

		return iceberg.NewLiteral(arr.Value(row)), nil
	case *array.Boolean:

		return iceberg.NewLiteral(arr.Value(row)), nil
	case *array.Binary:

		return iceberg.NewLiteral(arr.Value(row)), nil

	default:
		val := column.GetOneForMarshal(row)

		return nil, fmt.Errorf("unsupported value type: %T", val)
	}
}
