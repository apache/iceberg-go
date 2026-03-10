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
	"net/url"
	"sync"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/iceberg-go"
)

// WriterFactory manages the creation and lifecycle of RollingDataWriter instances
// for different partitions, providing shared configuration and coordination
// across all writers in a partitioned write operation.
type writerFactory struct {
	rootLocation       string
	args               recordWritingArgs
	meta               *MetadataBuilder
	taskSchema         *iceberg.Schema
	targetFileSize     int64
	writers            sync.Map
	nextCount          func() (int, bool)
	stopCount          func()
	partitionIDCounter atomic.Int64 // partitionIDCounter generates unique IDs for partitions
	mu                 sync.Mutex
}

// NewWriterFactory creates a new WriterFactory with the specified configuration
// for managing rolling data writerFactory across partitions.
func NewWriterFactory(rootLocation string, args recordWritingArgs, meta *MetadataBuilder, taskSchema *iceberg.Schema, targetFileSize int64) writerFactory {
	nextCount, stopCount := iter.Pull(args.counter)

	return writerFactory{
		rootLocation:   rootLocation,
		args:           args,
		meta:           meta,
		taskSchema:     taskSchema,
		targetFileSize: targetFileSize,
		nextCount:      nextCount,
		stopCount:      stopCount,
	}
}

// RollingDataWriter accumulates Arrow records for a specific partition and flushes
// them to data files when the target file size is reached, implementing a rolling
// file strategy to manage file sizes.
type RollingDataWriter struct {
	partitionKey     string
	partitionID      int          // unique ID for this partition
	fileCount        atomic.Int64 // counter for files in this partition
	recordCh         chan arrow.RecordBatch
	errorCh          chan error
	factory          *writerFactory
	partitionValues  map[int]any
	ctx              context.Context
	cancel           context.CancelFunc
	wg               sync.WaitGroup
	concurrentWriter *concurrentDataFileWriter
}

// NewRollingDataWriter creates a new RollingDataWriter for the specified partition
// with the given partition values.
func (w *writerFactory) NewRollingDataWriter(ctx context.Context, concurrentWriter *concurrentDataFileWriter, partition string, partitionValues map[int]any, outputDataFilesCh chan<- iceberg.DataFile) *RollingDataWriter {
	ctx, cancel := context.WithCancel(ctx)
	partitionID := int(w.partitionIDCounter.Add(1) - 1)
	writer := &RollingDataWriter{
		partitionKey:     partition,
		partitionID:      partitionID,
		recordCh:         make(chan arrow.RecordBatch, 64),
		errorCh:          make(chan error, 1),
		factory:          w,
		partitionValues:  partitionValues,
		ctx:              ctx,
		concurrentWriter: concurrentWriter,
		cancel:           cancel,
	}

	writer.wg.Add(1)
	go writer.stream(outputDataFilesCh)

	return writer
}

func (w *writerFactory) getOrCreateRollingDataWriter(ctx context.Context, concurrentWriter *concurrentDataFileWriter, partition string, partitionValues map[int]any, outputDataFilesCh chan<- iceberg.DataFile) (*RollingDataWriter, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if existing, ok := w.writers.Load(partition); ok {
		if writer, ok := existing.(*RollingDataWriter); ok {
			return writer, nil
		}

		return nil, fmt.Errorf("invalid writer type for partition: %s", partition)
	}

	writer := w.NewRollingDataWriter(ctx, concurrentWriter, partition, partitionValues, outputDataFilesCh)
	w.writers.Store(partition, writer)

	return writer, nil
}

// Add appends a record to the writer's buffer and flushes to a data file if the
// target file size is reached.
func (r *RollingDataWriter) Add(record arrow.RecordBatch) error {
	record.Retain()
	select {
	case r.recordCh <- record:
		return nil
	case err := <-r.errorCh:
		record.Release()

		return err
	case <-r.ctx.Done():
		record.Release()

		return r.ctx.Err()
	}
}

func (r *RollingDataWriter) stream(outputDataFilesCh chan<- iceberg.DataFile) {
	defer r.wg.Done()
	defer close(r.errorCh)

	recordIter := func(yield func(arrow.RecordBatch, error) bool) {
		for record := range r.recordCh {
			if !yield(record, nil) {
				return
			}
		}
	}

	binPackedRecords := binPackRecords(recordIter, defaultBinPackLookback, r.factory.targetFileSize)
	for batch := range binPackedRecords {
		if err := r.flushToDataFile(batch, outputDataFilesCh); err != nil {
			select {
			case r.errorCh <- err:
			default:
			}

			return
		}
	}
}

func (r *RollingDataWriter) flushToDataFile(batch []arrow.RecordBatch, outputDataFilesCh chan<- iceberg.DataFile) error {
	if len(batch) == 0 {
		return nil
	}

	tasks := iter.Seq[WriteTask](func(yield func(WriteTask) bool) {
		cnt, _ := r.factory.nextCount()
		fileCount := int(r.fileCount.Add(1))

		yield(WriteTask{
			Uuid:        *r.factory.args.writeUUID,
			ID:          cnt,
			PartitionID: r.partitionID,
			FileCount:   fileCount,
			Schema:      r.factory.taskSchema,
			Batches:     batch,
		})
	})

	parseDataLoc, err := url.Parse(r.factory.rootLocation)
	if err != nil {
		return fmt.Errorf("failed to parse rootLocation: %v", err)
	}

	partitionMeta := *r.factory.meta
	if partitionMeta.props == nil {
		partitionMeta.props = make(map[string]string)
	}
	partitionMeta.props[WriteDataPathKey] = parseDataLoc.JoinPath("data").JoinPath(r.partitionKey).String()

	outputDataFiles := r.concurrentWriter.writeFiles(r.ctx, r.factory.rootLocation, r.factory.args.fs, &partitionMeta, partitionMeta.props, r.partitionValues, tasks)
	for dataFile, err := range outputDataFiles {
		if err != nil {
			return err
		}
		outputDataFilesCh <- dataFile
	}

	for _, rec := range batch {
		rec.Release()
	}

	return nil
}

func (r *RollingDataWriter) close() {
	r.cancel()
	close(r.recordCh)
}

func (r *RollingDataWriter) closeAndWait() error {
	r.close()
	r.factory.writers.Delete(r.partitionKey)
	r.wg.Wait()

	select {
	case err := <-r.errorCh:
		if err != nil {
			return fmt.Errorf("error in rolling data writer: %w", err)
		}

		return nil
	default:

		return nil
	}
}

func (w *writerFactory) closeAll() error {
	defer w.stopCount()
	var writers []*RollingDataWriter
	w.writers.Range(func(key, value any) bool {
		writer, ok := value.(*RollingDataWriter)
		if ok {
			writers = append(writers, writer)
		}

		return true
	})

	var err error
	for _, writer := range writers {
		if closeErr := writer.closeAndWait(); closeErr != nil && err == nil {
			err = closeErr
		}
	}

	return err
}
