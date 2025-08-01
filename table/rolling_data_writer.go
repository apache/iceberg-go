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

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/iceberg-go"
)

type WriterFactory struct {
	rootLocation   string
	args           recordWritingArgs
	meta           *MetadataBuilder
	taskSchema     *iceberg.Schema
	targetFileSize int64
	writers        sync.Map
	nextCount      func() (int, bool)
	stopCount      func()
	mu             sync.Mutex
}

func NewWriterFactory(rootLocation string, args recordWritingArgs, meta *MetadataBuilder, taskSchema *iceberg.Schema, targetFileSize int64) WriterFactory {
	return WriterFactory{
		rootLocation:   rootLocation,
		args:           args,
		meta:           meta,
		taskSchema:     taskSchema,
		targetFileSize: targetFileSize,
	}
}

type RollingDataWriter struct {
	partitionKey    string
	data            []arrow.Record
	currentSize     int64
	factory         *WriterFactory
	mu              sync.Mutex
	partitionValues map[int]any
}

func (w *WriterFactory) NewRollingDataWriter(partition string, partitionValues map[int]any) *RollingDataWriter {
	return &RollingDataWriter{
		partitionKey:    partition,
		data:            make([]arrow.Record, 0),
		currentSize:     0,
		factory:         w,
		partitionValues: partitionValues,
	}
}

func (w *WriterFactory) getOrCreateRollingDataWriter(partition string, partitionValues map[int]any) (*RollingDataWriter, error) {
	rollingDataWriter, _ := w.writers.LoadOrStore(partition, w.NewRollingDataWriter(partition, partitionValues))
	writer, ok := rollingDataWriter.(*RollingDataWriter)
	if !ok {
		return nil, fmt.Errorf("failed to create rolling data writer: %s", partition)
	}
	return writer, nil
}

func (r *RollingDataWriter) Add(ctx context.Context, record arrow.Record, outputDataFilesCh chan<- iceberg.DataFile) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	recordSize := recordNBytes(record)
	record.Retain()

	if r.currentSize > 0 && r.currentSize+recordSize > r.factory.targetFileSize {
		if err := r.flushToDataFile(ctx, outputDataFilesCh); err != nil {
			return err
		}
	}

	r.data = append(r.data, record)
	r.currentSize += recordSize

	if r.currentSize > r.factory.targetFileSize {
		if err := r.flushToDataFile(ctx, outputDataFilesCh); err != nil {
			return err
		}
	}

	return nil
}

func (r *RollingDataWriter) flushToDataFile(ctx context.Context, outputDataFilesCh chan<- iceberg.DataFile) error {
	if len(r.data) == 0 {
		return nil
	}

	task := iter.Seq[WriteTask](func(yield func(WriteTask) bool) {
		r.factory.mu.Lock()
		cnt, _ := r.factory.nextCount()
		r.factory.mu.Unlock()

		yield(WriteTask{
			Uuid:    *r.factory.args.writeUUID,
			ID:      cnt,
			Schema:  r.factory.taskSchema,
			Batches: r.data,
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

	outputDataFiles := writeFiles(ctx, r.factory.rootLocation, r.factory.args.fs, &partitionMeta, r.partitionValues, task)
	for dataFile, err := range outputDataFiles {
		if err != nil {
			return err
		}
		outputDataFilesCh <- dataFile
	}
	r.clear()

	return nil
}

func (r *RollingDataWriter) clear() {
	for _, rec := range r.data {
		rec.Release()
	}
	r.data = r.data[:0]
	r.currentSize = 0
}

func (r *RollingDataWriter) close(ctx context.Context, outputDataFilesCh chan<- iceberg.DataFile) error {
	if r.currentSize > 0 {
		if err := r.flushToDataFile(ctx, outputDataFilesCh); err != nil {
			return fmt.Errorf("failed to flush remaining data on close: %w", err)
		}
	}

	r.factory.writers.Delete(r.partitionKey)
	return nil
}

func (w *WriterFactory) closeAll(ctx context.Context, outputDataFilesCh chan<- iceberg.DataFile) error {
	var err error
	w.writers.Range(func(key, value any) bool {
		writer, ok := value.(*RollingDataWriter)
		if !ok {
			err = fmt.Errorf("invalid writer type for partition %s", key)
			return false
		}
		if err = writer.close(ctx, outputDataFilesCh); err != nil {
			return false
		}
		return true
	})

	w.stopCount()
	return err
}
