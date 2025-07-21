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
	"net/url"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/iceberg-go"
)

type WriterFactory struct {
	rootLocation   string
	args           recordWritingArgs
	meta           *MetadataBuilder
	targetFileSize int64
	writers        sync.Map
	nextCount      func() (int, bool)
	stopCount      func()
}

func NewWriterFactory(rootLocation string, args recordWritingArgs, meta *MetadataBuilder, targetFileSize int64) WriterFactory {
	nextCount, stopCount := iter.Pull(args.counter)
	return WriterFactory{
		rootLocation:   rootLocation,
		args:           args,
		meta:           meta,
		targetFileSize: targetFileSize,
		nextCount:      nextCount,
		stopCount:      stopCount,
	}
}

type RollingDataWriter struct {
	partitionKey string
	data         []arrow.Record
	currentSize  int64
	factory      *WriterFactory
}

func (w *WriterFactory) NewRollingDataWriter(partition string) *RollingDataWriter {
	return &RollingDataWriter{
		partitionKey: partition,
		data:         make([]arrow.Record, 0),
		currentSize:  0,
		factory:      w,
	}
}

func (w *WriterFactory) getOrCreateRollingDataWriter(partition string) (*RollingDataWriter, error) {
	rollingDataWriter, _ := w.writers.LoadOrStore(partition, w.NewRollingDataWriter(partition))
	writer, ok := rollingDataWriter.(*RollingDataWriter)
	if !ok {
		return nil, errors.New("failed to create Rolling Data Writer")
	}
	return writer, nil
}

func (r *RollingDataWriter) Add(ctx context.Context, record arrow.Record, outputDataFilesCh chan<- iceberg.DataFile) {
	recordSize := recordNBytes(record)
	record.Retain()

	if r.currentSize > 0 && r.currentSize+recordSize > r.factory.targetFileSize {
		r.flushToDataFile(ctx, outputDataFilesCh)
	} else if recordSize > r.factory.targetFileSize {
		r.flushToDataFile(ctx, outputDataFilesCh)
	}

	record.Retain()
	r.data = append(r.data, record)
	r.currentSize += recordSize
}

func (r *RollingDataWriter) flushToDataFile(ctx context.Context, outputDataFilesCh chan<- iceberg.DataFile) {
	nameMapping := r.factory.meta.CurrentSchema().NameMapping()
	taskSchema, err := ArrowSchemaToIceberg(r.factory.args.sc, false, nameMapping)
	if err != nil {
		panic(err)
	}

	task := iter.Seq[WriteTask](func(yield func(WriteTask) bool) {
		cnt, _ := r.factory.nextCount()
		yield(WriteTask{
			Uuid:    *r.factory.args.writeUUID,
			ID:      cnt,
			Schema:  taskSchema,
			Batches: r.data,
		})
	})

	parseDataLoc, err := url.Parse(r.factory.rootLocation)
	if err != nil {
		fmt.Errorf("Failed to parse rootLocation: %v", err)
	}

	metaProps := *r.factory.meta
	metaProps.props = make(map[string]string)
	metaProps.props[WriteDataPathKey] = parseDataLoc.JoinPath("data").JoinPath(r.partitionKey).String()

	outputDataFiles := writeFiles(ctx, r.factory.rootLocation, r.factory.args.fs, &metaProps, task)
	for dataFile := range outputDataFiles {
		outputDataFilesCh <- dataFile
	}

	r.data = r.data[:0]
	r.currentSize = 0
}

func (r *RollingDataWriter) close(ctx context.Context, outputDataFilesCh chan<- iceberg.DataFile) {
	if r.currentSize > 0 {
		r.flushToDataFile(ctx, outputDataFilesCh)
	}

	r.factory.writers.Delete(r.partitionKey)
}

func (w *WriterFactory) closeAll(ctx context.Context, outputDataFilesCh chan<- iceberg.DataFile) {
	w.writers.Range(func(key, value interface{}) bool {
		writer, ok := value.(*RollingDataWriter)
		if !ok {
			return false
		}
		writer.close(ctx, outputDataFilesCh)
		return true
	})
	w.stopCount()
}
