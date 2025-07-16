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

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/iceberg-go"
)

type RollingDataWriter struct {
	fanoutArgs   fanoutArgs
	partitionKey string
	currentSize  int64
	data         []arrow.Record
}

func NewRollingDataWriter(partitionKey string, fanoutArgs fanoutArgs) *RollingDataWriter {
	return &RollingDataWriter{
		fanoutArgs:   fanoutArgs,
		partitionKey: partitionKey,
		currentSize:  0,
		data:         make([]arrow.Record, 0),
	}
}

func (r *RollingDataWriter) Add(ctx context.Context, record arrow.Record, outputDataFilesCh chan<- iceberg.DataFile) {
	recordSize := recordNBytes(record)

	if r.currentSize > 0 && r.currentSize+recordSize > r.fanoutArgs.targetFileSize {
		r.flushToDataFile(ctx, outputDataFilesCh)
	} else if recordSize > r.fanoutArgs.targetFileSize {
		r.flushToDataFile(ctx, outputDataFilesCh)
	}

	record.Retain()
	r.data = append(r.data, record)
	r.currentSize += recordSize
}

func (r *RollingDataWriter) flushToDataFile(ctx context.Context, outputDataFilesCh chan<- iceberg.DataFile) {
	nameMapping := r.fanoutArgs.meta.CurrentSchema().NameMapping()
	taskSchema, err := ArrowSchemaToIceberg(r.fanoutArgs.args.sc, false, nameMapping)
	if err != nil {
		panic(err)
	}

	task := iter.Seq[WriteTask](func(yield func(WriteTask) bool) {
		yield(WriteTask{
			Uuid:    *r.fanoutArgs.args.writeUUID,
			ID:      1, // need fix: counter
			Schema:  taskSchema,
			Batches: r.data,
		})
	})

	outputDataFiles := writeFiles(ctx, r.fanoutArgs.rootLocation, r.fanoutArgs.args.fs, r.fanoutArgs.meta, task)
	for dataFile := range outputDataFiles {
		outputDataFilesCh <- dataFile
	}

	r.data = r.data[:0]
	r.currentSize = 0
}
