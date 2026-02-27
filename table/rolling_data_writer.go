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
	iceio "github.com/apache/iceberg-go/io"
	tblutils "github.com/apache/iceberg-go/table/internal"
	"github.com/google/uuid"
)

// writerFactory manages the creation and lifecycle of RollingDataWriter instances
// for different partitions, providing shared configuration and coordination
// across all writers in a partitioned write operation.
type writerFactory struct {
	rootLocation   string
	rootURL        *url.URL
	fs             iceio.WriteFileIO
	writeUUID      *uuid.UUID
	taskSchema     *iceberg.Schema
	targetFileSize int64

	locProvider LocationProvider
	fileSchema  *iceberg.Schema
	arrowSchema *arrow.Schema
	writeProps  any
	statsCols   map[int]tblutils.StatisticsCollector
	currentSpec iceberg.PartitionSpec
	format      tblutils.FileFormat
	content     iceberg.ManifestEntryContent

	writers               sync.Map
	partitionLocProviders sync.Map
	nextCount             func() (int, bool)
	stopCount             func()
	countMu               sync.Mutex
	partitionIDCounter    atomic.Int64
	mu                    sync.Mutex
}

type writerFactoryOption func(*writerFactory)

func withContentType(content iceberg.ManifestEntryContent) writerFactoryOption {
	return func(w *writerFactory) {
		w.content = content
	}
}

func withFactoryFileSchema(schema *iceberg.Schema) writerFactoryOption {
	return func(w *writerFactory) {
		w.fileSchema = schema
		arrowSc, err := SchemaToArrowSchema(schema, nil, true, false)
		if err != nil {
			panic(fmt.Sprintf("withFactoryFileSchema: failed to convert schema: %v", err))
		}
		w.arrowSchema = arrowSc
	}
}

// newWriterFactory creates a writerFactory with precomputed, invariant write
// configuration derived from the table metadata.
func newWriterFactory(rootLocation string, args recordWritingArgs, meta *MetadataBuilder, taskSchema *iceberg.Schema, targetFileSize int64, opts ...writerFactoryOption) (*writerFactory, error) {
	nextCount, stopCount := iter.Pull(args.counter)

	rootURL, err := url.Parse(rootLocation)
	if err != nil {
		stopCount()

		return nil, err
	}

	locProvider, err := LoadLocationProvider(rootLocation, meta.props)
	if err != nil {
		stopCount()

		return nil, err
	}

	fileSchema := meta.CurrentSchema()
	sanitized, err := iceberg.SanitizeColumnNames(fileSchema)
	if err != nil {
		stopCount()

		return nil, err
	}
	if !sanitized.Equals(fileSchema) {
		fileSchema = sanitized
	}

	format := tblutils.GetFileFormat(iceberg.ParquetFile)

	arrowSchema, err := SchemaToArrowSchema(fileSchema, nil, true, false)
	if err != nil {
		stopCount()

		return nil, err
	}

	statsCols, err := computeStatsPlan(fileSchema, meta.props)
	if err != nil {
		stopCount()

		return nil, err
	}

	currentSpec, err := meta.CurrentSpec()
	if err != nil || currentSpec == nil {
		stopCount()

		return nil, fmt.Errorf("%w: cannot write files without a current spec", err)
	}

	f := &writerFactory{
		rootLocation:   rootLocation,
		rootURL:        rootURL,
		fs:             args.fs,
		writeUUID:      args.writeUUID,
		taskSchema:     taskSchema,
		targetFileSize: targetFileSize,
		locProvider:    locProvider,
		fileSchema:     fileSchema,
		arrowSchema:    arrowSchema,
		writeProps:     format.GetWriteProperties(meta.props),
		statsCols:      statsCols,
		currentSpec:    *currentSpec,
		format:         format,
		nextCount:      nextCount,
		stopCount:      stopCount,
	}
	for _, apply := range opts {
		apply(f)
	}

	return f, nil
}

func (w *writerFactory) openFileWriter(ctx context.Context, partitionPath string,
	partitionValues map[int]any, partitionID int, fileCount int,
) (tblutils.FileWriter, error) {
	w.countMu.Lock()
	cnt, _ := w.nextCount()
	w.countMu.Unlock()

	fileName := WriteTask{
		Uuid:        *w.writeUUID,
		ID:          cnt,
		PartitionID: partitionID,
		FileCount:   fileCount,
	}.GenerateDataFileName("parquet")

	var filePath string
	if partitionPath != "" {
		partitionLoc, err := w.partitionLocProvider(partitionPath)
		if err != nil {
			return nil, err
		}
		filePath = partitionLoc.NewDataLocation(fileName)
	} else {
		filePath = w.locProvider.NewDataLocation(fileName)
	}

	return w.format.NewFileWriter(ctx, w.fs, partitionValues, tblutils.WriteFileInfo{
		FileSchema: w.fileSchema,
		FileName:   filePath,
		StatsCols:  w.statsCols,
		WriteProps: w.writeProps,
		Spec:       w.currentSpec,
		Content:    w.content,
	}, w.arrowSchema)
}

func (w *writerFactory) partitionLocProvider(partitionPath string) (LocationProvider, error) {
	if cached, ok := w.partitionLocProviders.Load(partitionPath); ok {
		return cached.(LocationProvider), nil
	}

	partitionDataPath := w.rootURL.JoinPath("data", partitionPath).String()
	loc, err := LoadLocationProvider(w.rootLocation,
		iceberg.Properties{WriteDataPathKey: partitionDataPath})
	if err != nil {
		return nil, err
	}

	w.partitionLocProviders.Store(partitionPath, loc)

	return loc, nil
}

// RollingDataWriter writes Arrow records for a specific partition, rolling to
// new data files when the actual compressed file size reaches the target.
type RollingDataWriter struct {
	partitionKey     string
	partitionID      int
	fileCount        atomic.Int64
	recordCh         chan arrow.RecordBatch
	errorCh          chan error
	factory          *writerFactory
	partitionValues  map[int]any
	ctx              context.Context
	cancel           context.CancelFunc
	wg               sync.WaitGroup
	concurrentWriter *concurrentDataFileWriter
}

func (w *writerFactory) newRollingDataWriter(ctx context.Context, concurrentWriter *concurrentDataFileWriter, partition string, partitionValues map[int]any, outputDataFilesCh chan<- iceberg.DataFile) *RollingDataWriter {
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

	writer := w.newRollingDataWriter(ctx, concurrentWriter, partition, partitionValues, outputDataFilesCh)
	w.writers.Store(partition, writer)

	return writer, nil
}

// Add appends a record to the writer's buffer.
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

	var currentWriter tblutils.FileWriter
	defer func() {
		if currentWriter != nil {
			currentWriter.Close()
		}
	}()

	closeWriter := func() error {
		if currentWriter == nil {
			return nil
		}
		df, err := currentWriter.Close()
		currentWriter = nil
		if err != nil {
			return err
		}
		outputDataFilesCh <- df

		return nil
	}

	for record := range r.recordCh {
		if currentWriter == nil {
			fileCount := int(r.fileCount.Add(1))
			var err error
			currentWriter, err = r.factory.openFileWriter(
				r.ctx, r.partitionKey, r.partitionValues,
				r.partitionID, fileCount)
			if err != nil {
				record.Release()
				r.sendError(err)

				return
			}
		}

		converted, err := ToRequestedSchema(r.ctx, r.factory.fileSchema,
			r.factory.taskSchema, record, false, true, false)
		if err != nil {
			record.Release()
			r.sendError(err)

			return
		}

		err = currentWriter.Write(converted)
		converted.Release()
		record.Release()
		if err != nil {
			r.sendError(err)

			return
		}

		if currentWriter.BytesWritten() >= r.factory.targetFileSize {
			if err := closeWriter(); err != nil {
				r.sendError(err)

				return
			}
		}
	}

	if err := closeWriter(); err != nil {
		r.sendError(err)
	}
}

func (r *RollingDataWriter) sendError(err error) {
	select {
	case r.errorCh <- err:
	default:
	}
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
