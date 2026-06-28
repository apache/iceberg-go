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
	"maps"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/parquet/variant"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	tblutils "github.com/apache/iceberg-go/table/internal"
	"github.com/google/uuid"
)

// ErrWriterClosed is returned when records are added to a writer after its
// streaming goroutine has already stopped.
var ErrWriterClosed = errors.New("writer is closed")

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

	locProvider      LocationProvider
	tableProps       iceberg.Properties
	fileSchema       *iceberg.Schema
	arrowSchema      *arrow.Schema
	writeProps       any
	statsCols        map[int]tblutils.StatisticsCollector
	currentSpec      iceberg.PartitionSpec
	fileFormat       iceberg.FileFormat
	format           tblutils.FileFormat
	content          iceberg.ManifestEntryContent
	equalityFieldIDs []int
	sortKeys         []compute.SortKey

	// Variant shredding: per-file inference over the first shredBufferRows rows.
	shredEnabled    bool
	shredBufferRows int

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

func withFactoryEqualityFieldIDs(ids []int) writerFactoryOption {
	return func(w *writerFactory) {
		w.equalityFieldIDs = ids
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

	fileFormat, err := iceberg.FileFormatFromString(
		iceberg.Properties(meta.props).Get(WriteFormatDefaultKey, WriteFormatDefaultDefault))
	if err != nil {
		stopCount()

		return nil, err
	}

	format := tblutils.GetFileFormat(fileFormat)

	arrowSchema, err := SchemaToArrowSchema(fileSchema, nil, true, false)
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
		tableProps:     meta.props,
		fileSchema:     fileSchema,
		arrowSchema:    arrowSchema,
		writeProps:     format.GetWriteProperties(meta.props),
		currentSpec:    *currentSpec,
		fileFormat:     fileFormat,
		format:         format,
		nextCount:      nextCount,
		stopCount:      stopCount,
	}
	for _, apply := range opts {
		apply(f)
	}

	f.statsCols, err = computeStatsPlan(f.fileSchema, meta.props)
	if err != nil {
		stopCount()

		return nil, err
	}

	if f.content == iceberg.EntryContentData && meta.defaultSortOrderID != UnsortedSortOrderID {
		sortOrder, err := meta.GetSortOrderByID(meta.defaultSortOrderID)
		if err != nil {
			stopCount()

			return nil, err
		}
		f.sortKeys, err = resolveSortKeys(*sortOrder, f.fileSchema)
		if err != nil {
			stopCount()

			return nil, err
		}
	}

	// Shred top-level variant columns on data writes only.
	f.shredBufferRows = meta.props.GetInt(ParquetVariantBufferSizeKey, ParquetVariantBufferSizeDefault)
	if f.shredBufferRows < 1 {
		f.shredBufferRows = 1
	}
	if f.content == iceberg.EntryContentData &&
		meta.props.GetBool(ParquetShredVariantsKey, ParquetShredVariantsDefault) {
		for _, fld := range f.fileSchema.Fields() {
			if _, ok := fld.Type.(iceberg.VariantType); ok {
				f.shredEnabled = true

				break
			}
		}
	}

	return f, nil
}

func (w *writerFactory) openFileWriter(ctx context.Context, partitionPath string,
	partitionValues map[int]any, partitionID int, fileCount int, arrowSchema *arrow.Schema,
) (tblutils.FileWriter, error) {
	if arrowSchema == nil {
		arrowSchema = w.arrowSchema
	}
	w.countMu.Lock()
	cnt, _ := w.nextCount()
	w.countMu.Unlock()

	// Names files directly, not via WriteTask: it has no per-task sort claim.
	fileName := dataFileName(*w.writeUUID, cnt, partitionID, fileCount,
		strings.ToLower(string(w.fileFormat)))

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

	// No SortOrderID: batches are sorted only individually (see resolveSortKeys).
	// FileSchema stays logical; only arrowSchema carries the shredded layout.
	return w.format.NewFileWriter(ctx, w.fs, partitionValues, tblutils.WriteFileInfo{
		FileSchema:       w.fileSchema,
		FileName:         filePath,
		StatsCols:        w.statsCols,
		WriteProps:       w.writeProps,
		Spec:             w.currentSpec,
		Content:          w.content,
		EqualityFieldIDs: w.equalityFieldIDs,
	}, arrowSchema)
}

func (w *writerFactory) partitionLocProvider(partitionPath string) (LocationProvider, error) {
	if cached, ok := w.partitionLocProviders.Load(partitionPath); ok {
		return cached.(LocationProvider), nil
	}

	partitionDataPath := w.rootURL.JoinPath("data", partitionPath).String()
	partitionProps := make(iceberg.Properties, len(w.tableProps)+1)
	maps.Copy(partitionProps, w.tableProps)
	partitionProps[WriteDataPathKey] = partitionDataPath
	loc, err := LoadLocationProvider(w.rootLocation, partitionProps)
	if err != nil {
		return nil, err
	}

	w.partitionLocProviders.Store(partitionPath, loc)

	return loc, nil
}

// RollingDataWriter writes Arrow records for a specific partition, rolling to
// new data files when the actual compressed file size reaches the target.
type RollingDataWriter struct {
	partitionKey    string
	partitionID     int
	fileCount       atomic.Int64
	recordCh        chan arrow.RecordBatch
	errorCh         chan error
	factory         *writerFactory
	partitionValues map[int]any
	ctx             context.Context
	cancel          context.CancelFunc
	wg              sync.WaitGroup
}

func (w *writerFactory) newRollingDataWriter(ctx context.Context, partition string, partitionValues map[int]any, outputDataFilesCh chan<- iceberg.DataFile) *RollingDataWriter {
	ctx, cancel := context.WithCancel(ctx)
	partitionID := int(w.partitionIDCounter.Add(1) - 1)
	writer := &RollingDataWriter{
		partitionKey:    partition,
		partitionID:     partitionID,
		recordCh:        make(chan arrow.RecordBatch, 64),
		errorCh:         make(chan error, 1),
		factory:         w,
		partitionValues: partitionValues,
		ctx:             ctx,
		cancel:          cancel,
	}

	writer.wg.Add(1)
	go writer.stream(outputDataFilesCh)

	return writer
}

func (w *writerFactory) getOrCreateRollingDataWriter(ctx context.Context, partition string, partitionValues map[int]any, outputDataFilesCh chan<- iceberg.DataFile) (*RollingDataWriter, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if existing, ok := w.writers.Load(partition); ok {
		if writer, ok := existing.(*RollingDataWriter); ok {
			return writer, nil
		}

		return nil, fmt.Errorf("invalid writer type for partition: %s", partition)
	}

	writer := w.newRollingDataWriter(ctx, partition, partitionValues, outputDataFilesCh)
	w.writers.Store(partition, writer)

	return writer, nil
}

// Add appends a record to the writer's buffer.
func (r *RollingDataWriter) Add(record arrow.RecordBatch) error {
	record.Retain()
	select {
	case r.recordCh <- record:
		return nil
	case err, ok := <-r.errorCh:
		record.Release()
		if !ok {
			return ErrWriterClosed
		}

		return err
	case <-r.ctx.Done():
		record.Release()

		return r.ctx.Err()
	}
}

func (r *RollingDataWriter) stream(outputDataFilesCh chan<- iceberg.DataFile) {
	defer r.wg.Done()
	defer close(r.errorCh)
	defer r.factory.writers.CompareAndDelete(r.partitionKey, r)

	alloc := compute.GetAllocator(r.ctx)

	var currentWriter tblutils.FileWriter
	var fileArrowSchema *arrow.Schema // per-file shredded schema; nil => factory default

	// bootstrap buffer of converted batches, held until inference runs.
	var buf []arrow.RecordBatch
	var bufRows int64
	bootstrapping := r.factory.shredEnabled

	releaseBuf := func() {
		for _, b := range buf {
			b.Release()
		}
		buf = nil
		bufRows = 0
	}

	// Cleanup defer: recover a panic in the write path, release the bootstrap
	// buffer, and abort any in-progress file. stream runs in its own goroutine
	// with no caller to recover it, and FileWriter.Close can panic
	// (stats.ToDataFile panics when NewDataFileBuilder fails); convert that into
	// an error on errorCh instead of crashing the process, mirroring the recover
	// in equalityDeleteRecordsToDataFiles. Registered after close(r.errorCh) so
	// it runs first (LIFO) and can still send on the open channel before it is
	// closed.
	defer func() {
		if rec := recover(); rec != nil {
			switch e := rec.(type) {
			case error:
				r.sendError(fmt.Errorf("panic during rolling data file writing: %w", e))
			default:
				r.sendError(fmt.Errorf("panic during rolling data file writing: %v", e))
			}
		}
		releaseBuf()
		if currentWriter != nil {
			_ = currentWriter.Abort()
		}
	}()

	closeWriter := func() error {
		if currentWriter == nil {
			return nil
		}
		df, err := currentWriter.Close()
		currentWriter = nil
		fileArrowSchema = nil
		bootstrapping = r.factory.shredEnabled // next file re-bootstraps
		if err != nil {
			return err
		}
		outputDataFilesCh <- df

		return nil
	}

	openCurrent := func() error {
		fileCount := int(r.fileCount.Add(1))
		var err error
		currentWriter, err = r.factory.openFileWriter(
			r.ctx, r.partitionKey, r.partitionValues, r.partitionID, fileCount, fileArrowSchema)

		return err
	}

	// writeConverted owns converted: shred -> sort -> Write -> roll. allowRoll is
	// false during replay (no mid-replay roll).
	writeConverted := func(converted arrow.RecordBatch, allowRoll bool) error {
		if fileArrowSchema != nil {
			shredded, err := tblutils.ShredRecordVariants(converted, fileArrowSchema, alloc)
			converted.Release()
			if err != nil {
				return err
			}
			converted = shredded
		}

		// Sort each batch independently before writing. This is per-batch, not
		// per-file: rows are not merged into one globally sorted run across
		// batches. See resolveSortKeys for the full list of limitations.
		if len(r.factory.sortKeys) > 0 {
			sorted, err := compute.SortRecordBatch(r.ctx, converted, r.factory.sortKeys)
			converted.Release()
			if err != nil {
				return err
			}
			converted = sorted
		}

		err := currentWriter.Write(converted)
		converted.Release()
		if err != nil {
			return err
		}

		if allowRoll && currentWriter.BytesWritten() >= r.factory.targetFileSize {
			return closeWriter()
		}

		return nil
	}

	// flushBootstrap infers the schema, opens the file, and replays the buffer.
	flushBootstrap := func() error {
		if len(buf) == 0 {
			bootstrapping = false

			return nil
		}
		fileArrowSchema = tblutils.ShreddedArrowSchema(r.factory.arrowSchema,
			inferShreddingFromBatches(buf, r.factory.shredBufferRows))
		if err := openCurrent(); err != nil {
			releaseBuf()

			return err
		}
		// Detach so the deferred releaseBuf can't double-release the replayed batches.
		batches := buf
		buf = nil
		bufRows = 0
		bootstrapping = false
		for i, b := range batches {
			if err := writeConverted(b, false); err != nil {
				for _, rest := range batches[i+1:] {
					rest.Release()
				}

				return err
			}
		}
		if currentWriter != nil && currentWriter.BytesWritten() >= r.factory.targetFileSize {
			return closeWriter()
		}

		return nil
	}

	for record := range r.recordCh {
		converted, err := ToRequestedSchema(r.ctx, r.factory.fileSchema,
			r.factory.taskSchema, record, SchemaOptions{
				DowncastTimestamp: true,
				IncludeFieldIDs:   true,
				UseWriteDefault:   true,
			})
		record.Release()
		if err != nil {
			r.sendError(err)

			return
		}

		if bootstrapping {
			buf = append(buf, converted)
			bufRows += converted.NumRows()
			if bufRows >= int64(r.factory.shredBufferRows) {
				if err := flushBootstrap(); err != nil {
					r.sendError(err)

					return
				}
			}

			continue
		}

		if currentWriter == nil {
			if err := openCurrent(); err != nil {
				converted.Release()
				r.sendError(err)

				return
			}
		}
		if err := writeConverted(converted, true); err != nil {
			r.sendError(err)

			return
		}
	}

	// Channel closed: flush any partial bootstrap buffer, then finalize.
	if bootstrapping {
		if err := flushBootstrap(); err != nil {
			r.sendError(err)

			return
		}
	}
	if err := closeWriter(); err != nil {
		r.sendError(err)
	}
}

// inferShreddingFromBatches infers the inner type per top-level variant column,
// sampling up to limit non-null values from the buffer. Keyed by column index.
func inferShreddingFromBatches(buf []arrow.RecordBatch, limit int) map[int]arrow.DataType {
	if len(buf) == 0 {
		return nil
	}

	inferred := make(map[int]arrow.DataType)
	for col := 0; col < int(buf[0].NumCols()); col++ {
		if _, ok := buf[0].Column(col).(*extensions.VariantArray); !ok {
			continue
		}
		var sample []variant.Value
		for _, b := range buf {
			if len(sample) >= limit {
				break
			}
			va, ok := b.Column(col).(*extensions.VariantArray)
			if !ok {
				continue
			}
			for i := 0; i < va.Len() && len(sample) < limit; i++ {
				if va.Storage().IsNull(i) {
					continue
				}
				v, err := va.Value(i)
				if err != nil {
					continue
				}
				sample = append(sample, v)
			}
		}
		if dt, ok := tblutils.AnalyzeVariantShredding(sample); ok {
			inferred[col] = dt
		}
	}

	return inferred
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

	if err := <-r.errorCh; err != nil {
		return fmt.Errorf("error in rolling data writer: %w", err)
	}

	return nil
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
