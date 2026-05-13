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
	"cmp"
	"context"
	"fmt"
	"iter"
	"math"
	"slices"
	"strings"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/io"
	"golang.org/x/sync/errgroup"
)

const ScanNoLimit = -1

type keyDefaultMap[K comparable, V any] struct {
	defaultFactory func(K) V
	data           map[K]V

	mx sync.RWMutex
}

func (k *keyDefaultMap[K, V]) Get(key K) V {
	k.mx.RLock()
	if v, ok := k.data[key]; ok {
		k.mx.RUnlock()

		return v
	}

	k.mx.RUnlock()
	k.mx.Lock()
	defer k.mx.Unlock()

	// race check between RLock and Lock
	if v, ok := k.data[key]; ok {
		return v
	}

	v := k.defaultFactory(key)
	k.data[key] = v

	return v
}

func newKeyDefaultMap[K comparable, V any](factory func(K) V) *keyDefaultMap[K, V] {
	return &keyDefaultMap[K, V]{
		data:           make(map[K]V),
		defaultFactory: factory,
	}
}

func newKeyDefaultMapWrapErr[K comparable, V any](factory func(K) (V, error)) *keyDefaultMap[K, V] {
	return &keyDefaultMap[K, V]{
		data: make(map[K]V),
		defaultFactory: func(k K) V {
			v, err := factory(k)
			if err != nil {
				panic(err)
			}

			return v
		},
	}
}

type partitionRecord []any

func (p partitionRecord) Size() int            { return len(p) }
func (p partitionRecord) Get(pos int) any      { return p[pos] }
func (p partitionRecord) Set(pos int, val any) { p[pos] = val }

// manifestEntries holds the data, positional delete, and equality delete
// entries read from manifests.
type manifestEntries struct {
	dataEntries             []iceberg.ManifestEntry
	positionalDeleteEntries []iceberg.ManifestEntry
	equalityDeleteEntries   []iceberg.ManifestEntry
	dvEntries               []iceberg.ManifestEntry
	mu                      sync.Mutex
}

func newManifestEntries() *manifestEntries {
	return &manifestEntries{
		dataEntries:             make([]iceberg.ManifestEntry, 0),
		positionalDeleteEntries: make([]iceberg.ManifestEntry, 0),
		equalityDeleteEntries:   make([]iceberg.ManifestEntry, 0),
		dvEntries:               make([]iceberg.ManifestEntry, 0),
	}
}

func (m *manifestEntries) addDataEntry(e iceberg.ManifestEntry) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dataEntries = append(m.dataEntries, e)
}

func (m *manifestEntries) addPositionalDeleteEntry(e iceberg.ManifestEntry) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.positionalDeleteEntries = append(m.positionalDeleteEntries, e)
}

func (m *manifestEntries) addEqualityDeleteEntry(e iceberg.ManifestEntry) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.equalityDeleteEntries = append(m.equalityDeleteEntries, e)
}

func (m *manifestEntries) addDVEntry(e iceberg.ManifestEntry) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dvEntries = append(m.dvEntries, e)
}

func newPartitionRecord(partitionData map[int]any, partitionType *iceberg.StructType) partitionRecord {
	out := make(partitionRecord, len(partitionType.FieldList))
	for i, f := range partitionType.FieldList {
		out[i] = partitionData[f.ID]
	}

	return out
}

// GetPartitionRecord converts a DataFile's partition map into a positional
// record ordered by the fields of the given partition struct type.
func GetPartitionRecord(dataFile iceberg.DataFile, partitionType *iceberg.StructType) iceberg.StructLike {
	return newPartitionRecord(dataFile.Partition(), partitionType)
}

func openManifest(io io.IO, manifest iceberg.ManifestFile,
	partitionFilter, metricsEval func(iceberg.DataFile) (bool, error),
) ([]iceberg.ManifestEntry, error) {
	// Counts may be -1 (unset) on V1 manifests, so clamp before allocating.
	out := make([]iceberg.ManifestEntry, 0, max(0, int(manifest.AddedDataFiles())+int(manifest.ExistingDataFiles())))
	for entry, err := range manifest.Entries(io, true) {
		if err != nil {
			return nil, err
		}

		p, err := partitionFilter(entry.DataFile())
		if err != nil {
			return nil, err
		}

		m, err := metricsEval(entry.DataFile())
		if err != nil {
			return nil, err
		}

		if p && m {
			out = append(out, entry)
		}
	}

	return out, nil
}

func isDeletionVector(df iceberg.DataFile) bool {
	return df.ReferencedDataFile() != nil
}

type Scan struct {
	metadata       Metadata
	ioF            FSysF
	rowFilter      iceberg.BooleanExpression
	selectedFields []string
	caseSensitive  bool
	snapshotID     *int64
	asOfTimestamp  *int64
	options        iceberg.Properties
	limit          int64

	partitionFilters *keyDefaultMap[int, iceberg.BooleanExpression]
	concurrency      int
}

func (scan *Scan) UseRowLimit(n int64) *Scan {
	out := *scan
	out.limit = n

	return &out
}

func (scan *Scan) UseRef(name string) (*Scan, error) {
	if scan.snapshotID != nil {
		return nil, fmt.Errorf("%w: cannot override ref, already set snapshot id %d",
			iceberg.ErrInvalidArgument, *scan.snapshotID)
	}

	if snap := scan.metadata.SnapshotByName(name); snap != nil {
		out := *scan
		out.snapshotID = &snap.SnapshotID
		out.partitionFilters = newKeyDefaultMapWrapErr(out.buildPartitionProjection)

		return &out, nil
	}

	return nil, fmt.Errorf("%w: cannot scan unknown ref=%s", iceberg.ErrInvalidArgument, name)
}

func (scan *Scan) Snapshot() *Snapshot {
	if scan.snapshotID != nil {
		return scan.metadata.SnapshotByID(*scan.snapshotID)
	}

	if scan.asOfTimestamp != nil {
		entries := slices.Collect(scan.metadata.SnapshotLogs())
		for i := len(entries) - 1; i >= 0; i-- {
			entry := entries[i]
			if entry.TimestampMs <= *scan.asOfTimestamp {
				return scan.metadata.SnapshotByID(entry.SnapshotID)
			}
		}
	}

	return scan.metadata.CurrentSnapshot()
}

func (scan *Scan) Projection() (*iceberg.Schema, error) {
	curSchema := scan.metadata.CurrentSchema()
	curVersion := scan.metadata.Version()
	caseSensitive := scan.caseSensitive
	if scan.snapshotID != nil {
		snap := scan.metadata.SnapshotByID(*scan.snapshotID)
		if snap == nil {
			return nil, fmt.Errorf("%w: snapshot not found: %d", ErrInvalidOperation, *scan.snapshotID)
		}

		if snap.SchemaID != nil {
			for _, schema := range scan.metadata.Schemas() {
				if schema.ID == *snap.SchemaID {
					curSchema = schema

					break
				}
			}
		}
	}

	if slices.Contains(scan.selectedFields, "*") {
		return curSchema, nil
	}

	hasRowLineageMeta := selectedFieldsContainsMeta(scan.selectedFields, caseSensitive)
	schemaHasRowLineageMeta := schemaContainsMeta(curSchema)
	if hasRowLineageMeta && curVersion >= minFormatVersionRowLineage && !schemaHasRowLineageMeta {

		removedMetadataSlice, missingMetaFields := removeMetadataFromSelectedFields(scan.selectedFields, caseSensitive)
		sch, err := curSchema.Select(scan.caseSensitive, removedMetadataSlice...)

		if err != nil {
			return nil, err
		}

		return iceberg.NewSchema(sch.ID, append(sch.Fields(), missingMetaFields...)...), nil
	}

	return curSchema.Select(scan.caseSensitive, scan.selectedFields...)
}

func (scan *Scan) buildPartitionProjection(specID int) (iceberg.BooleanExpression, error) {
	return buildPartitionProjection(specID, scan.metadata, scan.rowFilter, scan.caseSensitive)
}

func buildPartitionProjection(specID int, meta Metadata, rowFilter iceberg.BooleanExpression, caseSensitive bool) (iceberg.BooleanExpression, error) {
	spec := meta.PartitionSpecByID(specID)
	if spec == nil {
		return nil, fmt.Errorf("%w: id %d", ErrPartitionSpecNotFound, specID)
	}
	project := newInclusiveProjection(meta.CurrentSchema(), *spec, caseSensitive)

	return project(rowFilter)
}

func (scan *Scan) buildManifestEvaluator(specID int) (func(iceberg.ManifestFile) (bool, error), error) {
	return buildManifestEvaluator(specID, scan.metadata, scan.partitionFilters, scan.caseSensitive)
}

func buildManifestEvaluator(specID int, metadata Metadata, partitionFilters *keyDefaultMap[int, iceberg.BooleanExpression], caseSensitive bool) (func(iceberg.ManifestFile) (bool, error), error) {
	spec := metadata.PartitionSpecByID(specID)
	if spec == nil {
		return nil, fmt.Errorf("%w: id %d", ErrPartitionSpecNotFound, specID)
	}

	return newManifestEvaluator(*spec, metadata.CurrentSchema(),
		partitionFilters.Get(specID), caseSensitive)
}

func (scan *Scan) buildPartitionEvaluator(specID int) (func(iceberg.DataFile) (bool, error), error) {
	return buildPartitionEvaluator(specID, scan.metadata, scan.partitionFilters, scan.caseSensitive)
}

func buildPartitionEvaluator(specID int, metadata Metadata, partitionFilters *keyDefaultMap[int, iceberg.BooleanExpression], caseSensitive bool) (func(iceberg.DataFile) (bool, error), error) {
	spec := metadata.PartitionSpecByID(specID)
	if spec == nil {
		return nil, fmt.Errorf("%w: id %d", ErrPartitionSpecNotFound, specID)
	}
	partType := spec.PartitionType(metadata.CurrentSchema())
	partSchema := iceberg.NewSchema(0, partType.FieldList...)

	fn, err := iceberg.ExpressionEvaluator(partSchema, partitionFilters.Get(specID), caseSensitive)
	if err != nil {
		return nil, err
	}

	return func(d iceberg.DataFile) (bool, error) {
		return fn(GetPartitionRecord(d, partType))
	}, nil
}

func (scan *Scan) checkSequenceNumber(minSeqNum int64, manifest iceberg.ManifestFile) bool {
	return manifest.ManifestContent() == iceberg.ManifestContentData ||
		(manifest.ManifestContent() == iceberg.ManifestContentDeletes &&
			manifest.SequenceNum() >= minSeqNum)
}

func minSequenceNum(manifests []iceberg.ManifestFile) int64 {
	var n int64 = math.MaxInt64
	for _, m := range manifests {
		if m.ManifestContent() == iceberg.ManifestContentData {
			n = min(n, m.MinSequenceNum())
		}
	}
	if n == math.MaxInt64 {
		return 0
	}

	return n
}

func matchDeletesToData(entry iceberg.ManifestEntry, positionalDeletes []iceberg.ManifestEntry) ([]iceberg.DataFile, error) {
	idx, _ := slices.BinarySearchFunc(positionalDeletes, entry, func(me1, me2 iceberg.ManifestEntry) int {
		return cmp.Compare(me1.SequenceNum(), me2.SequenceNum())
	})

	evaluator, err := newInclusiveMetricsEvaluator(iceberg.PositionalDeleteSchema,
		iceberg.EqualTo(iceberg.Reference("file_path"), entry.DataFile().FilePath()), true, false)
	if err != nil {
		return nil, err
	}

	out := make([]iceberg.DataFile, 0)
	for _, relevant := range positionalDeletes[idx:] {
		df := relevant.DataFile()
		ok, err := evaluator(df)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, df)
		}
	}

	return out, nil
}

// matchEqualityDeletesToData returns the equality delete files that apply to
// the given data entry. An equality delete applies when:
//   - it has a strictly greater sequence number than the data file
//   - it shares the same partition (for partitioned tables)
//
// The "strictly greater" rule ensures that data files committed in the same
// snapshot as the equality deletes are not affected — this is how RowDelta
// atomically adds new rows alongside deletes for old rows.
func matchEqualityDeletesToData(dataEntry iceberg.ManifestEntry, eqDeleteEntries []iceberg.ManifestEntry) []iceberg.DataFile {
	dataSeqNum := dataEntry.SequenceNum()
	dataPartition := dataEntry.DataFile().Partition()

	out := make([]iceberg.DataFile, 0)
	for _, del := range eqDeleteEntries {
		// Equality deletes only apply to data files with a strictly lower
		// sequence number.
		if del.SequenceNum() <= dataSeqNum {
			continue
		}

		// For partitioned tables, equality deletes must share the same
		// partition as the data file. Unpartitioned deletes (nil/empty
		// partition) apply globally.
		delPartition := del.DataFile().Partition()
		if len(delPartition) > 0 && len(dataPartition) > 0 {
			if !partitionsMatch(dataPartition, delPartition) {
				continue
			}
		}

		out = append(out, del.DataFile())
	}

	return out
}

func partitionsMatch(a, b map[int]any) bool {
	if len(a) != len(b) {
		return false
	}

	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}

	return true
}

// fetchPartitionSpecFilteredManifests retrieves the table's current snapshot,
// fetches its manifest files, and applies partition-spec filters to remove irrelevant manifests.
func (scan *Scan) fetchPartitionSpecFilteredManifests(ctx context.Context) ([]iceberg.ManifestFile, error) {
	snap := scan.Snapshot()
	if snap == nil {
		return nil, nil
	}

	afs, err := scan.ioF(ctx)
	if err != nil {
		return nil, err
	}
	// Fetch all manifests for the current snapshot.
	manifestList, err := snap.Manifests(afs)
	if err != nil {
		return nil, err
	}

	// Build per-spec manifest evaluators and filter out irrelevant manifests.
	manifestEvaluators := newKeyDefaultMapWrapErr(scan.buildManifestEvaluator)
	manifestList = slices.DeleteFunc(manifestList, func(mf iceberg.ManifestFile) bool {
		eval := manifestEvaluators.Get(int(mf.PartitionSpecID()))
		use, err := eval(mf)

		return !use || err != nil
	})

	return manifestList, nil
}

// collectManifestEntries concurrently opens manifests, applies partition and metrics
// filters, and accumulates both data entries and positional-delete entries.
func (scan *Scan) collectManifestEntries(
	ctx context.Context,
	manifestList []iceberg.ManifestFile,
) (*manifestEntries, error) {
	metricsEval, err := newInclusiveMetricsEvaluator(
		scan.metadata.CurrentSchema(),
		scan.rowFilter,
		scan.caseSensitive,
		scan.options["include_empty_files"] == "true",
	)
	if err != nil {
		return nil, err
	}

	minSeqNum := minSequenceNum(manifestList)
	concurrencyLimit := min(scan.concurrency, len(manifestList))

	entries := newManifestEntries()
	g, _ := errgroup.WithContext(ctx)
	g.SetLimit(concurrencyLimit)

	partitionEvaluators := newKeyDefaultMapWrapErr(scan.buildPartitionEvaluator)

	for _, mf := range manifestList {
		if !scan.checkSequenceNumber(minSeqNum, mf) {
			continue
		}

		g.Go(func() error {
			fs, err := scan.ioF(ctx)
			if err != nil {
				return err
			}
			partEval := partitionEvaluators.Get(int(mf.PartitionSpecID()))
			manifestEntries, err := openManifest(fs, mf, partEval, metricsEval)
			if err != nil {
				return err
			}

			for _, e := range manifestEntries {
				df := e.DataFile()
				switch df.ContentType() {
				case iceberg.EntryContentData:
					entries.addDataEntry(e)
				case iceberg.EntryContentPosDeletes:
					if isDeletionVector(e.DataFile()) {
						entries.addDVEntry(e)
					} else {
						entries.addPositionalDeleteEntry(e)
					}
				case iceberg.EntryContentEqDeletes:
					entries.addEqualityDeleteEntry(e)
				default:
					return fmt.Errorf("%w: unknown DataFileContent type (%s): %s",
						ErrInvalidMetadata, df.ContentType(), e)
				}
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return entries, nil
}

// PlanFiles orchestrates the fetching and filtering of manifests, and then
// building a list of FileScanTasks that match the current Scan criteria.
func (scan *Scan) PlanFiles(ctx context.Context) ([]FileScanTask, error) {
	if scan.asOfTimestamp != nil {
		var snapshot *Snapshot
		entries := slices.Collect(scan.metadata.SnapshotLogs())
		for i := len(entries) - 1; i >= 0; i-- {
			entry := entries[i]
			if entry.TimestampMs <= *scan.asOfTimestamp {
				snapshot = scan.metadata.SnapshotByID(entry.SnapshotID)

				break
			}
		}
		if snapshot == nil {
			return nil, fmt.Errorf("no snapshot found for timestamp %d", *scan.asOfTimestamp)
		}
		scan.snapshotID = &snapshot.SnapshotID
		scan.asOfTimestamp = nil
	}
	// Step 1: Retrieve filtered manifests based on snapshot and partition specs.
	manifestList, err := scan.fetchPartitionSpecFilteredManifests(ctx)
	if err != nil || len(manifestList) == 0 {
		return nil, err
	}

	// Step 2: Read manifest entries concurrently, accumulating data and positional deletes.
	entries, err := scan.collectManifestEntries(ctx, manifestList)
	if err != nil {
		return nil, err
	}

	// Step 3: Sort positional deletes and match them to data files.
	slices.SortFunc(entries.positionalDeleteEntries, func(a, b iceberg.ManifestEntry) int {
		return cmp.Compare(a.SequenceNum(), b.SequenceNum())
	})

	// Index DVs by referenced data file path for O(1) lookup.
	dvIndex := make(map[string][]iceberg.DataFile, len(entries.dvEntries))
	for _, del := range entries.dvEntries {
		if ref := del.DataFile().ReferencedDataFile(); ref != nil {
			dvIndex[*ref] = append(dvIndex[*ref], del.DataFile())
		}
	}

	results := make([]FileScanTask, 0, len(entries.dataEntries))
	for _, e := range entries.dataEntries {
		deleteFiles, err := matchDeletesToData(e, entries.positionalDeleteEntries)
		if err != nil {
			return nil, err
		}
		eqDeleteFiles := matchEqualityDeletesToData(e, entries.equalityDeleteEntries)

		task := FileScanTask{
			File:                e.DataFile(),
			DeleteFiles:         deleteFiles,
			EqualityDeleteFiles: eqDeleteFiles,
			DeletionVectorFiles: dvIndex[e.DataFile().FilePath()],
			Start:               0,
			Length:              e.DataFile().FileSizeBytes(),
		}
		// Row lineage constants: readers use these to synthesize _row_id and
		// _last_updated_sequence_number when requested.
		task.FirstRowID = e.DataFile().FirstRowID()
		if fseq := e.FileSequenceNum(); fseq != nil {
			task.DataSequenceNumber = fseq
		}
		results = append(results, task)
	}

	return results, nil
}

type FileScanTask struct {
	File                iceberg.DataFile
	DeleteFiles         []iceberg.DataFile // positional delete files
	EqualityDeleteFiles []iceberg.DataFile // equality delete files
	DeletionVectorFiles []iceberg.DataFile // deletion vectors (puffin files)
	Start, Length       int64

	// Row lineage (v3): constants used when reading to synthesize _row_id and _last_updated_sequence_number.
	// FirstRowID is the effective first_row_id for this file (from manifest entry, after inheritance).
	// DataSequenceNumber is the data sequence number of the file's manifest entry.
	FirstRowID         *int64
	DataSequenceNumber *int64
}

// ToArrowRecords returns the arrow schema of the expected records and an interator
// that can be used with a range expression to read the records as they are available.
// If an error is encountered, during the planning and setup then this will return the
// error directly. If the error occurs while iterating the records, it will be returned
// by the iterator.
//
// The purpose for returning the schema up front is to handle the case where there are no
// rows returned. The resulting Arrow Schema of the projection will still be known.
func (scan *Scan) ToArrowRecords(ctx context.Context) (*arrow.Schema, iter.Seq2[arrow.RecordBatch, error], error) {
	tasks, err := scan.PlanFiles(ctx)
	if err != nil {
		return nil, nil, err
	}

	return scan.ReadTasks(ctx, tasks)
}

// ReadTasks reads Arrow records from a specific set of FileScanTasks, applying the
// scan's projection, row filters, and positional delete handling. This is useful when
// the caller has already planned or selected specific tasks to read.
func (scan *Scan) ReadTasks(ctx context.Context, tasks []FileScanTask) (*arrow.Schema, iter.Seq2[arrow.RecordBatch, error], error) {
	var (
		boundFilter iceberg.BooleanExpression
		err         error
	)

	if scan.rowFilter != nil {
		boundFilter, err = iceberg.BindExpr(scan.metadata.CurrentSchema(), scan.rowFilter, scan.caseSensitive)
		if err != nil {
			return nil, nil, err
		}
	}

	schema, err := scan.Projection()
	if err != nil {
		return nil, nil, err
	}

	fs, err := scan.ioF(ctx)
	if err != nil {
		return nil, nil, err
	}

	return (&arrowScan{
		metadata:        scan.metadata,
		fs:              fs,
		projectedSchema: schema,
		boundRowFilter:  boundFilter,
		caseSensitive:   scan.caseSensitive,
		rowLimit:        scan.limit,
		options:         scan.options,
		concurrency:     scan.concurrency,
	}).GetRecords(ctx, tasks)
}

// ToArrowTable calls ToArrowRecords and then gathers all of the records together
// and returns an arrow.Table make from those records.
func (scan *Scan) ToArrowTable(ctx context.Context) (arrow.Table, error) {
	schema, itr, err := scan.ToArrowRecords(ctx)
	if err != nil {
		return nil, err
	}

	records := make([]arrow.RecordBatch, 0)
	for rec, err := range itr {
		if err != nil {
			return nil, err
		}

		defer rec.Release()
		records = append(records, rec)
	}

	return array.NewTableFromRecords(schema, records), nil
}

func schemaContainsMeta(schema *iceberg.Schema) bool {
	if schema == nil {
		return false
	}

	_, hasRowIdMeta := schema.FindFieldByID(iceberg.RowIDFieldID)
	_, hasSequenceMeta := schema.FindFieldByID(iceberg.LastUpdatedSequenceNumberFieldID)

	return hasRowIdMeta || hasSequenceMeta
}

func selectedFieldsContainsMeta(selectedFields []string, caseSensitive bool) bool {
	if !caseSensitive {
		selectedFieldsLower := []string{}

		for _, s := range selectedFields {
			selectedFieldsLower = append(selectedFieldsLower, strings.ToLower(s))
		}

		hasRowIdMeta := slices.Contains(selectedFieldsLower, iceberg.RowIDColumnName)
		hasSequenceMeta := slices.Contains(selectedFieldsLower, iceberg.LastUpdatedSequenceNumberColumnName)

		return hasRowIdMeta || hasSequenceMeta
	}
	hasRowIdMeta := slices.Contains(selectedFields, iceberg.RowIDColumnName)
	hasSequenceMeta := slices.Contains(selectedFields, iceberg.LastUpdatedSequenceNumberColumnName)

	return hasRowIdMeta || hasSequenceMeta
}

// Goes through a selectedFields and returns a slice of strings representing the selectedFields without
// any row lineage metadata and a slice of iceberg.NestedFields representing the row lineage metadata present
// in the selectedFields. Note that both returned slices will be in the same order as they were in selectedFields.
func removeMetadataFromSelectedFields(selectedFields []string, caseSensitive bool) ([]string, []iceberg.NestedField) {
	filteredFields := []string{}
	meta := []iceberg.NestedField{}

	if !caseSensitive {
		for _, field := range selectedFields {
			if strings.ToLower(field) == iceberg.RowIDColumnName {
				meta = append(meta, iceberg.RowID())
				continue
			}

			if strings.ToLower(field) == iceberg.LastUpdatedSequenceNumberColumnName {
				meta = append(meta, iceberg.LastUpdatedSequenceNumber())
				continue
			}

			filteredFields = append(filteredFields, field)
		}

		return filteredFields, meta
	}

	for _, field := range selectedFields {
		if field == iceberg.RowIDColumnName {
			meta = append(meta, iceberg.RowID())
			continue
		}

		if field == iceberg.LastUpdatedSequenceNumberColumnName {
			meta = append(meta, iceberg.LastUpdatedSequenceNumber())
			continue
		}

		filteredFields = append(filteredFields, field)
	}

	return filteredFields, meta
}
