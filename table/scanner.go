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
	"errors"
	"fmt"
	"iter"
	"slices"
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

// manifestEntries holds the data and positional delete entries read from manifests.
type manifestEntries struct {
	dataEntries             []iceberg.ManifestEntry
	positionalDeleteEntries []iceberg.ManifestEntry
	mu                      sync.Mutex
}

func newManifestEntries() *manifestEntries {
	return &manifestEntries{
		dataEntries:             make([]iceberg.ManifestEntry, 0),
		positionalDeleteEntries: make([]iceberg.ManifestEntry, 0),
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

func getPartitionRecord(dataFile iceberg.DataFile, partitionType *iceberg.StructType) partitionRecord {
	partitionData := dataFile.Partition()

	out := make(partitionRecord, len(partitionType.FieldList))
	for i, f := range partitionType.FieldList {
		out[i] = partitionData[f.ID]
	}

	return out
}

func openManifest(io io.IO, manifest iceberg.ManifestFile,
	partitionFilter, metricsEval func(iceberg.DataFile) (bool, error),
) ([]iceberg.ManifestEntry, error) {
	entries, err := manifest.FetchEntries(io, true)
	if err != nil {
		return nil, err
	}

	out := make([]iceberg.ManifestEntry, 0, len(entries))
	for _, entry := range entries {
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

type Scan struct {
	metadata       Metadata
	ioF            FSysF
	rowFilter      iceberg.BooleanExpression
	selectedFields []string
	caseSensitive  bool
	snapshotID     *int64
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

	return scan.metadata.CurrentSnapshot()
}

func (scan *Scan) Projection() (*iceberg.Schema, error) {
	curSchema := scan.metadata.CurrentSchema()
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

	return curSchema.Select(scan.caseSensitive, scan.selectedFields...)
}

func (scan *Scan) buildPartitionProjection(specID int) (iceberg.BooleanExpression, error) {
	project := newInclusiveProjection(scan.metadata.CurrentSchema(),
		scan.metadata.PartitionSpecs()[specID], true)

	return project(scan.rowFilter)
}

func (scan *Scan) buildManifestEvaluator(specID int) (func(iceberg.ManifestFile) (bool, error), error) {
	spec := scan.metadata.PartitionSpecs()[specID]

	return newManifestEvaluator(spec, scan.metadata.CurrentSchema(),
		scan.partitionFilters.Get(specID), scan.caseSensitive)
}

func (scan *Scan) buildPartitionEvaluator(specID int) func(iceberg.DataFile) (bool, error) {
	spec := scan.metadata.PartitionSpecs()[specID]
	partType := spec.PartitionType(scan.metadata.CurrentSchema())
	partSchema := iceberg.NewSchema(0, partType.FieldList...)
	partExpr := scan.partitionFilters.Get(specID)

	return func(d iceberg.DataFile) (bool, error) {
		fn, err := iceberg.ExpressionEvaluator(partSchema, partExpr, scan.caseSensitive)
		if err != nil {
			return false, err
		}

		return fn(getPartitionRecord(d, partType))
	}
}

func (scan *Scan) checkSequenceNumber(minSeqNum int64, manifest iceberg.ManifestFile) bool {
	return manifest.ManifestContent() == iceberg.ManifestContentData ||
		(manifest.ManifestContent() == iceberg.ManifestContentDeletes &&
			manifest.SequenceNum() >= minSeqNum)
}

func minSequenceNum(manifests []iceberg.ManifestFile) int64 {
	n := int64(0)
	for _, m := range manifests {
		if m.ManifestContent() == iceberg.ManifestContentData {
			n = min(n, m.MinSequenceNum())
		}
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

	partitionEvaluators := newKeyDefaultMap(scan.buildPartitionEvaluator)

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
					entries.addPositionalDeleteEntry(e)
				case iceberg.EntryContentEqDeletes:
					return errors.New("iceberg-go does not yet support equality deletes")
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

	results := make([]FileScanTask, 0, len(entries.dataEntries))
	for _, e := range entries.dataEntries {
		deleteFiles, err := matchDeletesToData(e, entries.positionalDeleteEntries)
		if err != nil {
			return nil, err
		}
		results = append(results, FileScanTask{
			File:        e.DataFile(),
			DeleteFiles: deleteFiles,
			Start:       0,
			Length:      e.DataFile().FileSizeBytes(),
		})
	}

	return results, nil
}

type FileScanTask struct {
	File          iceberg.DataFile
	DeleteFiles   []iceberg.DataFile
	Start, Length int64
}

// ToArrowRecords returns the arrow schema of the expected records and an interator
// that can be used with a range expression to read the records as they are available.
// If an error is encountered, during the planning and setup then this will return the
// error directly. If the error occurs while iterating the records, it will be returned
// by the iterator.
//
// The purpose for returning the schema up front is to handle the case where there are no
// rows returned. The resulting Arrow Schema of the projection will still be known.
func (scan *Scan) ToArrowRecords(ctx context.Context) (*arrow.Schema, iter.Seq2[arrow.Record, error], error) {
	tasks, err := scan.PlanFiles(ctx)
	if err != nil {
		return nil, nil, err
	}

	var boundFilter iceberg.BooleanExpression
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

	records := make([]arrow.Record, 0)
	for rec, err := range itr {
		if err != nil {
			return nil, err
		}

		defer rec.Release()
		records = append(records, rec)
	}

	return array.NewTableFromRecords(schema, records), nil
}

func toArrowTableFromRecords(schema *arrow.Schema, recordItr iter.Seq2[arrow.Record, error]) (arrow.Table, error) {
	records := make([]arrow.Record, 0)
	for rec, err := range recordItr {
		if err != nil {
			return nil, err
		}

		defer rec.Release()
		records = append(records, rec)
	}

	return array.NewTableFromRecords(schema, records), nil
}
