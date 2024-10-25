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
	"runtime"
	"slices"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/io"
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

func getPartitionRecord(dataFile iceberg.DataFile, partitionType *iceberg.StructType) partitionRecord {
	partitionData := dataFile.Partition()

	out := make(partitionRecord, len(partitionType.FieldList))
	for i, f := range partitionType.FieldList {
		out[i] = partitionData[f.Name]
	}
	return out
}

func openManifest(io io.IO, manifest iceberg.ManifestFile,
	partitionFilter, metricsEval func(iceberg.DataFile) (bool, error)) ([]iceberg.ManifestEntry, error) {

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
	io             io.IO
	rowFilter      iceberg.BooleanExpression
	selectedFields []string
	caseSensitive  bool
	snapshotID     *int64
	options        iceberg.Properties
	limit          int64

	partitionFilters *keyDefaultMap[int, iceberg.BooleanExpression]
}

func (s *Scan) UseRowLimit(n int64) *Scan {
	out := *s
	out.limit = n
	return &out
}

func (s *Scan) UseRef(name string) (*Scan, error) {
	if s.snapshotID != nil {
		return nil, fmt.Errorf("%w: cannot override ref, already set snapshot id %d",
			iceberg.ErrInvalidArgument, *s.snapshotID)
	}

	if snap := s.metadata.SnapshotByName(name); snap != nil {
		out := &Scan{
			metadata:       s.metadata,
			io:             s.io,
			rowFilter:      s.rowFilter,
			selectedFields: s.selectedFields,
			caseSensitive:  s.caseSensitive,
			snapshotID:     &snap.SnapshotID,
			options:        s.options,
		}
		out.partitionFilters = newKeyDefaultMapWrapErr(out.buildPartitionProjection)

		return out, nil
	}

	return nil, fmt.Errorf("%w: cannot scan unknown ref=%s", iceberg.ErrInvalidArgument, name)
}

func (s *Scan) Snapshot() *Snapshot {
	if s.snapshotID != nil {
		return s.metadata.SnapshotByID(*s.snapshotID)
	}
	return s.metadata.CurrentSnapshot()
}

func (s *Scan) Projection() (*iceberg.Schema, error) {
	curSchema := s.metadata.CurrentSchema()
	if s.snapshotID != nil {
		snap := s.metadata.SnapshotByID(*s.snapshotID)
		if snap == nil {
			return nil, fmt.Errorf("%w: snapshot not found: %d", ErrInvalidOperation, *s.snapshotID)
		}

		if snap.SchemaID != nil {
			for _, schema := range s.metadata.Schemas() {
				if schema.ID == *snap.SchemaID {
					curSchema = schema
					break
				}
			}
		}
	}

	if slices.Contains(s.selectedFields, "*") {
		return curSchema, nil
	}

	return curSchema.Select(s.caseSensitive, s.selectedFields...)
}

func (s *Scan) buildPartitionProjection(specID int) (iceberg.BooleanExpression, error) {
	project := newInclusiveProjection(s.metadata.CurrentSchema(),
		s.metadata.PartitionSpecs()[specID], true)
	return project(s.rowFilter)
}

func (s *Scan) buildManifestEvaluator(specID int) (func(iceberg.ManifestFile) (bool, error), error) {
	spec := s.metadata.PartitionSpecs()[specID]
	return newManifestEvaluator(spec, s.metadata.CurrentSchema(),
		s.partitionFilters.Get(specID), s.caseSensitive)
}

func (s *Scan) buildPartitionEvaluator(specID int) func(iceberg.DataFile) (bool, error) {
	spec := s.metadata.PartitionSpecs()[specID]
	partType := spec.PartitionType(s.metadata.CurrentSchema())
	partSchema := iceberg.NewSchema(0, partType.FieldList...)
	partExpr := s.partitionFilters.Get(specID)

	return func(d iceberg.DataFile) (bool, error) {
		fn, err := iceberg.ExpressionEvaluator(partSchema, partExpr, s.caseSensitive)
		if err != nil {
			return false, err
		}

		return fn(getPartitionRecord(d, partType))
	}
}

func (s *Scan) checkSequenceNumber(minSeqNum int64, manifest iceberg.ManifestFile) bool {
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

func (s *Scan) PlanFiles(ctx context.Context) ([]FileScanTask, error) {
	snap := s.Snapshot()
	if snap == nil {
		return nil, nil
	}

	// step 1: filter manifests using partition summaries
	// the filter depends on the partition spec used to write the manifest file
	// so create a cache of filters for each spec id
	manifestEvaluators := newKeyDefaultMapWrapErr(s.buildManifestEvaluator)
	manifestList, err := snap.Manifests(s.io)
	if err != nil {
		return nil, err
	}

	// remove any manifests that we don't need to use
	manifestList = slices.DeleteFunc(manifestList, func(mf iceberg.ManifestFile) bool {
		eval := manifestEvaluators.Get(int(mf.PartitionSpecID()))
		use, err := eval(mf)
		return !use || err != nil
	})

	// step 2: filter the data files in each manifest
	// this filter depends on the partition spec used to write the manifest file
	partitionEvaluators := newKeyDefaultMap(s.buildPartitionEvaluator)
	metricsEval, err := newInclusiveMetricsEvaluator(
		s.metadata.CurrentSchema(), s.rowFilter, s.caseSensitive, s.options["include_empty_files"] == "true")
	if err != nil {
		return nil, err
	}

	minSeqNum := minSequenceNum(manifestList)
	dataEntries := make([]iceberg.ManifestEntry, 0)
	positionalDeleteEntries := make([]iceberg.ManifestEntry, 0)

	nworkers := min(runtime.NumCPU(), len(manifestList))
	var wg sync.WaitGroup

	manifestChan := make(chan iceberg.ManifestFile, len(manifestList))
	entryChan := make(chan []iceberg.ManifestEntry, 20)

	ctx, cancel := context.WithCancelCause(ctx)
	for i := 0; i < nworkers; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for {
				select {
				case m, ok := <-manifestChan:
					if !ok {
						return
					}

					if !s.checkSequenceNumber(minSeqNum, m) {
						continue
					}

					entries, err := openManifest(s.io, m,
						partitionEvaluators.Get(int(m.PartitionSpecID())), metricsEval)
					if err != nil {
						cancel(err)
						break
					}

					entryChan <- entries
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(entryChan)
	}()

	for _, m := range manifestList {
		manifestChan <- m
	}
	close(manifestChan)

Loop:
	for {
		select {
		case <-ctx.Done():
			return nil, context.Cause(ctx)
		case entries, ok := <-entryChan:
			if !ok {
				// closed!
				break Loop
			}

			for _, e := range entries {
				df := e.DataFile()
				switch df.ContentType() {
				case iceberg.EntryContentData:
					dataEntries = append(dataEntries, e)
				case iceberg.EntryContentPosDeletes:
					positionalDeleteEntries = append(positionalDeleteEntries, e)
				case iceberg.EntryContentEqDeletes:
					return nil, fmt.Errorf("iceberg-go does not yet support equality deletes")
				default:
					return nil, fmt.Errorf("%w: unknown DataFileContent type (%s): %s",
						ErrInvalidMetadata, df.ContentType(), e)
				}
			}
		}
	}

	slices.SortFunc(positionalDeleteEntries, func(a, b iceberg.ManifestEntry) int {
		return cmp.Compare(a.SequenceNum(), b.SequenceNum())
	})

	results := make([]FileScanTask, 0)
	for _, e := range dataEntries {
		deleteFiles, err := matchDeletesToData(e, positionalDeleteEntries)
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

func (s *Scan) ToArrowRecords(ctx context.Context) (*arrow.Schema, iter.Seq2[arrow.Record, error], error) {
	tasks, err := s.PlanFiles(ctx)
	if err != nil {
		return nil, nil, err
	}

	var boundFilter iceberg.BooleanExpression
	if s.rowFilter != nil {
		boundFilter, err = iceberg.BindExpr(s.metadata.CurrentSchema(), s.rowFilter, s.caseSensitive)
		if err != nil {
			return nil, nil, err
		}
	}

	schema, err := s.Projection()
	if err != nil {
		return nil, nil, err
	}

	return (&arrowScan{
		metadata:        s.metadata,
		fs:              s.io,
		projectedSchema: schema,
		boundRowFilter:  boundFilter,
		caseSensitive:   s.caseSensitive,
		rowLimit:        s.limit,
		options:         s.options,
	}).GetRecords(ctx, tasks)
}

func (s *Scan) ToArrowTable(ctx context.Context) (arrow.Table, error) {
	schema, itr, err := s.ToArrowRecords(ctx)
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
