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
	"github.com/apache/iceberg-go/metrics"
	"golang.org/x/sync/errgroup"
)

const ScanNoLimit = -1

type keyDefaultMap[K comparable, V any] struct {
	defaultFactory func(K) V
	data           map[K]V

	mx sync.RWMutex
}

type keyDefaultMapErr[K comparable, V any] struct {
	defaultFactory func(K) (V, error)
	data           map[K]keyDefaultValueErr[V]

	mx sync.RWMutex
}

type keyDefaultValueErr[V any] struct {
	value V
	err   error
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

func (k *keyDefaultMapErr[K, V]) Get(key K) (V, error) {
	k.mx.RLock()
	if v, ok := k.data[key]; ok {
		k.mx.RUnlock()

		return v.value, v.err
	}

	k.mx.RUnlock()
	k.mx.Lock()
	defer k.mx.Unlock()

	// race check between RLock and Lock
	if v, ok := k.data[key]; ok {
		return v.value, v.err
	}

	value, err := k.defaultFactory(key)
	k.data[key] = keyDefaultValueErr[V]{value: value, err: err}

	return value, err
}

func newKeyDefaultMap[K comparable, V any](factory func(K) V) *keyDefaultMap[K, V] {
	return &keyDefaultMap[K, V]{
		data:           make(map[K]V),
		defaultFactory: factory,
	}
}

// newKeyDefaultMapWrapErr memoizes both successful values and deterministic
// factory errors, so the same failing key is not retried on subsequent reads.
func newKeyDefaultMapWrapErr[K comparable, V any](factory func(K) (V, error)) *keyDefaultMapErr[K, V] {
	return &keyDefaultMapErr[K, V]{
		data:           make(map[K]keyDefaultValueErr[V]),
		defaultFactory: factory,
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

// IsDeletionVector reports whether df is a deletion vector: a Puffin file with
// position-delete content. The content-type guard matters because df is an
// arbitrary DataFile, so a non-pos-delete Puffin from an external writer must
// not be misclassified. Keying on format rather than referenced_data_file
// avoids misclassifying a Parquet pos-delete that legally sets it.
func IsDeletionVector(df iceberg.DataFile) bool {
	return df.FileFormat() == iceberg.PuffinFile &&
		df.ContentType() == iceberg.EntryContentPosDeletes
}

type Scan struct {
	identifier       Identifier
	metadata         Metadata
	metadataLocation string
	ioF              FSysF
	planner          ScanPlanner
	planningMode     ScanPlanningMode
	// planIO, when non-nil, is a plan-scoped FileIO loader set by remote scan
	// planning; ReadTasks loads from it instead of ioF and closes it after the
	// returned iterator finishes. See PlanIO.
	planIO         PlanIO
	rowFilter      iceberg.BooleanExpression
	selectedFields []string
	caseSensitive  bool
	snapshotID     *int64
	asOfTimestamp  *int64
	options        iceberg.Properties
	limit          int64

	includeRowLineage bool

	concurrency int

	reporter metrics.Reporter
}

func (scan *Scan) UseRowLimit(n int64) *Scan {
	out := *scan
	out.limit = n

	return &out
}

// Reporter returns the metrics reporter for this scan, never nil. The
// scan-planning instrumentation emits its ScanReport through it.
func (scan *Scan) Reporter() metrics.Reporter {
	if scan.reporter == nil {
		return metrics.NopReporter{}
	}

	return scan.reporter
}

func (scan *Scan) UseRef(name string) (*Scan, error) {
	if scan.snapshotID != nil {
		return nil, fmt.Errorf("%w: cannot override ref, already set snapshot id %d",
			iceberg.ErrInvalidArgument, *scan.snapshotID)
	}

	if snap := scan.metadata.SnapshotByName(name); snap != nil {
		out := *scan
		out.snapshotID = &snap.SnapshotID

		return &out, nil
	}

	return nil, fmt.Errorf("%w: cannot scan unknown ref=%s", iceberg.ErrInvalidArgument, name)
}

// ResolveSnapshot resolves the snapshot selected by this scan. Live scans use
// the table's current snapshot; explicit snapshot IDs and as-of timestamps
// must resolve to an existing snapshot.
func (scan *Scan) ResolveSnapshot() (*Snapshot, error) {
	if scan.snapshotID != nil {
		snap := scan.metadata.SnapshotByID(*scan.snapshotID)
		if snap == nil {
			return nil, fmt.Errorf("%w: snapshot not found: %d", ErrInvalidOperation, *scan.snapshotID)
		}

		return snap, nil
	}

	if scan.asOfTimestamp != nil {
		entries := slices.Collect(scan.metadata.SnapshotLogs())
		for i := len(entries) - 1; i >= 0; i-- {
			entry := entries[i]
			if entry.TimestampMs <= *scan.asOfTimestamp {
				snap := scan.metadata.SnapshotByID(entry.SnapshotID)
				if snap == nil {
					break
				}

				return snap, nil
			}
		}

		return nil, fmt.Errorf("no snapshot found for timestamp %d", *scan.asOfTimestamp)
	}

	return scan.metadata.CurrentSnapshot(), nil
}

// Snapshot returns the snapshot selected by this scan. It returns nil when an
// explicit snapshot cannot be resolved; use ResolveSnapshot when the reason
// for that result must be distinguished from a table with no current snapshot.
func (scan *Scan) Snapshot() *Snapshot {
	snap, _ := scan.ResolveSnapshot()

	return snap
}

func (scan *Scan) Projection() (*iceberg.Schema, error) {
	curSchema, err := scan.effectiveSchema()
	if err != nil {
		return nil, err
	}
	curVersion := scan.metadata.Version()

	if scan.includeRowLineage && curVersion < minFormatVersionRowLineage {
		return nil, fmt.Errorf("%w: row lineage requires format version %d, table is v%d",
			ErrInvalidOperation, minFormatVersionRowLineage, curVersion)
	}

	var schema *iceberg.Schema
	if slices.Contains(scan.selectedFields, "*") {
		schema = curSchema
	} else {
		// Intercept row-lineage metadata column names (_row_id,
		// _last_updated_sequence_number) before calling Select: they are
		// reserved and never appear in the user schema's fields, so
		// Select would fail with "could not find column" on v3 tables
		// where they are otherwise legal to project. The scanner reads
		// them from file metadata (or synthesizes them) at scan time;
		// here we just need to ensure they survive into the projection.
		userFields, lineageFields := splitLineageMetadataFields(scan.selectedFields, scan.caseSensitive)
		if len(lineageFields) > 0 && curVersion < minFormatVersionRowLineage {
			// Reject explicitly so the contract lives in the code rather
			// than emerging from Select's "could not find column" path —
			// a future v2 schema field literally named _row_id should not
			// silently succeed here.
			return nil, fmt.Errorf("%w: row lineage column %q requires format version %d, table is v%d",
				ErrInvalidOperation, lineageFields[0].Name, minFormatVersionRowLineage, curVersion)
		}

		var err error
		schema, err = curSchema.Select(scan.caseSensitive, userFields...)
		if err != nil {
			return nil, err
		}
		// Skip the per-name append when scan.includeRowLineage is set: the
		// SchemaWithRowLineage call below adds both lineage columns
		// unconditionally, and appendMissingLineageFields would just be
		// redundant work whose result is overwritten.
		if len(lineageFields) > 0 && !scan.includeRowLineage {
			schema = appendMissingLineageFields(schema, lineageFields)
		}
	}

	if scan.includeRowLineage {
		schema = iceberg.SchemaWithRowLineage(schema)
	}

	return schema, nil
}

func (scan *Scan) effectiveSchema() (*iceberg.Schema, error) {
	curSchema := scan.metadata.CurrentSchema()
	if scan.snapshotID == nil && scan.asOfTimestamp == nil {
		// Live scans intentionally use the table's current schema. A schema-only
		// metadata update can advance CurrentSchema without creating a snapshot,
		// while explicit snapshot/as-of scans use the snapshot schema below.
		return curSchema, nil
	}

	snap, err := scan.ResolveSnapshot()
	if err != nil {
		return nil, err
	}

	if snap.SchemaID == nil {
		return curSchema, nil
	}

	for _, schema := range scan.metadata.Schemas() {
		if schema.ID == *snap.SchemaID {
			return schema, nil
		}
	}

	return nil, fmt.Errorf("%w: snapshot %d references unknown schema id %d",
		ErrInvalidMetadata, snap.SnapshotID, *snap.SchemaID)
}

// splitLineageMetadataFields partitions selectedFields into user fields and
// row-lineage metadata fields (_row_id, _last_updated_sequence_number). The
// returned lineage slice contains the canonical NestedField for each
// metadata column name found, in the order encountered.
func splitLineageMetadataFields(selectedFields []string, caseSensitive bool) (userFields []string, lineageFields []iceberg.NestedField) {
	matches := func(field, target string) bool {
		if caseSensitive {
			return field == target
		}

		return strings.EqualFold(field, target)
	}

	userFields = make([]string, 0, len(selectedFields))
	for _, field := range selectedFields {
		switch {
		case matches(field, iceberg.RowIDColumnName):
			lineageFields = append(lineageFields, iceberg.RowID())
		case matches(field, iceberg.LastUpdatedSequenceNumberColumnName):
			lineageFields = append(lineageFields, iceberg.LastUpdatedSequenceNumber())
		default:
			userFields = append(userFields, field)
		}
	}

	return userFields, lineageFields
}

// appendMissingLineageFields returns a new schema with each lineage field
// appended only if no field with that ID is already present. Idempotent so
// callers can pass schemas that already declare the reserved fields.
func appendMissingLineageFields(s *iceberg.Schema, lineageFields []iceberg.NestedField) *iceberg.Schema {
	existing := make(map[int]struct{}, len(s.Fields()))
	for _, f := range s.Fields() {
		existing[f.ID] = struct{}{}
	}

	fields := slices.Clone(s.Fields())
	for _, f := range lineageFields {
		if _, ok := existing[f.ID]; ok {
			continue
		}
		fields = append(fields, f)
		existing[f.ID] = struct{}{}
	}

	return iceberg.NewSchemaWithIdentifiers(s.ID, s.IdentifierFieldIDs, fields...)
}

func buildPartitionProjection(specID int, meta Metadata, schema *iceberg.Schema, rowFilter iceberg.BooleanExpression, caseSensitive bool) (iceberg.BooleanExpression, error) {
	spec := meta.PartitionSpecByID(specID)
	if spec == nil {
		return nil, fmt.Errorf("%w: id %d", ErrPartitionSpecNotFound, specID)
	}
	project := newInclusiveProjection(schema, *spec, caseSensitive)

	return project(rowFilter)
}

func buildManifestEvaluator(specID int, metadata Metadata, schema *iceberg.Schema, partitionFilters *keyDefaultMapErr[int, iceberg.BooleanExpression], caseSensitive bool) (func(iceberg.ManifestFile) (bool, error), error) {
	spec := metadata.PartitionSpecByID(specID)
	if spec == nil {
		return nil, fmt.Errorf("%w: id %d", ErrPartitionSpecNotFound, specID)
	}

	partitionFilter, err := partitionFilters.Get(specID)
	if err != nil {
		return nil, err
	}

	return newManifestEvaluator(*spec, schema,
		partitionFilter, caseSensitive)
}

func buildPartitionEvaluator(specID int, metadata Metadata, schema *iceberg.Schema, partitionFilters *keyDefaultMapErr[int, iceberg.BooleanExpression], caseSensitive bool) (func(iceberg.DataFile) (bool, error), error) {
	spec := metadata.PartitionSpecByID(specID)
	if spec == nil {
		return nil, fmt.Errorf("%w: id %d", ErrPartitionSpecNotFound, specID)
	}
	partType := spec.PartitionType(schema)
	partSchema := iceberg.NewSchema(0, partType.FieldList...)

	partitionFilter, err := partitionFilters.Get(specID)
	if err != nil {
		return nil, err
	}

	fn, err := iceberg.ExpressionEvaluator(partSchema, partitionFilter, caseSensitive)
	if err != nil {
		return nil, err
	}

	return func(d iceberg.DataFile) (bool, error) {
		return fn(GetPartitionRecord(d, partType))
	}, nil
}

func (scan *Scan) partitionFiltersForSchema(schema *iceberg.Schema) *keyDefaultMapErr[int, iceberg.BooleanExpression] {
	return newKeyDefaultMapWrapErr(func(specID int) (iceberg.BooleanExpression, error) {
		return buildPartitionProjection(specID, scan.metadata, schema, scan.rowFilter, scan.caseSensitive)
	})
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

// buildDVIndex indexes deletion vectors by the data file path they reference.
// The spec requires at most one DV per data file; a second entry for the same
// path is rejected with an error.
func buildDVIndex(dvEntries []iceberg.ManifestEntry) (map[string]iceberg.ManifestEntry, error) {
	dvIndex := make(map[string]iceberg.ManifestEntry, len(dvEntries))
	for _, del := range dvEntries {
		if ref := del.DataFile().ReferencedDataFile(); ref != nil {
			if _, exists := dvIndex[*ref]; exists {
				return nil, fmt.Errorf("can't index multiple deletion vectors for %s", *ref)
			}
			dvIndex[*ref] = del
		}
	}

	return dvIndex, nil
}

// matchDVToData returns the deletion vector that applies to the given data
// entry, if any. A DV applies when the data file's sequence number is less
// than or equal to the DV's sequence number.
//
// SequenceNum reports the -1 sentinel when an entry's sequence number is
// unset (see manifest.go). Entries arrive here already inherited, so a
// committed ADDED entry always carries a real (>= 0) sequence number; an
// unset value comes from an EXISTING/DELETED entry missing its required
// explicit sequence number, or a not-yet-committed manifest. Such an
// indeterminate sequence number — on either side — is treated as "applies":
// comparing a real data sequence number against an unset DV sequence (-1)
// would never satisfy dataSeq <= -1 and would silently drop the DV,
// resurfacing deleted rows; an unset data sequence likewise satisfies
// -1 <= dvSeq for any known DV sequence.
func matchDVToData(dataEntry iceberg.ManifestEntry, dvIndex map[string]iceberg.ManifestEntry) []iceberg.DataFile {
	dvEntry, ok := dvIndex[dataEntry.DataFile().FilePath()]
	if !ok {
		return nil
	}
	if dvSeq := dvEntry.SequenceNum(); dvSeq < 0 || dataEntry.SequenceNum() <= dvSeq {
		return []iceberg.DataFile{dvEntry.DataFile()}
	}

	return nil
}

// fetchPartitionSpecFilteredManifests retrieves the table's current snapshot,
// fetches its manifest files, and applies partition-spec filters to remove irrelevant manifests.
func (scan *Scan) fetchPartitionSpecFilteredManifests(ctx context.Context) ([]iceberg.ManifestFile, error) {
	schema, err := scan.effectiveSchema()
	if err != nil {
		return nil, err
	}

	return scan.fetchPartitionSpecFilteredManifestsWithSchema(ctx, schema)
}

func (scan *Scan) fetchPartitionSpecFilteredManifestsWithSchema(ctx context.Context, schema *iceberg.Schema) ([]iceberg.ManifestFile, error) {
	snap, err := scan.ResolveSnapshot()
	if err != nil {
		return nil, err
	}
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
	partitionFilters := scan.partitionFiltersForSchema(schema)
	manifestEvaluators := newKeyDefaultMapWrapErr(func(specID int) (func(iceberg.ManifestFile) (bool, error), error) {
		return buildManifestEvaluator(specID, scan.metadata, schema, partitionFilters, scan.caseSensitive)
	})
	filtered := make([]iceberg.ManifestFile, 0, len(manifestList))
	for _, mf := range manifestList {
		eval, err := manifestEvaluators.Get(int(mf.PartitionSpecID()))
		if err != nil {
			return nil, fmt.Errorf("failed to build manifest evaluator for spec %d: %w", mf.PartitionSpecID(), err)
		}
		use, err := eval(mf)
		if err != nil {
			return nil, fmt.Errorf("failed to evaluate manifest %s: %w", mf.FilePath(), err)
		}
		if use {
			filtered = append(filtered, mf)
		}
	}

	return filtered, nil
}

// collectManifestEntries concurrently opens manifests, applies partition and metrics
// filters, and accumulates both data entries and positional-delete entries.
func (scan *Scan) collectManifestEntries(
	ctx context.Context,
	manifestList []iceberg.ManifestFile,
) (*manifestEntries, error) {
	schema, err := scan.effectiveSchema()
	if err != nil {
		return nil, err
	}

	return scan.collectManifestEntriesWithSchema(ctx, manifestList, schema)
}

func (scan *Scan) collectManifestEntriesWithSchema(
	ctx context.Context,
	manifestList []iceberg.ManifestFile,
	schema *iceberg.Schema,
) (*manifestEntries, error) {
	metricsEval, err := newInclusiveMetricsEvaluator(
		schema,
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

	partitionFilters := scan.partitionFiltersForSchema(schema)
	partitionEvaluators := newKeyDefaultMapWrapErr(func(specID int) (func(iceberg.DataFile) (bool, error), error) {
		return buildPartitionEvaluator(specID, scan.metadata, schema, partitionFilters, scan.caseSensitive)
	})

	for _, mf := range manifestList {
		if !scan.checkSequenceNumber(minSeqNum, mf) {
			continue
		}

		g.Go(func() error {
			fs, err := scan.ioF(ctx)
			if err != nil {
				return err
			}
			partEval, err := partitionEvaluators.Get(int(mf.PartitionSpecID()))
			if err != nil {
				return fmt.Errorf("failed to build partition evaluator for spec %d: %w", mf.PartitionSpecID(), err)
			}
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
					if IsDeletionVector(e.DataFile()) {
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
		snapshot, err := scan.ResolveSnapshot()
		if err != nil {
			return nil, err
		}
		scan.snapshotID = &snapshot.SnapshotID
		scan.asOfTimestamp = nil
	}

	switch scan.planningMode {
	case ScanPlanningRemote:
		return scan.planFilesRemote(ctx)
	case ScanPlanningAuto:
		if scan.planner != nil && scan.planner.SupportsRemoteScanPlanning() {
			return scan.planFilesRemote(ctx)
		}
	case ScanPlanningLocal:
	default:
		return nil, fmt.Errorf("%w: unknown scan planning mode %q", iceberg.ErrInvalidArgument, scan.planningMode)
	}

	scan.planIO = nil

	schema, err := scan.effectiveSchema()
	if err != nil {
		return nil, err
	}

	// Step 1: Retrieve filtered manifests based on snapshot and partition specs.
	manifestList, err := scan.fetchPartitionSpecFilteredManifestsWithSchema(ctx, schema)
	if err != nil || len(manifestList) == 0 {
		return nil, err
	}

	// Step 2: Read manifest entries concurrently, accumulating data and positional deletes.
	entries, err := scan.collectManifestEntriesWithSchema(ctx, manifestList, schema)
	if err != nil {
		return nil, err
	}

	// Step 3: Sort positional deletes and match them to data files.
	slices.SortFunc(entries.positionalDeleteEntries, func(a, b iceberg.ManifestEntry) int {
		return cmp.Compare(a.SequenceNum(), b.SequenceNum())
	})

	dvIndex, err := buildDVIndex(entries.dvEntries)
	if err != nil {
		return nil, err
	}

	results := make([]FileScanTask, 0, len(entries.dataEntries))
	for _, e := range entries.dataEntries {
		// Spec §Scan Planning: when a deletion vector applies to a data
		// file, positional-delete files must NOT be applied. The DV is
		// guaranteed to encode all prior pos-delete positions; reading the
		// pos-delete Parquet too would be wasteful I/O, and on a buggy
		// writer whose DV omits prior positions, applying both would
		// over-delete. Mirrors Java's DeleteFileIndex.forDataFile.
		dvFiles := matchDVToData(e, dvIndex)
		var deleteFiles []iceberg.DataFile
		if len(dvFiles) == 0 {
			deleteFiles, err = matchDeletesToData(e, entries.positionalDeleteEntries)
			if err != nil {
				return nil, err
			}
		}
		eqDeleteFiles := matchEqualityDeletesToData(e, entries.equalityDeleteEntries)

		task := FileScanTask{
			File:                e.DataFile(),
			DeleteFiles:         deleteFiles,
			EqualityDeleteFiles: eqDeleteFiles,
			DeletionVectorFiles: dvFiles,
			Start:               0,
			Length:              e.DataFile().FileSizeBytes(),
		}
		// Row lineage constants: readers use these to synthesize _row_id and
		// _last_updated_sequence_number when requested. Per spec the
		// synthesized _last_updated_sequence_number is the manifest entry's
		// data sequence number (field id 3), not file sequence number
		// (field id 4); back-dated EXISTING entries can have the two
		// diverge and Java/iceberg-rust use the data sequence number.
		task.FirstRowID = e.DataFile().FirstRowID()
		if seq := e.SequenceNum(); seq >= 0 {
			s := seq
			task.DataSequenceNumber = &s
		}
		results = append(results, task)
	}

	return results, nil
}

func (scan *Scan) planFilesRemote(ctx context.Context) ([]FileScanTask, error) {
	if scan.planner == nil || !scan.planner.SupportsRemoteScanPlanning() {
		return nil, fmt.Errorf("%w: remote scan planning is unavailable", ErrInvalidOperation)
	}

	caseSensitive := scan.caseSensitive
	result, err := scan.planner.PlanFiles(ctx, ScanPlanningRequest{
		Identifier:       slices.Clone(scan.identifier),
		Metadata:         scan.metadata,
		MetadataLocation: scan.metadataLocation,
		SnapshotID:       scan.snapshotID,
		SelectedFields:   scan.selectedFields,
		RowFilter:        scan.rowFilter,
		CaseSensitive:    &caseSensitive,
	})
	if err != nil {
		return nil, err
	}

	scan.planIO = result.IO

	return result.Tasks, nil
}

type FileScanTask struct {
	File                iceberg.DataFile
	DeleteFiles         []iceberg.DataFile // positional delete files
	EqualityDeleteFiles []iceberg.DataFile // equality delete files
	DeletionVectorFiles []iceberg.DataFile // deletion vectors (puffin files)
	Start, Length       int64
	// Residual is the portion of the scan filter that must still be evaluated
	// for this task. Remote planners may simplify the original filter using
	// file metadata; nil means the caller did not provide a task residual.
	// ReadTasks currently applies the Scan's original row filter and does not
	// consume this per-task value. Remote integration must preserve that original
	// filter until per-task residual evaluation is wired; otherwise tasks read
	// outside their originating Scan could under-filter rows.
	Residual iceberg.BooleanExpression

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

	effectiveSchema, err := scan.effectiveSchema()
	if err != nil {
		return nil, nil, err
	}

	if scan.rowFilter != nil {
		boundFilter, err = iceberg.BindExpr(effectiveSchema, scan.rowFilter, scan.caseSensitive)
		if err != nil {
			return nil, nil, err
		}
	}

	schema, err := scan.Projection()
	if err != nil {
		return nil, nil, err
	}

	// A plan-scoped FileIO (from remote planning) takes precedence over the
	// table's default FileIO and is closed once the returned iterator finishes.
	var fs io.IO
	if scan.planIO != nil {
		fs, err = scan.planIO.Load(ctx)
	} else {
		fs, err = scan.ioF(ctx)
	}
	if err != nil {
		return nil, nil, err
	}

	outSchema, records, err := (&arrowScan{
		metadata:        scan.metadata,
		fs:              fs,
		projectedSchema: schema,
		boundRowFilter:  boundFilter,
		caseSensitive:   scan.caseSensitive,
		rowLimit:        scan.limit,
		options:         scan.options,
		concurrency:     scan.concurrency,
	}).GetRecords(ctx, tasks)
	if err != nil {
		// No iterator to drive cleanup on a setup error, so close here.
		if scan.planIO != nil {
			_ = scan.planIO.Close()
		}

		return nil, nil, err
	}

	if scan.planIO != nil {
		records = closePlanIOAfter(records, scan.planIO)
	}

	return outSchema, records, nil
}

// closePlanIOAfter wraps an arrow record iterator so the plan-scoped IO is
// closed once iteration ends — whether the consumer exhausts the iterator or
// stops early. A caller that never ranges over the iterator does not trigger
// the close; that is an accepted edge for an unread result.
func closePlanIOAfter(seq iter.Seq2[arrow.RecordBatch, error], pio PlanIO) iter.Seq2[arrow.RecordBatch, error] {
	return func(yield func(arrow.RecordBatch, error) bool) {
		defer func() { _ = pio.Close() }()
		for rec, err := range seq {
			if !yield(rec, err) {
				return
			}
		}
	}
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
