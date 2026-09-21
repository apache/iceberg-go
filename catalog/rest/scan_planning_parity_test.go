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

package rest

import (
	"bytes"
	"cmp"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This fixture plans from real Avro manifests on the server. It deliberately
// uses independent JSON maps for requests/responses, rather than echoing a
// client's tasks or using the REST wire structs on both ends.
func TestScanPlanningLocalRemoteParity(t *testing.T) {
	t.Parallel()
	meta, fs := newPlanningParityTable(t)
	cases := []struct {
		name        string
		filter      iceberg.BooleanExpression
		snapshot    int64
		ref         string
		insensitive bool
		split       bool
		wantFiles   int
	}{
		{name: "all", filter: iceberg.AlwaysTrue{}, wantFiles: 3},
		{name: "ranges", filter: iceberg.AlwaysTrue{}, split: true, wantFiles: 9},
		{name: "decimal", filter: iceberg.EqualTo(iceberg.Reference("amount"), "12.34"), wantFiles: 3},
		{name: "timestamp", filter: iceberg.EqualTo(iceberg.Reference("ts"), "2026-07-17T10:15:30.123456"), wantFiles: 3},
		{name: "timestamptz", filter: iceberg.EqualTo(iceberg.Reference("tstz"), "2026-07-17T10:15:30.123456+00:00"), wantFiles: 3},
		{name: "date", filter: iceberg.EqualTo(iceberg.Reference("day"), "2026-07-17"), wantFiles: 3},
		{name: "time", filter: iceberg.EqualTo(iceberg.Reference("clock"), "10:15:30.123456"), wantFiles: 3},
		{name: "uuid", filter: iceberg.EqualTo(iceberg.Reference("uuid"), "f79c3e09-677c-4bbd-a479-3f349cb785e7"), wantFiles: 3},
		{name: "binary", filter: iceberg.EqualTo(iceberg.Reference("blob"), []byte{0, 255, 16}), wantFiles: 3},
		{name: "fixed", filter: iceberg.EqualTo(iceberg.Reference("fixed"), []byte{0, 255, 16}), wantFiles: 3},
		{name: "nested", filter: iceberg.StartsWith(iceberg.Reference("nested.text"), "prefix"), wantFiles: 3},
		{name: "boolean", filter: iceberg.EqualTo(iceberg.Reference("flag"), true), wantFiles: 3},
		{name: "double", filter: iceberg.EqualTo(iceberg.Reference("score"), 1.25), wantFiles: 3},
		{name: "float", filter: iceberg.IsNaN(iceberg.Reference("ratio")), wantFiles: 3},
		{name: "long", filter: iceberg.EqualTo(iceberg.Reference("big"), int64(9007199254740993)), wantFiles: 3},
		{name: "none", filter: iceberg.AlwaysFalse{}, wantFiles: 0},
		{name: "partition", filter: iceberg.EqualTo(iceberg.Reference("tenant"), int32(1)), wantFiles: 2},
		{name: "metrics", filter: iceberg.GreaterThan(iceberg.Reference("renamed"), int64(25)), wantFiles: 2},
		{name: "and residual", filter: iceberg.NewAnd(iceberg.EqualTo(iceberg.Reference("tenant"), int32(1)), iceberg.GreaterThan(iceberg.Reference("renamed"), int64(25))), wantFiles: 1},
		{name: "or", filter: iceberg.NewOr(iceberg.EqualTo(iceberg.Reference("tenant"), int32(2)), iceberg.LessThan(iceberg.Reference("renamed"), int64(15))), wantFiles: 3},
		{name: "set", filter: iceberg.IsIn(iceberg.Reference("tenant"), int32(1), int32(3)), wantFiles: 2},
		{name: "not set", filter: iceberg.NotIn(iceberg.Reference("tenant"), int32(1), int32(3)), wantFiles: 1},
		{name: "not", filter: iceberg.NewNot(iceberg.EqualTo(iceberg.Reference("tenant"), int32(1))), wantFiles: 1},
		{name: "null", filter: iceberg.IsNull(iceberg.Reference("tenant")), wantFiles: 0},
		{name: "case insensitive", filter: iceberg.EqualTo(iceberg.Reference("TENANT"), int32(1)), insensitive: true, wantFiles: 2},
		{name: "historical schema", filter: iceberg.GreaterThan(iceberg.Reference("value"), int32(25)), snapshot: 10, wantFiles: 2},
		{name: "tag schema", filter: iceberg.GreaterThan(iceberg.Reference("value"), int32(25)), ref: "old", wantFiles: 2},
		{name: "branch current schema", filter: iceberg.GreaterThan(iceberg.Reference("renamed"), int64(25)), ref: "history", wantFiles: 2},
	}
	for _, mode := range []string{"inline", "fanout", "async"} {
		for _, tc := range cases {
			t.Run(mode+"/"+tc.name, func(t *testing.T) {
				t.Parallel()
				scanMeta := meta
				if tc.split {
					builder, err := table.MetadataBuilderFromBase(meta, "metadata.json")
					require.NoError(t, err)
					require.NoError(t, builder.SetProperties(iceberg.Properties{table.ReadSplitTargetSizeKey: "40"}))
					scanMeta, err = builder.Build()
					require.NoError(t, err)
				}
				cat := newPlanningParityServer(t, scanMeta, fs, mode)
				local := table.New(table.Identifier{"db", "tbl"}, scanMeta, "metadata.json",
					func(context.Context) (iceio.IO, error) { return fs, nil }, nil)
				remote := table.New(table.Identifier{"db", "tbl"}, scanMeta, "metadata.json",
					func(context.Context) (iceio.IO, error) {
						return nil, errors.New("remote planner attempted to read client manifests")
					}, cat)
				opts := []table.ScanOption{table.WithRowFilter(tc.filter), table.WithCaseSensitive(!tc.insensitive)}
				if tc.snapshot != 0 {
					opts = append(opts, table.WithSnapshotID(tc.snapshot))
				}
				makeScan := func(tbl *table.Table, mode table.ScanPlanningMode) *table.Scan {
					scan := tbl.Scan(append(slices.Clone(opts), table.WithScanPlanningMode(mode))...)
					t.Cleanup(func() { require.NoError(t, scan.Close()) })
					if tc.ref != "" {
						derived, err := scan.UseRef(tc.ref)
						require.NoError(t, err)
						t.Cleanup(func() { require.NoError(t, derived.Close()) })

						return derived
					}

					return scan
				}
				localScan := makeScan(local, table.ScanPlanningLocal)
				remoteScan := makeScan(remote, table.ScanPlanningRemote)
				expected, err := localScan.PlanFiles(t.Context())
				require.NoError(t, err)
				require.Len(t, expected, tc.wantFiles, "fixture must actually prune files")
				actual, err := remoteScan.PlanFiles(t.Context())
				require.NoError(t, err)
				schema, err := localScan.Projection()
				require.NoError(t, err)
				assertPlanningParity(t, expected, actual, schema, tc.filter, !tc.insensitive)
				if tc.name == "partition" {
					require.Len(t, actual, 2)
					for _, task := range actual {
						assert.Len(t, task.DeleteFiles, 1)
						assert.Len(t, task.EqualityDeleteFiles, 1)
						assert.True(t, task.Residual.Equals(iceberg.AlwaysTrue{}))
					}
				}
			})
		}
	}
}

func newPlanningParityTable(t *testing.T) (table.Metadata, *iceio.MemFS) {
	t.Helper()
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "tenant", Type: iceberg.PrimitiveTypes.Int32},
		iceberg.NestedField{ID: 2, Name: "value", Type: iceberg.PrimitiveTypes.Int32},
		iceberg.NestedField{ID: 3, Name: "amount", Type: iceberg.DecimalTypeOf(12, 2)},
		iceberg.NestedField{ID: 4, Name: "ts", Type: iceberg.PrimitiveTypes.Timestamp},
		iceberg.NestedField{ID: 5, Name: "tstz", Type: iceberg.PrimitiveTypes.TimestampTz},
		iceberg.NestedField{ID: 6, Name: "day", Type: iceberg.PrimitiveTypes.Date},
		iceberg.NestedField{ID: 7, Name: "clock", Type: iceberg.PrimitiveTypes.Time},
		iceberg.NestedField{ID: 8, Name: "uuid", Type: iceberg.PrimitiveTypes.UUID},
		iceberg.NestedField{ID: 9, Name: "blob", Type: iceberg.PrimitiveTypes.Binary},
		iceberg.NestedField{ID: 10, Name: "fixed", Type: iceberg.FixedTypeOf(3)},
		iceberg.NestedField{ID: 11, Name: "nested", Type: &iceberg.StructType{FieldList: []iceberg.NestedField{
			{ID: 16, Name: "text", Type: iceberg.PrimitiveTypes.String},
		}}},
		iceberg.NestedField{ID: 12, Name: "flag", Type: iceberg.PrimitiveTypes.Bool},
		iceberg.NestedField{ID: 13, Name: "score", Type: iceberg.PrimitiveTypes.Float64},
		iceberg.NestedField{ID: 14, Name: "ratio", Type: iceberg.PrimitiveTypes.Float32},
		iceberg.NestedField{ID: 15, Name: "big", Type: iceberg.PrimitiveTypes.Int64},
	)
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "tenant", Transform: iceberg.IdentityTransform{},
	})
	meta, err := table.NewMetadata(schema, &spec, table.UnsortedSortOrder, "mem://parity", nil)
	require.NoError(t, err)
	fs := iceio.NewMemFS()
	var entries []iceberg.ManifestEntry
	for i, tenant := range []int32{1, 1, 2} {
		b, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentData,
			fmt.Sprintf("mem://parity/data/%d.parquet", i), iceberg.ParquetFile,
			map[int]any{1000: tenant}, nil, nil, 10, 100)
		require.NoError(t, err)
		lo, err := iceberg.Int32Literal(10 + i*20).MarshalBinary()
		require.NoError(t, err)
		hi, err := iceberg.Int32Literal(19 + i*20).MarshalBinary()
		require.NoError(t, err)
		b.SplitOffsets([]int64{4, 40, 70})
		b.LowerBoundValues(map[int][]byte{2: lo}).UpperBoundValues(map[int][]byte{2: hi})
		b.ValueCounts(map[int]int64{1: 10, 2: 10}).NullValueCounts(map[int]int64{1: 0, 2: 0})
		entries = append(entries, iceberg.NewManifestEntryBuilder(iceberg.EntryStatusADDED, parityPtr(int64(10)), b.Build()).SequenceNum(1).Build())
	}
	var data bytes.Buffer
	mf, err := iceberg.WriteManifest("mem://parity/data.avro", &data, 2, spec, schema, 10, entries)
	require.NoError(t, err)
	require.NoError(t, fs.WriteFile(mf.FilePath(), data.Bytes()))
	var deletes bytes.Buffer
	writer, err := iceberg.NewManifestWriter(2, &deletes, spec, schema, 20,
		iceberg.WithManifestWriterContent(iceberg.ManifestContentDeletes))
	require.NoError(t, err)
	// Distinct delete paths across envelopes catch accidental global indexing
	// of envelope-local delete-file-references.
	for _, tenant := range []int32{1, 2} {
		for _, content := range []iceberg.ManifestEntryContent{iceberg.EntryContentPosDeletes, iceberg.EntryContentEqDeletes} {
			b, err := iceberg.NewDataFileBuilder(spec, content, fmt.Sprintf("mem://parity/delete/%d-%d.parquet", tenant, content),
				iceberg.ParquetFile, map[int]any{1000: tenant}, nil, nil, 1, 20)
			require.NoError(t, err)
			if content == iceberg.EntryContentEqDeletes {
				b.EqualityFieldIDs([]int{2})
			}
			require.NoError(t, writer.Add(iceberg.NewManifestEntryBuilder(iceberg.EntryStatusADDED, parityPtr(int64(20)), b.Build()).SequenceNum(2).Build()))
		}
	}
	require.NoError(t, writer.Close())
	dm, err := writer.ToManifestFile("mem://parity/deletes.avro", int64(deletes.Len()),
		iceberg.WithManifestFileContent(iceberg.ManifestContentDeletes))
	require.NoError(t, err)
	require.NoError(t, fs.WriteFile(dm.FilePath(), deletes.Bytes()))
	builder, err := table.MetadataBuilderFromBase(meta, "metadata.json")
	require.NoError(t, err)
	for i := range 2 {
		manifests := []iceberg.ManifestFile{mf}
		if i == 1 {
			manifests = append(manifests, dm)
		}
		snapshotID, seq := int64((i+1)*10), int64(i+1)
		path := fmt.Sprintf("mem://parity/list-%d.avro", snapshotID)
		var list bytes.Buffer
		require.NoError(t, iceberg.WriteManifestList(2, &list, snapshotID, nil, &seq, 0, manifests))
		require.NoError(t, fs.WriteFile(path, list.Bytes()))
		if i == 0 {
			committed, err := iceberg.ReadManifestList(bytes.NewReader(list.Bytes()))
			require.NoError(t, err)
			mf = committed[0]
		}
		require.NoError(t, builder.AddSnapshot(&table.Snapshot{
			SnapshotID: snapshotID, SequenceNumber: seq,
			TimestampMs: time.Now().UnixMilli() + int64(i+1), ManifestList: path, SchemaID: parityPtr(0),
		}))
	}
	require.NoError(t, builder.SetSnapshotRef("main", 20, table.BranchRef))
	require.NoError(t, builder.SetSnapshotRef("old", 10, table.TagRef))
	require.NoError(t, builder.SetSnapshotRef("history", 10, table.BranchRef))
	fields := meta.CurrentSchema().Fields()
	fields[1].Name = "renamed"
	evolved := iceberg.NewSchema(1, fields...)
	require.NoError(t, builder.AddSchema(evolved))
	require.NoError(t, builder.SetCurrentSchemaID(1))
	meta, err = builder.Build()
	require.NoError(t, err)

	return meta, fs
}

func newPlanningParityServer(t *testing.T, meta table.Metadata, fs iceio.IO, mode string) *Catalog {
	t.Helper()
	// Publish the completed plan and immutable task envelopes to handlers.
	var mu sync.RWMutex
	var envelopes []map[string]any
	plan := func(req *http.Request) (map[string]any, error) {
		var wire struct {
			SnapshotID        *int64          `json:"snapshot-id"`
			Filter            json.RawMessage `json:"filter"`
			Select            []string        `json:"select"`
			CaseSensitive     *bool           `json:"case-sensitive"`
			UseSnapshotSchema bool            `json:"use-snapshot-schema"`
		}
		if err := json.NewDecoder(req.Body).Decode(&wire); err != nil {
			return nil, err
		}
		schema := meta.CurrentSchema()
		opts := []table.ScanOption{table.WithScanPlanningMode(table.ScanPlanningLocal)}
		// REST returns whole files plus split offsets. Client-side splitting
		// must match local planning independently of the server's split size.
		serverBuilder, err := table.MetadataBuilderFromBase(meta, "metadata.json")
		if err != nil {
			return nil, err
		}
		if err := serverBuilder.SetProperties(iceberg.Properties{table.ReadSplitTargetSizeKey: "134217728"}); err != nil {
			return nil, err
		}
		serverMeta, err := serverBuilder.Build()
		if err != nil {
			return nil, err
		}
		if wire.SnapshotID != nil {
			if wire.UseSnapshotSchema {
				opts = append(opts, table.WithSnapshotID(*wire.SnapshotID))
				snap := meta.SnapshotByID(*wire.SnapshotID)
				if snap == nil {
					return nil, errors.New("unknown snapshot")
				}
				for _, candidate := range meta.Schemas() {
					if snap.SchemaID != nil && candidate.ID == *snap.SchemaID {
						schema = candidate
					}
				}
			} else {
				b, err := table.MetadataBuilderFromBase(serverMeta, "metadata.json")
				if err != nil {
					return nil, err
				}
				if err = b.SetSnapshotRef("main", *wire.SnapshotID, table.BranchRef); err != nil {
					return nil, err
				}
				serverMeta, err = b.Build()
				if err != nil {
					return nil, err
				}
			}
		}
		filter := iceberg.BooleanExpression(iceberg.AlwaysTrue{})
		if len(wire.Filter) != 0 {
			var err error
			filter, err = iceberg.ParseExpr(wire.Filter, schema)
			if err != nil {
				return nil, err
			}
		}
		opts = append(opts, table.WithRowFilter(filter))
		if wire.CaseSensitive != nil {
			opts = append(opts, table.WithCaseSensitive(*wire.CaseSensitive))
		}
		if len(wire.Select) != 0 {
			opts = append(opts, table.WithSelectedFields(wire.Select...))
		}
		tbl := table.New(table.Identifier{"db", "tbl"}, serverMeta, "metadata.json",
			func(context.Context) (iceio.IO, error) { return fs, nil }, nil)
		scan := tbl.Scan(opts...)
		defer scan.Close()
		tasks, err := scan.PlanFiles(req.Context())
		if err != nil {
			return nil, err
		}
		// Each fanout envelope owns its own zero-based delete references.
		envelopes = nil
		for _, task := range tasks {
			envelope, err := parityWireTasks([]table.FileScanTask{task}, filter, schema)
			if err != nil {
				return nil, err
			}
			envelopes = append(envelopes, envelope)
		}
		if mode == "inline" {
			return parityWireTasks(tasks, filter, schema)
		}
		tokens := make([]string, len(envelopes))
		for i := range tokens {
			tokens[i] = strconv.Itoa(i)
		}

		return map[string]any{"plan-tasks": tokens}, nil
	}
	var completed map[string]any
	respond := func(w http.ResponseWriter, body map[string]any, err error) {
		if err != nil {
			t.Errorf("server planning: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)

			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(body); err != nil {
			t.Error(err)
		}
	}

	return newScanPlanningTestCatalog(t, []endpoint{endpointPlanTableScan, endpointFetchPlanResult, endpointFetchScanTasks, endpointCancelPlanning}, func(mux *http.ServeMux) {
		mux.HandleFunc("POST /v1/namespaces/db/tables/tbl/plan", func(w http.ResponseWriter, req *http.Request) {
			mu.Lock()
			defer mu.Unlock()
			result, err := plan(req)
			if err != nil {
				respond(w, nil, err)

				return
			}
			result["status"] = "completed"
			result["plan-id"] = "parity"
			completed = result
			if mode == "async" {
				respond(w, map[string]any{"status": "submitted", "plan-id": "parity"}, nil)

				return
			}
			respond(w, result, nil)
		})
		mux.HandleFunc("GET /v1/namespaces/db/tables/tbl/plan/parity", func(w http.ResponseWriter, _ *http.Request) {
			mu.RLock()
			defer mu.RUnlock()
			respond(w, completed, nil)
		})
		mux.HandleFunc("DELETE /v1/namespaces/db/tables/tbl/plan/parity", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
		mux.HandleFunc("POST /v1/namespaces/db/tables/tbl/tasks", func(w http.ResponseWriter, req *http.Request) {
			mu.RLock()
			defer mu.RUnlock()
			var body struct {
				Token string `json:"plan-task"`
			}
			if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
				respond(w, nil, err)

				return
			}
			i, err := strconv.Atoi(body.Token)
			if err != nil || i < 0 || i >= len(envelopes) {
				respond(w, nil, errors.New("invalid plan token"))

				return
			}
			respond(w, envelopes[i], nil)
		})
	})
}

func parityWireTasks(tasks []table.FileScanTask, filter iceberg.BooleanExpression, schema *iceberg.Schema) (map[string]any, error) {
	files := make([]any, 0, len(tasks))
	deletes := make([]any, 0)
	for _, task := range tasks {
		refs := make([]int, 0)
		for _, group := range [][]iceberg.DataFile{task.DeleteFiles, task.EqualityDeleteFiles, task.DeletionVectorFiles} {
			for _, df := range group {
				refs = append(refs, len(deletes))
				deletes = append(deletes, parityWireFile(df))
			}
		}
		residual := task.Residual
		if residual == nil {
			residual = filter
		}
		bound, err := bindParityResidual(schema, residual, true)
		if err != nil {
			return nil, err
		}
		files = append(files, map[string]any{"data-file": parityWireFile(task.File), "delete-file-references": refs, "residual-filter": bound})
	}

	return map[string]any{"file-scan-tasks": files, "delete-files": deletes}, nil
}

func parityWireFile(file iceberg.DataFile) map[string]any {
	content := []string{"data", "position-deletes", "equality-deletes"}[file.ContentType()]
	out := map[string]any{
		"spec-id": file.SpecID(), "partition": []any{file.Partition()[1000]},
		"content": content, "file-path": file.FilePath(), "file-format": file.FileFormat(),
		"file-size-in-bytes": file.FileSizeBytes(), "record-count": file.Count(), "split-offsets": file.SplitOffsets(),
	}
	if content == "equality-deletes" {
		out["equality-ids"] = file.EqualityFieldIDs()
	}
	for key, bounds := range map[string]map[int][]byte{"lower-bounds": file.LowerBoundValues(), "upper-bounds": file.UpperBoundValues()} {
		if len(bounds) == 0 {
			continue
		}
		var keys []int
		for id := range bounds {
			keys = append(keys, id)
		}
		slices.Sort(keys)
		values := make([]string, len(keys))
		for i, id := range keys {
			values[i] = hex.EncodeToString(bounds[id])
		}
		out[key] = map[string]any{"keys": keys, "values": values}
	}

	return out
}

// Compare bound residuals semantically, without passing the expected value
// through the JSON codec under test. A faulty literal encoder must not be able
// to make both sides of the assertion look alike.
func assertPlanningParity(t *testing.T, expected, actual []table.FileScanTask, schema *iceberg.Schema, filter iceberg.BooleanExpression, sensitive bool) {
	t.Helper()
	require.Len(t, actual, len(expected))
	sortTasks := func(tasks []table.FileScanTask) []table.FileScanTask {
		sorted := slices.Clone(tasks)
		slices.SortFunc(sorted, func(a, b table.FileScanTask) int {
			return cmp.Or(cmp.Compare(a.File.FilePath(), b.File.FilePath()), cmp.Compare(a.Start, b.Start), cmp.Compare(a.Length, b.Length))
		})

		return sorted
	}
	expected, actual = sortTasks(expected), sortTasks(actual)
	for i, want := range expected {
		got := actual[i]
		assert.Equal(t, want.File.FilePath(), got.File.FilePath())
		assert.Equal(t, want.File.Partition(), got.File.Partition())
		assert.Equal(t, want.Start, got.Start)
		assert.Equal(t, want.Length, got.Length)
		assert.Equal(t, want.FirstRowID, got.FirstRowID)
		assert.Equal(t, parityDeletes(want.DeleteFiles), parityDeletes(got.DeleteFiles))
		assert.Equal(t, parityDeletes(want.EqualityDeleteFiles), parityDeletes(got.EqualityDeleteFiles))
		assert.Equal(t, parityDeletes(want.DeletionVectorFiles), parityDeletes(got.DeletionVectorFiles))
		effective := func(residual iceberg.BooleanExpression) iceberg.BooleanExpression {
			if residual == nil {
				residual = filter
			}
			bound, err := bindParityResidual(schema, residual, sensitive)
			require.NoError(t, err)

			return bound
		}
		wantResidual, gotResidual := effective(want.Residual), effective(got.Residual)
		assert.Truef(t, wantResidual.Equals(gotResidual), "residual mismatch for %s: want %s, got %s", want.File.FilePath(), wantResidual, gotResidual)
	}
}

// Keep delete categories separate and compare the actual metadata values,
// including the target and byte range of a deletion vector.
type parityDelete struct {
	Path                       string
	Format                     iceberg.FileFormat
	SpecID                     int32
	Partition                  map[int]any
	Count, Size                int64
	EqualityIDs                []int
	ReferencedDataFile         *string
	ContentOffset, ContentSize *int64
}

func parityDeletes(files []iceberg.DataFile) []parityDelete {
	out := make([]parityDelete, 0, len(files))
	for _, file := range files {
		out = append(out, parityDelete{Path: file.FilePath(), Format: file.FileFormat(), SpecID: file.SpecID(), Partition: file.Partition(), Count: file.Count(), Size: file.FileSizeBytes(), EqualityIDs: file.EqualityFieldIDs(), ReferencedDataFile: file.ReferencedDataFile(), ContentOffset: file.ContentOffset(), ContentSize: file.ContentSizeInBytes()})
	}
	slices.SortFunc(out, func(a, b parityDelete) int { return cmp.Compare(a.Path, b.Path) })

	return out
}

func parityPtr[T any](v T) *T { return &v }

// Local residuals are already bound; wire residuals and the original filter
// are unbound. Preserve the former and bind the latter without a JSON round
// trip, so the comparison does not normalize through the codec under test.
func bindParityResidual(schema *iceberg.Schema, expr iceberg.BooleanExpression, sensitive bool) (iceberg.BooleanExpression, error) {
	unbound, err := iceberg.VisitExpr(expr, parityUnboundVisitor{})
	if err != nil {
		return nil, err
	}
	if unbound {
		return iceberg.BindExpr(schema, expr, sensitive)
	}

	return expr, nil
}

type parityUnboundVisitor struct{}

func (parityUnboundVisitor) VisitTrue() bool                            { return false }
func (parityUnboundVisitor) VisitFalse() bool                           { return false }
func (parityUnboundVisitor) VisitNot(v bool) bool                       { return v }
func (parityUnboundVisitor) VisitAnd(a, b bool) bool                    { return a || b }
func (parityUnboundVisitor) VisitOr(a, b bool) bool                     { return a || b }
func (parityUnboundVisitor) VisitBound(iceberg.BoundPredicate) bool     { return false }
func (parityUnboundVisitor) VisitUnbound(iceberg.UnboundPredicate) bool { return true }
