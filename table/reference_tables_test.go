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

package table_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

// referenceTablesSentinel is the absolute root every path in the
// iceberg-verification fixtures is written under. CI symlinks it to the
// checkout, as reference-tables/README.md describes.
const referenceTablesSentinel = "/tmp/iceberg-verification"

// referenceTablesSkip names tables iceberg-go cannot read yet, with the reason.
var referenceTablesSkip = map[string]string{}

type referenceTablesIndex struct {
	Tables []struct {
		Name     string `json:"name"`
		Metadata string `json:"metadata"`
	} `json:"tables"`
}

type referenceTablesExpected struct {
	Snapshots []struct {
		SnapshotID string            `json:"snapshot-id"`
		Rows       []json.RawMessage `json:"rows"`
	} `json:"snapshots"`
}

// TestReferenceTables scans every table of an iceberg-verification checkout
// at every snapshot in its expected/scan.json and compares the rows.
func TestReferenceTables(t *testing.T) {
	dir := os.Getenv("ICEBERG_VERIFICATION_DIR")
	if dir == "" {
		t.Skip("ICEBERG_VERIFICATION_DIR is not set")
	}
	if _, err := os.Stat(filepath.Join(referenceTablesSentinel, "reference-tables", "index.json")); err != nil {
		t.Skipf("%s does not resolve to the checkout; run ln -s %q %s: %v",
			referenceTablesSentinel, dir, referenceTablesSentinel, err)
	}

	root := filepath.Join(dir, "reference-tables")
	var index referenceTablesIndex
	readReferenceJSON(t, filepath.Join(root, "index.json"), &index)
	require.NotEmpty(t, index.Tables)

	for _, entry := range index.Tables {
		t.Run(entry.Name, func(t *testing.T) {
			if reason, ok := referenceTablesSkip[entry.Name]; ok {
				t.Skip(reason)
			}

			metadataPath := filepath.Join(root, filepath.FromSlash(entry.Metadata))
			tbl, err := table.NewFromLocation(t.Context(), table.Identifier{entry.Name},
				metadataPath, io.LoadFSFunc(nil, metadataPath), nil)
			require.NoError(t, err)

			var expected referenceTablesExpected
			readReferenceJSON(t, filepath.Join(root, filepath.FromSlash(entry.Name), "expected", "scan.json"), &expected)
			require.NotEmpty(t, expected.Snapshots)

			for _, snap := range expected.Snapshots {
				snapshotID, err := strconv.ParseInt(snap.SnapshotID, 10, 64)
				require.NoError(t, err)

				scan := tbl.Scan(table.WithSnapshotID(snapshotID))
				schema, err := scan.Projection()
				require.NoError(t, err)
				fields := schema.Fields()

				counts := make(map[string]int)
				for _, raw := range snap.Rows {
					row, err := expectedReferenceRow(fields, raw)
					require.NoError(t, err, "%s snapshot %d: decoding expected row %s", entry.Name, snapshotID, raw)
					counts[row]++
				}

				arrTbl, err := scan.ToArrowTable(t.Context())
				require.NoError(t, err)
				rows, err := arrowReferenceRows(fields, arrTbl)
				arrTbl.Release()
				require.NoError(t, err, "%s snapshot %d", entry.Name, snapshotID)
				for _, row := range rows {
					counts[row]--
				}

				for _, row := range slices.Sorted(maps.Keys(counts)) {
					switch n := counts[row]; {
					case n > 0:
						t.Errorf("%s snapshot %d: missing row %s (x%d)", entry.Name, snapshotID, row, n)
					case n < 0:
						t.Errorf("%s snapshot %d: unexpected row %s (x%d)", entry.Name, snapshotID, row, -n)
					}
				}
			}
		})
	}
}

func readReferenceJSON(t *testing.T, path string, v any) {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(data, v), path)
}

// The canonical form of a value, for both sides: long as a decimal string,
// int and timestamptz (microseconds since epoch) as int64, double as float64,
// string as string, struct as a map keyed by field ID, list as a slice, and map
// as key/value pairs sorted by key. Rows are compared as its JSON encoding.

func expectedReferenceRow(fields []iceberg.NestedField, raw json.RawMessage) (string, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return "", err
	}
	row, err := expectedReferenceValue(&iceberg.StructType{FieldList: fields}, v)
	if err != nil {
		return "", err
	}

	return canonicalReferenceJSON(row)
}

func expectedReferenceValue(typ iceberg.Type, v any) (any, error) {
	if v == nil {
		return nil, nil
	}

	switch typ := typ.(type) {
	case iceberg.Int64Type:
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("long must be a JSON string, got %v", v)
		}
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return nil, err
		}

		return strconv.FormatInt(n, 10), nil
	case iceberg.Int32Type:
		num, ok := v.(json.Number)
		if !ok {
			return nil, fmt.Errorf("int must be a JSON number, got %v", v)
		}

		return strconv.ParseInt(num.String(), 10, 32)
	case iceberg.Float64Type:
		num, ok := v.(json.Number)
		if !ok {
			return nil, fmt.Errorf("double must be a JSON number, got %v", v)
		}

		return strconv.ParseFloat(num.String(), 64)
	case iceberg.StringType:
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("string must be a JSON string, got %v", v)
		}

		return s, nil
	case iceberg.TimestampTzType:
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("timestamptz must be a JSON string, got %v", v)
		}
		ts, err := time.Parse("2006-01-02T15:04:05.000000-07:00", s)
		if err != nil {
			return nil, err
		}

		return ts.UnixMicro(), nil
	case *iceberg.StructType:
		obj, ok := v.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("struct must be a JSON object, got %v", v)
		}
		out := make(map[string]any, len(typ.FieldList))
		for _, f := range typ.FieldList {
			key := strconv.Itoa(f.ID)
			raw, present := obj[key]
			if !present {
				return nil, fmt.Errorf("struct has no field %d", f.ID)
			}
			fv, err := expectedReferenceValue(f.Type, raw)
			if err != nil {
				return nil, fmt.Errorf("field %d: %w", f.ID, err)
			}
			out[key] = fv
		}
		if len(obj) != len(typ.FieldList) {
			return nil, fmt.Errorf("struct %v has keys the schema does not", obj)
		}

		return out, nil
	case *iceberg.ListType:
		elems, ok := v.([]any)
		if !ok {
			return nil, fmt.Errorf("list must be a JSON array, got %v", v)
		}
		out := make([]any, len(elems))
		for i, e := range elems {
			ev, err := expectedReferenceValue(typ.Element, e)
			if err != nil {
				return nil, err
			}
			out[i] = ev
		}

		return out, nil
	case *iceberg.MapType:
		obj, ok := v.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("map must be a JSON object, got %v", v)
		}
		keys, kok := obj["keys"].([]any)
		values, vok := obj["values"].([]any)
		if !kok || !vok || len(keys) != len(values) || len(obj) != 2 {
			return nil, fmt.Errorf("map must hold equal-length keys and values arrays, got %v", v)
		}
		pairs := make([][2]any, len(keys))
		for i := range keys {
			k, err := expectedReferenceValue(typ.KeyType, keys[i])
			if err != nil {
				return nil, err
			}
			val, err := expectedReferenceValue(typ.ValueType, values[i])
			if err != nil {
				return nil, err
			}
			pairs[i] = [2]any{k, val}
		}

		return sortReferencePairs(pairs)
	default:
		return nil, fmt.Errorf("type %s not handled yet", typ)
	}
}

func arrowReferenceRows(fields []iceberg.NestedField, tbl arrow.Table) ([]string, error) {
	if int(tbl.NumCols()) != len(fields) {
		return nil, fmt.Errorf("scan returned %d columns, projection has %d fields", tbl.NumCols(), len(fields))
	}

	rdr := array.NewTableReader(tbl, 0)
	defer rdr.Release()

	var rows []string
	for rdr.Next() {
		rec := rdr.RecordBatch()
		for i := range int(rec.NumRows()) {
			row := make(map[string]any, len(fields))
			for j, f := range fields {
				v, err := arrowReferenceValue(f.Type, rec.Column(j), i)
				if err != nil {
					return nil, fmt.Errorf("field %d: %w", f.ID, err)
				}
				row[strconv.Itoa(f.ID)] = v
			}
			s, err := canonicalReferenceJSON(row)
			if err != nil {
				return nil, err
			}
			rows = append(rows, s)
		}
	}

	return rows, rdr.Err()
}

func arrowReferenceValue(typ iceberg.Type, arr arrow.Array, i int) (any, error) {
	if arr.IsNull(i) {
		return nil, nil
	}

	switch typ := typ.(type) {
	case iceberg.Int64Type:
		a, ok := arr.(*array.Int64)
		if !ok {
			return nil, fmt.Errorf("long read as %s", arr.DataType())
		}

		return strconv.FormatInt(a.Value(i), 10), nil
	case iceberg.Int32Type:
		a, ok := arr.(*array.Int32)
		if !ok {
			return nil, fmt.Errorf("int read as %s", arr.DataType())
		}

		return int64(a.Value(i)), nil
	case iceberg.Float64Type:
		a, ok := arr.(*array.Float64)
		if !ok {
			return nil, fmt.Errorf("double read as %s", arr.DataType())
		}

		return a.Value(i), nil
	case iceberg.StringType:
		a, ok := arr.(interface{ Value(int) string })
		if !ok {
			return nil, fmt.Errorf("string read as %s", arr.DataType())
		}

		return a.Value(i), nil
	case iceberg.TimestampTzType:
		a, ok := arr.(*array.Timestamp)
		if !ok {
			return nil, fmt.Errorf("timestamptz read as %s", arr.DataType())
		}
		dt := a.DataType().(*arrow.TimestampType)
		if dt.TimeZone == "" {
			return nil, fmt.Errorf("timestamptz read as %s, without a time zone", dt)
		}

		return a.Value(i).ToTime(dt.Unit).UnixMicro(), nil
	case *iceberg.StructType:
		a, ok := arr.(*array.Struct)
		if !ok || a.NumField() != len(typ.FieldList) {
			return nil, fmt.Errorf("struct read as %s", arr.DataType())
		}
		out := make(map[string]any, len(typ.FieldList))
		for j, f := range typ.FieldList {
			v, err := arrowReferenceValue(f.Type, a.Field(j), i)
			if err != nil {
				return nil, fmt.Errorf("field %d: %w", f.ID, err)
			}
			out[strconv.Itoa(f.ID)] = v
		}

		return out, nil
	case *iceberg.MapType:
		a, ok := arr.(*array.Map)
		if !ok {
			return nil, fmt.Errorf("map read as %s", arr.DataType())
		}
		start, end := a.ValueOffsets(i)
		pairs := make([][2]any, 0, end-start)
		for j := int(start); j < int(end); j++ {
			k, err := arrowReferenceValue(typ.KeyType, a.Keys(), j)
			if err != nil {
				return nil, err
			}
			v, err := arrowReferenceValue(typ.ValueType, a.Items(), j)
			if err != nil {
				return nil, err
			}
			pairs = append(pairs, [2]any{k, v})
		}

		return sortReferencePairs(pairs)
	case *iceberg.ListType:
		a, ok := arr.(array.ListLike)
		if !ok {
			return nil, fmt.Errorf("list read as %s", arr.DataType())
		}
		start, end := a.ValueOffsets(i)
		out := make([]any, 0, end-start)
		for j := int(start); j < int(end); j++ {
			v, err := arrowReferenceValue(typ.Element, a.ListValues(), j)
			if err != nil {
				return nil, err
			}
			out = append(out, v)
		}

		return out, nil
	default:
		return nil, fmt.Errorf("type %s not handled yet", typ)
	}
}

func sortReferencePairs(pairs [][2]any) ([][2]any, error) {
	keys := make([]string, len(pairs))
	for i, p := range pairs {
		k, err := canonicalReferenceJSON(p[0])
		if err != nil {
			return nil, err
		}
		keys[i] = k
	}
	idx := make([]int, len(pairs))
	for i := range idx {
		idx[i] = i
	}
	slices.SortFunc(idx, func(a, b int) int { return strings.Compare(keys[a], keys[b]) })
	out := make([][2]any, len(pairs))
	for i, j := range idx {
		out[i] = pairs[j]
	}

	return out, nil
}

// canonicalReferenceJSON encodes a canonical value; encoding/json sorts map
// keys, so equal values encode to equal strings.
func canonicalReferenceJSON(v any) (string, error) {
	b, err := json.Marshal(v)

	return string(b), err
}
