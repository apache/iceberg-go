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

// Command collationsize measures the on-disk size of Iceberg manifests that
// carry multi-version collation-aware bounds, under whichever collation bounds
// layout is compiled into this branch (see iceberg.CollationLayoutName).
//
// The program is identical on every layout branch: it builds the same
// deterministic table and data files, sets their collation stats through the
// layout-neutral iceberg.DataFileBuilder.WithCollationBounds API, writes each
// scenario as a real v3 manifest with iceberg.WriteManifest (the production
// writer: real Avro encoding, real OCF container, deflate codec), reads it back
// with iceberg.ReadManifest to prove the bounds were actually persisted, and
// reports the file size. Comparing the CSV output of two branches with
// -compare isolates the cost of the layout itself.
//
//	go run ./cmd/collationsize                     # print the default sweep
//	go run ./cmd/collationsize -csv > layout.csv   # machine-readable
//	go run ./cmd/collationsize -compare a.csv b.csv
package main

import (
	"bufio"
	"bytes"
	"compress/flate"
	"encoding/binary"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"text/tabwriter"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/collation"
)

const (
	tableCols      = 10 // columns carrying ordinary column stats
	byteBoundTrunc = 16 // byte-order string bounds truncated like Iceberg's default truncate(16)
	plainStrLen    = 24 // length of values in non-collated string columns
	samplesPerFile = 8  // values sampled per column per file to derive min/max
	collationName  = "en_US-ci"
)

// versionStrings are the collation versions a file can carry bounds for. Bounds
// for every version are computed with the same collator (one Go process has one
// CLDR version); only the version tag differs, which is exactly what the
// on-disk layouts have to store.
var versionStrings = []string{"153.120.0.0", "153.121.0.0", "153.122.0.0"}

type scenario struct {
	Cols       int  // collated columns
	Versions   int  // collation versions per collated column per file
	BoundLen   int  // byte length of collated values (and thus of collation bounds)
	Entries    int  // data files in the manifest
	CodePoints bool // whether min/max code point stats are supplied
}

func (s scenario) key() string {
	return fmt.Sprintf("cols=%d ver=%d B=%d cp=%t n=%d", s.Cols, s.Versions, s.BoundLen, s.CodePoints, s.Entries)
}

type result struct {
	scenario
	OCFBytes      int // total manifest file size on disk
	HeaderBytes   int // OCF header (magic + metadata incl. Avro schema + table schema + sync)
	BodyDeflate   int // compressed data blocks incl. block framing
	BodyRaw       int // the same blocks' records, uncompressed
	TableMetadata int // one-time per-table metadata the layout needs (not in the manifest)
}

func main() {
	var (
		versionsF = flag.String("versions", "1,2,3", "collation versions per column to sweep")
		boundsF   = flag.String("bound-lens", "16,48,128", "collated value byte lengths to sweep")
		entriesF  = flag.String("entries", "1,1000,10000", "data files per manifest to sweep")
		wideCols  = flag.Int("wide-cols", 5, "collated columns in the wide-table case (0 disables)")
		seed      = flag.Int64("seed", 42, "base random seed")
		outDir    = flag.String("out", "", "if set, keep each manifest OCF in this directory")
		asCSV     = flag.Bool("csv", false, "emit CSV instead of a table")
		compare   = flag.Bool("compare", false, "compare two CSV files given as arguments: A B")
	)
	flag.Parse()

	if *compare {
		if flag.NArg() != 2 {
			fail(errors.New("-compare needs two CSV files: A B"))
		}
		fail(runCompare(flag.Arg(0), flag.Arg(1)))

		return
	}

	versions, err := ints(*versionsF)
	fail(err)
	boundLens, err := ints(*boundsF)
	fail(err)
	entries, err := ints(*entriesF)
	fail(err)

	var scenarios []scenario
	for _, v := range versions {
		for _, b := range boundLens {
			for _, n := range entries {
				scenarios = append(scenarios, scenario{Cols: 1, Versions: v, BoundLen: b, Entries: n, CodePoints: true})
			}
		}
	}
	for _, n := range entries {
		if *wideCols > 0 {
			scenarios = append(scenarios, scenario{Cols: *wideCols, Versions: 2, BoundLen: 48, Entries: n, CodePoints: true})
		}
	}
	// bounds only: no code-point stats supplied, so every layout stores the same information
	for _, n := range entries {
		scenarios = append(scenarios, scenario{Cols: 1, Versions: 2, BoundLen: 48, Entries: n, CodePoints: false})
	}

	results := make([]result, 0, len(scenarios))
	for _, sc := range scenarios {
		r, err := run(sc, *seed, *outDir)
		fail(err)
		results = append(results, r)
	}

	if *asCSV {
		fail(writeCSV(os.Stdout, results))

		return
	}
	printTable(results, *seed)
}

func run(sc scenario, seed int64, outDir string) (result, error) {
	if sc.Versions < 1 || sc.Versions > len(versionStrings) {
		return result{}, fmt.Errorf("versions must be in [1,%d]", len(versionStrings))
	}
	if sc.Cols < 1 || sc.Cols > tableCols {
		return result{}, fmt.Errorf("collated columns must be in [1,%d]", tableCols)
	}

	rng := rand.New(rand.NewSource(seed ^ int64(hash(sc.key()))))
	base := collation.MustParse(collationName)
	schema := tableSchema(sc.Cols, base.WithVersion(versionStrings[0]))
	spec := *iceberg.UnpartitionedSpec

	snapshotID := int64(1)
	entries := make([]iceberg.ManifestEntry, 0, sc.Entries)
	for i := range sc.Entries {
		df, err := dataFile(rng, i, sc, base, spec)
		if err != nil {
			return result{}, err
		}
		entries = append(entries, iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapshotID, nil, nil, df))
	}

	var buf bytes.Buffer
	mf, err := iceberg.WriteManifest("collationsize.avro", &buf, 3, spec, schema, snapshotID, entries)
	if err != nil {
		return result{}, fmt.Errorf("%s: write manifest: %w", sc.key(), err)
	}
	if err := verify(mf, buf.Bytes(), sc, base); err != nil {
		return result{}, fmt.Errorf("%s: %w", sc.key(), err)
	}

	header, deflated, raw, err := ocfSizes(buf.Bytes())
	if err != nil {
		return result{}, fmt.Errorf("%s: parse OCF: %w", sc.key(), err)
	}

	if outDir != "" {
		name := strings.NewReplacer(" ", "_", "=", "").Replace(sc.key()) + ".avro"
		if err := os.WriteFile(filepath.Join(outDir, name), buf.Bytes(), 0o644); err != nil {
			return result{}, err
		}
	}

	var refs []iceberg.CollationVersionRef
	for c := range sc.Cols {
		for v := range sc.Versions {
			refs = append(refs, iceberg.CollationVersionRef{
				FieldID: c + 1, Collation: base.String(), Version: versionStrings[v],
			})
		}
	}

	return result{
		scenario:      sc,
		OCFBytes:      buf.Len(),
		HeaderBytes:   header,
		BodyDeflate:   deflated,
		BodyRaw:       raw,
		TableMetadata: len(iceberg.CollationLayoutTableMetadata(refs)),
	}, nil
}

// tableSchema returns a 10-column table whose first cols columns are collated
// strings; the rest alternate between long and plain string columns.
func tableSchema(cols int, spec *collation.Spec) *iceberg.Schema {
	fields := make([]iceberg.NestedField, 0, tableCols)
	for id := 1; id <= tableCols; id++ {
		f := iceberg.NestedField{ID: id, Name: fmt.Sprintf("c%d", id)}
		switch {
		case id <= cols:
			f.Type = iceberg.StringTypeWithCollation(spec)
		case id%2 == 0:
			f.Type = iceberg.PrimitiveTypes.Int64
		default:
			f.Type = iceberg.PrimitiveTypes.String
		}
		fields = append(fields, f)
	}

	return iceberg.NewSchema(0, fields...)
}

func dataFile(rng *rand.Rand, seq int, sc scenario, base *collation.Spec, spec iceberg.PartitionSpec) (iceberg.DataFile, error) {
	path := fmt.Sprintf("s3://warehouse/db/table/data/part-%05d-%08x.parquet", seq, rng.Uint32())
	b, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentData, path, iceberg.ParquetFile,
		nil, nil, nil, int64(100_000+rng.Intn(900_000)), int64(10_000_000+rng.Intn(90_000_000)))
	if err != nil {
		return nil, err
	}

	sizes := make(map[int]int64, tableCols)
	values := make(map[int]int64, tableCols)
	nulls := make(map[int]int64, tableCols)
	lower := make(map[int][]byte, tableCols)
	upper := make(map[int][]byte, tableCols)
	collated := make(map[int]iceberg.CollationColumnStats, sc.Cols)

	for id := 1; id <= tableCols; id++ {
		sizes[id] = int64(rng.Intn(5_000_000))
		values[id] = int64(rng.Intn(1_000_000))
		nulls[id] = int64(rng.Intn(1000))

		if id%2 == 0 && id > sc.Cols { // long column
			lo, hi := rng.Int63(), rng.Int63()
			if lo > hi {
				lo, hi = hi, lo
			}
			lower[id], upper[id] = le64(lo), le64(hi)

			continue
		}

		n := plainStrLen
		if id <= sc.Cols {
			n = sc.BoundLen
		}
		samples := make([]string, samplesPerFile)
		for i := range samples {
			samples[i] = randString(rng, id, n)
		}
		lo, hi := samples[0], samples[0]
		for _, s := range samples[1:] {
			lo, hi = min(lo, s), max(hi, s)
		}
		lower[id], upper[id] = []byte(truncate(lo)), []byte(truncate(hi))

		if id > sc.Cols {
			continue
		}

		stats := iceberg.CollationColumnStats{}
		for v := range sc.Versions {
			entry, ok := iceberg.ComputeCollatedBounds(base.WithVersion(versionStrings[v]), samples)
			if !ok {
				return nil, errors.New("collated bounds not computed")
			}
			stats.Bounds = append(stats.Bounds, entry)
		}
		if sc.CodePoints {
			if lo, hi, ok := iceberg.ComputeCodePointRange(samples); ok {
				stats.MinCodePoint, stats.MaxCodePoint = &lo, &hi
			}
		}
		collated[id] = stats
	}

	return b.ColumnSizes(sizes).
		ValueCounts(values).
		NullValueCounts(nulls).
		LowerBoundValues(lower).
		UpperBoundValues(upper).
		WithCollationBounds(collated).
		Build(), nil
}

// verify reads the manifest back and checks that every collated column of every
// data file still carries a usable bound for every version written, i.e. the
// active layout really persisted the bounds.
func verify(mf iceberg.ManifestFile, data []byte, sc scenario, base *collation.Spec) error {
	got, err := iceberg.ReadManifest(mf, bytes.NewReader(data), false)
	if err != nil {
		return fmt.Errorf("read manifest: %w", err)
	}
	if len(got) != sc.Entries {
		return fmt.Errorf("read %d entries, wrote %d", len(got), sc.Entries)
	}
	for _, e := range got {
		p, ok := e.DataFile().(iceberg.CollationBoundsProvider)
		if !ok {
			return errors.New("data file does not expose collation bounds")
		}
		stats := p.CollationBounds()
		if len(stats) != sc.Cols {
			return fmt.Errorf("read collation stats for %d columns, wrote %d", len(stats), sc.Cols)
		}
		for id, s := range stats {
			for v := range sc.Versions {
				if _, ok := s.BoundFor(base.WithVersion(versionStrings[v])); !ok {
					return fmt.Errorf("column %d: no usable bound for version %s after round trip", id, versionStrings[v])
				}
			}
		}
	}

	return nil
}

// ocfSizes splits an Avro Object Container File into its header size, the size
// of its (deflate-compressed) data blocks including block framing, and the size
// of the same blocks' records after inflating them.
func ocfSizes(data []byte) (header, deflated, raw int, err error) {
	r := bytes.NewReader(data)
	magic := make([]byte, 4)
	if _, err := io.ReadFull(r, magic); err != nil || string(magic) != "Obj\x01" {
		return 0, 0, 0, errors.New("not an Avro OCF")
	}
	for { // header metadata: map<bytes>
		count, err := readLong(r)
		if err != nil {
			return 0, 0, 0, err
		}
		if count == 0 {
			break
		}
		if count < 0 {
			count = -count
			if _, err := readLong(r); err != nil {
				return 0, 0, 0, err
			}
		}
		for range count {
			for range 2 { // key, value
				n, err := readLong(r)
				if err != nil {
					return 0, 0, 0, err
				}
				if _, err := r.Seek(n, io.SeekCurrent); err != nil {
					return 0, 0, 0, err
				}
			}
		}
	}
	if _, err := r.Seek(16, io.SeekCurrent); err != nil { // sync marker
		return 0, 0, 0, err
	}
	header = len(data) - r.Len()

	for r.Len() > 0 {
		if _, err := readLong(r); err != nil { // object count
			return 0, 0, 0, err
		}
		size, err := readLong(r)
		if err != nil {
			return 0, 0, 0, err
		}
		block := make([]byte, size)
		if _, err := io.ReadFull(r, block); err != nil {
			return 0, 0, 0, err
		}
		n, err := io.Copy(io.Discard, flate.NewReader(bytes.NewReader(block)))
		if err != nil {
			return 0, 0, 0, fmt.Errorf("inflate block: %w", err)
		}
		raw += int(n)
		if _, err := r.Seek(16, io.SeekCurrent); err != nil {
			return 0, 0, 0, err
		}
	}

	return header, len(data) - header, raw, nil
}

func readLong(r io.ByteReader) (int64, error) {
	u, err := binary.ReadUvarint(r)
	if err != nil {
		return 0, err
	}

	return int64(u>>1) ^ -int64(u&1), nil
}

// randString returns a valid UTF-8 string of exactly n bytes: a column-specific
// shared prefix (a third of the length, as real string columns share prefixes)
// followed by mixed-case alphanumerics with occasional non-ASCII letters.
func randString(rng *rand.Rand, col, n int) string {
	const ascii = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
	accented := []rune("äöüéèßñçø")

	var sb strings.Builder
	sb.Grow(n)
	for i := 0; sb.Len() < n/3; i++ {
		sb.WriteByte(byte('a' + (col+i)%26))
	}
	for sb.Len() < n {
		if rng.Intn(10) == 0 && n-sb.Len() >= 2 {
			sb.WriteRune(accented[rng.Intn(len(accented))])

			continue
		}
		sb.WriteByte(ascii[rng.Intn(len(ascii))])
	}

	return sb.String()
}

// truncate shortens s to at most byteBoundTrunc bytes on a rune boundary.
func truncate(s string) string {
	if len(s) <= byteBoundTrunc {
		return s
	}
	cut := 0
	for i := range s {
		if i > byteBoundTrunc {
			break
		}
		cut = i
	}

	return s[:cut]
}

func le64(v int64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(v))

	return b
}

func hash(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))

	return h.Sum64()
}

func ints(s string) ([]int, error) {
	var out []int
	for _, f := range strings.Split(s, ",") {
		v, err := strconv.Atoi(strings.TrimSpace(f))
		if err != nil {
			return nil, fmt.Errorf("bad integer list %q: %w", s, err)
		}
		out = append(out, v)
	}

	return out, nil
}

func fail(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "collationsize:", err)
		os.Exit(1)
	}
}

var csvHeader = []string{
	"layout", "cols", "versions", "bound_len", "code_points", "entries",
	"ocf_bytes", "header_bytes", "body_deflate", "body_raw", "table_metadata_bytes",
}

func writeCSV(w io.Writer, results []result) error {
	cw := csv.NewWriter(w)
	if err := cw.Write(csvHeader); err != nil {
		return err
	}
	for _, r := range results {
		row := []string{
			iceberg.CollationLayoutName(),
			strconv.Itoa(r.Cols), strconv.Itoa(r.Versions), strconv.Itoa(r.BoundLen),
			strconv.FormatBool(r.CodePoints), strconv.Itoa(r.Entries),
			strconv.Itoa(r.OCFBytes), strconv.Itoa(r.HeaderBytes), strconv.Itoa(r.BodyDeflate),
			strconv.Itoa(r.BodyRaw), strconv.Itoa(r.TableMetadata),
		}
		if err := cw.Write(row); err != nil {
			return err
		}
	}
	cw.Flush()

	return cw.Error()
}

func printTable(results []result, seed int64) {
	fmt.Printf("layout: %s   seed: %d   %s\n", iceberg.CollationLayoutName(), seed, runtime.Version())
	fmt.Println("per-entry sizes exclude the OCF header (schema), which is paid once per manifest")
	fmt.Println()

	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', tabwriter.AlignRight)
	fmt.Fprintln(tw, "cols\tver\tB\tcp\tentries\tocf bytes\theader\tdeflate/entry\traw/entry\ttable md\t")
	for _, r := range results {
		fmt.Fprintf(tw, "%d\t%d\t%d\t%t\t%d\t%d\t%d\t%.1f\t%.1f\t%d\t\n",
			r.Cols, r.Versions, r.BoundLen, r.CodePoints, r.Entries, r.OCFBytes, r.HeaderBytes,
			float64(r.BodyDeflate)/float64(r.Entries), float64(r.BodyRaw)/float64(r.Entries), r.TableMetadata)
	}
	_ = tw.Flush()
}

type csvRow struct {
	layout string
	key    string
	vals   map[string]float64
}

func readCSV(path string) ([]csvRow, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	recs, err := csv.NewReader(bufio.NewReader(f)).ReadAll()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if len(recs) < 1 || strings.Join(recs[0], ",") != strings.Join(csvHeader, ",") {
		return nil, fmt.Errorf("%s: not a collationsize CSV", path)
	}

	rows := make([]csvRow, 0, len(recs)-1)
	for _, rec := range recs[1:] {
		row := csvRow{layout: rec[0], key: strings.Join(rec[1:6], "/"), vals: map[string]float64{}}
		for i, name := range csvHeader[6:] {
			v, err := strconv.ParseFloat(rec[6+i], 64)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", path, err)
			}
			row.vals[name] = v
		}
		row.vals["entries"], _ = strconv.ParseFloat(rec[5], 64)
		rows = append(rows, row)
	}

	return rows, nil
}

// runCompare prints, per scenario, the A-B difference of total manifest size and
// of the per-entry (header-excluded) deflate and raw sizes.
func runCompare(pathA, pathB string) error {
	a, err := readCSV(pathA)
	if err != nil {
		return err
	}
	b, err := readCSV(pathB)
	if err != nil {
		return err
	}
	byKey := make(map[string]csvRow, len(b))
	for _, r := range b {
		byKey[r.key] = r
	}
	if len(a) == 0 || len(b) == 0 {
		return errors.New("empty CSV")
	}

	fmt.Printf("A = %s (%s)\nB = %s (%s)\n\n", a[0].layout, pathA, b[0].layout, pathB)
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', tabwriter.AlignRight)
	fmt.Fprintln(tw, "cols/ver/B/cp/entries\tA ocf\tB ocf\tA-B ocf\tA defl/entry\tB defl/entry\tA-B defl\tA-B raw\tB table md\t")
	for _, ra := range a {
		rb, ok := byKey[ra.key]
		if !ok {
			continue
		}
		n := ra.vals["entries"]
		ad, bd := ra.vals["body_deflate"]/n, rb.vals["body_deflate"]/n
		ar, br := ra.vals["body_raw"]/n, rb.vals["body_raw"]/n
		fmt.Fprintf(tw, "%s\t%.0f\t%.0f\t%+.1f%%\t%.1f\t%.1f\t%+.1f%%\t%+.1f%%\t%.0f\t\n",
			ra.key, ra.vals["ocf_bytes"], rb.vals["ocf_bytes"],
			pct(ra.vals["ocf_bytes"], rb.vals["ocf_bytes"]),
			ad, bd, pct(ad, bd), pct(ar, br), rb.vals["table_metadata_bytes"])
	}

	return tw.Flush()
}

func pct(a, b float64) float64 {
	if b == 0 {
		return 0
	}

	return (a - b) / b * 100
}
