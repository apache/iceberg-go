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
	"bytes"
	"encoding/json"
	"errors"
	"testing"
)

func TestEagerMetadataV2Parity(t *testing.T) {
	fixture := benchmarkMetadataJSON(1, 1, 64, 16, 0)
	for _, test := range []struct {
		name string
		old  string
		new  string
		fast bool
	}{
		{name: "canonical", fast: true},
		{name: "escaped field", old: `"snapshots":`, new: `"snap\u0073hots":`},
		{name: "case folded field", old: `"snapshots":`, new: `"Snapshots":`},
		{name: "duplicate history", old: `"snapshots":`, new: `"snapshots":[], "snapshots":`},
		{name: "malformed overwritten history", old: `"snapshots":`, new: `"snapshots":[{}], "snapshots":`},
		{name: "leading zero", old: `"sequence-number":1,`, new: `"sequence-number":01,`},
		{name: "escaped summary value", old: `"append"`, new: `"a\u0070pend"`, fast: true},
		{name: "null summary", old: `"summary":{"operation":"append"}`, new: `"summary":null`, fast: true},
		{name: "duplicate summary", old: `"summary":{"operation":"append"}`, new: `"summary":{"a":"b"},"summary":{"operation":"append"}`, fast: true},
		{name: "duplicate operation", old: `"summary":{"operation":"append"}`, new: `"summary":{"operation":"","operation":"append"}`, fast: true},
		{name: "empty operation", old: `"summary":{"operation":"append"}`, new: `"summary":{"operation":""}`},
		{name: "empty summary", old: `"summary":{"operation":"append"}`, new: `"summary":{}`, fast: true},
		{name: "integer overflow", old: `"sequence-number":1,`, new: `"sequence-number":9223372036854775808,`},
		{name: "root trailing comma", old: `"snapshots":`, new: `"unknown":{"value":1,},"snapshots":`},
		{name: "summary trailing comma", old: `"summary":{"operation":"append"}`, new: `"summary":{"operation":"append",}`},
		{name: "control character", old: `"append"`, new: "\"ap\x01pend\""},
		{name: "invalid utf8", old: `"append"`, new: "\"ap\xffpend\""},
	} {
		t.Run(test.name, func(t *testing.T) {
			input := bytes.Clone(fixture)
			if test.old != "" {
				if !bytes.Contains(input, []byte(test.old)) {
					t.Fatalf("fixture lacks %q", test.old)
				}
				input = bytes.Replace(input, []byte(test.old), []byte(test.new), 1)
			}
			_, fast := parseEagerMetadataV2(input)
			if fast != test.fast {
				t.Fatalf("fast path = %v, want %v", fast, test.fast)
			}
			want, wantErr := parseMetadataBytes(input)
			got, gotErr := ParseMetadataBytes(input)
			if wantErr != nil || gotErr != nil {
				if wantErr == nil || gotErr == nil || wantErr.Error() != gotErr.Error() {
					t.Fatalf("got error %v, expected %v", gotErr, wantErr)
				}

				return
			}
			clear(input)
			checkEagerMetadataEqual(t, got, want)
		})
	}
}

func checkEagerMetadataEqual(t *testing.T, got, want Metadata) {
	t.Helper()
	if !got.Equals(want) {
		t.Fatal("parsed metadata differs from general decoder")
	}
	gotJSON, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	wantJSON, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotJSON, wantJSON) {
		t.Fatal("serialized metadata differs from general decoder")
	}
}

func FuzzEagerMetadataV2Parity(f *testing.F) {
	f.Add(benchmarkMetadataJSON(1, 1, 64, 16, 0))
	f.Fuzz(func(t *testing.T, input []byte) {
		got, fast := parseEagerMetadataV2(input)
		if !fast {
			return
		}
		want, err := parseMetadataBytes(input)
		if err != nil {
			t.Fatalf("specialized decoder accepted input rejected by general decoder: %v", err)
		}
		checkEagerMetadataEqual(t, got, want)
	})
}

func TestEagerMetadataV2LargeDocumentOwnership(t *testing.T) {
	fixture := benchmarkMetadataJSON(1, 1, 64, 16, 0)
	input := append([]byte(`{"future-annotation":"`), bytes.Repeat([]byte{'x'}, 1<<20)...)
	input = append(input, '"', ',')
	input = append(input, fixture[1:]...)

	want, err := parseMetadataBytes(input)
	if err != nil {
		t.Fatal(err)
	}
	got, fast := parseEagerMetadataV2(input)
	if !fast {
		t.Fatal("large metadata did not take the fast path")
	}
	clear(input)
	checkEagerMetadataEqual(t, got, want)
}

func TestEagerMetadataV2HistoryDepth(t *testing.T) {
	fixture := benchmarkMetadataJSON(1, 1, 64, 16, 0)
	for _, field := range []string{"snapshots"} {
		t.Run(field, func(t *testing.T) {
			entryStart := []byte(`"` + field + `":[{`)
			for _, depth := range []int{9997, 9998} {
				annotated := append(bytes.Clone(entryStart), `"annotation":`...)
				annotated = append(annotated, bytes.Repeat([]byte{'['}, depth)...)
				annotated = append(annotated, '0')
				annotated = append(annotated, bytes.Repeat([]byte{']'}, depth)...)
				annotated = append(annotated, ',')
				input := bytes.Replace(fixture, entryStart, annotated, 1)
				if bytes.Equal(input, fixture) {
					t.Fatal("missing history entry")
				}
				got, err := ParseMetadataBytes(input)
				if depth == 9998 {
					if _, ok := errors.AsType[*json.SyntaxError](err); !ok {
						t.Fatalf("got %v, want a JSON syntax error at excess depth", err)
					}

					continue
				}
				if err != nil {
					t.Fatalf("could not parse metadata at the depth limit: %v", err)
				}
				want, err := parseMetadataBytes(input)
				if err != nil {
					t.Fatal(err)
				}
				checkEagerMetadataEqual(t, got, want)
			}
		})
	}
}

func TestEagerMetadataV2LargeSnapshotFallback(t *testing.T) {
	fixture := benchmarkMetadataJSON(1, 1, 64, 16, 14)
	start := []byte(`"snapshots":[{`)
	annotated := append(bytes.Clone(start), `"annotation":"`...)
	annotated = append(annotated, bytes.Repeat([]byte{'x'}, 1<<20)...)
	annotated = append(annotated, '"', ',')
	input := bytes.Replace(fixture, start, annotated, 1)
	want, err := parseMetadataBytes(input)
	if err != nil {
		t.Fatal(err)
	}
	got, fast := parseEagerMetadataV2(input)
	if !fast {
		t.Fatal("one unusual snapshot prevented fast decoding of the history")
	}
	clear(input)
	checkEagerMetadataEqual(t, got, want)
}

func TestMetadataJSONCursorIntegerBoundaries(t *testing.T) {
	for _, tc := range []struct {
		input string
		want  int64
		ok    bool
	}{
		{input: "0", want: 0, ok: true},
		{input: "-0", want: 0, ok: true},
		{input: "1", want: 1, ok: true},
		{input: " \t -42", want: -42, ok: true},
		{input: "9223372036854775807", want: 9223372036854775807, ok: true},
		{input: "-9223372036854775808", want: -9223372036854775808, ok: true},
		{input: "9223372036854775808"},
		{input: "-9223372036854775809"},
		{input: "18446744073709551616"},
		{input: "01"},
		{input: "-01"},
		{input: ""},
	} {
		cursor := metadataJSONCursor{data: []byte(tc.input)}
		got, ok := cursor.integer()
		if ok != tc.ok || (ok && got != tc.want) {
			t.Errorf("integer(%q) = (%d, %v), want (%d, %v)", tc.input, got, ok, tc.want, tc.ok)
		}
	}
}

func TestPlainMetadataStringChecksEachByteLane(t *testing.T) {
	for length := 0; length <= 64; length++ {
		plain := bytes.Repeat([]byte{'a'}, length)
		if !plainMetadataString(plain) {
			t.Fatalf("plain ASCII of length %d rejected", length)
		}
		for offset := range plain {
			for _, invalid := range []byte{'\\', 0, 0x1f} {
				mutated := bytes.Clone(plain)
				mutated[offset] = invalid
				if plainMetadataString(mutated) {
					t.Fatalf("string scanner accepted %x at offset %d of length %d",
						invalid, offset, length)
				}
			}
		}
	}
}

