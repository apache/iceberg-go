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

package mumbling

import (
	"bufio"
	"encoding/hex"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// TestCrossClientPFORJavaFixtures checks both directions against the fixtures in
// testdata: the Go decoder reads the stored bytes, and the Go encoder reproduces them.
func TestCrossClientPFORJavaFixtures(t *testing.T) {
	path := filepath.Join("testdata", "pfor-java-fixtures.tsv")
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open fixtures: %v", err)
	}
	defer f.Close()

	cases := 0
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 1<<20)
	for lineNo := 1; sc.Scan(); lineNo++ {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "\t", 2)
		if len(parts) != 2 {
			t.Fatalf("line %d: malformed", lineNo)
		}
		want := parseInts(t, lineNo, parts[0])
		javaBytes, err := hex.DecodeString(parts[1])
		if err != nil {
			t.Fatalf("line %d: bad hex: %v", lineNo, err)
		}

		// direction 1: Java-encoded bytes decode correctly in Go
		got, err := pforDecode(javaBytes, len(want))
		if err != nil {
			t.Fatalf("line %d: decode: %v", lineNo, err)
		}
		if !equal(got, want) {
			t.Fatalf("line %d: Go decode of Java bytes = %v, want %v", lineNo, got, want)
		}

		// direction 2: Go re-encodes to the same bytes
		reEnc := pforEncode(want)
		if hex.EncodeToString(reEnc) != parts[1] {
			t.Fatalf("line %d (len %d): Go encode = %s, Java = %s",
				lineNo, len(want), hex.EncodeToString(reEnc), parts[1])
		}
		cases++
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if cases == 0 {
		t.Fatal("no fixtures loaded")
	}
	t.Logf("validated %d PFOR fixtures (both directions)", cases)
}

func parseInts(t *testing.T, lineNo int, csv string) []int {
	t.Helper()
	fields := strings.Split(csv, ",")
	out := make([]int, len(fields))
	for i, s := range fields {
		v, err := strconv.Atoi(s)
		if err != nil {
			t.Fatalf("line %d: bad int %q: %v", lineNo, s, err)
		}
		out[i] = v
	}

	return out
}
