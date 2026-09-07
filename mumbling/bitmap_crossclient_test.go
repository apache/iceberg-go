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
	"strings"
	"testing"
)

// TestCrossClientBitmapJavaFixtures validates the full-bitmap read path against
// the fixtures in testdata, asserting Cardinality and IsSet for set and unset positions.
func TestCrossClientBitmapJavaFixtures(t *testing.T) {
	path := filepath.Join("testdata", "bitmap-java-fixtures.tsv")
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
		buf, err := hex.DecodeString(parts[0])
		if err != nil {
			t.Fatalf("line %d: bad hex: %v", lineNo, err)
		}
		positions := parseInts(t, lineNo, parts[1])

		bm, err := New(buf)
		if err != nil {
			t.Fatalf("line %d: New: %v", lineNo, err)
		}
		if bm.Cardinality() != len(positions) {
			t.Fatalf("line %d: cardinality = %d, want %d", lineNo, bm.Cardinality(), len(positions))
		}

		set := make(map[int]bool, len(positions))
		maxPos := 0
		for _, p := range positions {
			set[p] = true
			if p > maxPos {
				maxPos = p
			}
			got, err := bm.IsSet(p)
			if err != nil {
				t.Fatalf("line %d: IsSet(%d): %v", lineNo, p, err)
			}
			if !got {
				t.Fatalf("line %d: IsSet(%d) = false, want true", lineNo, p)
			}
		}

		// spread of unlisted positions within range must be false
		checkedUnset := 0
		for p := 0; p <= maxPos && checkedUnset < 20; p++ {
			if set[p] {
				continue
			}
			got, err := bm.IsSet(p)
			if err != nil {
				t.Fatalf("line %d: IsSet(%d): %v", lineNo, p, err)
			}
			if got {
				t.Fatalf("line %d: IsSet(%d) = true, want false", lineNo, p)
			}
			checkedUnset++
		}
		cases++
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if cases == 0 {
		t.Fatal("no bitmap fixtures loaded")
	}
	t.Logf("validated %d bitmap fixtures", cases)
}
