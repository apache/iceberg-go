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
	"sync"
	"testing"
)

func combinationTestFile(id int, key string) *equalityDeleteFileSet {
	return newEqualityDeleteFileSet(id, &equalityDeleteSet{
		keys:     set[string]{key: {}},
		fieldIDs: []int{1},
		colNames: []string{"id"},
	})
}

func TestEqualityDeletePairCombinationCache(t *testing.T) {
	first := combinationTestFile(1, "a")
	second := combinationTestFile(2, "b")
	third := combinationTestFile(3, "c")

	t.Run("eager", func(t *testing.T) {
		multi := make(map[string]*equalityDeleteSet)
		pairs := make(map[equalityDeleteFilePair]*equalityDeleteSet)
		get := func(files ...*equalityDeleteFileSet) *equalityDeleteSet {
			return equalityDeleteSetForFiles(files, multi, pairs)
		}

		ab := get(first, second)
		if get(second, first) != ab || get(first, second, first) != ab {
			t.Fatal("equivalent pairs must share their merged delete set")
		}
		if len(ab.keys) != 2 {
			t.Fatalf("pair contains %d keys, want 2", len(ab.keys))
		}
		if get(first, third) == ab || get(first, second, third) == ab {
			t.Fatal("different combinations must not share delete sets")
		}
		if get(third, second, first) != get(first, second, third) {
			t.Fatal("equivalent three-file groups must share their merged delete set")
		}
		if get(first, first) != first.equalityDeleteSet {
			t.Fatal("a duplicate-only combination must reuse the original delete set")
		}
		if len(pairs) != 2 || len(multi) != 1 {
			t.Fatalf("unexpected cache entries: pairs=%d, multi=%d", len(pairs), len(multi))
		}

		files := []*equalityDeleteFileSet{first, second}
		if allocs := testing.AllocsPerRun(100, func() { _ = equalityDeleteSetForFiles(files, multi, pairs) }); allocs != 0 {
			t.Fatalf("cached eager pair lookup allocated %g times, want zero", allocs)
		}
	})

	t.Run("lazy", func(t *testing.T) {
		loader := new(lazyEqualityDeleteLoader)
		ab := loader.combine([]*equalityDeleteFileSet{first, second})
		if loader.combine([]*equalityDeleteFileSet{second, first}) != ab ||
			loader.combine([]*equalityDeleteFileSet{first, first, second}) != ab {
			t.Fatal("equivalent pairs must reuse the same lazy delete set")
		}
		if loader.combine([]*equalityDeleteFileSet{first, third}) == ab ||
			loader.combine([]*equalityDeleteFileSet{first, second, third}) == ab {
			t.Fatal("different combinations must not share lazy delete sets")
		}
		if loader.combine([]*equalityDeleteFileSet{third, second, first}) !=
			loader.combine([]*equalityDeleteFileSet{first, second, third}) {
			t.Fatal("equivalent three-file groups must share their lazy delete set")
		}
	})
}

func TestLazyEqualityDeletePairCombinationConcurrent(t *testing.T) {
	first := combinationTestFile(1, "a")
	second := combinationTestFile(2, "b")
	loader := new(lazyEqualityDeleteLoader)
	const count = 32
	var wg sync.WaitGroup
	results := make([]*equalityDeleteSet, count)
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			files := []*equalityDeleteFileSet{first, second}
			if i%2 == 0 {
				files[0], files[1] = files[1], files[0]
			}
			results[i] = loader.combine(files)
		}(i)
	}
	wg.Wait()
	for i := 1; i < count; i++ {
		if results[i] != results[0] {
			t.Fatalf("worker %d received a different merged delete set", i)
		}
	}
}

var benchmarkEqualityDeletePairCacheResult *equalityDeleteSet

func BenchmarkEqualityDeleteCombinationCache(b *testing.B) {
	first := combinationTestFile(1, "a")
	second := combinationTestFile(2, "b")
	third := combinationTestFile(3, "c")

	for _, tc := range []struct {
		name  string
		files []*equalityDeleteFileSet
	}{
		{name: "one", files: []*equalityDeleteFileSet{first}},
		{name: "two", files: []*equalityDeleteFileSet{first, second}},
		{name: "three", files: []*equalityDeleteFileSet{first, second, third}},
	} {
		b.Run(tc.name+"/eager", func(b *testing.B) {
			multi := make(map[string]*equalityDeleteSet)
			pairs := make(map[equalityDeleteFilePair]*equalityDeleteSet)
			benchmarkEqualityDeletePairCacheResult = equalityDeleteSetForFiles(tc.files, multi, pairs)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				benchmarkEqualityDeletePairCacheResult = equalityDeleteSetForFiles(tc.files, multi, pairs)
			}
		})
		b.Run(tc.name+"/lazy", func(b *testing.B) {
			loader := new(lazyEqualityDeleteLoader)
			benchmarkEqualityDeletePairCacheResult = loader.combine(tc.files)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				benchmarkEqualityDeletePairCacheResult = loader.combine(tc.files)
			}
		})
	}
}
