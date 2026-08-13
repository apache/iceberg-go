// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package table

import (
	"fmt"
	"testing"
)

// BenchmarkApplyURIEquivalence measures the per-path lookup cost as the
// number of paths and configured equivalence groups grows. Configuration is
// built before the timer starts so only the repeated lookup is measured.
func BenchmarkApplyURIEquivalence(b *testing.B) {
	for _, tc := range []struct {
		paths  int
		groups int
	}{
		{paths: 100, groups: 1},
		{paths: 100, groups: 100},
		{paths: 10_000, groups: 1},
		{paths: 10_000, groups: 100},
	} {
		b.Run(fmt.Sprintf("paths=%d/groups=%d", tc.paths, tc.groups), func(b *testing.B) {
			equivalences := make(map[string]string, tc.groups)
			for i := range tc.groups {
				equivalences[fmt.Sprintf("scheme-%d,scheme-%d-alt", i, i)] = "canonical"
			}
			cfg := newOrphanCleanupConfig(WithEqualSchemes(equivalences))

			schemes := make([]string, tc.paths)
			for i := range schemes {
				schemes[i] = fmt.Sprintf("scheme-%d", i%tc.groups)
			}

			b.ReportAllocs()
			var result string
			b.ResetTimer()
			for b.Loop() {
				for _, scheme := range schemes {
					result = applySchemeEquivalence(scheme, cfg.equalSchemes)
				}
			}
			b.StopTimer()
			if result == "" {
				b.Fatal("equivalence lookup returned an empty scheme")
			}
		})
	}
}
