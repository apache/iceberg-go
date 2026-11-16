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
	"iter"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

func TestUniqueSnapshotManifestsSingleUse(t *testing.T) {
	manifest := func(path string) iceberg.ManifestFile {
		return iceberg.NewManifestBuilder(2, path, 100, 0, 1).
			SequenceNumber(1, 1).AddedFiles(1).Build()
	}
	shared := manifest("shared.avro")
	removedA := manifest("removed-a.avro")
	removedB := manifest("removed-b.avro")
	added := manifest("added.avro")

	// An iter.Seq is not guaranteed to support a second iteration.
	oneShot := func(manifests ...iceberg.ManifestFile) (iter.Seq[iceberg.ManifestFile], *int) {
		runs := new(int)

		return func(yield func(iceberg.ManifestFile) bool) {
			*runs = *runs + 1
			if *runs != 1 {
				return
			}
			for _, m := range manifests {
				if !yield(m) {
					return
				}
			}
		}, runs
	}

	from, fromRuns := oneShot(shared, removedA, removedB)
	to, toRuns := oneShot(shared, added)
	fromOnly, toOnly := uniqueSnapshotManifests(from, to)

	require.Equal(t, 1, *fromRuns, "do not consume the source stream twice")
	require.Equal(t, 1, *toRuns)
	require.Len(t, fromOnly, 2)
	require.Equal(t, removedA.FilePath(), fromOnly[0].FilePath())
	require.Equal(t, removedB.FilePath(), fromOnly[1].FilePath())
	require.Len(t, toOnly, 1)
	require.Equal(t, added.FilePath(), toOnly[0].FilePath())
}
