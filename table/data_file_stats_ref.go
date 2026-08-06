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
	iceberg "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/internal"
)

type dataFileStatsRefer interface {
	DataFileStatsRef(internal.DataFileRef) (
		valueCounts map[int]int64,
		nullCounts map[int]int64,
		nanCounts map[int]int64,
		lowerBounds map[int][]byte,
		upperBounds map[int][]byte,
	)
}

type dataFilePartitionRefer interface {
	DataFilePartitionRef(internal.DataFileRef) map[int]any
}

// dataFileStats returns borrowed metadata for the concrete manifest data file
// and falls back to the public defensive-copying getters for other DataFile
// implementations. Borrowed maps are used only while evaluating one file and
// must never escape or be mutated.
func dataFileStats(file iceberg.DataFile) (
	valueCounts map[int]int64,
	nullCounts map[int]int64,
	nanCounts map[int]int64,
	lowerBounds map[int][]byte,
	upperBounds map[int][]byte,
) {
	if ref, ok := file.(dataFileStatsRefer); ok {
		return ref.DataFileStatsRef(internal.DataFileRef{})
	}

	return file.ValueCounts(), file.NullValueCounts(), file.NaNValueCounts(),
		file.LowerBoundValues(), file.UpperBoundValues()
}

// dataFilePartition returns a borrowed partition map for the concrete
// manifest data file and falls back to the public getter for other DataFile
// implementations. Callers must use the map only for the current planning
// operation; exported results must clone mutable values before returning them.
func dataFilePartition(file iceberg.DataFile) map[int]any {
	if ref, ok := file.(dataFilePartitionRefer); ok {
		return ref.DataFilePartitionRef(internal.DataFileRef{})
	}

	return file.Partition()
}
