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
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

type pathRewriteMetricsFile struct {
	iceberg.DataFile
	path   string
	values map[int]int64
	nulls  map[int]int64
	nans   map[int]int64
}

func (f pathRewriteMetricsFile) FilePath() string               { return f.path }
func (f pathRewriteMetricsFile) ValueCounts() map[int]int64     { return f.values }
func (f pathRewriteMetricsFile) NullValueCounts() map[int]int64 { return f.nulls }
func (f pathRewriteMetricsFile) NaNValueCounts() map[int]int64  { return f.nans }

func TestValidatePathRewriteMetricsAllowsMissingReplacementNaNStats(t *testing.T) {
	source := pathRewriteMetricsFile{
		path:   "source.parquet",
		values: map[int]int64{1: 2},
		nulls:  map[int]int64{1: 0},
		nans:   map[int]int64{1: 0},
	}
	replacement := pathRewriteMetricsFile{
		path:   "replacement.parquet",
		values: map[int]int64{1: 2},
		nulls:  map[int]int64{1: 0},
	}
	require.NoError(t, validatePathRewriteMetrics(
		[]iceberg.DataFile{source}, []iceberg.DataFile{replacement}, nil))
}
