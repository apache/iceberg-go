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
	"math"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

// IEEE equality treats both signed zero encodings as matching. Parquet Bloom
// filters hash the physical bits, so checking only canonical +0 can drop rows
// stored as -0 when the Bloom filter is otherwise conclusive.
func TestBloomPhysicalCandidatesIncludeBothSignedZeros(t *testing.T) {
	for _, tt := range []struct {
		name string
		lit iceberg.Literal
		positive, negative []byte
	}{
		{"float32 positive zero", iceberg.Float32Literal(0),
			[]byte{0, 0, 0, 0}, []byte{0, 0, 0, 0x80}},
		{"float32 negative zero", iceberg.Float32Literal(math.Float32frombits(1 << 31)),
			[]byte{0, 0, 0, 0}, []byte{0, 0, 0, 0x80}},
		{"float64 positive zero", iceberg.Float64Literal(0),
			[]byte{0, 0, 0, 0, 0, 0, 0, 0}, []byte{0, 0, 0, 0, 0, 0, 0, 0x80}},
		{"float64 negative zero", iceberg.Float64Literal(math.Float64frombits(uint64(1) << 63)),
			[]byte{0, 0, 0, 0, 0, 0, 0, 0}, []byte{0, 0, 0, 0, 0, 0, 0, 0x80}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			candidates, ok := literalToPruningPhysBytes(tt.lit)
			require.True(t, ok)
			require.Equal(t, [][]byte{tt.positive, tt.negative}, candidates)
		})
	}
}

func TestBloomPhysicalCandidatesSkipNaNAndPreserveOrdinaryValues(t *testing.T) {
	for _, lit := range []iceberg.Literal{
		iceberg.Float32Literal(float32(math.NaN())),
		iceberg.Float64Literal(math.NaN()),
	} {
		candidates, ok := literalToPruningPhysBytes(lit)
		require.False(t, ok)
		require.Nil(t, candidates)
	}

	candidates, ok := literalToPruningPhysBytes(iceberg.Float32Literal(1.25))
	require.True(t, ok)
	require.Len(t, candidates, 1)
	require.Equal(t, []byte{0, 0, 0xa0, 0x3f}, candidates[0])
}
