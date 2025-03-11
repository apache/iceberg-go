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

package internal_test

import (
	"math"
	"testing"

	"github.com/apache/iceberg-go/internal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBinPacking(t *testing.T) {
	tests := []struct {
		splits          []int
		tgtWeight       int64
		lookback        int
		largestBinFirst bool
		expected        [][]int
	}{
		{
			[]int{36, 36, 36, 36, 73, 110, 128},
			128, 2, true,
			[][]int{{110}, {128}, {36, 73}, {36, 36, 36}},
		},
		{
			[]int{36, 36, 36, 36, 73, 110, 128},
			128, 2, false,
			[][]int{{36, 36, 36}, {36, 73}, {110}, {128}},
		},
		{
			[]int{64, 64, 128, 32, 32, 32, 32},
			128, 1, true,
			[][]int{{64, 64}, {128}, {32, 32, 32, 32}},
		},
		{
			[]int{64, 64, 128, 32, 32, 32, 32},
			128, 1, false,
			[][]int{{64, 64}, {128}, {32, 32, 32, 32}},
		},
	}

	weightFn := func(i int) int64 {
		return int64(i)
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			p := &internal.SlicePacker[int]{
				TargetWeight:    tt.tgtWeight,
				Lookback:        tt.lookback,
				LargestBinFirst: tt.largestBinFirst,
			}

			bins := p.Pack(tt.splits, weightFn)
			assert.Equal(t, tt.expected, bins)
		})
	}
}

func TestReverseBinPackingLookback(t *testing.T) {
	intmax := math.MaxInt32

	tests := []struct {
		splits          []int
		tgtWeight       int64
		lookback        int
		largestBinFirst bool
		expected        [][]int
	}{
		// single lookback tests
		{[]int{1, 2, 3, 4, 5}, 3, 1, false, [][]int{{1, 2}, {3}, {4}, {5}}},
		{[]int{1, 2, 3, 4, 5}, 4, 1, false, [][]int{{1, 2}, {3}, {4}, {5}}},
		{[]int{1, 2, 3, 4, 5}, 5, 1, false, [][]int{{1}, {2, 3}, {4}, {5}}},
		{[]int{1, 2, 3, 4, 5}, 6, 1, false, [][]int{{1, 2, 3}, {4}, {5}}},
		{[]int{1, 2, 3, 4, 5}, 7, 1, false, [][]int{{1, 2}, {3, 4}, {5}}},
		{[]int{1, 2, 3, 4, 5}, 8, 1, false, [][]int{{1, 2}, {3, 4}, {5}}},
		{[]int{1, 2, 3, 4, 5}, 9, 1, false, [][]int{{1, 2, 3}, {4, 5}}},
		{[]int{1, 2, 3, 4, 5}, 11, 1, false, [][]int{{1, 2, 3}, {4, 5}}},
		{[]int{1, 2, 3, 4, 5}, 12, 1, false, [][]int{{1, 2}, {3, 4, 5}}},
		{[]int{1, 2, 3, 4, 5}, 14, 1, false, [][]int{{1}, {2, 3, 4, 5}}},
		{[]int{1, 2, 3, 4, 5}, 15, 1, false, [][]int{{1, 2, 3, 4, 5}}},
		// unlimited lookback tests
		{[]int{1, 2, 3, 4, 5}, 3, intmax, false, [][]int{{1, 2}, {3}, {4}, {5}}},
		{[]int{1, 2, 3, 4, 5}, 4, intmax, false, [][]int{{2}, {1, 3}, {4}, {5}}},
		{[]int{1, 2, 3, 4, 5}, 5, intmax, false, [][]int{{2, 3}, {1, 4}, {5}}},
		{[]int{1, 2, 3, 4, 5}, 6, intmax, false, [][]int{{3}, {2, 4}, {1, 5}}},
		{[]int{1, 2, 3, 4, 5}, 7, intmax, false, [][]int{{1}, {3, 4}, {2, 5}}},
		{[]int{1, 2, 3, 4, 5}, 8, intmax, false, [][]int{{1, 2, 4}, {3, 5}}},
		{[]int{1, 2, 3, 4, 5}, 9, intmax, false, [][]int{{1, 2, 3}, {4, 5}}},
		{[]int{1, 2, 3, 4, 5}, 10, intmax, false, [][]int{{2, 3}, {1, 4, 5}}},
		{[]int{1, 2, 3, 4, 5}, 11, intmax, false, [][]int{{1, 3}, {2, 4, 5}}},
		{[]int{1, 2, 3, 4, 5}, 12, intmax, false, [][]int{{1, 2}, {3, 4, 5}}},
		{[]int{1, 2, 3, 4, 5}, 13, intmax, false, [][]int{{2}, {1, 3, 4, 5}}},
		{[]int{1, 2, 3, 4, 5}, 14, intmax, false, [][]int{{1}, {2, 3, 4, 5}}},
		{[]int{1, 2, 3, 4, 5}, 15, intmax, false, [][]int{{1, 2, 3, 4, 5}}},
	}

	weightFn := func(i int) int64 {
		return int64(i)
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			p := &internal.SlicePacker[int]{
				TargetWeight:    tt.tgtWeight,
				Lookback:        tt.lookback,
				LargestBinFirst: tt.largestBinFirst,
			}

			bins := p.PackEnd(tt.splits, weightFn)
			assert.Equal(t, tt.expected, bins)
		})
	}
}

func TestDifference(t *testing.T) {
	tests := []struct {
		name     string
		a        []string
		b        []string
		expected []string
	}{
		{
			name:     "No elements in common",
			a:        []string{"a", "b", "c"},
			b:        []string{"d", "e", "f"},
			expected: []string{"a", "b", "c"},
		},
		{
			name:     "Some elements in common",
			a:        []string{"a", "b", "c"},
			b:        []string{"b"},
			expected: []string{"a", "c"},
		},
		{
			name:     "All elements in common",
			a:        []string{"a", "b", "c"},
			b:        []string{"a", "b", "c"},
			expected: []string{},
		},
		{
			name:     "Empty slice a",
			a:        []string{},
			b:        []string{"a", "b", "c"},
			expected: []string{},
		},
		{
			name:     "Empty slice b",
			a:        []string{"a", "b", "c"},
			b:        []string{},
			expected: []string{"a", "b", "c"},
		},
		{
			name:     "No elements in slice b present in slice a",
			a:        []string{"a", "b", "c"},
			b:        []string{"x", "y", "z"},
			expected: []string{"a", "b", "c"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert := require.New(t)
			result := internal.Difference(tt.a, tt.b)
			assert.ElementsMatch(tt.expected, result)
		})
	}
}
