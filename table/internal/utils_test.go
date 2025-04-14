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
	"slices"
	"testing"
	"time"

	"github.com/apache/iceberg-go/table/internal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetricsModePairs(t *testing.T) {
	tests := []struct {
		str      string
		expected internal.MetricsMode
	}{
		{"none", internal.MetricsMode{Typ: internal.MetricModeNone}},
		{"nOnE", internal.MetricsMode{Typ: internal.MetricModeNone}},
		{"counts", internal.MetricsMode{Typ: internal.MetricModeCounts}},
		{"Counts", internal.MetricsMode{Typ: internal.MetricModeCounts}},
		{"full", internal.MetricsMode{Typ: internal.MetricModeFull}},
		{"FuLl", internal.MetricsMode{Typ: internal.MetricModeFull}},
		{" FuLl", internal.MetricsMode{Typ: internal.MetricModeFull}},
		{"truncate(16)", internal.MetricsMode{Typ: internal.MetricModeTruncate, Len: 16}},
		{"trUncatE(16)", internal.MetricsMode{Typ: internal.MetricModeTruncate, Len: 16}},
		{"trUncatE(7)", internal.MetricsMode{Typ: internal.MetricModeTruncate, Len: 7}},
		{"trUncatE(07)", internal.MetricsMode{Typ: internal.MetricModeTruncate, Len: 7}},
	}

	for _, tt := range tests {
		t.Run(tt.str, func(t *testing.T) {
			mode, err := internal.MatchMetricsMode(tt.str)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, mode)
		})
	}

	_, err := internal.MatchMetricsMode("truncatE(-7)")
	assert.ErrorContains(t, err, "malformed truncate metrics mode: truncatE(-7)")

	_, err = internal.MatchMetricsMode("truncatE(0)")
	assert.ErrorContains(t, err, "invalid truncate length: 0")
}

func TestTruncateUpperBoundString(t *testing.T) {
	assert.Equal(t, "ab", internal.TruncateUpperBoundText("aaaa", 2))
	// \u10FFFF\u10FFFF\x00
	assert.Equal(t, "", internal.TruncateUpperBoundText("\xf4\x8f\xbf\xbf\xf4\x8f\xbf\xbf\x00", 2))
}

func TestTruncateUpperBoundBinary(t *testing.T) {
	assert.Equal(t, []byte{0x01, 0x03}, internal.TruncateUpperBoundBinary([]byte{0x01, 0x02, 0x03}, 2))
	assert.Nil(t, internal.TruncateUpperBoundBinary([]byte{0xff, 0xff, 0x00}, 2))
}

func TestMapExecFinish(t *testing.T) {
	var (
		ch = make(chan struct{}, 1)
		f  = func(i int) (int, error) {
			return i * 2, nil
		}
	)

	go func() {
		defer close(ch)
		for _, err := range internal.MapExec(3, slices.Values([]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}), f) {
			assert.NoError(t, err)
		}
	}()

	assert.Eventually(
		t,
		func() bool {
			<-ch

			return true
		},
		time.Second, 10*time.Millisecond,
	)
}
