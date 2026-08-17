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

package iceberg

import (
	"math"
	"math/rand"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/murmur3"
)

func TestBucketTransformDecimalHashMatchesMarshalBinary(t *testing.T) {
	values := []struct {
		name  string
		value Decimal
	}{
		{name: "zero", value: Decimal{Val: decimal128.FromI64(0)}},
		{name: "one", value: Decimal{Val: decimal128.FromI64(1)}},
		{name: "negative one", value: Decimal{Val: decimal128.FromI64(-1)}},
		{name: "127", value: Decimal{Val: decimal128.FromI64(127)}},
		{name: "128", value: Decimal{Val: decimal128.FromI64(128)}},
		{name: "129", value: Decimal{Val: decimal128.FromI64(129)}},
		{name: "negative 127", value: Decimal{Val: decimal128.FromI64(-127)}},
		{name: "negative 128", value: Decimal{Val: decimal128.FromI64(-128)}},
		{name: "negative 129", value: Decimal{Val: decimal128.FromI64(-129)}},
		{name: "int64 max", value: Decimal{Val: decimal128.FromI64(math.MaxInt64)}},
		{name: "int64 min", value: Decimal{Val: decimal128.FromI64(math.MinInt64)}},
		{name: "uint64 max", value: Decimal{Val: decimal128.FromU64(math.MaxUint64)}},
		{name: "int128 max", value: Decimal{Val: decimal128.New(math.MaxInt64, math.MaxUint64)}},
		{name: "int128 min", value: Decimal{Val: decimal128.New(math.MinInt64, 0)}},
		{name: "int128 min plus one", value: Decimal{Val: decimal128.New(math.MinInt64, 1)}},
	}

	for _, tt := range values {
		t.Run(tt.name, func(t *testing.T) {
			literal := DecimalLiteral(tt.value)
			encoded, err := literal.MarshalBinary()
			require.NoError(t, err)
			assert.Equal(t, murmur3.Sum32(encoded), hashDecimal(tt.value))
		})
	}

	// Generate random signed 128-bit values, including ordinary negative
	// values. The boundary cases above cover the sign-width changes.
	rng := rand.New(rand.NewSource(0))
	for i := range 10_000 {
		value := Decimal{Val: decimal128.New(int64(rng.Uint64()), rng.Uint64())}
		encoded, err := DecimalLiteral(value).MarshalBinary()
		require.NoError(t, err)
		assert.Equal(t, murmur3.Sum32(encoded), hashDecimal(value), "random value %d", i)
	}
}

func TestBucketTransformDecimalHashMatchesSpecExample(t *testing.T) {
	value := Decimal{Val: decimal128.FromI64(1420), Scale: 2}

	assert.Equal(t, int32(-500754589), int32(hashDecimal(value)))
	assert.Equal(t, hashDecimal(value), hashDecimal(Decimal{Val: value.Val, Scale: 7}))
}
