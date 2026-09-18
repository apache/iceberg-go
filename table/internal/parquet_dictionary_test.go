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

package internal

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDictionaryMatchesPredicatesByteArray(t *testing.T) {
	data := make([]byte, 0, 32)
	appendValue := func(value string) {
		var length [4]byte
		binary.LittleEndian.PutUint32(length[:], uint32(len(value)))
		data = append(data, length[:]...)
		data = append(data, value...)
	}
	appendValue("first")
	appendValue("second")

	page := file.NewDictionaryPage(memory.NewBufferBytes(data), 2, parquet.Encodings.Plain)
	matches, known := dictionaryMatchesPredicates(
		page,
		parquet.Types.ByteArray,
		-1,
		[]RowGroupDictionaryPred{{FieldID: 1, PhysBytes: [][]byte{[]byte("missing"), []byte("second")}}},
		[]int{0},
	)
	page.Release()

	require.True(t, known)
	require.Len(t, matches, 1)
	assert.True(t, matches[0])
}

func TestDictionaryMatchesPredicatesRejectsMalformedPage(t *testing.T) {
	data := []byte{5, 0, 0, 0, 'x'}
	page := file.NewDictionaryPage(memory.NewBufferBytes(data), 1, parquet.Encodings.Plain)
	matches, known := dictionaryMatchesPredicates(
		page,
		parquet.Types.ByteArray,
		-1,
		[]RowGroupDictionaryPred{{FieldID: 1, PhysBytes: [][]byte{[]byte("x")}}},
		[]int{0},
	)
	page.Release()

	assert.False(t, known)
	assert.Nil(t, matches)
}

func TestDictionaryMatchesPredicatesRejectsEmptyPage(t *testing.T) {
	page := file.NewDictionaryPage(memory.NewBufferBytes(nil), 0, parquet.Encodings.Plain)
	matches, known := dictionaryMatchesPredicates(
		page,
		parquet.Types.Int32,
		-1,
		[]RowGroupDictionaryPred{{FieldID: 1, PhysBytes: [][]byte{int32BytesForTest(1)}}},
		[]int{0},
	)
	page.Release()

	assert.False(t, known)
	assert.Nil(t, matches)
}

func TestDictionaryMatchesPredicatesTreatsSignedZeroAsEqual(t *testing.T) {
	data := make([]byte, 4)
	binary.LittleEndian.PutUint32(data, math.Float32bits(float32(math.Copysign(0, -1))))
	candidate := make([]byte, 4)
	binary.LittleEndian.PutUint32(candidate, math.Float32bits(0))

	page := file.NewDictionaryPage(memory.NewBufferBytes(data), 1, parquet.Encodings.Plain)
	matches, known := dictionaryMatchesPredicates(
		page,
		parquet.Types.Float,
		-1,
		[]RowGroupDictionaryPred{{FieldID: 1, PhysBytes: [][]byte{candidate}}},
		[]int{0},
	)
	page.Release()

	require.True(t, known)
	require.Len(t, matches, 1)
	assert.True(t, matches[0])
}

func appendFloat32DictionaryValue(data []byte, value float32) []byte {
	var encoded [4]byte
	binary.LittleEndian.PutUint32(encoded[:], math.Float32bits(value))

	return append(data, encoded[:]...)
}

func appendFloat64DictionaryValue(data []byte, value float64) []byte {
	var encoded [8]byte
	binary.LittleEndian.PutUint64(encoded[:], math.Float64bits(value))

	return append(data, encoded[:]...)
}

func float32DictionaryBytes(values ...float32) []byte {
	data := make([]byte, 0, len(values)*4)
	for _, value := range values {
		data = appendFloat32DictionaryValue(data, value)
	}

	return data
}

func float64DictionaryBytes(values ...float64) []byte {
	data := make([]byte, 0, len(values)*8)
	for _, value := range values {
		data = appendFloat64DictionaryValue(data, value)
	}

	return data
}

func TestDictionaryMatchesPredicatesHandlesNaN(t *testing.T) {
	tests := []struct {
		name         string
		physicalType parquet.Type
		data         []byte
		numValues    int32
		candidate    []byte
		wantMatch    bool
	}{
		{
			name:         "Float dictionary NaN does not match a number",
			physicalType: parquet.Types.Float,
			data:         float32DictionaryBytes(float32(math.NaN()), 1, 2),
			numValues:    3,
			candidate:    float32BytesForTest(99),
			wantMatch:    false,
		},
		{
			name:         "Double dictionary NaN does not match a number",
			physicalType: parquet.Types.Double,
			data:         float64DictionaryBytes(math.NaN(), 1, 2),
			numValues:    3,
			candidate:    float64BytesForTest(99),
			wantMatch:    false,
		},
		{
			name:         "Float NaN candidate keeps the group",
			physicalType: parquet.Types.Float,
			data:         float32DictionaryBytes(1),
			numValues:    1,
			candidate:    appendFloat32DictionaryValue(nil, float32(math.NaN())),
			wantMatch:    true,
		},
		{
			name:         "Double NaN candidate keeps the group",
			physicalType: parquet.Types.Double,
			data:         float64DictionaryBytes(1),
			numValues:    1,
			candidate:    appendFloat64DictionaryValue(nil, math.NaN()),
			wantMatch:    true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			page := file.NewDictionaryPage(memory.NewBufferBytes(test.data), test.numValues, parquet.Encodings.Plain)

			matches, known := dictionaryMatchesPredicates(
				page,
				test.physicalType,
				-1,
				[]RowGroupDictionaryPred{{FieldID: 1, PhysBytes: [][]byte{test.candidate}}},
				[]int{0},
			)
			page.Release()

			require.True(t, known)
			require.Len(t, matches, 1)
			assert.Equal(t, test.wantMatch, matches[0])
		})
	}
}

func float32BytesForTest(value float32) []byte {
	var encoded [4]byte
	binary.LittleEndian.PutUint32(encoded[:], math.Float32bits(value))

	return encoded[:]
}

func int32BytesForTest(value int32) []byte {
	var encoded [4]byte
	binary.LittleEndian.PutUint32(encoded[:], uint32(value))

	return encoded[:]
}

func float64BytesForTest(value float64) []byte {
	var encoded [8]byte
	binary.LittleEndian.PutUint64(encoded[:], math.Float64bits(value))

	return encoded[:]
}
