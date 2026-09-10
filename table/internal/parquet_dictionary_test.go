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
