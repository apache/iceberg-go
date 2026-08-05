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
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/geoarrow/geoarrow-go"
	"github.com/stretchr/testify/require"
)

func TestNormalizeWKBArray(t *testing.T) {
	typeDef := geoarrow.NewWKBType(geoarrow.WKBWithBinaryStorage())
	builder := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
	builder.Append(newWKBBuilder(wkbPoint|ewkbFlagSRID).u32(4326).f64(1, 2).bytes())
	builder.Append(newWKBBuilder(wkbPointZ).f64(3, 4, 5).bytes())
	builder.AppendNull()
	storage := builder.NewArray()
	builder.Release()
	ext := array.NewExtensionArrayWithStorage(typeDef, storage).(array.ExtensionArray)
	storage.Release()
	defer ext.Release()

	normalized, changed, err := normalizeWKBArray(ext)
	require.NoError(t, err)
	require.True(t, changed)
	defer normalized.Release()

	arr := normalized.(*geoarrow.WKBArray)
	require.Equal(t, newWKBBuilder(wkbPoint).f64(1, 2).bytes(), []byte(arr.Value(0)))
	require.Equal(t, newWKBBuilder(wkbPointZ).f64(3, 4, 5).bytes(), []byte(arr.Value(1)))
	require.True(t, arr.IsNull(2))
}
