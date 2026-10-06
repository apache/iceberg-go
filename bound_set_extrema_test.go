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
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBoundSetExtremaUnavailableForGeoLiterals(t *testing.T) {
	geoBytes := func(x, y float64) []byte {
		data := make([]byte, 16)
		binary.LittleEndian.PutUint64(data[:8], math.Float64bits(x))
		binary.LittleEndian.PutUint64(data[8:], math.Float64bits(y))

		return data
	}

	for _, tt := range []struct {
		name string
		typ  Type
	}{
		{name: "geometry", typ: GeometryType{}},
		{name: "geography", typ: GeographyType{}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			first, err := LiteralFromBytes(tt.typ, geoBytes(1, 2))
			require.NoError(t, err)
			second, err := LiteralFromBytes(tt.typ, geoBytes(3, 4))
			require.NoError(t, err)

			schema := NewSchema(1, NestedField{ID: 1, Name: "geo", Type: tt.typ})
			bound, err := BindExpr(
				schema,
				SetPredicate(OpIn, Reference("geo"), []Literal{first, second}),
				true,
			)
			require.NoError(t, err)
			pred, ok := bound.(BoundPredicate)
			require.True(t, ok)
			extrema, ok := pred.(boundSetExtremaRef)
			require.True(t, ok)

			minLit, maxLit, hasExtrema := extrema.boundSetExtremaRef()
			require.False(t, hasExtrema)
			require.Nil(t, minLit)
			require.Nil(t, maxLit)
		})
	}
}

