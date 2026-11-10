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

package iceberg_test

import (
	"strings"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

func TestStringLiteralMarshalBinaryOwnsBytes(t *testing.T) {
	for _, tt := range []struct {
		name string
		text string
	}{
		{name: "ASCII", text: "alpha"},
		{name: "UTF-8", text: "Grüße 🌍"},
		{name: "empty", text: ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			// Use heap-backed strings so the old unsafe implementation fails
			// as a value-corruption regression rather than a memory fault.
			value := iceberg.StringLiteral(strings.Clone(tt.text))
			encoded, err := value.MarshalBinary()
			require.NoError(t, err)
			require.Equal(t, []byte(tt.text), encoded)

			if len(encoded) > 0 {
				encoded[0] = 0
			}
			require.Equal(t, tt.text, value.Value())

			again, err := value.MarshalBinary()
			require.NoError(t, err)
			require.Equal(t, []byte(tt.text), again)
		})
	}
}

func TestStringLiteralUnmarshalBinaryOwnsBytes(t *testing.T) {
	for _, text := range []string{"alpha", "Grüße 🌍", ""} {
		t.Run(text, func(t *testing.T) {
			input := []byte(text)
			var value iceberg.StringLiteral
			require.NoError(t, value.UnmarshalBinary(input))
			require.Equal(t, text, value.Value())

			if len(input) > 0 {
				input[0] = 0
			}
			require.Equal(t, text, value.Value())
		})
	}
}

func TestStringLiteralFromBytesOwnsInput(t *testing.T) {
	input := []byte("partition-value")
	lit, err := iceberg.LiteralFromBytes(iceberg.PrimitiveTypes.String, input)
	require.NoError(t, err)
	require.IsType(t, iceberg.StringLiteral(""), lit)

	input[0] = 0
	require.Equal(t, "partition-value", lit.(iceberg.StringLiteral).Value())
}
