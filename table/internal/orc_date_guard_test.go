// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package internal

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

func TestORCRejectsDatesAtAnyDepth(t *testing.T) {
	date := arrow.FixedWidthTypes.Date32
	for _, typ := range []arrow.DataType{
		date,
		arrow.StructOf(arrow.Field{Name: "date", Type: date}),
		arrow.ListOf(date),
		arrow.MapOf(arrow.BinaryTypes.String, date),
	} {
		sc := arrow.NewSchema([]arrow.Field{{Name: "value", Type: typ}}, nil)
		require.ErrorIs(t, validateORCDates(sc), iceberg.ErrNotImplemented)
	}
	sc := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int64}}, nil)
	require.NoError(t, validateORCDates(sc))
}
