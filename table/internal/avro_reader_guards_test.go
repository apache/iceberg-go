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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package internal

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

func TestAvroRejectsReservedFieldIDs(t *testing.T) {
	for _, tc := range []struct {
		id    float64
		valid bool
	}{
		{2147483447, true},
		{2147483448, false},
		{2147483647, false},
	} {
		field, err := addAvroFieldID(arrow.Field{Name: "value", Type: arrow.PrimitiveTypes.Int64},
			map[string]any{"field-id": tc.id}, "field-id", make(map[int]struct{}))
		if tc.valid {
			require.NoError(t, err)
			require.NotEmpty(t, field.Metadata.Keys())
		} else {
			require.ErrorIs(t, err, iceberg.ErrInvalidSchema)
		}
	}
}

func TestAvroRecordReaderErrorIsTerminal(t *testing.T) {
	reader := &avroRecordReader{readErr: iceberg.ErrInvalidSchema}
	require.False(t, reader.Next())
	require.ErrorIs(t, reader.Err(), iceberg.ErrInvalidSchema)
}

func TestAvroSourceRejectsCanceledContextBeforeIO(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := (&AvroFileSource{}).GetReader(ctx)
	require.ErrorIs(t, err, context.Canceled)
}

