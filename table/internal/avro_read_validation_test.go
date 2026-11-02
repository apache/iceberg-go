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
	"context"
	"os"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table/internal"
	"github.com/stretchr/testify/require"
)

func TestAvroRejectUnsafeNestedSchemas(t *testing.T) {
	for _, tc := range []struct{ name, typ string }{
		{"nullable record", `["null",{"type":"record","name":"A","fields":[{"name":"x","type":"long","field-id":2}]}]`},
		{"nullable record in list", `{"type":"array","element-id":2,"items":["null",{"type":"record","name":"A","fields":[{"name":"x","type":"long","field-id":3}]}]}`},
		{"nullable record in map", `{"type":"map","key-id":2,"value-id":3,"values":["null",{"type":"record","name":"A","fields":[{"name":"x","type":"long","field-id":4}]}]}`},
		{"list of lists", `{"type":"array","element-id":2,"items":{"type":"array","element-id":3,"items":"long"}}`},
		{"list of maps", `{"type":"array","element-id":2,"items":{"type":"map","key-id":3,"value-id":4,"values":"long"}}`},
		{"nested map", `{"type":"map","key-id":2,"value-id":3,"values":{"type":"map","key-id":4,"value-id":5,"values":"long"}}`},
		{"nullable list of lists", `["null",{"type":"array","element-id":2,"items":{"type":"array","element-id":3,"items":"long"}}]`},
		{"nullable map of lists", `["null",{"type":"map","key-id":2,"value-id":3,"values":{"type":"array","element-id":4,"items":"long"}}]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			schema := `{"type":"record","name":"root","fields":[{"name":"a","field-id":1,"type":` + tc.typ + `}]}`
			path := writeAvroOCF(t, schema)
			stat, err := os.Stat(path)
			require.NoError(t, err)
			df, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, iceberg.EntryContentData, path, iceberg.AvroFile, nil, nil, nil, 0, stat.Size())
			require.NoError(t, err)
			source, err := internal.GetFile(context.Background(), iceio.LocalFS{}, df.Build(), false)
			require.NoError(t, err)
			_, err = source.GetReader(context.Background())
			require.ErrorIs(t, err, iceberg.ErrNotImplemented)
		})
	}
}

func TestAvroRejectUnsafeNestedDataBeforeDecoding(t *testing.T) {
	for _, tc := range []struct {
		name, typ string
		value     any
	}{
		{"nullable record", `["null",{"type":"record","name":"A","fields":[{"name":"x","type":"long","field-id":2}]}]`, map[string]any{"x": int64(42)}},
		{"list of maps", `{"type":"array","element-id":2,"items":{"type":"map","key-id":3,"value-id":4,"values":"long"}}`, []any{map[string]any{"key": int64(42)}}},
		{"list of lists", `{"type":"array","element-id":2,"items":{"type":"array","element-id":3,"items":"long"}}`, []any{[]any{int64(42), int64(43)}}},
		{"map of maps", `{"type":"map","key-id":2,"value-id":3,"values":{"type":"map","key-id":4,"value-id":5,"values":"long"}}`, map[string]any{"outer": map[string]any{"inner": int64(42)}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			schema := `{"type":"record","name":"root","fields":[{"name":"a","field-id":1,"type":` + tc.typ + `}]}`
			path := writeAvroOCF(t, schema, map[string]any{"a": tc.value})
			stat, err := os.Stat(path)
			require.NoError(t, err)
			df, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, iceberg.EntryContentData, path, iceberg.AvroFile, nil, nil, nil, 1, stat.Size())
			require.NoError(t, err)
			source, err := internal.GetFile(context.Background(), iceio.LocalFS{}, df.Build(), false)
			require.NoError(t, err)
			_, err = source.GetReader(context.Background())
			require.ErrorIs(t, err, iceberg.ErrNotImplemented)
		})
	}
}
