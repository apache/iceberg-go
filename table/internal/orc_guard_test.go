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
	"context"
	"os"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	orcpb "github.com/scritchley/orc/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/protoadapt"
)

func assertORCReaderRejected(t *testing.T, path string) {
	t.Helper()
	stat, err := os.Stat(path)
	require.NoError(t, err)
	file, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, iceberg.EntryContentData, path, iceberg.OrcFile, nil, nil, nil, 1, stat.Size())
	require.NoError(t, err)
	source, err := GetFile(context.Background(), iceio.LocalFS{}, file.Build(), false)
	require.NoError(t, err)
	_, err = source.GetReader(context.Background())
	require.ErrorIs(t, err, iceberg.ErrNotImplemented)
}

func TestORCRejectsTimestampData(t *testing.T) {
	for _, unit := range []string{"", "MICROS", "NANOS"} {
		t.Run(unit, func(t *testing.T) {
			attrs := map[string]string{"iceberg.id": "1"}
			if unit != "" {
				attrs["iceberg.timestamp-unit"] = unit
			}
			path := writeORCFixture(t, "struct<ts:timestamp>", map[int]map[string]string{1: attrs}, []any{time.Unix(12345, 0).UTC()})
			assertORCReaderRejected(t, path)
		})
	}
}

func TestORCRejectsAmbiguousRootNames(t *testing.T) {
	for _, name := range []string{"a.b", "", "*"} {
		t.Run(name, func(t *testing.T) {
			path := writeORCFixture(t, "struct<a:struct<b:bigint>,c:bigint>", map[int]map[string]string{1: {"iceberg.id": "1"}, 2: {"iceberg.id": "2"}, 3: {"iceberg.id": "3"}}, []any{[]any{int64(111)}, int64(222)})
			data, err := os.ReadFile(path)
			require.NoError(t, err)
			psLen := int(data[len(data)-1])
			psStart := len(data) - 1 - psLen
			var ps orcpb.PostScript
			require.NoError(t, proto.Unmarshal(data[psStart:len(data)-1], protoadapt.MessageV2Of(&ps)))
			footerStart := psStart - int(ps.GetFooterLength())
			var footer orcpb.Footer
			require.NoError(t, proto.Unmarshal(data[footerStart:psStart], protoadapt.MessageV2Of(&footer)))
			footer.Types[0].FieldNames[1] = name
			encoded, err := proto.Marshal(protoadapt.MessageV2Of(&footer))
			require.NoError(t, err)
			ps.FooterLength = new(uint64(len(encoded)))
			postscript, err := proto.Marshal(protoadapt.MessageV2Of(&ps))
			require.NoError(t, err)
			updated := append([]byte{}, data[:footerStart]...)
			updated = append(updated, encoded...)
			updated = append(updated, postscript...)
			updated = append(updated, byte(len(postscript)))
			require.NoError(t, os.WriteFile(path, updated, 0o644))
			assertORCReaderRejected(t, path)
		})
	}
}

func TestORCRejectsUnsupportedMapAndWideDecimal(t *testing.T) {
	for _, typ := range []*orcpb.Type{
		{Kind: orcpb.Type_MAP.Enum(), Subtypes: []uint32{1, 2}},
		{Kind: orcpb.Type_DECIMAL.Enum(), Precision: new(uint32(19)), Scale: new(uint32(0))},
		{Kind: orcpb.Type_DECIMAL.Enum(), Precision: new(uint32(38)), Scale: new(uint32(18))},
	} {
		_, err := orcSchemaField([]*orcpb.Type{typ}, 0, "column", make(map[int]struct{}))
		require.ErrorIs(t, err, iceberg.ErrNotImplemented)
	}
}
