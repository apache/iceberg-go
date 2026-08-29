// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package iceberg

import (
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/twmb/avro/atype"
)

var avroEncodePartitionDataBenchmarkSink int

func BenchmarkAvroEncodePartitionData(b *testing.B) {
	for _, tc := range []struct {
		name       string
		fieldCount int
		logical    bool
	}{
		{name: "empty", fieldCount: 0},
		{name: "primitive_4", fieldCount: 4},
		{name: "logical_4", fieldCount: 4, logical: true},
		{name: "primitive_16", fieldCount: 16},
		{name: "logical_16", fieldCount: 16, logical: true},
	} {
		partition, fields := avroEncodePartitionDataBenchmarkData(tc.fieldCount, tc.logical)

		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				converted, err := avroEncodePartitionData(partition, fields)
				if err != nil {
					b.Fatal(err)
				}
				avroEncodePartitionDataBenchmarkSink = len(converted)
			}
		})
	}
}

func avroEncodePartitionDataBenchmarkData(fieldCount int, logical bool) (map[int]any, dataFileFieldMaps) {
	partition := make(map[int]any, fieldCount)
	fields := dataFileFieldMaps{
		nameToID:      make(map[string]int, fieldCount),
		idToType:      make(map[int]string),
		idToFixedSize: make(map[int]int),
	}

	for i := range fieldCount {
		id := 1000 + i
		name := fmt.Sprintf("field_%02d", i)
		fields.nameToID[name] = id

		switch {
		case logical && i%4 == 0:
			fields.idToType[id] = atype.Date
			partition[id] = Date(i)
		case logical && i%4 == 1:
			fields.idToType[id] = atype.TimestampMicros
			partition[id] = Timestamp(i * 1_000)
		case logical && i%4 == 2:
			fields.idToType[id] = atype.Decimal
			fields.idToFixedSize[id] = 16
			partition[id] = Decimal{Val: decimal128.FromI64(int64(i + 1)), Scale: 2}
		default:
			partition[id] = int32(i)
		}
	}

	return partition, fields
}
