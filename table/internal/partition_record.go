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
	"slices"

	"github.com/apache/iceberg-go"
)

type PartitionRecord []any

func (p PartitionRecord) Size() int            { return len(p) }
func (p PartitionRecord) Get(pos int) any      { return p[pos] }
func (p PartitionRecord) Set(pos int, val any) { p[pos] = val }

func NewPartitionRecord(partitionData map[int]any, partitionType *iceberg.StructType) PartitionRecord {
	out := make(PartitionRecord, partitionType.NumFields())
	for i := range partitionType.NumFields() {
		value := partitionData[partitionType.Field(i).ID]
		if bytes, ok := value.([]byte); ok {
			out[i] = slices.Clone(bytes)
		} else {
			out[i] = value
		}
	}

	return out
}
