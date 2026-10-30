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

package table

import (
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/iceberg-go"
)

var partitionPathBenchmarkSink string

// referencePartitionPathQueryEscape is the previous format implementation,
// kept in the benchmark to compare on the same Go version and machine.
func referencePartitionPathQueryEscape(p partitionPathPlan, data iceberg.StructLike) string {
	if len(p.fields) == 0 {
		return ""
	}

	var builder strings.Builder
	builder.Grow(p.estimatedSize)
	for i, field := range p.fields {
		if i > 0 {
			builder.WriteByte('/')
		}
		builder.WriteString(field.escapedName)
		builder.WriteByte('=')
		value := field.transform.ToHumanString(field.resultType, data.Get(i))
		builder.WriteString(url.QueryEscape(value))
	}

	return builder.String()
}

func BenchmarkPartitionPathQueryEscapeComparison(b *testing.B) {
	cases := []struct {
		name  string
		value string
	}{
		{"ASCII", "customer-region-2026-oct-europe"},
		{"Escaped", "Paris / Île-de-France ? + travel / cities"},
		{"LongEscaped", strings.Repeat("some/part?=%\t+é", 24)},
		{"LongPlain", strings.Repeat("customerseg2026", 30)},
	}

	for _, tc := range cases {
		for _, count := range []int{1, 4, 16} {
			fields := make([]partitionPathField, count)
			values := make(partitionRecord, count)
			estimatedSize := 0
			for j := range fields {
				name := "p" + strconv.Itoa(j)
				fields[j] = partitionPathField{
					escapedName: name,
					transform:   iceberg.IdentityTransform{},
					resultType:  iceberg.PrimitiveTypes.String,
				}
				values[j] = tc.value
				estimatedSize += len(name) + 20
			}
			plan := partitionPathPlan{fields: fields, estimatedSize: estimatedSize}
			if want, got := referencePartitionPathQueryEscape(plan, values), plan.format(values); want != got {
				b.Fatalf("mismatched partition paths: %q vs %q", want, got)
			}

			b.Run(tc.name+"/"+strconv.Itoa(count)+"Fields", func(b *testing.B) {
				b.Run("QueryEscape", func(b *testing.B) {
					b.ReportAllocs()
					for range b.N {
						partitionPathBenchmarkSink = referencePartitionPathQueryEscape(plan, values)
					}
				})
				b.Run("DirectAppend", func(b *testing.B) {
					b.ReportAllocs()
					for range b.N {
						partitionPathBenchmarkSink = plan.format(values)
					}
				})
			})
		}
	}
}
