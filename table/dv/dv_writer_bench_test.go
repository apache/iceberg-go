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

package dv

import "testing"

func BenchmarkDVWriterAddSinglePosition(b *testing.B) {
	w := NewDVWriter(nil, nil)
	const dataFilePath = "s3://bucket/data/file.parquet"
	// Seed a fixed-size bitmap so the timed loop does not measure bitmap growth.
	if err := w.Add(dataFilePath, []int64{0}, 0, nil); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		if err := w.Add(dataFilePath, []int64{1}, 0, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDVWriterAddPosition(b *testing.B) {
	w := NewDVWriter(nil, nil)
	const dataFilePath = "s3://bucket/data/file.parquet"
	// Seed a fixed-size bitmap so the timed loop does not measure bitmap growth.
	if err := w.AddPosition(dataFilePath, 0, 0, nil); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		if err := w.AddPosition(dataFilePath, 1, 0, nil); err != nil {
			b.Fatal(err)
		}
	}
}
