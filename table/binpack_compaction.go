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

import "fmt"

// Bin-pack streams rows in input order. Sort strategies can accept a mixed
// group because they split records by the output partition before writing.
func (binPackStrategy) validatePartition(tasks []FileScanTask) error {
	var firstKey string
	for i, task := range tasks {
		file := task.File
		key, err := canonicalPartitionKey(file.SpecID(), dataFilePartition(file))
		if err != nil {
			return fmt.Errorf("%w: invalid partition for compaction data file %q: %v",
				ErrInvalidOperation, file.FilePath(), err)
		}
		if i == 0 {
			firstKey = key
		} else if key != firstKey {
			return fmt.Errorf("%w: compaction group contains files from different partitions: %q and %q",
				ErrInvalidOperation, tasks[0].File.FilePath(), file.FilePath())
		}
	}

	return nil
}
