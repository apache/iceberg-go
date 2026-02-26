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
	"fmt"

	"github.com/google/uuid"
)

type WriteTask struct {
	Uuid        uuid.UUID
	ID          int
	PartitionID int
	FileCount   int
}

func (w WriteTask) GenerateDataFileName(extension string) string {
	// Mimics the behavior in the Java API:
	// https://github.com/apache/iceberg/blob/03ed4ba9af4e47d32bdb22b7e3d033eb2a4b2c83/core/src/main/java/org/apache/iceberg/io/OutputFileFactory.java#L93
	// Format: {partitionId:05d}-{taskId}-{operationId}-{fileCount:05d}.{extension}
	return fmt.Sprintf("%05d-%d-%s-%05d.%s", w.PartitionID, w.ID, w.Uuid, w.FileCount, extension)
}
