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
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/apache/iceberg-go"
)

// embeddedManifestSpecHeader is the partition spec from a v1 Avro manifest
// header. Legacy writers may omit the spec ID or individual field IDs.
type embeddedManifestSpecHeader struct {
	id     int32
	hasID  bool
	fields []iceberg.PartitionField
}

func parseEmbeddedManifestSpecHeader(metadata map[string][]byte) (embeddedManifestSpecHeader, error) {
	var header embeddedManifestSpecHeader
	if raw, present := metadata["partition-spec-id"]; present {
		id, err := strconv.ParseInt(string(raw), 10, 32)
		if err != nil || id < 0 {
			return header, fmt.Errorf("invalid partition-spec-id %q", raw)
		}
		header.id, header.hasID = int32(id), true
	}
	if err := json.Unmarshal(metadata["partition-spec"], &header.fields); err != nil {
		return header, fmt.Errorf("invalid v1 partition-spec: %w", err)
	}
	for i := range header.fields {
		if header.fields[i].FieldID == 0 {
			header.fields[i].FieldID = iceberg.PartitionDataIDStart + i
		}
	}

	return header, nil
}

// resolve uses a header ID if present, including unknown IDs that the caller
// may want to report. Without one, it only infers an ID from exactly one
// matching historical spec. The caller decides whether an absent or ambiguous
// match is inconclusive (audit) or an error (statistics writer).
func (h embeddedManifestSpecHeader) resolve(specs map[int32]iceberg.PartitionSpec) (id int32, known, ambiguous bool) {
	if h.hasID {
		return h.id, true, false
	}
	for candidateID, candidate := range specs {
		actual := iceberg.NewPartitionSpecID(int(candidateID), h.fields...)
		if !actual.Equals(candidate) {
			continue
		}
		if known {
			return 0, false, true
		}
		id, known = candidateID, true
	}

	return id, known, false
}
