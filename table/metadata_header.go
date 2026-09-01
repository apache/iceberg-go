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
	"bytes"
	"encoding/json"
	"fmt"
)

type metadataHeader struct {
	formatVersion               int
	needsPartitionNormalization bool
}

type rawMetadataHeader struct {
	FormatVersion  json.RawMessage `json:"format-version"`
	LastUpdatedMS  json.RawMessage `json:"last-updated-ms"`
	PartitionSpec  json.RawMessage `json:"partition-spec"`
	PartitionSpecs json.RawMessage `json:"partition-specs"`
}

func inspectMetadataHeader(data []byte) (metadataHeader, error) {
	var raw rawMetadataHeader
	if err := json.Unmarshal(data, &raw); err != nil {
		return metadataHeader{}, fmt.Errorf("%w: %w", ErrInvalidMetadata, err)
	}

	var header metadataHeader
	if len(raw.FormatVersion) == 0 {
		return metadataHeader{}, ErrInvalidMetadataFormatVersion
	}
	if err := json.Unmarshal(raw.FormatVersion, &header.formatVersion); err != nil {
		return metadataHeader{}, fmt.Errorf("%w: %w", ErrInvalidMetadata, err)
	}
	if header.formatVersion < 1 || header.formatVersion > supportedTableFormatVersion {
		return metadataHeader{}, ErrInvalidMetadataFormatVersion
	}
	if len(raw.LastUpdatedMS) == 0 || bytes.Equal(bytes.TrimSpace(raw.LastUpdatedMS), []byte("null")) {
		return metadataHeader{}, fmt.Errorf("%w: last-updated-ms is absent or null", ErrInvalidMetadata)
	}

	needsNormalization, err := partitionSpecsNeedNormalization(raw.PartitionSpec, raw.PartitionSpecs)
	if err != nil {
		return metadataHeader{}, err
	}
	header.needsPartitionNormalization = needsNormalization

	return header, nil
}

func partitionSpecsNeedNormalization(rawLegacySpec, rawSpecs json.RawMessage) (bool, error) {
	if len(rawSpecs) != 0 {
		var specs []json.RawMessage
		if err := json.Unmarshal(rawSpecs, &specs); err != nil {
			return false, err
		}

		needsNormalization := false
		for i, rawSpec := range specs {
			missingFieldID, err := partitionSpecNeedsNormalization(rawSpec, true, i)
			if err != nil {
				return false, err
			}
			needsNormalization = needsNormalization || missingFieldID
		}

		return needsNormalization, nil
	}
	if len(rawLegacySpec) == 0 {
		return false, nil
	}

	var fields []map[string]json.RawMessage
	if err := json.Unmarshal(rawLegacySpec, &fields); err != nil {
		return false, err
	}

	return partitionFieldsNeedNormalization(fields), nil
}

func partitionSpecNeedsNormalization(rawSpec json.RawMessage, requireID bool, index int) (bool, error) {
	var spec map[string]json.RawMessage
	if err := json.Unmarshal(rawSpec, &spec); err != nil {
		return false, fmt.Errorf("%w: invalid partition spec at index %d: %w", ErrInvalidMetadata, index, err)
	}
	if requireID {
		rawID, ok := spec["spec-id"]
		if !ok || bytes.Equal(bytes.TrimSpace(rawID), []byte("null")) {
			return false, fmt.Errorf("%w: partition spec at index %d is missing required spec-id", ErrInvalidMetadata, index)
		}

		var id int
		if err := json.Unmarshal(rawID, &id); err != nil {
			return false, err
		}
	}

	rawFields, ok := spec["fields"]
	if !ok || bytes.Equal(bytes.TrimSpace(rawFields), []byte("null")) {
		return false, nil
	}
	var fields []map[string]json.RawMessage
	if err := json.Unmarshal(rawFields, &fields); err != nil {
		return false, err
	}

	return partitionFieldsNeedNormalization(fields), nil
}

func partitionFieldsNeedNormalization(fields []map[string]json.RawMessage) bool {
	for _, field := range fields {
		if _, ok := field["field-id"]; !ok {
			return true
		}
	}

	return false
}
