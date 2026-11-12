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

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/puffin"
)

// StatisticsFile represents a statistics file in the Puffin format, that can be used to read table data more
// efficiently.
//
// Statistics are informational. A reader can choose to ignore statistics information.
// Statistics support is not required to read the table correctly.
type StatisticsFile struct {
	SnapshotID            int64          `json:"snapshot-id"`
	StatisticsPath        string         `json:"statistics-path"`
	FileSizeInBytes       int64          `json:"file-size-in-bytes"`
	FileFooterSizeInBytes int64          `json:"file-footer-size-in-bytes"`
	KeyMetadata           *string        `json:"key-metadata,omitempty"`
	BlobMetadata          []BlobMetadata `json:"blob-metadata"`
}

// MarshalJSON writes nil BlobMetadata as [], since the spec requires the field.
func (s StatisticsFile) MarshalJSON() ([]byte, error) {
	type statisticsFile StatisticsFile
	if s.BlobMetadata == nil {
		s.BlobMetadata = []BlobMetadata{}
	}

	return json.Marshal(statisticsFile(s))
}

// UnmarshalJSON rejects missing required fields rather than treating them as
// zero values, as required for table statistics metadata.
func (s *StatisticsFile) UnmarshalJSON(data []byte) error {
	type plain StatisticsFile
	var decoded plain
	if err := unmarshalRequiredStatisticsFields(data, &decoded,
		"snapshot-id", "statistics-path", "file-size-in-bytes",
		"file-footer-size-in-bytes", "blob-metadata"); err != nil {
		return err
	}

	*s = StatisticsFile(decoded)

	return nil
}

// BlobMetadata is the metadata of a statistics or indices blob.
// Its type uses the same Puffin blob identifiers as the file reader and writer.
type BlobMetadata struct {
	Type           puffin.BlobType   `json:"type"`
	SnapshotID     int64             `json:"snapshot-id"`
	SequenceNumber int64             `json:"sequence-number"`
	Fields         []int32           `json:"fields"`
	Properties     map[string]string `json:"properties,omitempty"`
}

// MarshalJSON writes nil Fields as [], since the spec requires the field.
func (b BlobMetadata) MarshalJSON() ([]byte, error) {
	type blobMetadata BlobMetadata
	if b.Fields == nil {
		b.Fields = []int32{}
	}

	return json.Marshal(blobMetadata(b))
}

// UnmarshalJSON checks the required fields of each statistics blob.
func (b *BlobMetadata) UnmarshalJSON(data []byte) error {
	type plain BlobMetadata
	var decoded plain
	if err := unmarshalRequiredStatisticsFields(data, &decoded,
		"type", "snapshot-id", "sequence-number", "fields"); err != nil {
		return err
	}

	*b = BlobMetadata(decoded)

	return nil
}

// PartitionStatisticsFile represents a partition statistics file that can be used to read table data more efficiently.
//
// Statistics are informational. A reader can choose to ignore statistics information. Statistics
// support is not required to read the table correctly.
type PartitionStatisticsFile struct {
	SnapshotID      int64  `json:"snapshot-id"`
	StatisticsPath  string `json:"statistics-path"`
	FileSizeInBytes int64  `json:"file-size-in-bytes"`
}

// UnmarshalJSON checks the required fields of partition statistics metadata.
func (p *PartitionStatisticsFile) UnmarshalJSON(data []byte) error {
	type plain PartitionStatisticsFile
	var decoded plain
	if err := unmarshalRequiredStatisticsFields(data, &decoded,
		"snapshot-id", "statistics-path", "file-size-in-bytes"); err != nil {
		return err
	}

	*p = PartitionStatisticsFile(decoded)

	return nil
}

// unmarshalRequiredStatisticsFields checks presence and nullability at the
// JSON boundary. Unmarshalling directly into value fields would silently
// replace missing or null numbers and strings with zero values.
func unmarshalRequiredStatisticsFields(data []byte, target any, required ...string) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	if fields == nil {
		return fmt.Errorf("%w: statistics metadata must be an object", iceberg.ErrInvalidArgument)
	}

	for _, field := range required {
		value, ok := fields[field]
		if !ok || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return fmt.Errorf("%w: required statistics field %q is missing or null", iceberg.ErrInvalidArgument, field)
		}
	}

	return json.Unmarshal(data, target)
}
