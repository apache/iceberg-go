package table

import (
	"encoding/json"
	"fmt"
)

type BlobType string

const (
	BlobTypeApacheDatasketchesThetaV1 BlobType = "apache-datasketches-theta-v1"
	BlobTypeDeletionVectorV1          BlobType = "deletion-vector-v1"
)

func (bt *BlobType) IsValid() bool {
	switch *bt {
	case BlobTypeApacheDatasketchesThetaV1, BlobTypeDeletionVectorV1:
		return true
	default:
		return false
	}
}

func (bt *BlobType) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}

	*bt = BlobType(s)
	if !bt.IsValid() {
		return fmt.Errorf("invalid blob type: %s", s)
	}

	return nil
}

type Statistics struct {
	SnapshotID            int64          `json:"snapshot-id"`
	StatisticsPath        string         `json:"statistics-path"`
	FileSizeInBytes       int64          `json:"file-size-in-bytes"`
	FileFooterSizeInBytes int64          `json:"file-footer-size-in-bytes"`
	KeyMetadata           *string        `json:"key-metadata,omitempty"`
	BlobMetadata          []BlobMetadata `json:"blob-metadata"`
}

type BlobMetadata struct {
	Type           BlobType          `json:"type"`
	SnapshotID     int64             `json:"snapshot-id"`
	SequenceNumber int64             `json:"sequence-number"`
	Fields         []int32           `json:"fields"`
	Properties     map[string]string `json:"properties"`
}

type PartitionStatistics struct {
	SnapshotID      int64  `json:"snapshot-id"`
	StatisticsPath  string `json:"statistics-path"`
	FileSizeInBytes int64  `json:"file-size-in-bytes"`
}
