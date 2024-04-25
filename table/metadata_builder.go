package table

import (
	"github.com/google/uuid"
	"github.com/polarsignals/iceberg-go"
)

type MetadataV1Builder struct {
	*MetadataV1
}

// NewMetadataV1Builder returns a new MetadataV1Builder.
func NewMetadataV1Builder(
	location string,
	schema *iceberg.Schema,
	lastUpdatesMs int64,
	lastColumnId int,
) *MetadataV1Builder {
	return &MetadataV1Builder{
		MetadataV1: &MetadataV1{
			Schema:    schema,
			Partition: []iceberg.PartitionField{}, // Deprecated: use partition-specs and default-spec-id instead. See: https://iceberg.apache.org/spec/#table-metadata
			commonMetadata: commonMetadata{
				FormatVersion:   1,
				Loc:             location,
				LastUpdatedMS:   lastUpdatesMs,
				LastColumnId:    lastColumnId,
				CurrentSchemaID: schema.ID,
				Specs:           []iceberg.PartitionSpec{iceberg.NewPartitionSpec()},
				Props:           iceberg.Properties{},
			},
		},
	}
}

func CloneMetadataV1(m Metadata) *MetadataV1Builder {
	b := &MetadataV1Builder{
		MetadataV1: &MetadataV1{
			Schema:    m.CurrentSchema(),
			Partition: []iceberg.PartitionField{}, // Deprecated: use partition-specs and default-spec-id instead. See: https://iceberg.apache.org/spec/#table-metadata
			commonMetadata: commonMetadata{
				FormatVersion:      m.Version(),
				UUID:               m.TableUUID(),
				Loc:                m.Location(),
				Specs:              m.PartitionSpecs(),
				DefaultSpecID:      m.DefaultPartitionSpec(),
				Props:              m.Properties(),
				SchemaList:         m.Schemas(),
				CurrentSchemaID:    m.SchemaID(),
				LastPartitionID:    m.LastPartitionSpecID(),
				SnapshotList:       m.Snapshots(),
				CurrentSnapshotID:  m.SnapshotID(),
				SnapshotLog:        m.GetSnapshotLog(),
				MetadataLog:        m.GetMetadataLog(),
				SortOrderList:      m.SortOrders(),
				DefaultSortOrderID: m.SortOrderID(),
				Refs:               m.SnapshotRefs(),
			},
		},
	}
	return b

}

func (b *MetadataV1Builder) WithLastUpdatedMs(lastUpdatedMs int64) *MetadataV1Builder {
	b.LastUpdatedMS = lastUpdatedMs
	return b
}

func (b *MetadataV1Builder) WithFormatVersion(version int) *MetadataV1Builder {
	b.FormatVersion = version
	return b
}

func (b *MetadataV1Builder) WithLocation(location string) *MetadataV1Builder {
	b.Loc = location
	return b
}

func (b *MetadataV1Builder) WithSchema(schema *iceberg.Schema) *MetadataV1Builder {
	b.Schema = schema
	b.CurrentSchemaID = schema.ID
	b.LastColumnId = schema.NumFields()
	b.SchemaList = append(b.SchemaList, schema)
	return b
}

// WithTableUUID sets the optional table-uuid field of the metadata.
func (b *MetadataV1Builder) WithTableUUID(id uuid.UUID) *MetadataV1Builder {
	b.UUID = id
	return b
}

// WithSchemas sets the optional schemas field of the metadata.
func (b *MetadataV1Builder) WithSchemas(schemas []*iceberg.Schema) *MetadataV1Builder {
	b.SchemaList = append(b.SchemaList, schemas...)
	return b
}

// WithCurrentSchemaID sets the optional current-schema-id field of the metadata.
func (b *MetadataV1Builder) WithCurrentSchemaID(currentSchemaID int) *MetadataV1Builder {
	b.CurrentSchemaID = currentSchemaID
	return b
}

// WithProperties sets the optional partition-specs field of the metadata.
func (b *MetadataV1Builder) WithPartitionSpecs(specs []iceberg.PartitionSpec) *MetadataV1Builder {
	b.Specs = specs
	return b
}

// WithDefaultSpecID sets the optional default-spec-id field of the metadata.
func (b *MetadataV1Builder) WithDefaultSpecID(defaultSpecID int) *MetadataV1Builder {
	b.DefaultSpecID = defaultSpecID
	return b
}

// WithLastPartitionID sets the optional last-partition-id field of the metadata.
func (b *MetadataV1Builder) WithLastPartitionID(lastPartitionID int) *MetadataV1Builder {
	l := lastPartitionID // copy the value to prevent modification after build
	b.LastPartitionID = &l
	return b
}

// WithProperties sets the optional properties field of the metadata.
func (b *MetadataV1Builder) WithProperties(properties iceberg.Properties) *MetadataV1Builder {
	b.Props = properties
	return b
}

// WithCurrentSnapshotID sets the optional current-snapshot-id field of the metadata.
func (b *MetadataV1Builder) WithCurrentSnapshotID(currentSnapshotID int64) *MetadataV1Builder {
	id := currentSnapshotID // copy the value to prevent modification after build
	b.CurrentSnapshotID = &id
	return b
}

// WithSnapshots sets the optional snapshots field of the metadata.
func (b *MetadataV1Builder) WithSnapshots(snapshots []Snapshot) *MetadataV1Builder {
	b.SnapshotList = snapshots
	return b
}

// WithSnapshotLog sets the optional snapshot-log field of the metadata.
func (b *MetadataV1Builder) WithSnapshotLog(snapshotLog []SnapshotLogEntry) *MetadataV1Builder {
	b.SnapshotLog = snapshotLog
	return b
}

// WithMetadataLog sets the optional metadata-log field of the metadata.
func (b *MetadataV1Builder) WithMetadataLog(metadataLog []MetadataLogEntry) *MetadataV1Builder {
	b.MetadataLog = metadataLog
	return b
}

// WithSortOrders sets the optional sort-orders field of the metadata.
func (b *MetadataV1Builder) WithSortOrders(sortOrders []SortOrder) *MetadataV1Builder {
	b.SortOrderList = sortOrders
	return b
}

// WithDefaultSortOrderID sets the optional default-sort-order-id field of the metadata.
func (b *MetadataV1Builder) WithDefaultSortOrderID(defaultSortOrderID int) *MetadataV1Builder {
	b.DefaultSortOrderID = defaultSortOrderID
	return b
}

// TODO: implement setting table statistics field

func (b *MetadataV1Builder) Build() Metadata {
	return b.MetadataV1
}
