package iceberg

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"text/template"
	"time"

	"github.com/hamba/avro/v2/ocf"
)

func NewManifestEntryV1(entryStatus ManifestEntryStatus, snapshotID int64, data DataFile) ManifestEntry {
	return &manifestEntryV1{
		EntryStatus: entryStatus,
		Snapshot:    snapshotID,
		Data:        data,
	}
}

type DataFileBuilder struct {
	*dataFile
}

func NewDataFileV1Builder(
	FilePath string,
	FileFormat FileFormat,
	PartitionSpec map[string]any,
	RecordCount int64,
	FileSizeBytes int64,
) DataFileBuilder {
	return DataFileBuilder{
		dataFile: &dataFile{
			Path:          FilePath,
			Format:        FileFormat,
			PartitionData: PartitionSpec,
			RecordCount:   RecordCount,
			FileSize:      FileSizeBytes,
		},
	}
}

func (builder DataFileBuilder) Build() DataFile {
	return builder.dataFile
}

func (builder DataFileBuilder) WithColumnSizes(columnSizes map[int]int64) DataFileBuilder {
	builder.ColSizes = avroColMapFromMap[int, int64](columnSizes)
	return builder
}

func (builder DataFileBuilder) WithValueCounts(valueCounts map[int]int64) DataFileBuilder {
	builder.ValCounts = avroColMapFromMap[int, int64](valueCounts)
	return builder
}

func (builder DataFileBuilder) WithNullValueCounts(nullValueCounts map[int]int64) DataFileBuilder {
	builder.NullCounts = avroColMapFromMap[int, int64](nullValueCounts)
	return builder
}

func (builder DataFileBuilder) WithNanValueCounts(nanValueCounts map[int]int64) DataFileBuilder {
	builder.NaNCounts = avroColMapFromMap[int, int64](nanValueCounts)
	return builder
}

func (builder DataFileBuilder) WithDistinctCounts(distinctCounts map[int]int64) DataFileBuilder {
	builder.DistinctCounts = avroColMapFromMap[int, int64](distinctCounts)
	return builder
}

func (builder DataFileBuilder) WithLowerBounds(lowerBounds map[int][]byte) DataFileBuilder {
	builder.LowerBounds = avroColMapFromMap[int, []byte](lowerBounds)
	return builder
}

func (builder DataFileBuilder) WithUpperBounds(upperBounds map[int][]byte) DataFileBuilder {
	builder.UpperBounds = avroColMapFromMap[int, []byte](upperBounds)
	return builder
}

func (builder DataFileBuilder) WithKeyMetadata(keyMetadata []byte) DataFileBuilder {
	builder.Key = &keyMetadata
	return builder
}

func (builder DataFileBuilder) WithSplitOffsets(splitOffsets []int64) DataFileBuilder {
	builder.Splits = &splitOffsets
	return builder
}

func (builder DataFileBuilder) WithSortOrderID(sortOrderID int) DataFileBuilder {
	builder.SortOrder = &sortOrderID
	return builder
}

func WriteManifestListV1(w io.Writer, files []ManifestFile) error {
	enc, err := ocf.NewEncoder(
		AvroManifestListV1Schema,
		w,
		ocf.WithMetadata(map[string][]byte{
			"avro.codec": []byte("deflate"),
		}),
		ocf.WithCodec(ocf.Deflate),
	)
	if err != nil {
		return err
	}
	defer enc.Close()

	for _, file := range files {
		if err := enc.Encode(file); err != nil {
			return err
		}
	}

	return nil
}

func WriteManifestV1(w io.Writer, schema *Schema, entries []ManifestEntry) error {
	b, err := json.Marshal(schema)
	if err != nil {
		return fmt.Errorf("failed to marshal schema: %w", err)
	}

	enc, err := ocf.NewEncoder(
		AvroSchemaFromEntriesV1(entries),
		w,
		ocf.WithMetadata(map[string][]byte{
			"format-version": []byte("1"),
			"schema":         b,
			//"partition-spec": []byte("todo"), // TODO
			"avro.codec": []byte("deflate"),
		}),
		ocf.WithCodec(ocf.Deflate),
	)
	if err != nil {
		return err
	}
	defer enc.Close()

	for _, entry := range entries {
		if err := enc.Encode(entry); err != nil {
			return err
		}
	}

	return nil
}

// AvroSchemaFromEntriesV1 creates an Avro schema from the given manifest entries.
// The entries must all share the same partition spec.
func AvroSchemaFromEntriesV1(entries []ManifestEntry) string {
	partitions := entries[0].DataFile().Partition() // Pull the first entries partition spec since they are expected to be the same for all entries.
	partitionFieldID := 1000                        // According to the spec partition field IDs start at 1000. https://iceberg.apache.org/spec/#partition-evolution
	b := &bytes.Buffer{}
	if err := template.Must(
		template.New("EntryV1Schema").
			Funcs(template.FuncMap{
				"PartitionFieldID": func(_ any) int {
					prev := partitionFieldID
					partitionFieldID++
					return prev
				},
				"Type": func(i any) string {
					switch t := i.(type) {
					case string:
						return `["null", "string"]`
					case int:
						return `["null", "int"]`
					case int64:
						return `["null", "long"]`
					case []byte:
						return `["null", "bytes"]`
					case time.Time:
						return `["null", {"type": "int", "logicalType": "date"}]`
					default:
						panic(fmt.Sprintf("unsupported type %T", t))
					}
				},
			}).
			Parse(AvroEntryV1SchemaTmpl)).Execute(b, partitions); err != nil {
		panic(err)
	}

	return b.String()
}
