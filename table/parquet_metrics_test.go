package table_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func parquetMetaFromSchema(t *testing.T, sch *iceberg.Schema, jsonRows string) ([]byte, *metadata.FileMetaData) {
	t.Helper()

	arrowSch, err := table.SchemaToArrowSchema(sch, nil, true, false)
	require.NoError(t, err)

	rec, _, err := array.RecordFromJSON(memory.DefaultAllocator, arrowSch, strings.NewReader(jsonRows))
	require.NoError(t, err)
	defer rec.Release()

	var buf bytes.Buffer
	wr, err := pqarrow.NewFileWriter(arrowSch, &buf,
		parquet.NewWriterProperties(parquet.WithStats(true)),
		pqarrow.DefaultWriterProps())
	require.NoError(t, err)
	require.NoError(t, wr.Write(rec))
	require.NoError(t, wr.Close())

	rdr, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	defer rdr.Close()

	return buf.Bytes(), rdr.MetaData()
}

func TestDataFileFromParquetMetadata_DataFile(t *testing.T) {
	sch := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: false},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
	spec := iceberg.NewPartitionSpec()

	const jsonRows = `[
		{"id": 1, "name": "alpha"},
		{"id": 2, "name": "beta"},
		{"id": 3, "name": "gamma"}
	]`

	pqBytes, pqMeta := parquetMetaFromSchema(t, sch, jsonRows)

	const filePath = "s3://bucket/data/test-data-001.parquet"

	df, err := table.DataFileFromParquetMetadata(table.ParquetDataFileArgs{
		Schema:          sch,
		Spec:            spec,
		ParquetMetadata: pqMeta,
		FilePath:        filePath,
		FileSize:        int64(len(pqBytes)),
		Content:         iceberg.EntryContentData,
	})
	require.NoError(t, err)
	require.NotNil(t, df)

	assert.Equal(t, iceberg.EntryContentData, df.ContentType())
	assert.Equal(t, iceberg.ParquetFile, df.FileFormat())
	assert.Equal(t, filePath, df.FilePath())
	assert.Equal(t, int64(len(pqBytes)), df.FileSizeBytes())
	assert.EqualValues(t, 3, df.Count())

	assert.Len(t, df.ValueCounts(), 2, "one value-count entry per column")
	assert.Len(t, df.LowerBoundValues(), 2, "one lower-bound entry per column")
	assert.Len(t, df.UpperBoundValues(), 2, "one upper-bound entry per column")

	assert.Nil(t, df.EqualityFieldIDs(), "data files must not have equality field IDs")
}

func TestDataFileFromParquetMetadata_EqualityDeleteFile(t *testing.T) {
	keySchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
	)
	spec := iceberg.NewPartitionSpec()

	pqBytes, pqMeta := parquetMetaFromSchema(t, keySchema, `[{"id": 10}, {"id": 20}]`)

	const filePath = "s3://bucket/data/eq-del-001.parquet"

	df, err := table.DataFileFromParquetMetadata(table.ParquetDataFileArgs{
		Schema:           keySchema,
		Spec:             spec,
		ParquetMetadata:  pqMeta,
		FilePath:         filePath,
		FileSize:         int64(len(pqBytes)),
		Content:          iceberg.EntryContentEqDeletes,
		EqualityFieldIDs: []int{1},
	})
	require.NoError(t, err)
	require.NotNil(t, df)

	assert.Equal(t, iceberg.EntryContentEqDeletes, df.ContentType())
	assert.Equal(t, iceberg.ParquetFile, df.FileFormat())
	assert.Equal(t, filePath, df.FilePath())
	assert.EqualValues(t, 2, df.Count())
	assert.Equal(t, []int{1}, df.EqualityFieldIDs())
}

func TestDataFileFromParquetMetadata_PositionalDeleteFile(t *testing.T) {
	spec := iceberg.NewPartitionSpec()

	const dataFilePath = "s3://bucket/data/data-001.parquet"
	pqBytes, pqMeta := parquetMetaFromSchema(
		t,
		iceberg.PositionalDeleteSchema,
		`[
			{"file_path": "`+dataFilePath+`", "pos": 0},
			{"file_path": "`+dataFilePath+`", "pos": 5}
		]`,
	)

	const filePath = "s3://bucket/data/pos-del-001.parquet"

	df, err := table.DataFileFromParquetMetadata(table.ParquetDataFileArgs{
		Schema:          iceberg.PositionalDeleteSchema,
		Spec:            spec,
		ParquetMetadata: pqMeta,
		FilePath:        filePath,
		FileSize:        int64(len(pqBytes)),
		Content:         iceberg.EntryContentPosDeletes,
	})
	require.NoError(t, err)
	require.NotNil(t, df)

	assert.Equal(t, iceberg.EntryContentPosDeletes, df.ContentType())
	assert.Equal(t, iceberg.ParquetFile, df.FileFormat())
	assert.Equal(t, filePath, df.FilePath())
	assert.EqualValues(t, 2, df.Count())
	assert.Nil(t, df.EqualityFieldIDs(), "positional delete files must not have equality field IDs")
}
