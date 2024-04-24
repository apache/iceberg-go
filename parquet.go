package iceberg

import (
	"fmt"
	"io"

	"github.com/parquet-go/parquet-go"
)

func ManifestEntryV1FromParquet(path string, size int64, r io.ReaderAt) (ManifestEntry, *Schema, error) {
	df, schema, err := DataFileFromParquet(path, size, r)
	if err != nil {
		return nil, nil, err
	}

	return NewManifestEntryV1(EntryStatusADDED, size, df), schema, nil
}

func DataFileFromParquet(path string, size int64, r io.ReaderAt) (DataFile, *Schema, error) {
	f, err := parquet.OpenFile(r, size)
	if err != nil {
		return nil, nil, err
	}

	bldr := NewDataFileV1Builder(
		path,
		ParquetFile,
		map[string]any{}, // TODO: At present Parquet writes are assumed to be unpartitioned.
		f.NumRows(),
		size,
	)

	// Create the upper and lower bounds for each column.
	numColumns := len(f.Metadata().RowGroups[0].Columns)
	upper, lower := make(map[int][]byte, numColumns), make(map[int][]byte, numColumns)
	for i := 0; i < numColumns; i++ {
		upper[i] = maxColValue(i, f)
		lower[i] = minColValue(i, f)
	}

	bldr.WithLowerBounds(lower)
	bldr.WithUpperBounds(upper)
	bldr.WithColumnSizes(colSizes(f))

	// Create the schema.
	return bldr.Build(), parquetSchemaToIcebergSchema(-1, f.Schema()), nil
}

func colSizes(f *parquet.File) map[int]int64 {
	sizes := make(map[int]int64, len(f.Metadata().RowGroups[0].Columns))
	for _, rg := range f.Metadata().RowGroups {
		for i, chunk := range rg.Columns {
			sizes[i] += chunk.MetaData.TotalUncompressedSize
		}
	}
	return sizes
}

// maxColValue returns the maximum value of a column in a parquet file.
func maxColValue(col int, r *parquet.File) []byte {
	var (
		maxval                   parquet.Value
		foundRowGroup, foundPage int
	)
	for i, rg := range r.RowGroups() {
		index, err := rg.ColumnChunks()[col].ColumnIndex()
		if err != nil {
			return nil
		}

		for j := 0; j < index.NumPages(); j++ {
			v := index.MaxValue(j)
			if maxval.IsNull() {
				maxval = v
				foundPage = j
				foundRowGroup = i
				continue
			}

			if compare(maxval, v) == -1 {
				foundPage = j
				foundRowGroup = i
				maxval = v
			}
		}
	}

	// Find the bytes representation of the max value.
	numColumns := len(r.Metadata().RowGroups[0].Columns)
	return r.ColumnIndexes()[(foundRowGroup*numColumns)+col].MaxValues[foundPage]
}

// minColValue returns the minimum value of a column in a parquet file.
func minColValue(col int, r *parquet.File) []byte {
	var (
		minval                   parquet.Value
		foundRowGroup, foundPage int
	)
	for i, rg := range r.RowGroups() {
		index, err := rg.ColumnChunks()[col].ColumnIndex()
		if err != nil {
			return nil
		}

		for j := 0; j < index.NumPages(); j++ {
			v := index.MinValue(j)
			if minval.IsNull() {
				minval = v
				foundPage = j
				foundRowGroup = i
				continue
			}

			if compare(minval, v) == 1 {
				foundPage = j
				foundRowGroup = i
				minval = v
			}
		}
	}

	// Find the bytes representation of the min value.
	numColumns := len(r.Metadata().RowGroups[0].Columns)
	return r.ColumnIndexes()[(foundRowGroup*numColumns)+col].MinValues[foundPage]
}

// compares two parquet values. 0 if they are equal, -1 if v1 < v2, 1 if v1 > v2.
func compare(v1, v2 parquet.Value) int {
	switch v1.Kind() {
	case parquet.Int32:
		return parquet.Int32Type.Compare(v1, v2)
	case parquet.Int64:
		return parquet.Int64Type.Compare(v1, v2)
	case parquet.Float:
		return parquet.FloatType.Compare(v1, v2)
	case parquet.Double:
		return parquet.DoubleType.Compare(v1, v2)
	case parquet.ByteArray, parquet.FixedLenByteArray:
		return parquet.ByteArrayType.Compare(v1, v2)
	case parquet.Boolean:
		return parquet.BooleanType.Compare(v1, v2)
	default:
		panic(fmt.Sprintf("unsupported value comparison: %v", v1.Kind()))
	}
}

func parquetSchemaToIcebergSchema(id int, schema *parquet.Schema) *Schema {
	fields := make([]NestedField, 0, len(schema.Fields()))
	for i, f := range schema.Fields() {
		fields = append(fields, NestedField{
			Type:     parquetTypeToIcebergType(f.Type()),
			ID:       i,
			Name:     f.Name(),
			Required: f.Required(),
		})
	}
	return NewSchema(id, fields...)
}

func parquetTypeToIcebergType(t parquet.Type) Type {
	switch t.Kind() {
	case parquet.Boolean:
		return BooleanType{}
	case parquet.Int32:
		return Int32Type{}
	case parquet.Int64:
		return Int64Type{}
	case parquet.Float:
		return Float32Type{}
	case parquet.Double:
		return Float64Type{}
	case parquet.ByteArray:
		return BinaryType{}
	default:
		panic(fmt.Sprintf("unsupported parquet type: %v", t.Kind()))
	}
}
