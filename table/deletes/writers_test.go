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

package deletes

import (
	"errors"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table/internal"
	"github.com/stretchr/testify/assert"
)

// Mock filesystem for testing
type mockFS struct{}

func (fs *mockFS) Open(name string) (iceio.File, error) {
	return nil, errors.New("mock filesystem: open not supported")
}

func (fs *mockFS) Create(name string) (iceio.FileWriter, error) {
	return nil, errors.New("mock filesystem: create not supported")
}

func (fs *mockFS) Remove(name string) error {
	return errors.New("mock filesystem: remove not supported")
}

func (fs *mockFS) WriteFile(name string, data []byte) error {
	return errors.New("mock filesystem: write file not supported")
}

// Mock location provider for testing
type mockLocationProvider struct {
	counter int
}

func (p *mockLocationProvider) NewDeleteLocation(extension string) string {
	p.counter++
	return fmt.Sprintf("s3://bucket/delete-%d.%s", p.counter, extension)
}

func createTestSchema() *iceberg.Schema {
	return iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false},
		iceberg.NestedField{ID: 3, Name: "price", Type: iceberg.PrimitiveTypes.Float64, Required: false},
	)
}

func TestNewEqualityDeleteWriter_Creation(t *testing.T) {
	fs := &mockFS{}
	locProvider := &mockLocationProvider{}
	schema := createTestSchema()
	equalityFields := []int{1, 2} // id and name
	fileFormat := internal.GetFileFormat(iceberg.ParquetFile)
	writeProps := fileFormat.GetWriteProperties(nil)
	mem := memory.NewGoAllocator()

	writer := NewEqualityDeleteWriter(
		fs, locProvider, schema, equalityFields,
		fileFormat, writeProps, mem,
	)

	assert.NotNil(t, writer)
	assert.Equal(t, fs, writer.fs)
	assert.Equal(t, locProvider, writer.locProvider)
	assert.Equal(t, equalityFields, writer.equalityFields)
	assert.Equal(t, fileFormat, writer.fileFormat)
	assert.Equal(t, 1000, writer.maxBatchSize)
	assert.Empty(t, writer.writtenFiles)
	assert.Empty(t, writer.currentBatch)
}

func TestNewPositionDeleteWriter_Creation(t *testing.T) {
	fs := &mockFS{}
	locProvider := &mockLocationProvider{}
	schema := createTestSchema()
	writeProps := make(iceberg.Properties)

	writer := NewPositionDeleteWriter(fs, locProvider, schema, writeProps)

	assert.NotNil(t, writer)
	assert.Equal(t, fs, writer.fs)
	assert.Equal(t, locProvider, writer.locProvider)
	assert.Equal(t, 1000, writer.maxBatchSize)
	assert.Empty(t, writer.writtenFiles)
	assert.Empty(t, writer.currentBatch)

	// Check that position delete schema has the correct fields
	fields := writer.schema.Fields()
	assert.Len(t, fields, 2)
	assert.Equal(t, "file_path", fields[0].Name)
	assert.Equal(t, iceberg.PrimitiveTypes.String, fields[0].Type)
	assert.Equal(t, "pos", fields[1].Name)
	assert.Equal(t, iceberg.PrimitiveTypes.Int64, fields[1].Type)
}

func TestRollingEqualityDeleteWriter_Creation(t *testing.T) {
	fs := &mockFS{}
	locProvider := &mockLocationProvider{}
	schema := createTestSchema()
	equalityFields := []int{1} // id only
	fileFormat := internal.GetFileFormat(iceberg.ParquetFile)
	writeProps := fileFormat.GetWriteProperties(nil)
	mem := memory.NewGoAllocator()
	maxFileSize := int64(1024 * 1024) // 1MB

	writer := NewRollingEqualityDeleteWriter(
		fs, locProvider, schema, equalityFields,
		fileFormat, writeProps, mem, maxFileSize,
	)

	assert.NotNil(t, writer)
	assert.Equal(t, fs, writer.fs)
	assert.Equal(t, locProvider, writer.locProvider)
	assert.Equal(t, schema, writer.tableSchema)
	assert.Equal(t, equalityFields, writer.equalityFields)
	assert.Equal(t, maxFileSize, writer.maxFileSize)
	assert.Empty(t, writer.writtenFiles)
	assert.Nil(t, writer.currentWriter)
}

func TestEqualityDelete_Creation(t *testing.T) {
	filePath := "s3://bucket/data-001.parquet"
	fieldValues := map[int]interface{}{
		1: int64(123),
		2: "test_name",
	}

	delete := NewEqualityDelete(filePath, fieldValues)

	assert.NotNil(t, delete)
	assert.Equal(t, filePath, delete.FilePath)
	assert.Equal(t, fieldValues, delete.Values)

	// Test string representation
	str := delete.String()
	assert.Contains(t, str, filePath)
	assert.Contains(t, str, "EqualityDelete")
}

func TestEqualityDelete_Methods(t *testing.T) {
	delete := NewEqualityDelete("test.parquet", nil)

	// Test AddFieldValue
	delete.AddFieldValue(1, int64(100))
	delete.AddFieldValue(2, "name")

	assert.Len(t, delete.Values, 2)

	// Test GetFieldValue
	value, exists := delete.GetFieldValue(1)
	assert.True(t, exists)
	assert.Equal(t, int64(100), value)

	_, exists = delete.GetFieldValue(999)
	assert.False(t, exists)

	// Test GetEqualityFields
	fields := delete.GetEqualityFields()
	assert.Len(t, fields, 2)
	assert.Contains(t, fields, 1)
	assert.Contains(t, fields, 2)

	// Test ValidateAgainstSchema
	schema := createTestSchema()
	err := delete.ValidateAgainstSchema(schema)
	assert.NoError(t, err)

	// Test validation with invalid field
	delete.AddFieldValue(999, "invalid")
	err = delete.ValidateAgainstSchema(schema)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "field ID 999 not found")
}

func TestPositionDelete_Creation(t *testing.T) {
	filePath := "s3://bucket/data-001.parquet"
	position := int64(42)

	delete := NewPositionDelete(filePath, position)

	assert.NotNil(t, delete)
	assert.Equal(t, filePath, delete.FilePath)
	assert.Equal(t, position, delete.Position)

	// Test string representation
	str := delete.String()
	assert.Contains(t, str, filePath)
	assert.Contains(t, str, "42")
	assert.Contains(t, str, "PositionDelete")

	// Test validation
	assert.True(t, delete.IsValid())

	// Test invalid cases
	invalidDelete1 := NewPositionDelete("", position)
	assert.False(t, invalidDelete1.IsValid())

	invalidDelete2 := NewPositionDelete(filePath, -1)
	assert.False(t, invalidDelete2.IsValid())
}

func TestDeleteWriterFactory_Creation(t *testing.T) {
	fs := &mockFS{}
	locProvider := &mockLocationProvider{}
	schema := createTestSchema()
	equalityFields := []int{1}
	mem := memory.NewGoAllocator()

	config := DefaultDeleteWriterConfig(fs, locProvider, schema, equalityFields, mem)
	factory := NewDeleteWriterFactory(config)

	assert.NotNil(t, factory)
	assert.Equal(t, config, factory.config)
}

func TestDeleteWriterFactory_CreateEqualityDeleteWriter(t *testing.T) {
	fs := &mockFS{}
	locProvider := &mockLocationProvider{}
	schema := createTestSchema()
	equalityFields := []int{1}
	mem := memory.NewGoAllocator()

	// Test basic writer creation
	config := DefaultDeleteWriterConfig(fs, locProvider, schema, equalityFields, mem)
	config.WriterType = BasicDeleteWriter
	factory := NewDeleteWriterFactory(config)

	writer, err := factory.CreateEqualityDeleteWriter()
	assert.NoError(t, err)
	assert.NotNil(t, writer)

	// Test rolling writer creation
	config.WriterType = RollingDeleteWriter
	factory = NewDeleteWriterFactory(config)

	writer, err = factory.CreateEqualityDeleteWriter()
	assert.NoError(t, err)
	assert.NotNil(t, writer)

	// Test error case - no equality fields
	config.EqualityFields = []int{}
	factory = NewDeleteWriterFactory(config)

	writer, err = factory.CreateEqualityDeleteWriter()
	assert.Error(t, err)
	assert.Nil(t, writer)
	assert.Contains(t, err.Error(), "equality fields must be specified")

	// Test unsupported writer type
	config.EqualityFields = equalityFields
	config.WriterType = ClusteredDeleteWriter
	factory = NewDeleteWriterFactory(config)

	writer, err = factory.CreateEqualityDeleteWriter()
	assert.Error(t, err)
	assert.Nil(t, writer)
	assert.Contains(t, err.Error(), "not yet implemented")
}

func TestSimpleLocationProvider_Creation(t *testing.T) {
	baseLocation := "s3://bucket/table"
	provider := NewSimpleLocationProvider(baseLocation)

	assert.NotNil(t, provider)
	assert.Equal(t, baseLocation, provider.baseLocation)
	assert.Equal(t, 0, provider.fileCounter)

	// Test location generation
	loc1 := provider.NewDeleteLocation("parquet")
	assert.Equal(t, "s3://bucket/table/delete-1.parquet", loc1)

	loc2 := provider.NewDeleteLocation("parquet")
	assert.Equal(t, "s3://bucket/table/delete-2.parquet", loc2)

	loc3 := provider.NewDeleteLocation("orc")
	assert.Equal(t, "s3://bucket/table/delete-3.orc", loc3)
}

func TestEqualityDeleteBatchWriter_Creation(t *testing.T) {
	fs := &mockFS{}
	locProvider := &mockLocationProvider{}
	schema := createTestSchema()
	equalityFields := []int{1}
	fileFormat := internal.GetFileFormat(iceberg.ParquetFile)
	writeProps := fileFormat.GetWriteProperties(nil)
	mem := memory.NewGoAllocator()

	underlyingWriter := NewEqualityDeleteWriter(
		fs, locProvider, schema, equalityFields,
		fileFormat, writeProps, mem,
	)

	batchWriter := NewEqualityDeleteBatchWriter(underlyingWriter, 100)

	assert.NotNil(t, batchWriter)
	assert.Equal(t, underlyingWriter, batchWriter.writer)
	assert.Equal(t, 100, batchWriter.batchSize)
	assert.Empty(t, batchWriter.batch)

	// Test default batch size
	batchWriter2 := NewEqualityDeleteBatchWriter(underlyingWriter, 0)
	assert.Equal(t, 1000, batchWriter2.batchSize)
}

func TestDefaultDeleteWriterConfig_Creation(t *testing.T) {
	fs := &mockFS{}
	locProvider := &mockLocationProvider{}
	schema := createTestSchema()
	equalityFields := []int{1, 2}
	mem := memory.NewGoAllocator()

	config := DefaultDeleteWriterConfig(fs, locProvider, schema, equalityFields, mem)

	assert.Equal(t, BasicDeleteWriter, config.WriterType)
	assert.Equal(t, fs, config.FileSystem)
	assert.Equal(t, locProvider, config.LocationProvider)
	assert.Equal(t, schema, config.TableSchema)
	assert.Equal(t, equalityFields, config.EqualityFields)
	assert.NotNil(t, config.FileFormat)
	assert.NotNil(t, config.WriteProperties)
	assert.Equal(t, mem, config.MemoryAllocator)
	assert.Equal(t, int64(128*1024*1024), config.MaxFileSize) // 128MB
} 