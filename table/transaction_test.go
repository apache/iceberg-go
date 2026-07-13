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

//go:build integration

package table_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/decimal"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/apache/arrow-go/v18/parquet/variant"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/internal/recipe"
	iceio "github.com/apache/iceberg-go/io"
	_ "github.com/apache/iceberg-go/io/gocloud"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go/modules/compose"
)

type SparkIntegrationTestSuite struct {
	suite.Suite

	ctx   context.Context
	cat   catalog.Catalog
	props iceberg.Properties
	stack *compose.DockerCompose
}

func (s *SparkIntegrationTestSuite) SetupSuite() {
	var err error
	s.stack, err = recipe.Start(s.T())
	s.Require().NoError(err)
}

func (s *SparkIntegrationTestSuite) SetupTest() {
	s.ctx = context.Background()
	s.props = iceberg.Properties{
		iceio.S3Region:          "us-east-1",
		iceio.S3EndpointURL:     "http://localhost:9000",
		iceio.S3AccessKeyID:     "admin",
		iceio.S3SecretAccessKey: "password",
	}

	cat, err := rest.NewCatalog(s.ctx, "rest", "http://localhost:8181", rest.WithAdditionalProps(s.props))
	s.Require().NoError(err)
	s.cat = cat
}

func (s *SparkIntegrationTestSuite) requireSpark4() {
	s.T().Helper()
	major, err := recipe.SparkMajorVersion()
	s.Require().NoError(err, "spark version extraction failed")
	if major < 4 {
		s.T().Skipf("requires Spark 4+ (running Spark %d)", major)
	}
}

func (s *SparkIntegrationTestSuite) TestSetProperties() {
	icebergSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "foo", Type: iceberg.PrimitiveTypes.Bool},
		iceberg.NestedField{ID: 2, Name: "bar", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 3, Name: "baz", Type: iceberg.PrimitiveTypes.Int32},
	)

	tbl, err := s.cat.CreateTable(s.ctx, catalog.ToIdentifier("default", "go_test_set_properties"), icebergSchema)
	s.Require().NoError(err)

	tx := tbl.NewTransaction()

	err = tx.SetProperties(iceberg.Properties{
		table.ParquetCompressionKey:      "snappy",
		table.ManifestMergeEnabledKey:    "true",
		table.ManifestMinMergeCountKey:   "1",
		table.ManifestTargetSizeBytesKey: "1",
	})
	s.Require().NoError(err)

	_, err = tx.Commit(s.ctx)
	s.Require().NoError(err)

	output, err := recipe.ExecuteSpark(s.T(), "./validation.py", "--sql", "SHOW TBLPROPERTIES default.go_test_set_properties")
	s.Require().NoError(err)
	s.Require().Contains(output, `+----------------------------------+---------------+
|key                               |value          |
+----------------------------------+---------------+
|commit.manifest-merge.enabled     |true           |
|commit.manifest.min-count-to-merge|1              |
|commit.manifest.target-size-bytes |1              |
|current-snapshot-id               |none           |
|format                            |iceberg/parquet|
|format-version                    |2              |
|write.parquet.compression-codec   |snappy         |
+----------------------------------+---------------+`)
}

func (s *SparkIntegrationTestSuite) TestAddFile() {
	const filename = "s3://warehouse/default/test_partitioned_by_days/data/ts_day=2023-03-13/supertest.parquet"

	tbl, err := s.cat.LoadTable(s.ctx, catalog.ToIdentifier("default", "test_partitioned_by_days"))
	s.Require().NoError(err)

	sc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	if err != nil {
		panic(err)
	}

	bldr := array.NewRecordBuilder(memory.DefaultAllocator, sc)
	defer bldr.Release()

	tm := time.Date(2023, 0o3, 13, 13, 22, 0, 0, time.UTC)
	ts, _ := arrow.TimestampFromTime(tm, arrow.Microsecond)
	bldr.Field(0).(*array.Date32Builder).Append(arrow.Date32FromTime(tm))
	bldr.Field(1).(*array.TimestampBuilder).Append(ts)
	bldr.Field(2).(*array.Int32Builder).Append(13)
	bldr.Field(3).(*array.StringBuilder).Append("m")

	rec := bldr.NewRecordBatch()
	defer rec.Release()

	fw, err := mustFS(s.T(), tbl).(iceio.WriteFileIO).Create(filename)
	if err != nil {
		panic(err)
	}
	defer fw.Close()

	if err := pqarrow.WriteTable(array.NewTableFromRecords(sc, []arrow.RecordBatch{rec}), fw, rec.NumRows(), parquet.NewWriterProperties(), pqarrow.DefaultWriterProps()); err != nil {
		panic(err)
	}

	tx := tbl.NewTransaction()
	err = tx.AddFiles(s.ctx, []string{filename}, nil, false)
	s.Require().NoError(err)

	tbl, err = tx.Commit(s.ctx)
	s.Require().NoError(err)

	expectedOutput := `
+--------+
|count(1)|
+--------+
|13      |
+--------+
`

	output, err := recipe.ExecuteSpark(s.T(), "./validation.py", "--sql", "SELECT COUNT(*) FROM default.test_partitioned_by_days")
	s.Require().NoError(err)
	s.Require().True(
		strings.HasSuffix(strings.TrimSpace(output), strings.TrimSpace(expectedOutput)),
		"result does not contain expected output: %s", expectedOutput,
	)
}

func (s *SparkIntegrationTestSuite) TestDifferentDataTypes() {
	icebergSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "bool", Type: iceberg.PrimitiveTypes.Bool},
		iceberg.NestedField{ID: 2, Name: "string", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 3, Name: "string_long", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 4, Name: "int", Type: iceberg.PrimitiveTypes.Int32},
		iceberg.NestedField{ID: 5, Name: "long", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 6, Name: "float", Type: iceberg.PrimitiveTypes.Float32},
		iceberg.NestedField{ID: 7, Name: "double", Type: iceberg.PrimitiveTypes.Float64},
		iceberg.NestedField{ID: 8, Name: "timestamp", Type: iceberg.PrimitiveTypes.Timestamp},
		iceberg.NestedField{ID: 9, Name: "timestamptz", Type: iceberg.PrimitiveTypes.TimestampTz},
		iceberg.NestedField{ID: 10, Name: "date", Type: iceberg.PrimitiveTypes.Date},
		iceberg.NestedField{ID: 11, Name: "uuid", Type: iceberg.PrimitiveTypes.UUID},
		iceberg.NestedField{ID: 12, Name: "binary", Type: iceberg.PrimitiveTypes.Binary},
		iceberg.NestedField{ID: 13, Name: "fixed", Type: iceberg.FixedTypeOf(16)},
		iceberg.NestedField{ID: 14, Name: "small_dec", Type: iceberg.DecimalTypeOf(8, 2)},
		iceberg.NestedField{ID: 15, Name: "med_dec", Type: iceberg.DecimalTypeOf(16, 2)},
		iceberg.NestedField{ID: 16, Name: "large_dec", Type: iceberg.DecimalTypeOf(24, 2)},
		iceberg.NestedField{
			ID: 17, Name: "list", Type: &iceberg.ListType{
				ElementID: 18,
				Element:   iceberg.PrimitiveTypes.Int32,
			},
		},
	)

	arrowSchema, err := table.SchemaToArrowSchema(icebergSchema, nil, true, false)
	s.Require().NoError(err)

	arrTable, err := array.TableFromJSON(memory.DefaultAllocator, arrowSchema, []string{
		`[
			{
				"bool": false,
				"string": "a",
				"string_long": "` + strings.Repeat("a", 22) + `",
				"int": 1,
				"long": 1,
				"float": 0.0,
				"double": 0.0,
				"timestamp": "2023-01-01T19:25:00.000000+08:00",
				"timestamptz": "2023-01-01T19:25:00.000000Z",
				"date": "2023-01-01",
				"uuid": "00000000-0000-0000-0000-000000000000",
				"binary": "AQ==",
				"fixed": "AAAAAAAAAAAAAAAAAAAAAA==",
				"small_dec": "123456.78",
				"med_dec": "12345678901234.56",
				"large_dec": "1234567890123456789012.34",
				"list": [1, 2, 3]
			},
			{
				"bool": null,
				"string": null,
				"string_long": null,
				"int": null,
				"long": null,
				"float": null,
				"double": null,
				"timestamp": null,
				"timestamptz": null,
				"date": null,
				"uuid": null,
				"binary": null,
				"fixed": null,
				"small_dec": null,
				"med_dec": null,
				"large_dec": null,
				"list": null
			},
			{
				"bool": true,
				"string": "z",
				"string_long": "` + strings.Repeat("z", 22) + `",
				"int": 9,
				"long": 9,
				"float": 0.9,
				"double": 0.9,
				"timestamp": "2023-03-01T19:25:00.000000+08:00",
				"timestamptz": "2023-03-01T19:25:00.000000Z",
				"date": "2023-03-01",
				"uuid": "11111111-1111-1111-1111-111111111111",
				"binary": "Eg==",
				"fixed": "EREREREREREREREREREREQ==",
				"small_dec": "876543.21",
				"med_dec": "65432109876543.21",
				"large_dec": "4321098765432109876543.21",
				"list": [-1, -2, -3]
			}
		 ]`,
	})
	s.Require().NoError(err)
	defer arrTable.Release()

	tbl, err := s.cat.CreateTable(s.ctx, catalog.ToIdentifier("default", "go_test_different_data_types"), icebergSchema)
	s.Require().NoError(err)

	tx := tbl.NewTransaction()
	err = tx.AppendTable(s.ctx, arrTable, 1, nil)
	s.Require().NoError(err)

	_, err = tx.Commit(s.ctx)
	s.Require().NoError(err)

	output, err := recipe.ExecuteSpark(s.T(), "./validation.py", "--sql", "DESCRIBE TABLE EXTENDED default.go_test_different_data_types")
	s.Require().NoError(err)
	s.Require().Contains(output, `+------------------+-------------+-------+
|col_name          |data_type    |comment|
+------------------+-------------+-------+
|bool              |boolean      |NULL   |
|string            |string       |NULL   |
|string_long       |string       |NULL   |
|int               |int          |NULL   |
|long              |bigint       |NULL   |
|float             |float        |NULL   |
|double            |double       |NULL   |
|timestamp         |timestamp_ntz|NULL   |
|timestamptz       |timestamp    |NULL   |
|date              |date         |NULL   |
|uuid              |string       |NULL   |
|binary            |binary       |NULL   |
|fixed             |binary       |NULL   |
|small_dec         |decimal(8,2) |NULL   |
|med_dec           |decimal(16,2)|NULL   |
|large_dec         |decimal(24,2)|NULL   |
|list              |array<int>   |NULL   |`)

	output, err = recipe.ExecuteSpark(s.T(), "./validation.py", "--sql", "SELECT * FROM default.go_test_different_data_types")
	s.Require().NoError(err)
	s.Require().Contains(output, `+-----+------+----------------------+----+----+-----+------+-------------------+-------------------+----------+------------------------------------+------+-------------------------------------------------+---------+-----------------+-------------------------+------------+
|bool |string|string_long           |int |long|float|double|timestamp          |timestamptz        |date      |uuid                                |binary|fixed                                            |small_dec|med_dec          |large_dec                |list        |
+-----+------+----------------------+----+----+-----+------+-------------------+-------------------+----------+------------------------------------+------+-------------------------------------------------+---------+-----------------+-------------------------+------------+
|false|a     |aaaaaaaaaaaaaaaaaaaaaa|1   |1   |0.0  |0.0   |2023-01-01 11:25:00|2023-01-01 19:25:00|2023-01-01|00000000-0000-0000-0000-000000000000|[01]  |[00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00]|123456.78|12345678901234.56|1234567890123456789012.34|[1, 2, 3]   |
|NULL |NULL  |NULL                  |NULL|NULL|NULL |NULL  |NULL               |NULL               |NULL      |NULL                                |NULL  |NULL                                             |NULL     |NULL             |NULL                     |NULL        |
|true |z     |zzzzzzzzzzzzzzzzzzzzzz|9   |9   |0.9  |0.9   |2023-03-01 11:25:00|2023-03-01 19:25:00|2023-03-01|11111111-1111-1111-1111-111111111111|[12]  |[11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11]|876543.21|65432109876543.21|4321098765432109876543.21|[-1, -2, -3]|
+-----+------+----------------------+----+----+-----+------+-------------------+-------------------+----------+------------------------------------+------+-------------------------------------------------+---------+-----------------+-------------------------+------------+`)
}

func (s *SparkIntegrationTestSuite) TestUpdateSpec() {
	icebergSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "foo", Type: iceberg.PrimitiveTypes.Bool},
		iceberg.NestedField{ID: 2, Name: "bar", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 3, Name: "baz", Type: iceberg.PrimitiveTypes.Int32},
	)

	partitionSpec := iceberg.NewPartitionSpec(
		iceberg.PartitionField{SourceIDs: []int{2}, FieldID: 1000, Transform: iceberg.TruncateTransform{Width: 5}, Name: "bar_truncate"},
	)

	tbl, err := s.cat.CreateTable(
		s.ctx,
		catalog.ToIdentifier("default", "go_test_update_spec"),
		icebergSchema,
		catalog.WithPartitionSpec(&partitionSpec),
	)
	s.Require().NoError(err)

	tx := tbl.NewTransaction()
	err = tx.UpdateSpec(false).
		AddField("baz", iceberg.BucketTransform{NumBuckets: 3}, "").
		Commit()
	s.Require().NoError(err)
	_, err = tx.Commit(s.ctx)

	output, err := recipe.ExecuteSpark(s.T(), "./validation.py", "--sql", "DESCRIBE TABLE EXTENDED default.go_test_update_spec")
	s.Require().NoError(err)
	s.Require().Contains(output, `|# Partitioning              |                                            |       |
|Part 0                      |truncate(5, bar)                            |       |
|Part 1                      |bucket(3, baz)                              |       |
|                            |                                            |       |`)
}

func (s *SparkIntegrationTestSuite) TestVariantWriteAndScan() {
	s.requireSpark4()

	icebergSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "ts", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "event", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 3, Name: "payload", Type: iceberg.VariantType{}},
	)

	tbl, err := s.cat.CreateTable(
		s.ctx,
		catalog.ToIdentifier("default", "go_variant_events"),
		icebergSchema,
		catalog.WithProperties(iceberg.Properties{table.PropertyFormatVersion: "3"}),
	)
	s.Require().NoError(err)

	arrowSchema, err := table.SchemaToArrowSchema(icebergSchema, nil, true, false)
	s.Require().NoError(err)

	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(s.T(), 0)

	scanMem := memory.DefaultAllocator

	tsBldr := array.NewInt64Builder(mem)
	defer tsBldr.Release()
	evtBldr := array.NewStringBuilder(mem)
	defer evtBldr.Release()
	payloadBldr := extensions.NewVariantBuilder(mem, extensions.NewDefaultVariantType())
	defer payloadBldr.Release()

	mkVariant := func(v any) variant.Value {
		var b variant.Builder
		s.Require().NoError(b.Append(v))
		val, err := b.Build()
		s.Require().NoError(err)

		return val
	}

	tsBldr.Append(1713700000)
	evtBldr.Append("click")
	payloadBldr.Append(mkVariant(map[string]any{
		"x": int64(320), "y": int64(480), "target": "button-submit",
	}))

	tsBldr.Append(1713700005)
	evtBldr.Append("metric")
	payloadBldr.Append(mkVariant(float64(98.6)))

	tsBldr.Append(1713700010)
	evtBldr.Append("flag")
	payloadBldr.Append(mkVariant(true))

	tsBldr.Append(1713700015)
	evtBldr.AppendNull()
	payloadBldr.AppendNull()

	tsBldr.Append(1713700020)
	evtBldr.Append("tags")
	payloadBldr.Append(mkVariant([]any{"prod", "us-west-2", int64(7)}))

	tsArr := tsBldr.NewInt64Array()
	defer tsArr.Release()
	evtArr := evtBldr.NewStringArray()
	defer evtArr.Release()
	payloadArr := payloadBldr.NewArray()
	defer payloadArr.Release()

	rec := array.NewRecord(arrowSchema, []arrow.Array{tsArr, evtArr, payloadArr}, 5)
	defer rec.Release()

	arrTable := array.NewTableFromRecords(arrowSchema, []arrow.Record{rec})
	defer arrTable.Release()

	tx := tbl.NewTransaction()
	s.Require().NoError(tx.AppendTable(s.ctx, arrTable, 2048, nil))
	tbl, err = tx.Commit(s.ctx)
	s.Require().NoError(err)

	ctx := compute.WithAllocator(s.ctx, scanMem)
	results, err := tbl.Scan().ToArrowTable(ctx)
	s.Require().NoError(err)
	defer results.Release()

	s.EqualValues(5, results.NumRows())
	s.EqualValues(3, results.NumCols())

	variantCol := results.Column(2).Data().Chunk(0).(*extensions.VariantArray)

	v0, err := variantCol.Value(0)
	s.Require().NoError(err)
	obj, ok := v0.Value().(variant.ObjectValue)
	s.Require().True(ok)
	s.EqualValues(3, obj.NumElements())

	v1, err := variantCol.Value(1)
	s.Require().NoError(err)
	s.InDelta(98.6, v1.Value(), 0.01)

	v2, err := variantCol.Value(2)
	s.Require().NoError(err)
	s.EqualValues(true, v2.Value())

	s.True(variantCol.IsNull(3))

	v4, err := variantCol.Value(4)
	s.Require().NoError(err)
	arrVal, ok := v4.Value().(variant.ArrayValue)
	s.Require().True(ok)
	s.EqualValues(3, arrVal.Len())

	out, err := recipe.ExecuteSpark(s.T(), "./validation.py", "--sql",
		"SELECT ts, event, to_json(payload) AS pj FROM default.go_variant_events ORDER BY ts")
	s.Require().NoError(err)
	s.Require().Contains(out, `"target":"button-submit"`)
	s.Require().Contains(out, `98.6`)
	s.Require().Contains(out, `true`)
	s.Require().Contains(out, `["prod","us-west-2",7]`)
}

func (s *SparkIntegrationTestSuite) TestShreddedVariantSparkWriteGoRead() {
	s.requireSpark4()

	icebergSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.VariantType{}},
	)

	_, err := s.cat.CreateTable(
		s.ctx,
		catalog.ToIdentifier("default", "go_shredded_variant"),
		icebergSchema,
		catalog.WithProperties(iceberg.Properties{
			table.PropertyFormatVersion:    "3",
			"write.parquet.shred-variants": "true",
		}),
	)
	s.Require().NoError(err)

	// Bulk rows share one object shape {a, b, c:{x, y}} so Spark shreds it; the
	// 1000+/2000+ rows exercise residual merge, scalars, arrays and nulls.
	const bulk = 200
	insert := "INSERT INTO default.go_shredded_variant VALUES\n"
	rows := make([]string, 0, bulk+8)
	for id := 0; id < bulk; id++ {
		rows = append(rows, fmt.Sprintf(
			"(%d, parse_json(format_string('{\"a\": %d, \"b\": \"row\", \"c\": {\"x\": %d, \"y\": \"y%d\"}}', %d, %d, %d)))",
			id, id, id, id, id, id, id))
	}
	// 1000/1001: an extra "rare" field falls to residual; a/b/c stay shredded.
	rows = append(rows,
		`(1000, parse_json('{"a": 1000, "b": "row", "c": {"x": 1000, "y": "y1000"}, "rare": "R1000"}'))`,
		`(1001, parse_json('{"a": 1001, "b": "row", "c": {"x": 1001, "y": "y1001"}, "rare": "R1001"}'))`)
	// 2000: top-level scalar. 2001: array. 2002: JSON null. 2003: SQL null.
	rows = append(rows,
		`(2000, parse_json('42'))`,
		`(2001, parse_json('[1, 2, 3]'))`,
		`(2002, parse_json('null'))`,
		`(2003, CAST(NULL AS VARIANT))`)
	_, err = recipe.ExecuteSpark(s.T(), "./validation.py", "--sql", insert+strings.Join(rows, ",\n"))
	s.Require().NoError(err)

	tbl, err := s.cat.LoadTable(s.ctx, catalog.ToIdentifier("default", "go_shredded_variant"))
	s.Require().NoError(err)

	// Confirm Spark actually shredded (else this test is a false green).
	tasks, err := tbl.Scan().PlanFiles(s.ctx)
	s.Require().NoError(err)
	s.Require().NotEmpty(tasks)
	s.assertVariantFileShredded(tbl, tasks[0].File.FilePath())

	scanMem := memory.DefaultAllocator
	ctx := compute.WithAllocator(s.ctx, scanMem)
	results, err := tbl.Scan().ToArrowTable(ctx)
	s.Require().NoError(err)
	defer results.Release()

	s.EqualValues(bulk+6, results.NumRows())
	s.EqualValues(2, results.NumCols())

	// Index payload by id so assertions are order-independent.
	byID := map[int64]variant.Value{}
	nullIDs := map[int64]bool{}
	idChunks := results.Column(0).Data().Chunks()
	pChunks := results.Column(1).Data().Chunks()
	for ck := range idChunks {
		idArr := idChunks[ck].(*array.Int64)
		pArr := pChunks[ck].(*extensions.VariantArray)
		for i := 0; i < idArr.Len(); i++ {
			id := idArr.Value(i)
			// Storage().IsNull, not IsNull: a present variant-null must reach byID.
			if pArr.Storage().IsNull(i) {
				nullIDs[id] = true

				continue
			}
			v, err := pArr.Value(i)
			s.Require().NoError(err, "id %d", id)
			byID[id] = v
		}
	}

	s.Run("nested object reassembles", func() {
		for _, id := range []int64{0, 99, 199} {
			v, ok := byID[id]
			s.Require().True(ok, "missing id %d", id)
			obj, ok := v.Value().(variant.ObjectValue)
			s.Require().True(ok, "id %d not an object", id)
			s.EqualValues(id, s.variantField(obj, "a"))
			s.Equal("row", s.variantField(obj, "b"))
			c, ok := s.variantField(obj, "c").(variant.ObjectValue)
			s.Require().True(ok, "id %d field c not an object", id)
			s.EqualValues(id, s.variantField(c, "x"))
			s.Equal(fmt.Sprintf("y%d", id), s.variantField(c, "y"))
		}
	})

	s.Run("partial shred merges residual", func() {
		for _, id := range []int64{1000, 1001} {
			v, ok := byID[id]
			s.Require().True(ok, "missing id %d", id)
			obj, ok := v.Value().(variant.ObjectValue)
			s.Require().True(ok, "id %d not an object", id)
			s.EqualValues(id, s.variantField(obj, "a"))
			s.Equal(fmt.Sprintf("R%d", id), s.variantField(obj, "rare"), "residual field must merge")
		}
	})

	s.Run("top-level scalar", func() {
		v, ok := byID[2000]
		s.Require().True(ok, "missing id 2000")
		s.EqualValues(42, v.Value())
	})

	s.Run("top-level array", func() {
		v, ok := byID[2001]
		s.Require().True(ok, "missing id 2001")
		arr, ok := v.Value().(variant.ArrayValue)
		s.Require().True(ok, "id 2001 should be an array")
		s.EqualValues(3, arr.Len())
	})

	s.Run("json null is present", func() {
		v, ok := byID[2002]
		s.Require().True(ok, "id 2002 (parse_json('null')) must be present, not a physical null")
		s.Equal(variant.Null, v.Type())
	})

	s.Run("sql null is physical null", func() {
		s.True(nullIDs[2003], "id 2003 should be a physical SQL null")
	})

	s.Run("spark reads merged variant back", func() {
		out, err := recipe.ExecuteSpark(s.T(), "./validation.py", "--sql",
			"SELECT to_json(payload) AS pj FROM default.go_shredded_variant WHERE id = 1000")
		s.Require().NoError(err)
		s.Require().Contains(out, "1000")
		s.Require().Contains(out, "R1000")
	})
}

// variantField returns the Go value of an object field, failing if absent.
func (s *SparkIntegrationTestSuite) variantField(obj variant.ObjectValue, key string) any {
	f, err := obj.ValueByKey(key)
	s.Require().NoError(err, "missing field %q", key)

	return f.Value.Value()
}

// assertVariantFileShredded fails unless the payload column was written shredded.
func (s *SparkIntegrationTestSuite) assertVariantFileShredded(tbl *table.Table, path string) {
	fs, err := tbl.FS(s.ctx)
	s.Require().NoError(err)
	f, err := fs.Open(path)
	s.Require().NoError(err)
	defer f.Close()

	pf, err := file.NewParquetReader(f)
	s.Require().NoError(err)
	defer pf.Close()

	root := pf.MetaData().Schema.Root()
	var payload *schema.GroupNode
	for i := 0; i < root.NumFields(); i++ {
		if g, ok := root.Field(i).(*schema.GroupNode); ok && g.Name() == "payload" {
			payload = g

			break
		}
	}
	s.Require().NotNil(payload, "payload must be a group node")

	names := map[string]bool{}
	var typedValue *schema.GroupNode
	for i := 0; i < payload.NumFields(); i++ {
		fld := payload.Field(i)
		names[fld.Name()] = true
		if g, ok := fld.(*schema.GroupNode); ok && fld.Name() == "typed_value" {
			typedValue = g
		}
	}
	s.Require().True(names["typed_value"], "Spark must have written a shredded variant (typed_value child); got children %v", names)

	// c is a nested object, so it must itself be shredded (its own typed_value).
	s.Require().NotNil(typedValue, "typed_value must be a group")
	var cField *schema.GroupNode
	for i := 0; i < typedValue.NumFields(); i++ {
		if g, ok := typedValue.Field(i).(*schema.GroupNode); ok && g.Name() == "c" {
			cField = g
		}
	}
	s.Require().NotNil(cField, "typed_value must contain shredded field c")
	cNames := map[string]bool{}
	for i := 0; i < cField.NumFields(); i++ {
		cNames[cField.Field(i).Name()] = true
	}
	s.True(cNames["typed_value"], "nested object c must itself be shredded; got %v", cNames)
}

func (s *SparkIntegrationTestSuite) TestShreddedVariantGoWriteSparkRead() {
	s.requireSpark4()

	icebergSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.VariantType{}},
	)

	tbl, err := s.cat.CreateTable(
		s.ctx,
		catalog.ToIdentifier("default", "go_shred_write"),
		icebergSchema,
		catalog.WithProperties(iceberg.Properties{
			table.PropertyFormatVersion:    "3",
			"write.parquet.shred-variants": "true",
		}),
	)
	s.Require().NoError(err)

	arrowSchema, err := table.SchemaToArrowSchema(icebergSchema, nil, true, false)
	s.Require().NoError(err)

	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(s.T(), 0)

	idBldr := array.NewInt64Builder(mem)
	defer idBldr.Release()
	payloadBldr := extensions.NewVariantBuilder(mem, extensions.NewDefaultVariantType())
	defer payloadBldr.Release()

	mkVariant := func(v any) variant.Value {
		var b variant.Builder
		s.Require().NoError(b.Append(v))
		val, err := b.Build()
		s.Require().NoError(err)

		return val
	}

	// 12 uniform rows {a, b, c:{x,y}, d:decimal} so the analyzer shreds a, b, the
	// nested object c, and the decimal d; assertVariantFileShredded requires c too.
	// d (123.45, precision 5) exercises the spec decimal4->INT32 typed_value path.
	const nRows = 12
	for i := 0; i < nRows; i++ {
		idBldr.Append(int64(i))
		payloadBldr.Append(mkVariant(map[string]any{
			"a": int64(5_000_000_000 + i),
			"b": "row",
			"c": map[string]any{"x": int64(i * 2), "y": int64(i * 3)},
			"d": variant.DecimalValue[decimal.Decimal32]{Scale: 2, Value: decimal.Decimal32(12345)},
		}))
	}

	idArr := idBldr.NewInt64Array()
	defer idArr.Release()
	payloadArr := payloadBldr.NewArray()
	defer payloadArr.Release()

	rec := array.NewRecord(arrowSchema, []arrow.Array{idArr, payloadArr}, nRows)
	defer rec.Release()
	arrTable := array.NewTableFromRecords(arrowSchema, []arrow.Record{rec})
	defer arrTable.Release()

	tx := tbl.NewTransaction()
	s.Require().NoError(tx.AppendTable(s.ctx, arrTable, 2048, nil))
	tbl, err = tx.Commit(s.ctx)
	s.Require().NoError(err)

	// Prove the GO writer actually shredded the file (and nested c too).
	tasks, err := tbl.Scan().PlanFiles(s.ctx)
	s.Require().NoError(err)
	s.Require().NotEmpty(tasks)
	s.assertVariantFileShredded(tbl, tasks[0].File.FilePath())

	// Spark reads the Go-written shredded file back and reassembles values.
	out, err := recipe.ExecuteSpark(s.T(), "./validation.py", "--sql",
		"SELECT id, to_json(payload) AS pj FROM default.go_shred_write ORDER BY id")
	s.Require().NoError(err)
	s.Require().Contains(out, `"a":5000000000`)
	s.Require().Contains(out, `"c":{"x":0,"y":0}`)
	s.Require().Contains(out, `"c":{"x":22,"y":33}`)
	// Decimal round-trips: Spark renders variant decimals as bare numerics (see
	// TestDifferentDataTypes), so the spec decimal4->INT32 typed_value reassembles.
	s.Require().Contains(out, `"d":123.45`)
}

// TestShreddedVariantPartitionedGoWriteSparkRead is the cross-engine check for the
// partitioned write path: a table partitioned by p, shredding a decimal, so AppendTable
// fans out concurrent partition writers (the path that raced on the shared write-props
// slice). Every partition file must be shredded and Spark must reassemble the values.
func (s *SparkIntegrationTestSuite) TestShreddedVariantPartitionedGoWriteSparkRead() {
	s.requireSpark4()

	icebergSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "p", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 3, Name: "payload", Type: iceberg.VariantType{}},
	)
	partitionSpec := iceberg.NewPartitionSpec(
		iceberg.PartitionField{SourceIDs: []int{2}, FieldID: 1000, Transform: iceberg.IdentityTransform{}, Name: "p"},
	)

	tbl, err := s.cat.CreateTable(
		s.ctx,
		catalog.ToIdentifier("default", "go_shred_partitioned"),
		icebergSchema,
		catalog.WithPartitionSpec(&partitionSpec),
		catalog.WithProperties(iceberg.Properties{
			table.PropertyFormatVersion:    "3",
			"write.parquet.shred-variants": "true",
		}),
	)
	s.Require().NoError(err)

	arrowSchema, err := table.SchemaToArrowSchema(icebergSchema, nil, true, false)
	s.Require().NoError(err)

	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(s.T(), 0)

	idBldr := array.NewInt64Builder(mem)
	defer idBldr.Release()
	pBldr := array.NewInt64Builder(mem)
	defer pBldr.Release()
	payloadBldr := extensions.NewVariantBuilder(mem, extensions.NewDefaultVariantType())
	defer payloadBldr.Release()

	mkVariant := func(v any) variant.Value {
		var b variant.Builder
		s.Require().NoError(b.Append(v))
		val, err := b.Build()
		s.Require().NoError(err)

		return val
	}

	// 12 rows across 3 partitions (p = i%3); each payload shreds a, b, nested c, and the
	// decimal d, so every partition file carries a shredded decimal typed_value.
	const nRows, nPart = 12, 3
	for i := 0; i < nRows; i++ {
		idBldr.Append(int64(i))
		pBldr.Append(int64(i % nPart))
		payloadBldr.Append(mkVariant(map[string]any{
			"a": int64(5_000_000_000 + i),
			"b": "row",
			"c": map[string]any{"x": int64(i * 2), "y": int64(i * 3)},
			"d": variant.DecimalValue[decimal.Decimal32]{Scale: 2, Value: decimal.Decimal32(12345)},
		}))
	}

	idArr := idBldr.NewInt64Array()
	defer idArr.Release()
	pArr := pBldr.NewInt64Array()
	defer pArr.Release()
	payloadArr := payloadBldr.NewArray()
	defer payloadArr.Release()

	rec := array.NewRecord(arrowSchema, []arrow.Array{idArr, pArr, payloadArr}, nRows)
	defer rec.Release()
	arrTable := array.NewTableFromRecords(arrowSchema, []arrow.Record{rec})
	defer arrTable.Release()

	tx := tbl.NewTransaction()
	s.Require().NoError(tx.AppendTable(s.ctx, arrTable, 2048, nil))
	tbl, err = tx.Commit(s.ctx)
	s.Require().NoError(err)

	// Every partition produced its own file, and each was shredded (incl. nested c).
	tasks, err := tbl.Scan().PlanFiles(s.ctx)
	s.Require().NoError(err)
	s.Require().Len(tasks, nPart, "one file per partition")
	for _, tsk := range tasks {
		s.assertVariantFileShredded(tbl, tsk.File.FilePath())
	}

	// Spark reads the Go-written partitioned shredded files back and reassembles values.
	out, err := recipe.ExecuteSpark(s.T(), "./validation.py", "--sql",
		"SELECT id, p, to_json(payload) AS pj FROM default.go_shred_partitioned ORDER BY id")
	s.Require().NoError(err)
	s.Require().Contains(out, `"a":5000000000`)
	s.Require().Contains(out, `"c":{"x":0,"y":0}`)
	s.Require().Contains(out, `"c":{"x":22,"y":33}`)
	s.Require().Contains(out, `"d":123.45`)
}

func (s *SparkIntegrationTestSuite) TestUnknownTypeWriteAndScan() {
	s.requireSpark4()

	icebergSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "note", Type: iceberg.UnknownType{}, Required: false},
	)

	tbl, err := s.cat.CreateTable(
		s.ctx,
		catalog.ToIdentifier("default", "go_unknown_table"),
		icebergSchema,
		catalog.WithProperties(iceberg.Properties{table.PropertyFormatVersion: "3"}),
	)
	s.Require().NoError(err)

	arrowSchema, err := table.SchemaToArrowSchema(icebergSchema, nil, true, false)
	s.Require().NoError(err)

	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(s.T(), 0)

	idBldr := array.NewInt64Builder(mem)
	defer idBldr.Release()
	idBldr.AppendValues([]int64{1, 2, 3}, nil)
	idArr := idBldr.NewInt64Array()
	defer idArr.Release()

	nullArr := array.NewNull(3)
	defer nullArr.Release()

	rec := array.NewRecord(arrowSchema, []arrow.Array{idArr, nullArr}, 3)
	defer rec.Release()

	arrTable := array.NewTableFromRecords(arrowSchema, []arrow.Record{rec})
	defer arrTable.Release()

	tx := tbl.NewTransaction()
	s.Require().NoError(tx.AppendTable(s.ctx, arrTable, 2048, nil))
	tbl, err = tx.Commit(s.ctx)
	s.Require().NoError(err)

	scanMem := memory.DefaultAllocator
	ctx := compute.WithAllocator(s.ctx, scanMem)
	results, err := tbl.Scan().ToArrowTable(ctx)
	s.Require().NoError(err)
	defer results.Release()
	s.EqualValues(3, results.NumRows())
	s.EqualValues(2, results.NumCols())

	desc, err := recipe.ExecuteSpark(s.T(), "./validation.py", "--sql",
		"DESCRIBE default.go_unknown_table")
	s.Require().NoError(err)
	s.Require().Contains(desc, "note")
	s.Require().Contains(desc, "void")
}

func (s *SparkIntegrationTestSuite) TestOverwriteBasic() {
	icebergSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "foo", Type: iceberg.PrimitiveTypes.Bool},
		iceberg.NestedField{ID: 2, Name: "bar", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 3, Name: "baz", Type: iceberg.PrimitiveTypes.Int32},
	)

	tbl, err := s.cat.CreateTable(s.ctx, catalog.ToIdentifier("default", "go_test_overwrite_basic"), icebergSchema)
	s.Require().NoError(err)

	// Create initial data
	arrowSchema, err := table.SchemaToArrowSchema(icebergSchema, nil, true, false)
	s.Require().NoError(err)

	initialTable, err := array.TableFromJSON(memory.DefaultAllocator, arrowSchema, []string{
		`[
			{"foo": true, "bar": "initial", "baz": 100},
			{"foo": false, "bar": "old_data", "baz": 200}
		]`,
	})
	s.Require().NoError(err)
	defer initialTable.Release()

	tx := tbl.NewTransaction()
	err = tx.AppendTable(s.ctx, initialTable, 2, nil)
	s.Require().NoError(err)
	tbl, err = tx.Commit(s.ctx)
	s.Require().NoError(err)

	overwriteTable, err := array.TableFromJSON(memory.DefaultAllocator, arrowSchema, []string{
		`[
			{"foo": false, "bar": "overwritten", "baz": 300},
			{"foo": true, "bar": "new_data", "baz": 400}
		]`,
	})
	s.Require().NoError(err)
	defer overwriteTable.Release()

	tx = tbl.NewTransaction()
	err = tx.OverwriteTable(s.ctx, overwriteTable, 2, nil)
	s.Require().NoError(err)
	_, err = tx.Commit(s.ctx)
	s.Require().NoError(err)

	output, err := recipe.ExecuteSpark(s.T(), "./validation.py", "--sql", "SELECT * FROM default.go_test_overwrite_basic ORDER BY baz")
	s.Require().NoError(err)
	s.Require().Contains(output, `+-----+-----------+---+
|foo  |bar        |baz|
+-----+-----------+---+
|false|overwritten|300|
|true |new_data   |400|
+-----+-----------+---+`)
}

func (s *SparkIntegrationTestSuite) TestOverwriteWithFilter() {
	icebergSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "foo", Type: iceberg.PrimitiveTypes.Bool},
		iceberg.NestedField{ID: 2, Name: "bar", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 3, Name: "baz", Type: iceberg.PrimitiveTypes.Int32},
	)

	tbl, err := s.cat.CreateTable(s.ctx, catalog.ToIdentifier("default", "go_test_overwrite_filter"), icebergSchema)
	s.Require().NoError(err)

	arrowSchema, err := table.SchemaToArrowSchema(icebergSchema, nil, true, false)
	s.Require().NoError(err)

	initialTable, err := array.TableFromJSON(memory.DefaultAllocator, arrowSchema, []string{
		`[
			{"foo": true, "bar": "should_be_replaced", "baz": 100},
			{"foo": false, "bar": "should_remain", "baz": 200},
			{"foo": true, "bar": "also_replaced", "baz": 300}
		]`,
	})
	s.Require().NoError(err)
	defer initialTable.Release()

	tx := tbl.NewTransaction()
	err = tx.AppendTable(s.ctx, initialTable, 3, nil)
	s.Require().NoError(err)
	tbl, err = tx.Commit(s.ctx)
	s.Require().NoError(err)

	overwriteTable, err := array.TableFromJSON(memory.DefaultAllocator, arrowSchema, []string{
		`[
			{"foo": true, "bar": "new_replacement", "baz": 999}
		]`,
	})
	s.Require().NoError(err)
	defer overwriteTable.Release()

	filter := iceberg.EqualTo(iceberg.Reference("foo"), true)
	tx = tbl.NewTransaction()
	err = tx.OverwriteTable(s.ctx, overwriteTable, 1, nil, table.WithOverwriteFilter(filter))
	s.Require().NoError(err)
	_, err = tx.Commit(s.ctx)
	s.Require().NoError(err)

	output, err := recipe.ExecuteSpark(s.T(), "./validation.py", "--sql", "SELECT * FROM default.go_test_overwrite_filter ORDER BY baz")
	s.Require().NoError(err)
	s.Require().Contains(output, `+-----+---------------+---+
|foo  |bar            |baz|
+-----+---------------+---+
|false|should_remain  |200|
|true |new_replacement|999|
+-----+---------------+---+`)
}

func (s *SparkIntegrationTestSuite) TestDelete() {
	icebergSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "first_name", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 2, Name: "last_name", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 3, Name: "age", Type: iceberg.PrimitiveTypes.Int32},
	)

	tbl, err := s.cat.CreateTable(s.ctx, catalog.ToIdentifier("default", "go_test_delete"), icebergSchema)
	s.Require().NoError(err)

	arrowSchema, err := table.SchemaToArrowSchema(icebergSchema, nil, true, false)
	s.Require().NoError(err)

	initialTable, err := array.TableFromJSON(memory.DefaultAllocator, arrowSchema, []string{
		`[
			{"first_name": "alan", "last_name": "gopher", "age": 7},
			{"first_name": "steve", "last_name": "gopher", "age": 5},
			{"first_name": "dead", "last_name": "gopher", "age": 97}
		]`,
	})
	s.Require().NoError(err)
	defer initialTable.Release()

	tx := tbl.NewTransaction()
	err = tx.AppendTable(s.ctx, initialTable, 3, nil)
	s.Require().NoError(err)
	tbl, err = tx.Commit(s.ctx)
	s.Require().NoError(err)

	// Delete the dead gopher and confirm that alan and steve are still present
	filter := iceberg.EqualTo(iceberg.Reference("first_name"), "dead")
	tx = tbl.NewTransaction()
	err = tx.Delete(s.ctx, filter, nil)
	s.Require().NoError(err)
	_, err = tx.Commit(s.ctx)
	s.Require().NoError(err)

	output, err := recipe.ExecuteSpark(s.T(), "./validation.py", "--sql", "SELECT * FROM default.go_test_delete ORDER BY age")
	s.Require().NoError(err)
	s.Require().Contains(output, `|first_name|last_name|age|
+----------+---------+---+
|steve     |gopher   |5  |
|alan      |gopher   |7  |
+----------+---------+---+`)
}

func (s *SparkIntegrationTestSuite) TestDeleteInsensitive() {
	icebergSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "first_name", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 2, Name: "last_name", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 3, Name: "age", Type: iceberg.PrimitiveTypes.Int32},
	)

	tbl, err := s.cat.CreateTable(s.ctx, catalog.ToIdentifier("default", "go_test_delete_insensitive"), icebergSchema)
	s.Require().NoError(err)

	arrowSchema, err := table.SchemaToArrowSchema(icebergSchema, nil, true, false)
	s.Require().NoError(err)

	initialTable, err := array.TableFromJSON(memory.DefaultAllocator, arrowSchema, []string{
		`[
			{"first_name": "alan", "last_name": "gopher", "age": 7},
			{"first_name": "steve", "last_name": "gopher", "age": 5},
			{"first_name": "dead", "last_name": "gopher", "age": 97}
		]`,
	})
	s.Require().NoError(err)
	defer initialTable.Release()

	tx := tbl.NewTransaction()
	err = tx.AppendTable(s.ctx, initialTable, 3, nil)
	s.Require().NoError(err)
	tbl, err = tx.Commit(s.ctx)
	s.Require().NoError(err)

	// Delete the dead gopher and confirm that alan and steve are still present
	filter := iceberg.EqualTo(iceberg.Reference("FIRST_NAME"), "dead")
	tx = tbl.NewTransaction()
	err = tx.Delete(s.ctx, filter, nil, table.WithDeleteCaseInsensitive())
	s.Require().NoError(err)
	_, err = tx.Commit(s.ctx)
	s.Require().NoError(err)

	output, err := recipe.ExecuteSpark(s.T(), "./validation.py", "--sql", "SELECT * FROM default.go_test_delete_insensitive ORDER BY age")
	s.Require().NoError(err)
	s.Require().Contains(output, `|first_name|last_name|age|
+----------+---------+---+
|steve     |gopher   |5  |
|alan      |gopher   |7  |
+----------+---------+---+`)
}

func (s *SparkIntegrationTestSuite) TestDeleteMergeOnReadUnpartitioned() {
	icebergSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "first_name", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 2, Name: "last_name", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 3, Name: "age", Type: iceberg.PrimitiveTypes.Int32},
	)

	tbl, err := s.cat.CreateTable(s.ctx, catalog.ToIdentifier("default", "go_test_merge_on_read_delete"), icebergSchema,
		catalog.WithProperties(
			map[string]string{
				table.WriteDeleteModeKey: table.WriteModeMergeOnRead,
			},
		),
	)
	s.Require().NoError(err)

	arrowSchema, err := table.SchemaToArrowSchema(icebergSchema, nil, true, false)
	s.Require().NoError(err)

	initialTable, err := array.TableFromJSON(memory.DefaultAllocator, arrowSchema, []string{
		`[
			{"first_name": "alan", "last_name": "gopher", "age": 7},
			{"first_name": "steve", "last_name": "gopher", "age": 5},
			{"first_name": "dead", "last_name": "gopher", "age": 97}
		]`,
	})
	s.Require().NoError(err)
	defer initialTable.Release()

	tx := tbl.NewTransaction()
	err = tx.AppendTable(s.ctx, initialTable, 3, nil)
	s.Require().NoError(err)
	tbl, err = tx.Commit(s.ctx)
	s.Require().NoError(err)

	// Delete the dead gopher and confirm that alan and steve are still present
	filter := iceberg.EqualTo(iceberg.Reference("first_name"), "dead")
	tx = tbl.NewTransaction()
	err = tx.Delete(s.ctx, filter, nil)
	s.Require().NoError(err)
	_, err = tx.Commit(s.ctx)
	s.Require().NoError(err)

	output, err := recipe.ExecuteSpark(s.T(), "./validation.py", "--sql", "SELECT * FROM default.go_test_merge_on_read_delete ORDER BY age")
	s.Require().NoError(err)
	s.Require().Contains(output, `|first_name|last_name|age|
+----------+---------+---+
|steve     |gopher   |5  |
|alan      |gopher   |7  |
+----------+---------+---+`)
}

func (s *SparkIntegrationTestSuite) TestDeleteMergeOnReadPartitioned() {
	icebergSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "first_name", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 2, Name: "last_name", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 3, Name: "age", Type: iceberg.PrimitiveTypes.Int32},
	)

	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{3},
		Name:      "age_bucket",
		Transform: iceberg.BucketTransform{
			NumBuckets: 2,
		},
	})
	tbl, err := s.cat.CreateTable(s.ctx, catalog.ToIdentifier("default", "go_test_merge_on_read_delete_partitioned"), icebergSchema,
		catalog.WithProperties(
			map[string]string{
				table.WriteDeleteModeKey: table.WriteModeMergeOnRead,
			},
		),
		catalog.WithPartitionSpec(&spec),
	)
	s.Require().NoError(err)

	arrowSchema, err := table.SchemaToArrowSchema(icebergSchema, nil, true, false)
	s.Require().NoError(err)

	initialTable, err := array.TableFromJSON(memory.DefaultAllocator, arrowSchema, []string{
		`[
			{"first_name": "alan", "last_name": "gopher", "age": 7},
			{"first_name": "steve", "last_name": "gopher", "age": 5},
			{"first_name": "dead", "last_name": "gopher", "age": 97},
			{"first_name": "uncle", "last_name": "gopher", "age": 90}
		]`,
	})
	s.Require().NoError(err)
	defer initialTable.Release()

	tx := tbl.NewTransaction()
	err = tx.AppendTable(s.ctx, initialTable, 3, nil)
	s.Require().NoError(err)
	tbl, err = tx.Commit(s.ctx)
	s.Require().NoError(err)

	// Delete the dead gopher and confirm that alan and steve are still present
	filter := iceberg.NewAnd(iceberg.GreaterThan(iceberg.Reference("age"), "50"), iceberg.EqualTo(iceberg.Reference("first_name"), "dead"))
	tx = tbl.NewTransaction()
	err = tx.Delete(s.ctx, filter, nil)
	s.Require().NoError(err)
	_, err = tx.Commit(s.ctx)
	s.Require().NoError(err)

	output, err := recipe.ExecuteSpark(s.T(), "./validation.py", "--sql", "SELECT * FROM default.go_test_merge_on_read_delete_partitioned ORDER BY age")
	s.Require().NoError(err)
	s.Require().Contains(output, `|first_name|last_name|age|
+----------+---------+---+
|steve     |gopher   |5  |
|alan      |gopher   |7  |
|uncle     |gopher   |90 |
+----------+---------+---+`)
}

func TestSparkIntegration(t *testing.T) {
	suite.Run(t, new(SparkIntegrationTestSuite))
}
