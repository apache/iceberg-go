<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

# Getting Started

Create an Iceberg table in a local SQLite catalog, append three rows with Arrow,
and read the rows back. The program uses a fresh temporary catalog and warehouse
on each run and removes them when it exits.

## Install

This walkthrough uses Iceberg Go v0.7.0 and requires Go 1.25.9 or later.
Create a new directory and run:

```sh
go mod init iceberg-go-tutorial
go get github.com/apache/iceberg-go@v0.7.0 \
  github.com/apache/arrow-go/v18@v18.8.0 \
  github.com/uptrace/bun/driver/sqliteshim@v1.2.18
```

## Create, write, and read a table

Save the following as `main.go`. The SQL catalog stores table registrations in
SQLite. The Iceberg metadata and Parquet data files go in the local warehouse.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	_ "github.com/apache/iceberg-go/catalog/sql"
	"github.com/uptrace/bun/driver/sqliteshim"
)

func run(ctx context.Context) error {
	dir, err := os.MkdirTemp("", "iceberg-tutorial-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)

	cat, err := catalog.Load(ctx, "tutorial", iceberg.Properties{
		"type":        "sql",
		"uri":         "file:" + filepath.Join(dir, "catalog.db"),
		"sql.driver":  sqliteshim.ShimName,
		"sql.dialect": "sqlite",
		"warehouse":   "file://" + filepath.Join(dir, "warehouse"),
	})
	if err != nil {
		return err
	}
	if closer, ok := cat.(io.Closer); ok {
		defer closer.Close()
	}

	if err := cat.CreateNamespace(ctx, catalog.ToIdentifier("taxi"), nil); err != nil {
		return err
	}
	trips := catalog.ToIdentifier("taxi", "trips")
	schema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "trip_id", Type: iceberg.PrimitiveTypes.Int64, Required: true})
	tbl, err := cat.CreateTable(ctx, trips, schema)
	if err != nil {
		return err
	}

	arrowSchema := arrow.NewSchema([]arrow.Field{{Name: "trip_id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	records := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer records.Release()
	records.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	batch := records.NewRecord()
	defer batch.Release()
	input := array.NewTableFromRecords(arrowSchema, []arrow.Record{batch})
	defer input.Release()
	tbl, err = tbl.AppendTable(ctx, input, 1024, nil)
	if err != nil {
		return err
	}

	output, err := tbl.Scan().ToArrowTable(ctx)
	if err != nil {
		return err
	}
	defer output.Release()
	if tbl.CurrentSnapshot() == nil || output.NumRows() != 3 {
		return fmt.Errorf("expected a committed snapshot and 3 rows, got %d rows", output.NumRows())
	}
	fmt.Printf("Read %d rows from taxi.trips and verified a committed snapshot.\n", output.NumRows())
	return nil
}

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
```

Run the program:

```sh
go mod tidy
go run .
```

```text
Read 3 rows from taxi.trips and verified a committed snapshot.
```

The append returns a new table handle with the committed snapshot. The scan
uses that handle to read the rows back.

## Where to go next

- [Filter and project columns](./api.md#projection-and-filters) or
  [stream Arrow record batches](./api.md#streaming-record-batches).
- [Evolve a schema](./api.md#schema-evolution) or
  [work with branches and tags](./api.md#branches-and-tags).
- [Configure another catalog](./configuration.md#catalog-options) or
  [inspect a table from the CLI](./cli.md#inspecting-tables).
- For an S3, GCS, or Azure warehouse, see
  [file-system credentials](./configuration.md#file-system-credentials) for
  backend imports and credentials.
- See [Releases](./releases.md#using-a-release) for other library versions and
  [API](./api.md) for examples that follow the development branch.
