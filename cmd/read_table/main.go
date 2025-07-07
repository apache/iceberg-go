package main

import (
	"context"
	"fmt"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"
	"log"
	"os"
	"strings"
)

func main() {
	ctx := context.Background()
	os.Setenv("AWS_ACCESS_KEY_ID", "admin")
	os.Setenv("AWS_SECRET_ACCESS_KEY", "password")
	os.Setenv("AWS_REGION", "us-east-1")
	os.Setenv("AWS_S3_ENDPOINT", "http://localhost:9000")

	catalog, err := rest.NewCatalog(
		ctx,
		"rest_catalog",
		"http://localhost:8181")
	if err != nil {
		panic(err)
	}
	tableID := table.Identifier{"default", "test_table_add_column"}
	icebergTable, err := catalog.LoadTable(ctx, tableID, nil)
	if err != nil {
		panic(err)
	}
	scan := icebergTable.Scan(table.WithLimit(10))
	arrowTable, err := scan.ToArrowTable(ctx)
	if err != nil {
		log.Fatalf("Failed to convert to arrow table: %v", err)
	}
	defer arrowTable.Release()
	// Print column headers
	schema := arrowTable.Schema()
	headers := make([]string, schema.NumFields())
	for i, field := range schema.Fields() {
		headers[i] = field.Name
	}
	fmt.Println(strings.Join(headers, ", "))

	// Print all rows
	numRows := int(arrowTable.NumRows())
	for row := 0; row < numRows; row++ {
		values := make([]string, schema.NumFields())

		for col := 0; col < len(values); col++ {
			column := arrowTable.Column(col)
			values[col] = "NULL"
			// Get value from the appropriate chunk
			rowInChunk := row
			for _, chunk := range column.Data().Chunks() {
				if rowInChunk < chunk.Len() {
					if chunk.IsValid(rowInChunk) {
						values[col] = chunk.ValueStr(rowInChunk)
					}
					break
				}
				rowInChunk -= chunk.Len()
			}
		}
		fmt.Println(strings.Join(values, ", "))
	}
}
