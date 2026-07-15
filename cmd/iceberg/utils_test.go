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

package main

import (
	"context"
	"encoding/json"
	"maps"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

func TestParseProperties(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  iceberg.Properties
		isErr bool
	}{
		{
			name:  "empty string",
			input: "",
			want:  iceberg.Properties{},
		},
		{
			name:  "single property",
			input: "key1=value1",
			want:  iceberg.Properties{"key1": "value1"},
		},
		{
			name:  "multiple properties",
			input: "key1=value1,key2=value2,key3=value3",
			want:  iceberg.Properties{"key1": "value1", "key2": "value2", "key3": "value3"},
		},
		{
			name:  "with spaces",
			input: " key1 = value1 , key2 = value2 ",
			want:  iceberg.Properties{"key1": "value1", "key2": "value2"},
		},
		{
			name:  "invalid format - no equals",
			input: "key1value1",
			isErr: true,
		},
		{
			name:  "invalid format - empty key",
			input: "=value1",
			isErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseProperties(tt.input)
			if (err != nil) != tt.isErr {
				t.Errorf("parseProperties() error = %v, isErr %v", err, tt.isErr)

				return
			}
			if !tt.isErr && !maps.Equal(got, tt.want) {
				t.Errorf("parseProperties() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParsePartitionSpec(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{
			ID:       10,
			Name:     "customer_id",
			Type:     iceberg.PrimitiveTypes.String,
			Required: false,
		},
		iceberg.NestedField{
			ID:       20,
			Name:     "event_time",
			Type:     iceberg.PrimitiveTypes.TimestampNs,
			Required: false,
		},
	)

	tests := []struct {
		name          string
		input         string
		wantSourceIDs [][]int
		wantFieldIDs  []int
		errContains   string
		isErr         bool
	}{
		{
			name:  "empty string",
			input: "", // keeps backward compatibility for no partitioning
		},
		{
			name:  "single field",
			input: "customer_id",
			wantSourceIDs: [][]int{
				{10},
			},
			wantFieldIDs: []int{1000},
		},
		{
			name:  "multiple fields",
			input: "customer_id,event_time",
			wantSourceIDs: [][]int{
				{10},
				{20},
			},
			wantFieldIDs: []int{1000, 1001},
		},
		{
			name:  "with spaces",
			input: " customer_id , event_time ",
			wantSourceIDs: [][]int{
				{10},
				{20},
			},
			wantFieldIDs: []int{1000, 1001},
		},
		{
			name:        "unknown field",
			input:       "no_such_field",
			isErr:       true,
			errContains: "cannot find source column with name: no_such_field in schema",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parsePartitionSpec(tt.input, schema)
			if (err != nil) != tt.isErr {
				t.Errorf("parsePartitionSpec() error = %v, isErr %v", err, tt.isErr)

				return
			}
			if tt.isErr {
				if tt.errContains != "" {
					require.ErrorContains(t, err, tt.errContains)
				}

				return
			}
			if !tt.isErr && got == nil {
				t.Errorf("parsePartitionSpec() returned nil for valid input")
			}
			if len(tt.wantSourceIDs) == 0 {
				require.Equal(t, iceberg.UnpartitionedSpec, got)

				return
			}
			require.Equal(t, len(tt.wantSourceIDs), got.NumFields())
			for i, wantIDs := range tt.wantSourceIDs {
				require.Equal(t, wantIDs, got.Field(i).SourceIDs)
			}
			for i, wantID := range tt.wantFieldIDs {
				require.Equal(t, wantID, got.Field(i).FieldID)
			}
		})
	}
}

func TestParseSortOrder(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 42, Name: "field1", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 10, Name: "field2", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 27, Name: "field3", Type: iceberg.PrimitiveTypes.Timestamp},
		iceberg.NestedField{ID: 50, Name: "nested", Type: &iceberg.StructType{FieldList: []iceberg.NestedField{
			{ID: 51, Name: "value", Type: iceberg.PrimitiveTypes.String},
		}}},
	)

	tests := []struct {
		name                string
		input               string
		isErr               bool
		expectedFieldsCount int
		expectedNullOrders  []table.NullOrder // for validation
		expectedDirections  []table.SortDirection
		expectedSourceIDs   []int
	}{
		{
			name:                "empty string",
			input:               "",
			expectedFieldsCount: 0,
		},
		{
			name:                "single field ascending (default null order)",
			input:               "field1:asc",
			expectedFieldsCount: 1,
			expectedDirections:  []table.SortDirection{table.SortASC},
			expectedNullOrders:  []table.NullOrder{table.NullsFirst},
			expectedSourceIDs:   []int{42},
		},
		{
			name:                "single field descending (default null order)",
			input:               "field1:desc",
			expectedFieldsCount: 1,
			expectedDirections:  []table.SortDirection{table.SortDESC},
			expectedNullOrders:  []table.NullOrder{table.NullsLast},
			expectedSourceIDs:   []int{42},
		},
		{
			name:                "single field with explicit nulls-first",
			input:               "field1:asc:nulls-first",
			expectedFieldsCount: 1,
			expectedDirections:  []table.SortDirection{table.SortASC},
			expectedNullOrders:  []table.NullOrder{table.NullsFirst},
			expectedSourceIDs:   []int{42},
		},
		{
			name:                "single field with explicit nulls-last",
			input:               "field1:desc:nulls-last",
			expectedFieldsCount: 1,
			expectedDirections:  []table.SortDirection{table.SortDESC},
			expectedNullOrders:  []table.NullOrder{table.NullsLast},
			expectedSourceIDs:   []int{42},
		},
		{
			name:                "asc with nulls-last (overriding default)",
			input:               "field1:asc:nulls-last",
			expectedFieldsCount: 1,
			expectedDirections:  []table.SortDirection{table.SortASC},
			expectedNullOrders:  []table.NullOrder{table.NullsLast},
			expectedSourceIDs:   []int{42},
		},
		{
			name:                "desc with nulls-first (overriding default)",
			input:               "field1:desc:nulls-first",
			expectedFieldsCount: 1,
			expectedDirections:  []table.SortDirection{table.SortDESC},
			expectedNullOrders:  []table.NullOrder{table.NullsFirst},
			expectedSourceIDs:   []int{42},
		},
		{
			name:                "multiple fields with mixed null orders",
			input:               "field1:asc,field2:desc:nulls-first,field3:asc:nulls-last",
			expectedFieldsCount: 3,
			expectedDirections:  []table.SortDirection{table.SortASC, table.SortDESC, table.SortASC},
			expectedNullOrders:  []table.NullOrder{table.NullsFirst, table.NullsFirst, table.NullsLast},
			expectedSourceIDs:   []int{42, 10, 27},
		},
		{
			name:                "with spaces",
			input:               " field1 : asc : nulls-last , field2 : desc ",
			expectedFieldsCount: 2,
			expectedDirections:  []table.SortDirection{table.SortASC, table.SortDESC},
			expectedNullOrders:  []table.NullOrder{table.NullsLast, table.NullsLast},
			expectedSourceIDs:   []int{42, 10},
		},
		{
			name:                "nested field path",
			input:               "nested.value",
			expectedFieldsCount: 1,
			expectedDirections:  []table.SortDirection{table.SortASC},
			expectedNullOrders:  []table.NullOrder{table.NullsFirst},
			expectedSourceIDs:   []int{51},
		},
		{
			name:  "invalid direction",
			input: "field1:invalid",
			isErr: true,
		},
		{
			name:  "invalid null order",
			input: "field1:asc:invalid-order",
			isErr: true,
		},
		{
			name:  "unknown field",
			input: "missing:asc",
			isErr: true,
		},
		{
			name:  "duplicate field",
			input: "field1:asc,field1:desc",
			isErr: true,
		},
		{
			name:  "non-primitive field",
			input: "nested",
			isErr: true,
		},
		{
			name:  "too many components",
			input: "field1:asc:nulls-first:extra",
			isErr: true,
		},
		{
			name:  "empty field",
			input: "field1,",
			isErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseSortOrder(tt.input, schema)
			if (err != nil) != tt.isErr {
				t.Errorf("parseSortOrder() error = %v, isErr %v", err, tt.isErr)

				return
			}
			if !tt.isErr {
				// For an empty string, we expect UnsortedSortOrder with OrderID 0
				require.NoError(t, err)
				if tt.input == "" {
					if got.OrderID() != 0 {
						t.Errorf("parseSortOrder() for empty string should return OrderID 0, got %d", got.OrderID())
					}
				} else if got.OrderID() == 0 {
					t.Errorf("parseSortOrder() returned invalid sort order for valid input")
				}

				// Validate the number of fields
				if got.Len() != tt.expectedFieldsCount {
					t.Errorf("parseSortOrder() returned %d fields, expected %d", got.Len(), tt.expectedFieldsCount)

					return
				}

				// Validate sort directions and null orders
				i := 0
				for _, field := range got.Fields() {
					if i < len(tt.expectedSourceIDs) && field.SourceID() != tt.expectedSourceIDs[i] {
						t.Errorf("parseSortOrder() field %d source ID = %v, expected %v", i, field.SourceID(), tt.expectedSourceIDs[i])
					}
					if i < len(tt.expectedDirections) && field.Direction != tt.expectedDirections[i] {
						t.Errorf("parseSortOrder() field %d direction = %v, expected %v", i, field.Direction, tt.expectedDirections[i])
					}
					if i < len(tt.expectedNullOrders) && field.NullOrder != tt.expectedNullOrders[i] {
						t.Errorf("parseSortOrder() field %d null order = %v, expected %v", i, field.NullOrder, tt.expectedNullOrders[i])
					}
					i++
				}
			}
		})
	}

	_, err := parseSortOrder("field1", nil)
	require.ErrorContains(t, err, "schema is required")
}

func TestParsedLayoutSendsFieldIDsInRestCreatePayload(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{
			ID:       42,
			Name:     "customer_id",
			Type:     iceberg.PrimitiveTypes.String,
			Required: false,
		},
		iceberg.NestedField{
			ID:       10,
			Name:     "event_time",
			Type:     iceberg.PrimitiveTypes.TimestampNs,
			Required: false,
		},
	)

	spec, err := parsePartitionSpec("customer_id,event_time", schema)
	require.NoError(t, err)
	require.NotNil(t, spec)
	sortOrder, err := parseSortOrder("event_time:desc,customer_id:asc", schema)
	require.NoError(t, err)

	var lastCreateBody map[string]any
	createCalled := false

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/v1/config":
			w.Header().Set("Content-Type", "application/json")
			require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
				"defaults":  map[string]any{},
				"overrides": map[string]any{},
				"endpoints": []string{},
			}))

			return
		case "/v1/namespaces/db/tables":
			require.Equal(t, http.MethodPost, req.Method)
			createCalled = true

			require.NoError(t, json.NewDecoder(req.Body).Decode(&lastCreateBody))

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{
				"metadata-location": "s3://warehouse/db/tbl/metadata/v1.json",
				"metadata": ` + tableMetadataJSON("") + `
			}`))

			return
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	cat, err := rest.NewCatalog(context.Background(), "rest", srv.URL)
	require.NoError(t, err)

	_, err = cat.CreateTable(context.Background(), table.Identifier{"db", "test_table"}, schema,
		catalog.WithPartitionSpec(spec), catalog.WithSortOrder(sortOrder))
	require.NoError(t, err)
	require.True(t, createCalled, "create endpoint should be called")

	rawPartitionSpec, ok := lastCreateBody["partition-spec"].(map[string]any)
	require.True(t, ok)
	fields, ok := rawPartitionSpec["fields"].([]any)
	require.True(t, ok)
	require.Len(t, fields, 2)

	field0, ok := fields[0].(map[string]any)
	require.True(t, ok)
	field1, ok := fields[1].(map[string]any)
	require.True(t, ok)
	require.EqualValues(t, 1000, field0["field-id"])
	require.EqualValues(t, 1001, field1["field-id"])
	require.EqualValues(t, 42, field0["source-id"])
	require.EqualValues(t, 10, field1["source-id"])

	rawSortOrder, ok := lastCreateBody["write-order"].(map[string]any)
	require.True(t, ok)
	sortFields, ok := rawSortOrder["fields"].([]any)
	require.True(t, ok)
	require.Len(t, sortFields, 2)

	sortField0, ok := sortFields[0].(map[string]any)
	require.True(t, ok)
	sortField1, ok := sortFields[1].(map[string]any)
	require.True(t, ok)
	require.EqualValues(t, 10, sortField0["source-id"])
	require.EqualValues(t, 42, sortField1["source-id"])
}

func tableMetadataJSON(tableUUID string) string {
	if tableUUID == "" {
		tableUUID = "bf289591-dcc0-4234-ad4f-5c3eed811a29"
	}

	return `{
		"format-version": 1,
		"table-uuid": "` + tableUUID + `",
		"location": "s3://warehouse/db/tbl",
		"last-updated-ms": 1657810967051,
		"last-column-id": 42,
		"schema": {
			"type": "struct",
			"schema-id": 0,
			"fields": [
				{"id": 42, "name": "customer_id", "required": false, "type": "string"},
				{"id": 10, "name": "event_time", "required": false, "type": "timestamp_ns"}
			]
		},
		"current-schema-id": 0,
		"schemas": [{
			"type": "struct",
			"schema-id": 0,
			"fields": [
				{"id": 42, "name": "customer_id", "required": false, "type": "string"},
				{"id": 10, "name": "event_time", "required": false, "type": "timestamp_ns"}
			]
		}],
		"partition-spec": [],
		"default-spec-id": 0,
		"last-partition-id": 999,
		"default-sort-order-id": 0,
		"sort-orders": [{"order-id": 0, "fields": []}],
		"properties": {},
		"current-snapshot-id": -1,
		"refs": {},
		"snapshots": [],
		"snapshot-log": [],
		"metadata-log": []
	}`
}
