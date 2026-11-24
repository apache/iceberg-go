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

package iceberg_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

// typesFixturesSkip names type cases iceberg-go does not pass yet, with the
// reason. A type iceberg-go does not model goes here as "unsupported: ...".
// Listed cases still run, and fail once they pass so the entry gets removed.
var typesFixturesSkip = map[string]string{}

type typesFixtureCase struct {
	ID             string          `json:"id"`
	Valid          bool            `json:"valid"`
	Input          json.RawMessage `json:"input"`
	Decoded        json.RawMessage `json:"decoded"`
	Canonical      *string         `json:"canonical"`
	NormativeLevel string          `json:"normative_level"`
}

// TestVerificationTypes runs the table-spec/types cases of an
// iceberg-verification checkout, as table-spec/types/README.md describes.
func TestVerificationTypes(t *testing.T) {
	dir := os.Getenv("ICEBERG_VERIFICATION_DIR")
	if dir == "" {
		t.Skip("ICEBERG_VERIFICATION_DIR is not set")
	}

	seen := make(map[string]bool)
	for _, surface := range []string{"primitive", "nested", "variant", "geospatial"} {
		data, err := os.ReadFile(filepath.Join(dir, "table-spec", "types", surface, "cases.json"))
		require.NoError(t, err)
		var file struct {
			Cases []typesFixtureCase `json:"cases"`
		}
		require.NoError(t, json.Unmarshal(data, &file), surface)
		require.NotEmpty(t, file.Cases, surface)

		for _, c := range file.Cases {
			name := surface + "/" + c.ID
			seen[name] = true
			t.Run(name, func(t *testing.T) {
				err := checkTypesFixtureCase(c)
				if reason, ok := typesFixturesSkip[name]; ok {
					if err == nil {
						t.Fatal("case passes now; remove it from typesFixturesSkip")
					}
					t.Skipf("%s: %v", reason, err)
				}
				if err != nil {
					if c.NormativeLevel == "should" {
						t.Skipf("advisory_fail: %v", err)
					}
					t.Error(err)
				}
			})
		}
	}

	for name := range typesFixturesSkip {
		if !seen[name] {
			t.Errorf("typesFixturesSkip names %s, which is not a case", name)
		}
	}
}

func checkTypesFixtureCase(c typesFixtureCase) error {
	// Parse the input the way a schema field's type is read.
	fieldJSON := fmt.Sprintf(`{"id":1,"name":"f","required":true,"type":%s}`, c.Input)
	var field iceberg.NestedField
	err := json.Unmarshal([]byte(fieldJSON), &field)
	if !c.Valid {
		if err == nil {
			return fmt.Errorf("input %s parsed as %s, want a rejection", c.Input, field.Type)
		}

		return nil
	}
	if err != nil {
		return fmt.Errorf("input %s: %w", c.Input, err)
	}

	var want any
	if err := json.Unmarshal(c.Decoded, &want); err != nil {
		return err
	}
	gotJSON, err := json.Marshal(decodedTypeOf(field.Type))
	if err != nil {
		return err
	}
	var got any
	if err := json.Unmarshal(gotJSON, &got); err != nil {
		return err
	}
	if !reflect.DeepEqual(got, want) {
		return fmt.Errorf("input %s decoded as %s, want %s", c.Input, gotJSON, c.Decoded)
	}

	if c.Canonical != nil {
		written, err := json.Marshal(field)
		if err != nil {
			return err
		}
		var out struct {
			Type any `json:"type"`
		}
		if err := json.Unmarshal(written, &out); err != nil {
			return err
		}
		if out.Type != *c.Canonical {
			return fmt.Errorf("input %s re-serialized as %v, want %q", c.Input, out.Type, *c.Canonical)
		}
	}

	return nil
}

// decodedTypeOf maps a type to the language-neutral decoded shape of
// table-spec/types/README.md.
func decodedTypeOf(typ iceberg.Type) map[string]any {
	switch typ := typ.(type) {
	case iceberg.DecimalType:
		return map[string]any{"type": "decimal", "precision": typ.Precision(), "scale": typ.Scale()}
	case iceberg.FixedType:
		return map[string]any{"type": "fixed", "length": typ.Len()}
	case iceberg.GeometryType:
		return map[string]any{"type": "geometry", "crs": typ.CRS()}
	case iceberg.GeographyType:
		return map[string]any{"type": "geography", "crs": typ.CRS(), "algorithm": typ.Algorithm()}
	case *iceberg.StructType:
		fields := []any{}
		for _, f := range typ.Fields() {
			fields = append(fields, map[string]any{
				"id": f.ID, "name": f.Name, "required": f.Required, "type": decodedTypeOf(f.Type),
			})
		}

		return map[string]any{"type": "struct", "fields": fields}
	case *iceberg.ListType:
		return map[string]any{
			"type": "list", "element-id": typ.ElementID,
			"element-required": typ.ElementRequired, "element": decodedTypeOf(typ.Element),
		}
	case *iceberg.MapType:
		return map[string]any{
			"type": "map", "key-id": typ.KeyID, "key": decodedTypeOf(typ.KeyType),
			"value-id": typ.ValueID, "value-required": typ.ValueRequired, "value": decodedTypeOf(typ.ValueType),
		}
	default:
		return map[string]any{"type": typ.Type()}
	}
}
