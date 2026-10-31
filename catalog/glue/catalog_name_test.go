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

package glue

import (
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
)

func TestGlueCatalogName(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts []Option
	}{
		{name: "glue"},
		{name: "analytics", opts: []Option{WithName("analytics")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cat, err := NewCatalog(tc.opts...)
			if err != nil {
				t.Fatal(err)
			}
			if got := cat.Name(); got != tc.name {
				t.Fatalf("catalog name = %q, want %q", got, tc.name)
			}
		})
	}
}

func TestGlueCatalogRegistrationForwardsName(t *testing.T) {
	const name = "analytics"
	cat, err := catalog.Load(t.Context(), name, iceberg.Properties{
		"type":          "glue",
		Region:          "us-east-1",
		AccessKeyID:     "test-access-key",
		SecretAccessKey: "test-secret-key",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := cat.Close(); err != nil {
			t.Errorf("close Glue catalog: %v", err)
		}
	})
	if got := cat.Name(); got != name {
		t.Fatalf("registered catalog name = %q, want %q", got, name)
	}
	if got := cat.CatalogType(); got != catalog.Glue {
		t.Fatalf("registered catalog type = %q, want %q", got, catalog.Glue)
	}
}
