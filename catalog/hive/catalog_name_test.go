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

// Package catalog provides an interface for Catalog implementations along with
// a registry for registering catalog implementations.
//
// Subpackages of this package provide some default implementations for select
// catalog types which will register themselves if imported. For instance,
// adding the following import:
//
//	import _ "github.com/apache/iceberg-go/catalog/rest"
//
// Will register the REST catalog implementation.
package hive

import "testing"

func TestHiveCatalogName(t *testing.T) {
	cat := NewCatalogWithClient(nil, nil)
	if got := cat.Name(); got != "hive" {
		t.Fatalf("default catalog name = %q, want %q", got, "hive")
	}

	WithName("analytics")(cat.opts)
	if got := cat.Name(); got != "analytics" {
		t.Fatalf("configured catalog name = %q, want %q", got, "analytics")
	}
}
