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

package rest_test

import (
	"context"
	"testing"

	"github.com/DataDog/iceberg-go"
	"github.com/DataDog/iceberg-go/catalog"
	"github.com/DataDog/iceberg-go/catalog/catalogtest"
	"github.com/DataDog/iceberg-go/io"
	"github.com/stretchr/testify/require"
)

func TestRestCatalogConformance(t *testing.T) {
	catalogtest.RunCatalogTests(t, catalogtest.Config{
		NewCatalog: func(t *testing.T) catalog.Catalog {
			cat, err := catalog.Load(context.Background(), "local", iceberg.Properties{
				"type":               "rest",
				"uri":                "http://localhost:8181",
				io.S3Region:          "us-east-1",
				io.S3AccessKeyID:     "admin",
				io.S3SecretAccessKey: "password",
			})
			require.NoError(t, err)
			t.Cleanup(func() {
				if closer, ok := cat.(catalog.Closer); ok {
					require.NoError(t, closer.Close())
				}
			})

			return cat
		},
	})
}
