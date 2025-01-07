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

package catalog_test

import (
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/stretchr/testify/assert"
)

func TestCatalogRegistry(t *testing.T) {
	assert.ElementsMatch(t, []string{
		"rest",
		"http",
		"https",
		"glue",
	}, catalog.GetRegisteredCatalogs())

	catalog.Register("foobar", catalog.RegistrarFunc(func(s string, p iceberg.Properties) (catalog.Catalog, error) {
		assert.Equal(t, "foobar", s)
		assert.Equal(t, "baz", p.Get("foo", ""))
		return nil, nil
	}))

	assert.ElementsMatch(t, []string{
		"rest",
		"http",
		"foobar",
		"https",
		"glue",
	}, catalog.GetRegisteredCatalogs())

	c, err := catalog.Load("foobar", iceberg.Properties{"foo": "baz"})
	assert.Nil(t, c)
	assert.NoError(t, err)

	catalog.Register("foobar", catalog.RegistrarFunc(func(s string, p iceberg.Properties) (catalog.Catalog, error) {
		assert.Equal(t, "foobar://helloworld", s)
		assert.Equal(t, "baz", p.Get("foo", ""))
		return nil, nil
	}))

	c, err = catalog.Load("foobar://helloworld", iceberg.Properties{"foo": "baz"})
	assert.Nil(t, c)
	assert.NoError(t, err)

	catalog.Unregister("foobar")
	assert.ElementsMatch(t, []string{
		"rest",
		"http",
		"https",
		"glue",
	}, catalog.GetRegisteredCatalogs())
}
