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

	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

func TestValidateTableIdentifier(t *testing.T) {
	require.NoError(t, catalog.ValidateTableIdentifier(table.Identifier{"namespace", "table"}))
	require.NoError(t, catalog.ValidateTableIdentifier(table.Identifier{"parent", "namespace", "table"}))

	require.ErrorIs(t, catalog.ValidateTableIdentifier(nil), catalog.ErrNoSuchTable)
	require.ErrorIs(t, catalog.ValidateTableIdentifier(table.Identifier{}), catalog.ErrNoSuchTable)
	require.ErrorIs(t, catalog.ValidateTableIdentifier(table.Identifier{"table"}), catalog.ErrNoSuchTable)
}
