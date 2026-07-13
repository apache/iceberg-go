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

	for _, ident := range []table.Identifier{
		nil,
		{},
		{"table"},
		{"namespace", ""},
		{"namespace", "."},
		{"namespace", ".."},
		{"namespace", "table/name"},
		{"namespace", "table\nname"},
		{"", "table"},
		{"namespace/child", "table"},
	} {
		require.ErrorIs(t, catalog.ValidateTableIdentifier(ident), catalog.ErrNoSuchTable)
	}
}

func TestValidateViewIdentifier(t *testing.T) {
	require.NoError(t, catalog.ValidateViewIdentifier(table.Identifier{"namespace", "view"}))

	require.ErrorIs(t, catalog.ValidateViewIdentifier(table.Identifier{"view"}), catalog.ErrNoSuchView)
	require.ErrorIs(t, catalog.ValidateViewIdentifier(table.Identifier{"namespace", "view/name"}), catalog.ErrNoSuchView)
	require.NotErrorIs(t, catalog.ValidateViewIdentifier(table.Identifier{"namespace", "view/name"}), catalog.ErrNoSuchTable)
}

func TestValidateFunctionIdentifier(t *testing.T) {
	require.NoError(t, catalog.ValidateFunctionIdentifier(table.Identifier{"namespace", "function"}))

	require.ErrorIs(t, catalog.ValidateFunctionIdentifier(table.Identifier{"function"}), catalog.ErrNoSuchFunction)
	require.ErrorIs(t, catalog.ValidateFunctionIdentifier(table.Identifier{"namespace", "function/name"}), catalog.ErrNoSuchFunction)
	require.NotErrorIs(t, catalog.ValidateFunctionIdentifier(table.Identifier{"namespace", "function/name"}), catalog.ErrNoSuchTable)
}

func TestObjectNameFromIdent(t *testing.T) {
	require.Equal(t, "object", catalog.ObjectNameFromIdent(table.Identifier{"namespace", "object"}))
	require.Equal(t, "object", catalog.ObjectNameFromIdent(table.Identifier{"parent", "namespace", "object"}))
	require.Equal(t, "object", catalog.ObjectNameFromIdent(table.Identifier{"object"}))
	require.Equal(t, "", catalog.ObjectNameFromIdent(table.Identifier{}))

	// TableNameFromIdent stays as the table-flavored alias of the same rule.
	require.Equal(t, "object", catalog.TableNameFromIdent(table.Identifier{"namespace", "object"}))
}
