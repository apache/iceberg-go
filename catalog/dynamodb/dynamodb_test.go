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

package dynamodb

import (
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestJavaItemLayout(t *testing.T) {
	ns := table.Identifier{"db", "child"}
	key := namespaceKey(ns)
	require.Equal(t, "NAMESPACE", attrString(key[identifierCol]))
	require.Equal(t, "db.child", attrString(key[namespaceCol]))
	entry := newTableEntry(table.Identifier{"db", "child", "table"}, "s3://bucket/table/metadata/00000.metadata.json")
	require.Equal(t, "db.child.table", attrString(entry[identifierCol]))
	require.Equal(t, "db.child", attrString(entry[namespaceCol]))
	require.Equal(t, "ICEBERG", attrString(entry["p.table_type"]))
	require.NotEmpty(t, attrString(entry[metadataLocationProp]))
	_, err := uuid.Parse(attrString(entry[versionCol]))
	require.NoError(t, err)
	require.IsType(t, &types.AttributeValueMemberN{}, entry["created_at"])
	createdAt := entry["created_at"]
	oldVersion := attrString(entry[versionCol])
	setEntryMetadata(entry, false)
	require.NotEqual(t, oldVersion, attrString(entry[versionCol]))
	require.Equal(t, createdAt, entry["created_at"])
	entry["p.comment"] = stringAttr("hello")
	require.Equal(t, "hello", properties(entry)["comment"])
	require.NotContains(t, properties(entry), "v")
}

func TestIdentifierValidation(t *testing.T) {
	for _, ident := range []table.Identifier{nil, {}, {"table"}, {"", "table"}, {"db.child", "table"}, {"db", "table.name"}, {"db", ""}} {
		require.Error(t, validateIdentifier(ident))
	}
	require.NoError(t, validateIdentifier(table.Identifier{"db", "child", "table"}))
	require.NoError(t, validateNamespace(nil, true))
	require.ErrorIs(t, validateNamespace(nil, false), catalog.ErrNoSuchNamespace)
}

func TestConstructorRequiresWarehouse(t *testing.T) {
	_, err := NewCatalog(t.Context(), "test", aws.Config{}, iceberg.Properties{})
	require.ErrorContains(t, err, "requires a warehouse")
}

func TestConditionalFailureClassification(t *testing.T) {
	require.False(t, conditionFailed(nil))
	require.True(t, conditionFailed(&types.ConditionalCheckFailedException{}))
	require.True(t, conditionFailed(&types.TransactionCanceledException{CancellationReasons: []types.CancellationReason{{Code: aws.String("ConditionalCheckFailed")}}}))
	require.False(t, conditionFailed(&types.TransactionCanceledException{CancellationReasons: []types.CancellationReason{{Code: aws.String("TransactionConflict")}}}))
}

func TestTransactionConflictClassification(t *testing.T) {
	require.False(t, transactionConflict(nil))
	require.True(t, transactionConflict(&types.TransactionConflictException{}))
	require.True(t, transactionConflict(&types.TransactionCanceledException{CancellationReasons: []types.CancellationReason{{Code: aws.String("TransactionConflict")}}}))
	require.False(t, transactionConflict(&types.TransactionCanceledException{CancellationReasons: []types.CancellationReason{{Code: aws.String("ValidationError")}}}))
}
