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
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
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

const activeCatalogTable = `{"Table":{"TableName":"iceberg","TableStatus":"ACTIVE",
"AttributeDefinitions":[{"AttributeName":"identifier","AttributeType":"S"},{"AttributeName":"namespace","AttributeType":"S"}],
"KeySchema":[{"AttributeName":"identifier","KeyType":"HASH"},{"AttributeName":"namespace","KeyType":"RANGE"}],
"GlobalSecondaryIndexes":[{"IndexName":"namespace-identifier","IndexStatus":"ACTIVE",
"KeySchema":[{"AttributeName":"namespace","KeyType":"HASH"},{"AttributeName":"identifier","KeyType":"RANGE"}]}]}}`

func describeTableServer(t *testing.T) (*httptest.Server, *atomic.Int32) {
	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		require.Equal(t, "DynamoDB_20120810.DescribeTable", r.Header.Get("X-Amz-Target"))
		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		_, _ = w.Write([]byte(activeCatalogTable))
	}))
	t.Cleanup(server.Close)

	return server, &hits
}

func TestNewCatalogAppliesClientOptions(t *testing.T) {
	cfg := aws.Config{Region: "us-east-1", Credentials: aws.AnonymousCredentials{}}
	withEndpoint := func(url string) Option {
		return WithClientOptions(func(o *sdk.Options) { o.BaseEndpoint = aws.String(url) })
	}

	t.Run("client options", func(t *testing.T) {
		server, hits := describeTableServer(t)
		c, err := NewCatalog(t.Context(), "test", cfg, iceberg.Properties{"warehouse": "s3://bucket/wh"}, withEndpoint(server.URL))
		require.NoError(t, err)
		require.NoError(t, c.Close())
		require.Positive(t, hits.Load())
	})

	t.Run("endpoint property wins", func(t *testing.T) {
		optionServer, optionHits := describeTableServer(t)
		propServer, propHits := describeTableServer(t)
		c, err := NewCatalog(t.Context(), "test", cfg, iceberg.Properties{"warehouse": "s3://bucket/wh", EndpointKey: propServer.URL}, withEndpoint(optionServer.URL))
		require.NoError(t, err)
		require.NoError(t, c.Close())
		require.Positive(t, propHits.Load())
		require.Zero(t, optionHits.Load())
	})
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
