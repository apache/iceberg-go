//go:build integration

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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/catalogtest"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/utils"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func localCatalog(t *testing.T) *Catalog {
	t.Helper()
	endpoint := os.Getenv("DYNAMODB_ENDPOINT")
	if endpoint == "" {
		t.Skip("set DYNAMODB_ENDPOINT to run DynamoDB Local coverage")
	}
	cfg := aws.Config{Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""), RetryMaxAttempts: 1}
	c, err := NewCatalog(t.Context(), "test", cfg, iceberg.Properties{
		"warehouse":  "file://" + filepath.ToSlash(t.TempDir()),
		TableNameKey: "iceberg-go-" + uuid.NewString(), EndpointKey: endpoint,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, c.Close())
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err := c.client.DeleteTable(ctx, &sdk.DeleteTableInput{TableName: aws.String(c.tableName)})
		require.NoError(t, err)
	})

	return c
}

func TestDynamoDBCatalogConformance(t *testing.T) {
	catalogtest.RunCatalogTests(t, catalogtest.Config{
		NewCatalog:                  func(t *testing.T) catalog.Catalog { return localCatalog(t) },
		SupportsNamespaceProperties: true, SupportsRenameTable: true,
	})
}

func createLocalTable(t *testing.T, c *Catalog) *table.Table {
	t.Helper()
	require.NoError(t, c.CreateNamespace(t.Context(), table.Identifier{"db"}, nil))
	tbl, err := c.CreateTable(t.Context(), table.Identifier{"db", "table"}, catalogtest.Schema)
	require.NoError(t, err)

	return tbl
}

func TestDynamoDBJavaFixtureAndRegistration(t *testing.T) {
	c := localCatalog(t)
	tbl := createLocalTable(t, c)
	namespace, err := c.get(t.Context(), namespaceKey(table.Identifier{"db"}))
	require.NoError(t, err)
	require.Equal(t, "NAMESPACE", attrString(namespace[identifierCol]))
	entry, err := c.get(t.Context(), tableKey(tbl.Identifier()))
	require.NoError(t, err)
	require.Equal(t, tbl.MetadataLocation(), attrString(entry["p.metadata_location"]))
	_, err = uuid.Parse(attrString(entry[versionCol]))
	require.NoError(t, err)
	require.IsType(t, &types.AttributeValueMemberN{}, entry["created_at"])

	// Write the same raw attributes used by Java, without Go's create helpers.
	fixtureID := table.Identifier{"db", "java_table"}
	fixture := item{
		"identifier": stringAttr("db.java_table"), "namespace": stringAttr("db"),
		"v": stringAttr(uuid.NewString()), "p.table_type": stringAttr("ICEBERG"),
		"p.metadata_location": stringAttr(tbl.MetadataLocation()),
		"created_at":          &types.AttributeValueMemberN{Value: "123"},
		"updated_at":          &types.AttributeValueMemberN{Value: "123"},
		"p.java-owned":        stringAttr("keep"),
	}
	_, err = c.client.PutItem(t.Context(), &sdk.PutItemInput{TableName: aws.String(c.tableName), Item: fixture})
	require.NoError(t, err)
	loaded, err := c.LoadTable(t.Context(), fixtureID)
	require.NoError(t, err)
	require.Equal(t, tbl.MetadataLocation(), loaded.MetadataLocation())
	_, _, err = c.CommitTable(t.Context(), fixtureID, nil, []table.Update{table.NewSetPropertiesUpdate(iceberg.Properties{"comment": "updated"})})
	require.NoError(t, err)
	entry, err = c.get(t.Context(), tableKey(fixtureID))
	require.NoError(t, err)
	require.Equal(t, "keep", attrString(entry["p.java-owned"]))
	require.Equal(t, tbl.MetadataLocation(), attrString(entry["p.previous_metadata_location"]))

	registered, err := c.RegisterTable(t.Context(), table.Identifier{"db", "registered"}, tbl.MetadataLocation())
	require.NoError(t, err)
	require.Equal(t, tbl.MetadataLocation(), registered.MetadataLocation())
	_, err = c.RegisterTable(t.Context(), registered.Identifier(), tbl.MetadataLocation())
	require.ErrorIs(t, err, catalog.ErrTableAlreadyExists)
}

func TestDynamoDBNamespaceLocationAndHierarchy(t *testing.T) {
	c := localCatalog(t)
	location := "file://" + filepath.ToSlash(t.TempDir())
	require.NoError(t, c.CreateNamespace(t.Context(), table.Identifier{"a"}, iceberg.Properties{DefaultLocationKey: location}))
	require.NoError(t, c.CreateNamespace(t.Context(), table.Identifier{"ab"}, nil))
	require.NoError(t, c.CreateNamespace(t.Context(), table.Identifier{"a", "child"}, nil))
	require.NoError(t, c.CreateNamespace(t.Context(), table.Identifier{"a", "child", "deep"}, nil))
	children, err := c.ListNamespaces(t.Context(), table.Identifier{"a"})
	require.NoError(t, err)
	require.Equal(t, []table.Identifier{{"a", "child"}}, children)
	require.ErrorIs(t, c.DropNamespace(t.Context(), table.Identifier{"a"}), catalog.ErrNamespaceNotEmpty)
	tbl, err := c.CreateTable(t.Context(), table.Identifier{"a", "table"}, catalogtest.Schema)
	require.NoError(t, err)
	require.Equal(t, location+"/table", tbl.Location())
}

type transactionTransport struct {
	enabled                   atomic.Bool
	once                      sync.Once
	staged                    chan struct{}
	release                   chan struct{}
	loseResponse              bool
	failBeforeWrite           bool
	failReadsAfterTransaction bool
	transactionFinished       atomic.Bool
}

func (tr *transactionTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if tr.enabled.Load() && tr.failReadsAfterTransaction && tr.transactionFinished.Load() &&
		strings.HasSuffix(request.Header.Get("X-Amz-Target"), ".GetItem") {
		return nil, errors.New("status lookup failed")
	}
	intercept := tr.enabled.Load() && strings.HasSuffix(request.Header.Get("X-Amz-Target"), ".TransactWriteItems")
	if intercept && tr.staged != nil {
		tr.once.Do(func() { close(tr.staged); <-tr.release })
	}
	if intercept && tr.failBeforeWrite {
		return nil, errors.New("transaction response unavailable")
	}
	response, err := http.DefaultTransport.RoundTrip(request)
	if intercept {
		tr.transactionFinished.Store(true)
	}
	if intercept && tr.loseResponse && err == nil {
		io.Copy(io.Discard, response.Body)
		response.Body.Close()

		return nil, errors.New("commit response lost")
	}

	return response, err
}

func installTransport(c *Catalog, tr http.RoundTripper) {
	cfg := c.awsConfig.Copy()
	cfg.HTTPClient = &http.Client{Transport: tr}
	c.client = sdk.NewFromConfig(cfg, func(o *sdk.Options) { o.BaseEndpoint = aws.String(c.props[EndpointKey]) })
}

func TestDynamoDBCompetingCommits(t *testing.T) {
	for _, tc := range []struct {
		name             string
		failStatusLookup bool
		metadataFiles    int
	}{
		{"confirmed failure removes staged metadata", false, 2},
		{"unknown status keeps staged metadata", true, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := localCatalog(t)
			tbl := createLocalTable(t, c)
			metadataDir := filepath.Dir(strings.TrimPrefix(tbl.MetadataLocation(), "file://"))
			other, err := NewCatalog(t.Context(), "other", c.awsConfig, c.props)
			require.NoError(t, err)
			defer other.Close()
			transport := &transactionTransport{staged: make(chan struct{}), release: make(chan struct{}), failReadsAfterTransaction: tc.failStatusLookup}
			installTransport(c, transport)
			transport.enabled.Store(true)
			done := make(chan error, 1)
			go func() {
				_, _, err := c.CommitTable(t.Context(), tbl.Identifier(), nil, []table.Update{table.NewSetPropertiesUpdate(iceberg.Properties{"loser": "true"})})
				done <- err
			}()
			select {
			case <-transport.staged:
			case <-time.After(10 * time.Second):
				close(transport.release)
				t.Fatal("commit did not reach conditional write")
			}
			_, _, err = other.CommitTable(t.Context(), tbl.Identifier(), nil, []table.Update{table.NewSetPropertiesUpdate(iceberg.Properties{"winner": "true"})})
			close(transport.release)
			require.NoError(t, err)
			require.ErrorIs(t, <-done, table.ErrCommitFailed)
			loaded, err := other.LoadTable(t.Context(), tbl.Identifier())
			require.NoError(t, err)
			require.Equal(t, "true", loaded.Properties()["winner"])
			require.NotContains(t, loaded.Properties(), "loser")
			files, err := os.ReadDir(metadataDir)
			require.NoError(t, err)
			require.Len(t, files, tc.metadataFiles)
		})
	}
}

func TestDynamoDBLostCommitResponse(t *testing.T) {
	c := localCatalog(t)
	tbl := createLocalTable(t, c)
	transport := &transactionTransport{loseResponse: true}
	installTransport(c, transport)
	transport.enabled.Store(true)
	metadata, location, err := c.CommitTable(t.Context(), tbl.Identifier(), nil, []table.Update{table.NewSetPropertiesUpdate(iceberg.Properties{"comment": "committed"})})
	require.NoError(t, err)
	require.Equal(t, "committed", metadata.Properties()["comment"])
	loaded, err := c.LoadTable(t.Context(), tbl.Identifier())
	require.NoError(t, err)
	require.Equal(t, location, loaded.MetadataLocation())
	fs, err := loaded.FS(t.Context())
	require.NoError(t, err)
	file, err := fs.Open(t.Context(), location)
	require.NoError(t, err)
	require.NoError(t, file.Close())
}

func TestDynamoDBUnknownCommitOutcomeKeepsMetadata(t *testing.T) {
	c := localCatalog(t)
	tbl := createLocalTable(t, c)
	metadataDir := filepath.Dir(strings.TrimPrefix(tbl.MetadataLocation(), "file://"))
	transport := &transactionTransport{failBeforeWrite: true}
	installTransport(c, transport)
	transport.enabled.Store(true)
	_, _, err := c.CommitTable(t.Context(), tbl.Identifier(), nil, []table.Update{table.NewSetPropertiesUpdate(iceberg.Properties{"comment": "new"})})
	require.ErrorContains(t, err, "transaction response unavailable")
	loaded, err := c.LoadTable(t.Context(), tbl.Identifier())
	require.NoError(t, err)
	require.Equal(t, tbl.MetadataLocation(), loaded.MetadataLocation())
	files, err := os.ReadDir(metadataDir)
	require.NoError(t, err)
	require.Len(t, files, 2)
}

type namespaceQueryTransport struct {
	indexResponse string
	indexQueries  atomic.Int32
	scans         atomic.Int32
}

func (tr *namespaceQueryTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	target := request.Header.Get("X-Amz-Target")
	if strings.HasSuffix(target, ".Scan") {
		tr.scans.Add(1)
	}
	if strings.HasSuffix(target, ".Query") {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			return nil, err
		}
		request.Body = io.NopCloser(bytes.NewReader(body))
		var input struct{ IndexName string }
		if err := json.Unmarshal(body, &input); err != nil {
			return nil, err
		}
		if input.IndexName == namespaceIndex {
			tr.indexQueries.Add(1)
			if tr.indexResponse != "" {
				return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/x-amz-json-1.0"}},
					Body: io.NopCloser(strings.NewReader(tr.indexResponse)), Request: request}, nil
			}
		}
	}

	return http.DefaultTransport.RoundTrip(request)
}

func TestDynamoDBDropNamespaceIndexAndConsistentScan(t *testing.T) {
	for _, tc := range []struct {
		name          string
		withTable     bool
		indexResponse string
		notEmpty      bool
		wantScans     int32
	}{
		{"index finds live table", true, `{"Items":[{"identifier":{"S":"db.table"},"namespace":{"S":"db"}}]}`, true, 0},
		{"index misses live table", true, `{"Items":[]}`, true, 1},
		{"stale index entry", false, `{"Items":[{"identifier":{"S":"db.missing"},"namespace":{"S":"db"}}]}`, false, 1},
		{"empty namespace", false, "", false, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := localCatalog(t)
			if tc.withTable {
				createLocalTable(t, c)
			} else {
				require.NoError(t, c.CreateNamespace(t.Context(), table.Identifier{"db"}, nil))
			}
			transport := &namespaceQueryTransport{indexResponse: tc.indexResponse}
			installTransport(c, transport)
			err := c.DropNamespace(t.Context(), table.Identifier{"db"})
			if tc.notEmpty {
				require.ErrorIs(t, err, catalog.ErrNamespaceNotEmpty)
			} else {
				require.NoError(t, err)
			}
			require.EqualValues(t, 1, transport.indexQueries.Load())
			require.Equal(t, tc.wantScans, transport.scans.Load())
		})
	}
}

func TestDynamoDBNamespaceDropCreateRace(t *testing.T) {
	c := localCatalog(t)
	require.NoError(t, c.CreateNamespace(t.Context(), table.Identifier{"db"}, nil))
	other, err := NewCatalog(t.Context(), "other", c.awsConfig, c.props)
	require.NoError(t, err)
	defer other.Close()
	transport := &transactionTransport{staged: make(chan struct{}), release: make(chan struct{})}
	installTransport(c, transport)
	transport.enabled.Store(true)
	done := make(chan error, 1)
	go func() { done <- c.DropNamespace(t.Context(), table.Identifier{"db"}) }()
	select {
	case <-transport.staged:
	case <-time.After(10 * time.Second):
		close(transport.release)
		t.Fatal("drop did not reach conditional write")
	}
	_, err = other.CreateTable(t.Context(), table.Identifier{"db", "table"}, catalogtest.Schema)
	close(transport.release)
	require.NoError(t, err)
	require.ErrorIs(t, <-done, table.ErrCommitFailed)
	exists, err := c.CheckNamespaceExists(t.Context(), table.Identifier{"db"})
	require.NoError(t, err)
	require.True(t, exists)
}

func TestDynamoDBCompetingRenames(t *testing.T) {
	c := localCatalog(t)
	first := createLocalTable(t, c)
	_, err := c.CreateTable(t.Context(), table.Identifier{"db", "other"}, catalogtest.Schema)
	require.NoError(t, err)
	start := make(chan struct{})
	done := make(chan error, 2)
	for _, from := range []table.Identifier{first.Identifier(), {"db", "other"}} {
		go func() {
			<-start
			_, err := c.RenameTable(t.Context(), from, table.Identifier{"db", "target"})
			done <- err
		}()
	}
	close(start)
	successes := 0
	for range 2 {
		err := <-done
		if err == nil {
			successes++
		} else {
			require.ErrorIs(t, err, catalog.ErrTableAlreadyExists)
		}
	}
	require.Equal(t, 1, successes)
}

func TestDynamoDBPagination(t *testing.T) {
	c := localCatalog(t)
	// A service middleware forces small pages while keeping real DynamoDB query
	// behavior and continuation keys. This covers both base-table and GSI queries.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		if strings.HasSuffix(r.Header.Get("X-Amz-Target"), ".Query") {
			var input map[string]any
			if err := json.Unmarshal(body, &input); err != nil {
				t.Error(err)
				return
			}
			input["Limit"] = 1
			body, _ = json.Marshal(input)
		}
		request, err := http.NewRequestWithContext(r.Context(), r.Method, c.props[EndpointKey], bytes.NewReader(body))
		if err != nil {
			t.Error(err)
			return
		}
		request.Header = r.Header.Clone()
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Error(err)
			return
		}
		defer response.Body.Close()
		w.WriteHeader(response.StatusCode)
		io.Copy(w, response.Body)
	}))
	defer server.Close()
	originalClient := c.client
	defer func() { c.client = originalClient }()
	c.client = sdk.NewFromConfig(c.awsConfig, func(o *sdk.Options) { o.BaseEndpoint = aws.String(server.URL) })
	for _, ns := range []string{"a", "b", "c"} {
		require.NoError(t, c.CreateNamespace(t.Context(), table.Identifier{ns}, nil))
	}
	namespaces, err := c.ListNamespaces(t.Context(), nil)
	require.NoError(t, err)
	require.Len(t, namespaces, 3)
	for _, name := range []string{"one", "two", "three"} {
		_, err := c.CreateTable(t.Context(), table.Identifier{"a", name}, catalogtest.Schema)
		require.NoError(t, err)
	}
	count := 0
	for _, err := range c.ListTables(t.Context(), table.Identifier{"a"}) {
		require.NoError(t, err)
		count++
	}
	require.Equal(t, 3, count)
}

func TestDynamoDBNamespaceParentAndJavaGaps(t *testing.T) {
	c := localCatalog(t)
	require.ErrorIs(t, c.CreateNamespace(t.Context(), table.Identifier{"missing", "child"}, nil), catalog.ErrNoSuchNamespace)
	require.NoError(t, c.CreateNamespace(t.Context(), table.Identifier{"parent"}, nil))
	gap := namespaceKey(table.Identifier{"parent", "missing", "deep"})
	setEntryMetadata(gap, true)
	_, err := c.client.PutItem(t.Context(), &sdk.PutItemInput{TableName: aws.String(c.tableName), Item: gap})
	require.NoError(t, err)
	require.ErrorIs(t, c.DropNamespace(t.Context(), table.Identifier{"parent"}), catalog.ErrNamespaceNotEmpty)
}

func TestDynamoDBNamespaceDropChildCreationRace(t *testing.T) {
	c := localCatalog(t)
	require.NoError(t, c.CreateNamespace(t.Context(), table.Identifier{"db"}, nil))
	other, err := NewCatalog(t.Context(), "other", c.awsConfig, c.props)
	require.NoError(t, err)
	defer other.Close()
	transport := &transactionTransport{staged: make(chan struct{}), release: make(chan struct{})}
	installTransport(c, transport)
	transport.enabled.Store(true)
	done := make(chan error, 1)
	go func() { done <- c.DropNamespace(t.Context(), table.Identifier{"db"}) }()
	select {
	case <-transport.staged:
	case <-time.After(10 * time.Second):
		close(transport.release)
		t.Fatal("drop did not reach conditional write")
	}
	err = other.CreateNamespace(t.Context(), table.Identifier{"db", "child"}, nil)
	close(transport.release)
	require.NoError(t, err)
	require.ErrorIs(t, <-done, table.ErrCommitFailed)
	exists, err := c.CheckNamespaceExists(t.Context(), table.Identifier{"db"})
	require.NoError(t, err)
	require.True(t, exists)
}

func TestDynamoDBRejectsNumericKeySchema(t *testing.T) {
	c := localCatalog(t)
	name := "incompatible-" + uuid.NewString()
	_, err := c.client.CreateTable(t.Context(), &sdk.CreateTableInput{
		TableName: aws.String(name), BillingMode: types.BillingModePayPerRequest,
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String(identifierCol), AttributeType: types.ScalarAttributeTypeN},
			{AttributeName: aws.String(namespaceCol), AttributeType: types.ScalarAttributeTypeN},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String(identifierCol), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String(namespaceCol), KeyType: types.KeyTypeRange},
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{{
			IndexName: aws.String(namespaceIndex),
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String(namespaceCol), KeyType: types.KeyTypeHash},
				{AttributeName: aws.String(identifierCol), KeyType: types.KeyTypeRange},
			}, Projection: &types.Projection{ProjectionType: types.ProjectionTypeKeysOnly},
		}},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_, err := c.client.DeleteTable(context.Background(), &sdk.DeleteTableInput{TableName: aws.String(name)})
		require.NoError(t, err)
	})
	props := maps.Clone(c.props)
	props[TableNameKey] = name
	_, err = NewCatalog(t.Context(), "invalid", c.awsConfig, props)
	require.ErrorContains(t, err, "must have string type")
}

func TestDynamoDBReconcilesCreateAndRenameResponses(t *testing.T) {
	c := localCatalog(t)
	transport := &transactionTransport{loseResponse: true}
	installTransport(c, transport)
	transport.enabled.Store(true)
	require.NoError(t, c.CreateNamespace(t.Context(), table.Identifier{"db"}, nil))
	tbl, err := c.CreateTable(t.Context(), table.Identifier{"db", "table"}, catalogtest.Schema)
	require.NoError(t, err)
	renamed, err := c.RenameTable(t.Context(), tbl.Identifier(), table.Identifier{"db", "renamed"})
	require.NoError(t, err)
	require.Equal(t, tbl.MetadataLocation(), renamed.MetadataLocation())
}

func TestDynamoDBRegistry(t *testing.T) {
	c := localCatalog(t)
	props := maps.Clone(c.props)
	props["type"] = "dynamodb"
	loaded, err := catalog.Load(utils.WithAWSConfig(t.Context(), &c.awsConfig), "registry", props)
	require.NoError(t, err)
	require.Equal(t, "registry", loaded.Name())
	require.Equal(t, catalog.DynamoDB, loaded.CatalogType())
	require.NoError(t, loaded.Close())
}
