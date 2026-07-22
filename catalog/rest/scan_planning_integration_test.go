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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	stdio "io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
)

const (
	runIntegrationTestsEnv            = "RUN_INTEGRATION_TESTS"
	integrationAccessDelegation       = "vended-credentials"
	integrationAccessDelegationHeader = "X-Iceberg-Access-Delegation"
	integrationGCSToken               = "scan-planning-integration-token"
	integrationGCSTokenKey            = "gcs.oauth2.token"
)

func (s *RestIntegrationSuite) TestScanPlanningJavaInteroperability() {
	s.requireScanPlanningIntegration()

	transport := newScanPlanningCaptureTransport()
	s.T().Cleanup(transport.CloseIdleConnections)
	planningCatalog, err := rest.NewCatalog(s.ctx, "scan-planning-java-wire", "http://localhost:8181",
		rest.WithCustomTransport(transport))
	s.Require().NoError(err)
	s.T().Cleanup(func() { s.Require().NoError(planningCatalog.Close()) })
	s.requireJavaScanPlanningCapabilities(planningCatalog)

	ident := catalog.ToIdentifier(TestNamespaceIdent, "scan-planning-java-wire")
	tbl, dataPath := s.createScanPlanningTable(ident)

	filter, err := json.Marshal(iceberg.EqualTo(iceberg.Reference("foo"), "hello"))
	s.Require().NoError(err)
	snapshotID := tbl.CurrentSnapshot().SnapshotID
	caseSensitive := true
	useSnapshotSchema := false
	minRowsRequested := int64(1)

	response, err := planningCatalog.PlanTableScan(s.ctx, ident, rest.PlanTableScanRequest{
		AccessDelegation:  stringPointer(integrationAccessDelegation),
		SnapshotID:        &snapshotID,
		Select:            []string{"foo", "bar"},
		Filter:            filter,
		MinRowsRequested:  &minRowsRequested,
		CaseSensitive:     &caseSensitive,
		UseSnapshotSchema: &useSnapshotSchema,
		StatsFields:       []string{"foo", "bar"},
	})
	s.Require().NoError(err)
	s.Require().Equal(rest.PlanStatusCompleted, response.Status)
	s.Require().NotNil(response.PlanID)
	s.Require().NotEmpty(*response.PlanID)
	planID := *response.PlanID
	planCancelled := false
	s.T().Cleanup(func() {
		if planCancelled {
			return
		}

		_ = cancelPlanningWithTimeout(planningCatalog, ident, planID)
	})
	s.Empty(response.PlanTasks)
	s.Require().Len(response.FileScanTasks, 1)
	s.Empty(response.DeleteFiles)
	s.assertJavaPlanningCredentials(response.StorageCredentials)

	rawResponses := transport.PlanResponses()
	s.Require().Len(rawResponses, 1)
	requestHeaders := transport.PlanRequestHeaders()
	s.Require().Len(requestHeaders, 1)
	s.Equal(integrationAccessDelegation, requestHeaders[0].Get(integrationAccessDelegationHeader))
	raw := parseRawPlanningFixture(s.T(), string(rawResponses[0]))
	s.Equal("completed", raw.Status)
	s.Equal(*response.PlanID, raw.PlanID)
	s.Empty(raw.PlanTasks)
	s.Require().Len(raw.FileScanTasks, 1)
	s.Empty(raw.DeleteFiles)

	task := raw.FileScanTasks[0]
	s.Equal("data", task.DataFile.Content)
	s.Equal(dataPath, task.DataFile.FilePath)
	s.Require().Len(task.DataFile.Partition, 1)
	s.Equal("17", string(task.DataFile.Partition[0]),
		"Java partitions must remain typed JSON values, not binary-bound strings")
	s.JSONEq(`{"type":"eq","term":"foo","value":"hello"}`, string(task.ResidualFilter))
	s.Empty(task.DeleteFileReferences)

	lowerBounds := valueMapByFieldID(s, task.DataFile.LowerBounds)
	upperBounds := valueMapByFieldID(s, task.DataFile.UpperBounds)
	s.ElementsMatch([]int{1, 2}, task.DataFile.LowerBounds.Keys)
	s.ElementsMatch([]int{1, 2}, task.DataFile.UpperBounds.Keys)
	s.Equal("68656C6C6F", lowerBounds[1], "Java string lower bounds must be uppercase hexadecimal")
	s.Equal("68656C6C6F", upperBounds[1], "Java string upper bounds must be uppercase hexadecimal")
	s.Equal("11000000", lowerBounds[2], "Java int lower bounds must use Iceberg little-endian binary")
	s.Equal("11000000", upperBounds[2], "Java int upper bounds must use Iceberg little-endian binary")

	s.Require().Len(raw.StorageCredentials, 1)
	s.Equal("gcp", raw.StorageCredentials[0].Prefix)
	s.Equal(integrationGCSToken, raw.StorageCredentials[0].Config[integrationGCSTokenKey])

	// The reference fixture retains its synchronous plan state until the client
	// releases it, so this also verifies the advertised DELETE route end to end.
	s.Require().NoError(cancelPlanningWithTimeout(planningCatalog, ident, planID))
	planCancelled = true
}

func (s *RestIntegrationSuite) TestScanPlanningJavaErrorTypes() {
	s.requireScanPlanningIntegration()
	s.requireJavaScanPlanningCapabilities(s.cat)
	s.ensureNamespace()
	// The published fixture defaults to synchronous inline planning and exposes
	// no container setting for async planning or a smaller plan-task page size.
	// The not-found cases still exercise the live GET and POST routes and their
	// Java error models; deterministic successful polling/fanout remains covered
	// by planfake until the reference image exposes those controls.

	missingTable := catalog.ToIdentifier(TestNamespaceIdent, "missing-scan-planning-table")
	_, err := s.cat.PlanTableScan(s.ctx, missingTable, rest.PlanTableScanRequest{})
	s.ErrorIs(err, catalog.ErrNoSuchTable)

	ident := catalog.ToIdentifier(TestNamespaceIdent, "scan-planning-java-errors")
	tbl, err := s.cat.CreateTable(s.ctx, ident, tableSchemaSimple)
	s.Require().NoError(err)
	s.Require().NotNil(tbl)
	s.T().Cleanup(func() { s.Require().NoError(s.cat.DropTable(s.ctx, ident)) })

	_, err = s.cat.FetchPlanningResult(s.ctx, ident, "missing-plan-id", rest.FetchPlanningResultOptions{})
	s.ErrorIs(err, rest.ErrPlanExpired)

	_, err = s.cat.FetchScanTasks(s.ctx, ident, rest.FetchScanTasksRequest{PlanTask: "missing-plan-task"})
	s.ErrorIs(err, rest.ErrNoSuchPlanTask)
}

func (s *RestIntegrationSuite) requireScanPlanningIntegration() {
	s.T().Helper()
	if os.Getenv(runIntegrationTestsEnv) != "1" {
		s.T().Skipf("set %s=1 to run Java REST scan-planning integration tests", runIntegrationTestsEnv)
	}
}

func (s *RestIntegrationSuite) requireJavaScanPlanningCapabilities(cat *rest.Catalog) {
	s.T().Helper()
	s.Require().True(cat.SupportsPlanTableScan(), "Java fixture must advertise planTableScan")
	s.Require().True(cat.SupportsFullRemoteScanPlanning(),
		"Java fixture must advertise plan, poll, cancel, and task-fetch endpoints")
}

func cancelPlanningWithTimeout(cat *rest.Catalog, ident table.Identifier, planID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return cat.CancelPlanning(ctx, ident, planID)
}

func (s *RestIntegrationSuite) createScanPlanningTable(ident table.Identifier) (*table.Table, string) {
	s.T().Helper()
	s.ensureNamespace()

	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{2},
		FieldID:   1000,
		Name:      "bar",
		Transform: iceberg.IdentityTransform{},
	})
	tbl, err := s.cat.CreateTable(s.ctx, ident, tableSchemaSimple, catalog.WithPartitionSpec(&spec))
	s.Require().NoError(err)
	s.Require().NotNil(tbl)
	s.T().Cleanup(func() { s.Require().NoError(s.cat.DropTable(s.ctx, ident)) })

	arrowSchema, err := table.SchemaToArrowSchema(tableSchemaSimple, nil, false, false)
	s.Require().NoError(err)
	arrowTable, err := array.TableFromJSON(memory.DefaultAllocator, arrowSchema,
		[]string{`[{"foo":"hello","bar":17,"baz":true}]`})
	s.Require().NoError(err)
	defer arrowTable.Release()

	dataPath, err := url.JoinPath(tbl.Location(), "data", "bar=17", "data.parquet")
	s.Require().NoError(err)
	file, err := mustFS(s.T(), tbl).(iceio.WriteFileIO).Create(dataPath)
	s.Require().NoError(err)
	s.Require().NoError(pqarrow.WriteTable(arrowTable, file, arrowTable.NumRows(),
		nil, pqarrow.DefaultWriterProps()))
	s.T().Cleanup(func() { s.Require().NoError(mustFS(s.T(), tbl).Remove(dataPath)) })

	txn := tbl.NewTransaction()
	s.Require().NoError(txn.AddFiles(s.ctx, []string{dataPath}, nil, false))
	updated, err := txn.Commit(s.ctx)
	s.Require().NoError(err)
	s.Require().NotNil(updated.CurrentSnapshot())

	return updated, dataPath
}

func (s *RestIntegrationSuite) assertJavaPlanningCredentials(credentials []rest.StorageCredential) {
	s.T().Helper()
	s.Require().Len(credentials, 1)
	s.Equal("gcp", credentials[0].Prefix)
	s.Equal(integrationGCSToken, credentials[0].Config[integrationGCSTokenKey])
}

func valueMapByFieldID(s *RestIntegrationSuite, valueMap rawValueMapFixture) map[int]string {
	s.T().Helper()
	s.Require().Len(valueMap.Values, len(valueMap.Keys))

	values := make(map[int]string, len(valueMap.Keys))
	for i, fieldID := range valueMap.Keys {
		values[fieldID] = valueMap.Values[i]
	}

	return values
}

func stringPointer(value string) *string {
	return &value
}

type scanPlanningCaptureTransport struct {
	base *http.Transport

	mu            sync.Mutex
	planRequests  []http.Header
	planResponses [][]byte
}

func newScanPlanningCaptureTransport() *scanPlanningCaptureTransport {
	return &scanPlanningCaptureTransport{base: http.DefaultTransport.(*http.Transport).Clone()}
}

func (t *scanPlanningCaptureTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	isPlanRequest := request.Method == http.MethodPost && strings.HasSuffix(request.URL.Path, "/plan")
	if isPlanRequest {
		t.mu.Lock()
		t.planRequests = append(t.planRequests, request.Header.Clone())
		t.mu.Unlock()
	}

	response, err := t.base.RoundTrip(request)
	if err != nil || !isPlanRequest {
		return response, err
	}

	body, err := stdio.ReadAll(response.Body)
	if err != nil {
		_ = response.Body.Close()

		return nil, fmt.Errorf("read Java plan response: %w", err)
	}
	if err := response.Body.Close(); err != nil {
		return nil, fmt.Errorf("close Java plan response: %w", err)
	}
	response.Body = stdio.NopCloser(bytes.NewReader(body))

	t.mu.Lock()
	t.planResponses = append(t.planResponses, bytes.Clone(body))
	t.mu.Unlock()

	return response, nil
}

func (t *scanPlanningCaptureTransport) PlanRequestHeaders() []http.Header {
	t.mu.Lock()
	defer t.mu.Unlock()

	headers := make([]http.Header, len(t.planRequests))
	for i, header := range t.planRequests {
		headers[i] = header.Clone()
	}

	return headers
}

func (t *scanPlanningCaptureTransport) PlanResponses() [][]byte {
	t.mu.Lock()
	defer t.mu.Unlock()

	responses := make([][]byte, len(t.planResponses))
	for i, response := range t.planResponses {
		responses[i] = bytes.Clone(response)
	}

	return responses
}

func (t *scanPlanningCaptureTransport) CloseIdleConnections() {
	t.base.CloseIdleConnections()
}
