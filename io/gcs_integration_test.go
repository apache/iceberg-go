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

package io_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	sqlcat "github.com/apache/iceberg-go/catalog/sql"
	"github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/suite"
	"github.com/uptrace/bun/driver/sqliteshim"
)

const (
	gcsEndpoint   = "localhost:4443/storage/v1"
	gcsProtocol   = "http"
	gcsBucketName = "warehouse"
)

type GCSIOTestSuite struct {
	suite.Suite

	ctx context.Context
}

func (s *GCSIOTestSuite) cleanBucket() {
	// Clean the bucket: list and delete all objects
	listURL := fmt.Sprintf("http://%s/b/%s/o", gcsEndpoint, gcsBucketName)
	resp, err := http.Get(listURL)
	if err != nil {
		s.Require().NoError(err)
	}
	defer resp.Body.Close()
	var list struct {
		Items []struct {
			Name string `json:"name"`
		} `json:"items"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		// If the bucket is empty, items may be nil, that's fine
		list.Items = nil
	}
	for _, item := range list.Items {
		objURL := fmt.Sprintf("http://%s/b/%s/o/%s", gcsEndpoint, gcsBucketName, item.Name)
		// URL-encode the object name
		objURL = objURL[:len(objURL)-len(item.Name)] + url.PathEscape(item.Name)
		req, err := http.NewRequest(http.MethodDelete, objURL, nil)
		if err != nil {
			s.Require().NoError(err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			s.Require().NoError(err)
		}
		resp.Body.Close()
	}
}

func (s *GCSIOTestSuite) SetupTest() {
	s.ctx = context.Background()

	// Create the bucket in fake-gcs-server (correct API)
	url := fmt.Sprintf("http://%s/b?project=fake-project-id", gcsEndpoint)
	body, _ := json.Marshal(map[string]string{"name": gcsBucketName})
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		s.Require().NoError(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		s.Require().NoError(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusConflict {
		s.Require().NoError(fmt.Errorf("failed to create bucket: %s", resp.Status))
	}

	// Clean the bucket before running the test
	s.cleanBucket()
}

func (s *GCSIOTestSuite) TestGCSWarehouse() {
	path := "iceberg-test-gcs/test-table-gcs"
	properties := iceberg.Properties{
		"uri":             ":memory:",
		sqlcat.DriverKey:  sqliteshim.ShimName,
		sqlcat.DialectKey: string(sqlcat.SQLite),
		"type":            "sql",
		"warehouse":       fmt.Sprintf("gs://%s/iceberg/", gcsBucketName),
		io.GCSEndpoint:    fmt.Sprintf("http://%s/", gcsEndpoint),
		io.GCSUseJsonAPI:  "true",
	}

	cat, err := catalog.Load(context.Background(), "default", properties)
	s.Require().NoError(err)
	s.Require().NotNil(cat)

	c := cat.(*sqlcat.Catalog)
	s.Require().NoError(c.CreateNamespace(s.ctx, catalog.ToIdentifier("iceberg-test-gcs"), nil))

	tbl, err := c.CreateTable(s.ctx,
		catalog.ToIdentifier("iceberg-test-gcs", "test-table-gcs"),
		iceberg.NewSchema(0, iceberg.NestedField{
			Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true, ID: 1,
		}), catalog.WithLocation(fmt.Sprintf("gs://%s/iceberg/%s", gcsBucketName, path)))
	s.Require().NoError(err)
	s.Require().NotNil(tbl)

	tbl, err = c.LoadTable(s.ctx, catalog.ToIdentifier("iceberg-test-gcs", "test-table-gcs"))
	s.Require().NoError(err)
	s.Require().NotNil(tbl)
}

func TestGCSIOIntegration(t *testing.T) {
	suite.Run(t, new(GCSIOTestSuite))
}
