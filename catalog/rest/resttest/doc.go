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

// Package resttest provides an in-memory Iceberg REST catalog server for
// tests. NewHandler returns an http.Handler that keeps namespaces and table
// pointers in memory and writes table metadata files through the FileIO for
// the configured warehouse, so the catalog/rest client can be driven end to
// end, create namespace through commit and load, without a container:
//
//	srv := resttest.NewServer(t)
//	cat, err := rest.NewCatalog(ctx, "test", srv.URL)
//
// The server implements the config, namespace, table, rename and register
// endpoints of the REST catalog spec. Commits validate their requirements
// against the current metadata and swap the metadata pointer under a lock, so
// a stale requirement fails with 409 and the client reports
// table.ErrCommitFailed.
//
// Views, multi-table transactions, scan planning, credential vending, metrics
// and authentication are not implemented. They are not advertised in the
// config response, and requests for them return 501. Listing returns a single
// page, and dropping a table with purge does not delete its files.
package resttest
