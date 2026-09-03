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

package rest

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/apache/iceberg-go/table"
)

func BenchmarkCollectScanTasks64Handles(b *testing.B) {
	const (
		handleCount = 64
		latency     = 10 * time.Millisecond
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		time.Sleep(latency)
		_, _ = w.Write([]byte(`{"file-scan-tasks":[]}`))
	}))
	b.Cleanup(server.Close)

	serverURL, err := url.Parse(server.URL)
	if err != nil {
		b.Fatal(err)
	}

	catalog := &Catalog{
		baseURI:   serverURL.JoinPath("v1"),
		cl:        server.Client(),
		endpoints: newEndpointSet([]endpoint{endpointFetchScanTasks}),
	}
	handles := make([]string, handleCount)
	for i := range handles {
		handles[i] = fmt.Sprintf("task-%d", i)
	}
	tasks := ScanTasks{PlanTasks: handles}
	ident := table.Identifier{"db", "tbl"}

	for _, maxConcurrency := range []int{1, 2, 4, 8, 16, 32, 64} {
		b.Run(fmt.Sprintf("concurrency-%d", maxConcurrency), func(b *testing.B) {
			b.ResetTimer()
			for range b.N {
				_, err := catalog.collectScanTasksWithConcurrency(
					context.Background(), ident, tasks, maxConcurrency)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
