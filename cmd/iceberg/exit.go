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

package main

import (
	"log"
	"os"
	"sync"

	"github.com/apache/iceberg-go/catalog"
)

// osExit is an indirection over os.Exit so command handlers can be exercised in
// tests by swapping it for a func that records the exit code and unwinds.
var osExit = os.Exit

// closeCatalogBeforeExit routes command exits through catalog cleanup and also
// closes the catalog when the command returns normally. The returned function
// restores the previous exit hook.
func closeCatalogBeforeExit(cat catalog.Catalog, reportCloseError func(error)) func() {
	previousExit := osExit
	var closeOnce sync.Once
	closeCatalog := func() {
		closeOnce.Do(func() {
			if err := cat.Close(); err != nil && reportCloseError != nil {
				reportCloseError(err)
			}
		})
	}

	osExit = func(code int) {
		closeCatalog()
		previousExit(code)
	}

	return func() {
		osExit = previousExit
		closeCatalog()
	}
}

func logFatal(v ...any) {
	log.Print(v...)
	osExit(1)
}

func logFatalf(format string, v ...any) {
	log.Printf(format, v...)
	osExit(1)
}
