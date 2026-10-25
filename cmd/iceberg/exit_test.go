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
	"errors"
	"testing"

	"github.com/apache/iceberg-go/catalog"
)

type closeCountingCatalog struct {
	catalog.Catalog
	closeCalls int
	closeErr   error
}

func (c *closeCountingCatalog) Close() error {
	c.closeCalls++

	return c.closeErr
}

func TestCloseCatalogBeforeExit(t *testing.T) {
	closeErr := errors.New("close failed")
	tests := []struct {
		name         string
		exit         bool
		exitCode     int
		closeErr     error
		wantCloseErr error
	}{
		{name: "normal return"},
		{name: "exit closes first", exit: true, exitCode: 7},
		{name: "close error does not block exit", exit: true, exitCode: 9, closeErr: closeErr, wantCloseErr: closeErr},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			previousExit := osExit
			t.Cleanup(func() { osExit = previousExit })

			cat := &closeCountingCatalog{closeErr: tt.closeErr}
			exitCalled := false
			codeAtExit := 0
			closesAtExit := 0
			var reportedCloseErr error
			osExit = func(code int) {
				exitCalled = true
				codeAtExit = code
				closesAtExit = cat.closeCalls
			}

			restore := closeCatalogBeforeExit(cat, func(err error) { reportedCloseErr = err })
			if tt.exit {
				osExit(tt.exitCode)
			}
			restore()

			if cat.closeCalls != 1 {
				t.Fatalf("Close called %d times, want 1", cat.closeCalls)
			}
			if exitCalled != tt.exit {
				t.Fatalf("exit called = %t, want %t", exitCalled, tt.exit)
			}
			if tt.exit && closesAtExit != 1 {
				t.Fatalf("Close calls at exit = %d, want 1", closesAtExit)
			}
			if tt.exit && codeAtExit != tt.exitCode {
				t.Fatalf("exit code = %d, want %d", codeAtExit, tt.exitCode)
			}
			if !errors.Is(reportedCloseErr, tt.wantCloseErr) {
				t.Fatalf("reported close error = %v, want %v", reportedCloseErr, tt.wantCloseErr)
			}
		})
	}
}

func TestOutputErrorClosesCatalogBeforeExit(t *testing.T) {
	for _, tt := range []struct {
		name   string
		output Output
	}{
		{name: "text", output: textOutput{}},
		{name: "json", output: jsonOutput{}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			previousExit := osExit
			t.Cleanup(func() { osExit = previousExit })

			cat := &closeCountingCatalog{}
			exitCode := 0
			closesAtExit := 0
			osExit = func(code int) {
				exitCode = code
				closesAtExit = cat.closeCalls
			}

			restore := closeCatalogBeforeExit(cat, nil)
			tt.output.Error(errors.New("command failed"))
			restore()

			if exitCode != 1 {
				t.Fatalf("exit code = %d, want 1", exitCode)
			}
			if closesAtExit != 1 {
				t.Fatalf("Close calls before exit = %d, want 1", closesAtExit)
			}
			if cat.closeCalls != 1 {
				t.Fatalf("Close calls = %d, want 1", cat.closeCalls)
			}
		})
	}
}
