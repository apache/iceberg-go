// Copyright 2019 The Go Cloud Development Kit Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/go-cmdtest"
)

var update = flag.Bool("update", false, "replace test file contents with output")

func Test(t *testing.T) {
	ts, err := cmdtest.Read(".")
	if err != nil {
		t.Fatal(err)
	}
	ts.Commands["gocdk-blob"] = cmdtest.InProcessProgram("gocdk-blob", run)
	ts.Setup = func(rootdir string) error {
		// On Windows, convert "\" to "/" and add a leading "/":
		slashdir := filepath.ToSlash(rootdir)
		if os.PathSeparator != '/' && !strings.HasPrefix(slashdir, "/") {
			slashdir = "/" + slashdir
		}
		return os.Setenv("ROOTDIR_URL", "file://"+slashdir)
	}
	ts.Run(t, *update)
}
