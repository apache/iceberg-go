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
	"context"
	"errors"
	"fmt"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
)

// runFiles lists a table's files. By default it walks the manifests itself,
// which shows the manifest tree. The files come from a scan plan instead when
// --scan-planning-mode is remote or auto, or when the catalog requires
// server-side planning (scan-planning-mode=server), since that directive
// forbids reading manifests with the table's own credentials.
func runFiles(ctx context.Context, output Output, cat catalog.Catalog, cmd *FilesCmd) {
	mode := table.ScanPlanningMode(cmd.ScanPlanningMode)
	if err := validateScanPlanningMode(mode); err != nil {
		output.Error(err)
		osExit(1)

		return
	}

	tbl := loadTable(ctx, output, cat, cmd.TableID)

	planned, err := filesFromScanPlan(tbl, mode, cmd.History)
	if err != nil {
		output.Error(err)
		osExit(1)

		return
	}

	if !planned {
		output.Files(tbl, cmd.History)

		return
	}

	tasks, err := planCurrentFiles(ctx, tbl, mode)
	if err != nil {
		output.Error(err)
		osExit(1)

		return
	}

	output.PlannedFiles(tbl, tasks)
}

func validateScanPlanningMode(mode table.ScanPlanningMode) error {
	switch mode {
	case "", table.ScanPlanningLocal, table.ScanPlanningRemote, table.ScanPlanningAuto:
		return nil
	default:
		return fmt.Errorf("invalid --scan-planning-mode %q: expected %q, %q or %q",
			mode, table.ScanPlanningLocal, table.ScanPlanningRemote, table.ScanPlanningAuto)
	}
}

// filesFromScanPlan reports whether files must list a scan plan rather than
// walk the manifests. A remote or auto mode that conflicts with a `client`
// directive is left for PlanFiles to reject.
func filesFromScanPlan(tbl *table.Table, mode table.ScanPlanningMode, history bool) (bool, error) {
	directive, err := tbl.ScanPlanningDirective()
	if err != nil {
		return false, err
	}

	var planned bool
	switch mode {
	case "":
		planned = directive == table.ScanPlanningDirectiveServer
	case table.ScanPlanningLocal:
		if directive == table.ScanPlanningDirectiveServer {
			return false, fmt.Errorf("the catalog requires server-side scan planning (%s=%s), so files cannot read manifests locally",
				table.ScanPlanningModeKey, table.ScanPlanningDirectiveServer)
		}
	default:
		planned = true
	}

	if planned && history {
		if mode == "" {
			return false, fmt.Errorf("--history reads every snapshot's manifests, which the catalog forbids (%s=%s)",
				table.ScanPlanningModeKey, table.ScanPlanningDirectiveServer)
		}

		return false, errors.New("--history reads every snapshot's manifests and cannot be combined with --scan-planning-mode " + string(mode))
	}

	return planned, nil
}

// planCurrentFiles plans a full scan of the current snapshot. A table without
// a snapshot has no files, so it is not planned.
func planCurrentFiles(ctx context.Context, tbl *table.Table, mode table.ScanPlanningMode) ([]table.FileScanTask, error) {
	if tbl.CurrentSnapshot() == nil {
		return nil, nil
	}

	var opts []table.ScanOption
	if mode != "" {
		opts = append(opts, table.WithScanPlanningMode(mode))
	}

	scan := tbl.Scan(opts...)
	defer scan.Close()

	return scan.PlanFiles(ctx)
}

type plannedFile struct {
	Path        string   `json:"file-path"`
	DeleteFiles []string `json:"delete-files,omitempty"`
}

// plannedDataFiles collapses scan tasks to one entry per data file. A planner
// may split a large file into several tasks, which share its delete files.
func plannedDataFiles(tasks []table.FileScanTask) []plannedFile {
	files := make([]plannedFile, 0, len(tasks))
	seen := make(map[string]bool, len(tasks))
	for _, task := range tasks {
		path := task.File.FilePath()
		if seen[path] {
			continue
		}
		seen[path] = true

		file := plannedFile{Path: path}
		for _, deletes := range [][]iceberg.DataFile{task.DeleteFiles, task.EqualityDeleteFiles, task.DeletionVectorFiles} {
			for _, df := range deletes {
				file.DeleteFiles = append(file.DeleteFiles, df.FilePath())
			}
		}
		files = append(files, file)
	}

	return files
}
