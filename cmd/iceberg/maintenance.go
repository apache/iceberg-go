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
	"bufio"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"

	"golang.org/x/term"
)

// Command structs

type SnapshotsCmd struct {
	TableID string `arg:"positional,required" help:"full path to a table"`
}

type RefsCmd struct {
	TableID string `arg:"positional,required" help:"full path to a table"`
	Type    string `arg:"--type" help:"filter by ref type (branch or tag)"`
}

type PartitionStatsCmd struct {
	TableID    string `arg:"positional,required" help:"full path to a table"`
	SnapshotID *int64 `arg:"--snapshot-id" help:"filter by snapshot ID"`
	All        bool   `arg:"--all" help:"show partition statistics for all snapshots"`
}

type ExpireSnapshotsCmd struct {
	TableID    string `arg:"positional,required" help:"full path to a table"`
	OlderThan  string `arg:"--older-than" help:"expire snapshots older than duration (e.g. 7d, 168h)"`
	RetainLast *int   `arg:"--retain-last" help:"minimum number of snapshots to retain"`
	DryRun     bool   `arg:"--dry-run" help:"show what would be expired without committing"`
	Yes        bool   `arg:"--yes" help:"skip confirmation prompt"`
}

type CleanOrphanFilesCmd struct {
	TableID   string `arg:"positional,required" help:"full path to a table"`
	OlderThan string `arg:"--older-than" default:"72h" help:"delete files older than duration (default 72h)"`
	DryRun    bool   `arg:"--dry-run" help:"list orphan files without deleting"`
	Yes       bool   `arg:"--yes" help:"skip confirmation prompt"`
	Location  string `arg:"--location" help:"override location to scan for orphan files"`
}

type UpgradeCmd struct {
	TableID       string `arg:"positional,required" help:"full path to a table"`
	FormatVersion int    `arg:"positional,required" help:"target format version (e.g. 2, 3)"`
	DryRun        bool   `arg:"--dry-run" help:"show what would change without committing"`
	Yes           bool   `arg:"--yes" help:"skip confirmation prompt"`
}

type RollbackCmd struct {
	TableID    string `arg:"positional,required" help:"full path to a table"`
	SnapshotID int64  `arg:"--snapshot-id,required" help:"snapshot ID to roll back to"`
	Yes        bool   `arg:"--yes" help:"skip confirmation prompt"`
}

type BranchCreateCmd struct {
	TableID            string `arg:"positional,required" help:"full path to a table"`
	BranchName         string `arg:"positional,required" help:"branch name"`
	SnapshotID         *int64 `arg:"--snapshot-id" help:"snapshot ID (defaults to current snapshot)"`
	MaxRefAge          string `arg:"--max-ref-age" help:"max ref age duration (e.g. 7d, 168h)"`
	MaxSnapshotAge     string `arg:"--max-snapshot-age" help:"max snapshot age duration"`
	MinSnapshotsToKeep *int   `arg:"--min-snapshots-to-keep" help:"minimum snapshots to keep"`
	Yes                bool   `arg:"--yes" help:"skip confirmation prompt"`
}

type BranchCmd struct {
	Create *BranchCreateCmd `arg:"subcommand:create" help:"create a branch"`
}

type TagCreateCmd struct {
	TableID    string `arg:"positional,required" help:"full path to a table"`
	TagName    string `arg:"positional,required" help:"tag name"`
	SnapshotID *int64 `arg:"--snapshot-id" help:"snapshot ID (defaults to current snapshot)"`
	MaxRefAge  string `arg:"--max-ref-age" help:"max ref age duration (e.g. 7d, 168h)"`
	Yes        bool   `arg:"--yes" help:"skip confirmation prompt"`
}

type TagCmd struct {
	Create *TagCreateCmd `arg:"subcommand:create" help:"create a tag"`
}

// Result types

type SnapshotEntry struct {
	SnapshotID       int64  `json:"snapshot_id"`
	Timestamp        string `json:"timestamp"`
	ParentSnapshotID *int64 `json:"parent_snapshot_id"`
	Operation        string `json:"operation"`
	AddedDataFiles   string `json:"added_data_files"`
	DeletedDataFiles string `json:"deleted_data_files"`
}

type RefEntry struct {
	Name               string `json:"name"`
	Type               string `json:"type"`
	SnapshotID         int64  `json:"snapshot_id"`
	MaxRefAgeMs        *int64 `json:"max_ref_age_ms"`
	MaxSnapshotAgeMs   *int64 `json:"max_snapshot_age_ms"`
	MinSnapshotsToKeep *int   `json:"min_snapshots_to_keep"`
}

type PartitionStatsEntry struct {
	SnapshotID int64  `json:"snapshot_id"`
	Path       string `json:"path"`
	SizeBytes  int64  `json:"size_bytes"`
}

type SchemaFieldWithDefaults struct {
	FieldID        int    `json:"field_id"`
	Name           string `json:"name"`
	Type           string `json:"type"`
	Required       bool   `json:"required"`
	InitialDefault any    `json:"initial_default"`
	WriteDefault   any    `json:"write_default"`
}

type ExpireSnapshotsResult struct {
	DryRun               bool            `json:"dry_run"`
	Table                string          `json:"table"`
	ExpiredSnapshotCount int             `json:"expired_snapshot_count"`
	ExpiredSnapshots     []SnapshotEntry `json:"expired_snapshots"`
}

type CleanOrphanFilesResult struct {
	DryRun          bool              `json:"dry_run"`
	Table           string            `json:"table"`
	OrphanFileCount int               `json:"orphan_file_count"`
	TotalSizeBytes  int64             `json:"total_size_bytes"`
	OrphanFiles     []OrphanFileEntry `json:"orphan_files"`
}

type OrphanFileEntry struct {
	Path      string `json:"path"`
	SizeBytes int64  `json:"size_bytes,omitempty"`
}

type UpgradeResult struct {
	DryRun          bool   `json:"dry_run"`
	Table           string `json:"table"`
	PreviousVersion int    `json:"previous_version"`
	TargetVersion   int    `json:"target_version"`
	SpecURL         string `json:"spec_url"`
}

type RollbackResult struct {
	Table                  string `json:"table"`
	PreviousSnapshotID     *int64 `json:"previous_snapshot_id"`
	RolledBackToSnapshotID int64  `json:"rolled_back_to_snapshot_id"`
}

type RefCreatedResult struct {
	Table              string `json:"table"`
	RefName            string `json:"ref_name"`
	RefType            string `json:"ref_type"`
	SnapshotID         int64  `json:"snapshot_id"`
	MaxRefAgeMs        *int64 `json:"max_ref_age_ms,omitempty"`
	MaxSnapshotAgeMs   *int64 `json:"max_snapshot_age_ms,omitempty"`
	MinSnapshotsToKeep *int   `json:"min_snapshots_to_keep,omitempty"`
}

// Shared helpers

func parseDuration(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, errors.New("empty duration string")
	}

	if strings.HasSuffix(s, "d") {
		daysStr := strings.TrimSuffix(s, "d")
		days, err := strconv.ParseFloat(daysStr, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid day duration %q: %w", s, err)
		}

		return time.Duration(days * float64(24*time.Hour)), nil
	}

	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("invalid duration %q: %w", s, err)
	}

	return d, nil
}

func confirmAction(prompt string, skipConfirm bool) error {
	if skipConfirm {
		return nil
	}

	if !term.IsTerminal(int(os.Stdin.Fd())) {
		return errors.New("stdin is not a terminal: use --yes to confirm in non-interactive mode")
	}

	fmt.Fprintf(os.Stderr, "%s [y/N] ", prompt)

	reader := bufio.NewReader(os.Stdin)
	answer, err := reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("failed to read confirmation: %w", err)
	}

	answer = strings.TrimSpace(strings.ToLower(answer))
	if answer != "y" && answer != "yes" {
		return errors.New("aborted")
	}

	return nil
}

func formatDurationMs(ms *int64) string {
	if ms == nil {
		return "-"
	}

	d := time.Duration(*ms) * time.Millisecond
	hours := int64(d.Hours())

	if hours > 0 && d == time.Duration(hours)*time.Hour {
		return fmt.Sprintf("%dh", hours)
	}

	return d.String()
}

// Ensure unused imports are satisfied for stub files.
var (
	_ = (*catalog.Catalog)(nil)
	_ = (*table.Table)(nil)
	_ = (*iceberg.Schema)(nil)
)
