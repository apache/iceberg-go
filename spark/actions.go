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

package spark

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/apache/iceberg-go/table"
)

// SparkActions provides high-level table action operations through Spark
type SparkActions struct {
	util  *SparkUtil
	batch *SparkBatch
}

// NewSparkActions creates a new SparkActions instance
func NewSparkActions(util *SparkUtil) *SparkActions {
	return &SparkActions{
		util:  util,
		batch: NewSparkBatch(util),
	}
}

// ActionResult contains the result of an action execution
type ActionResult struct {
	ActionType     string
	Success        bool
	Message        string
	RecordsRead    int64
	RecordsWritten int64
	FilesChanged   int64
	ExecutionTime  int64 // in milliseconds
	Details        map[string]interface{}
}

// RewriteDataFilesAction rewrites data files to optimize table layout
type RewriteDataFilesAction struct {
	TableIdentifier table.Identifier
	Strategy        CompactionStrategy
	SortColumns     []string
	Filter          string
	TargetSizeBytes int64
	MaxConcurrency  int
}

// ExpireSnapshotsAction removes old snapshots from a table
type ExpireSnapshotsAction struct {
	TableIdentifier      table.Identifier
	OlderThanTimestampMs int64
	RetainLastSnapshots  int
	MaxSnapshotAgeMs     int64
	SnapshotIds          []int64
}

// DeleteOrphanFilesAction removes orphaned data files
type DeleteOrphanFilesAction struct {
	TableIdentifier table.Identifier
	OlderThanMs     int64
	Location        string
	DryRun          bool
}

// RewriteManifestsAction rewrites manifest files
type RewriteManifestsAction struct {
	TableIdentifier table.Identifier
	UseSparkServer  bool
}

// ComputeTableStatsAction computes table statistics
type ComputeTableStatsAction struct {
	TableIdentifier table.Identifier
	Columns         []string
	SnapshotId      *int64
}

// ExecuteRewriteDataFiles executes a rewrite data files action
func (a *SparkActions) ExecuteRewriteDataFiles(ctx context.Context, action *RewriteDataFilesAction) (*ActionResult, error) {
	tableName := FormatTableIdentifier(action.TableIdentifier)

	// Build the rewrite data files procedure call
	var args []string
	args = append(args, fmt.Sprintf("table => '%s'", tableName))

	if action.Strategy == CompactionStrategySort && len(action.SortColumns) > 0 {
		args = append(args, "strategy => 'sort'")
		args = append(args, fmt.Sprintf("sort_order => '%s'", strings.Join(action.SortColumns, ",")))
	} else {
		args = append(args, "strategy => 'binpack'")
	}

	if action.Filter != "" {
		args = append(args, fmt.Sprintf("where => '%s'", action.Filter))
	}

	if action.TargetSizeBytes > 0 {
		args = append(args, fmt.Sprintf("target_file_size_bytes => %d", action.TargetSizeBytes))
	}

	if action.MaxConcurrency > 0 {
		args = append(args, fmt.Sprintf("max_concurrent_file_group_rewrites => %d", action.MaxConcurrency))
	}

	sql := fmt.Sprintf("CALL iceberg.system.rewrite_data_files(%s)", strings.Join(args, ", "))

	df, err := a.util.ExecuteSQL(ctx, sql)
	if err != nil {
		return &ActionResult{
			ActionType: "rewrite_data_files",
			Success:    false,
			Message:    err.Error(),
		}, err
	}

	// Collect results to get metrics
	rows, err := df.Collect(ctx)
	if err != nil {
		return &ActionResult{
			ActionType: "rewrite_data_files",
			Success:    false,
			Message:    err.Error(),
		}, err
	}

	result := &ActionResult{
		ActionType: "rewrite_data_files",
		Success:    true,
		Message:    "Data files rewritten successfully",
		Details:    make(map[string]interface{}),
	}

	// Parse result metrics
	if len(rows) > 0 && len(rows[0]) > 0 {
		// Assuming the procedure returns metrics in the first row
		result.Details["rewrite_result"] = rows[0]
	}

	return result, nil
}

// ExecuteExpireSnapshots executes an expire snapshots action
func (a *SparkActions) ExecuteExpireSnapshots(ctx context.Context, action *ExpireSnapshotsAction) (*ActionResult, error) {
	tableName := FormatTableIdentifier(action.TableIdentifier)

	var args []string
	args = append(args, fmt.Sprintf("table => '%s'", tableName))

	if action.OlderThanTimestampMs > 0 {
		args = append(args, fmt.Sprintf("older_than => TIMESTAMP '%d'", action.OlderThanTimestampMs))
	}

	if action.RetainLastSnapshots > 0 {
		args = append(args, fmt.Sprintf("retain_last => %d", action.RetainLastSnapshots))
	}

	if action.MaxSnapshotAgeMs > 0 {
		args = append(args, fmt.Sprintf("max_snapshot_age_ms => %d", action.MaxSnapshotAgeMs))
	}

	if len(action.SnapshotIds) > 0 {
		ids := make([]string, len(action.SnapshotIds))
		for i, id := range action.SnapshotIds {
			ids[i] = strconv.FormatInt(id, 10)
		}
		args = append(args, fmt.Sprintf("snapshot_ids => ARRAY[%s]", strings.Join(ids, ",")))
	}

	sql := fmt.Sprintf("CALL iceberg.system.expire_snapshots(%s)", strings.Join(args, ", "))

	df, err := a.util.ExecuteSQL(ctx, sql)
	if err != nil {
		return &ActionResult{
			ActionType: "expire_snapshots",
			Success:    false,
			Message:    err.Error(),
		}, err
	}

	rows, err := df.Collect(ctx)
	if err != nil {
		return &ActionResult{
			ActionType: "expire_snapshots",
			Success:    false,
			Message:    err.Error(),
		}, err
	}

	result := &ActionResult{
		ActionType: "expire_snapshots",
		Success:    true,
		Message:    "Snapshots expired successfully",
		Details:    make(map[string]interface{}),
	}

	if len(rows) > 0 {
		result.Details["expire_result"] = rows[0]
	}

	return result, nil
}

// ExecuteDeleteOrphanFiles executes a delete orphan files action
func (a *SparkActions) ExecuteDeleteOrphanFiles(ctx context.Context, action *DeleteOrphanFilesAction) (*ActionResult, error) {
	tableName := FormatTableIdentifier(action.TableIdentifier)

	var args []string
	args = append(args, fmt.Sprintf("table => '%s'", tableName))

	if action.OlderThanMs > 0 {
		args = append(args, fmt.Sprintf("older_than => TIMESTAMP '%d'", action.OlderThanMs))
	}

	if action.Location != "" {
		args = append(args, fmt.Sprintf("location => '%s'", action.Location))
	}

	if action.DryRun {
		args = append(args, "dry_run => true")
	}

	sql := fmt.Sprintf("CALL iceberg.system.remove_orphan_files(%s)", strings.Join(args, ", "))

	df, err := a.util.ExecuteSQL(ctx, sql)
	if err != nil {
		return &ActionResult{
			ActionType: "delete_orphan_files",
			Success:    false,
			Message:    err.Error(),
		}, err
	}

	rows, err := df.Collect(ctx)
	if err != nil {
		return &ActionResult{
			ActionType: "delete_orphan_files",
			Success:    false,
			Message:    err.Error(),
		}, err
	}

	result := &ActionResult{
		ActionType: "delete_orphan_files",
		Success:    true,
		Message:    "Orphan files processed successfully",
		Details:    make(map[string]interface{}),
	}

	if len(rows) > 0 {
		result.Details["orphan_files_result"] = rows[0]
	}

	return result, nil
}

// ExecuteRewriteManifests executes a rewrite manifests action
func (a *SparkActions) ExecuteRewriteManifests(ctx context.Context, action *RewriteManifestsAction) (*ActionResult, error) {
	tableName := FormatTableIdentifier(action.TableIdentifier)

	var args []string
	args = append(args, fmt.Sprintf("table => '%s'", tableName))

	if action.UseSparkServer {
		args = append(args, "use_caching => true")
	}

	sql := fmt.Sprintf("CALL iceberg.system.rewrite_manifests(%s)", strings.Join(args, ", "))

	df, err := a.util.ExecuteSQL(ctx, sql)
	if err != nil {
		return &ActionResult{
			ActionType: "rewrite_manifests",
			Success:    false,
			Message:    err.Error(),
		}, err
	}

	rows, err := df.Collect(ctx)
	if err != nil {
		return &ActionResult{
			ActionType: "rewrite_manifests",
			Success:    false,
			Message:    err.Error(),
		}, err
	}

	result := &ActionResult{
		ActionType: "rewrite_manifests",
		Success:    true,
		Message:    "Manifests rewritten successfully",
		Details:    make(map[string]interface{}),
	}

	if len(rows) > 0 {
		result.Details["rewrite_manifests_result"] = rows[0]
	}

	return result, nil
}

// ExecuteComputeTableStats executes a compute table statistics action
func (a *SparkActions) ExecuteComputeTableStats(ctx context.Context, action *ComputeTableStatsAction) (*ActionResult, error) {
	tableName := FormatTableIdentifier(action.TableIdentifier)

	var sql string
	if len(action.Columns) > 0 {
		columnList := strings.Join(action.Columns, ", ")
		sql = fmt.Sprintf("ANALYZE TABLE %s COMPUTE STATISTICS FOR COLUMNS %s", tableName, columnList)
	} else {
		sql = fmt.Sprintf("ANALYZE TABLE %s COMPUTE STATISTICS", tableName)
	}

	// Add snapshot specification if provided
	if action.SnapshotId != nil {
		sql = fmt.Sprintf("ANALYZE TABLE %s VERSION AS OF %d COMPUTE STATISTICS", tableName, *action.SnapshotId)
		if len(action.Columns) > 0 {
			columnList := strings.Join(action.Columns, ", ")
			sql += " FOR COLUMNS " + columnList
		}
	}

	_, err := a.util.ExecuteSQL(ctx, sql)
	if err != nil {
		return &ActionResult{
			ActionType: "compute_table_stats",
			Success:    false,
			Message:    err.Error(),
		}, err
	}

	return &ActionResult{
		ActionType: "compute_table_stats",
		Success:    true,
		Message:    "Table statistics computed successfully",
		Details:    make(map[string]interface{}),
	}, nil
}

// ExecuteSchemaEvolution executes schema evolution operations
func (a *SparkActions) ExecuteSchemaEvolution(ctx context.Context, identifier table.Identifier, evolution *SchemaEvolution) (*ActionResult, error) {
	tableName := FormatTableIdentifier(identifier)

	var sqls []string

	// Add columns
	for _, addCol := range evolution.AddColumns {
		sql := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", tableName, addCol.Name, addCol.Type)
		if addCol.Comment != "" {
			sql += fmt.Sprintf(" COMMENT '%s'", addCol.Comment)
		}
		sqls = append(sqls, sql)
	}

	// Drop columns
	for _, dropCol := range evolution.DropColumns {
		sql := fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", tableName, dropCol)
		sqls = append(sqls, sql)
	}

	// Rename columns
	for oldName, newName := range evolution.RenameColumns {
		sql := fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s", tableName, oldName, newName)
		sqls = append(sqls, sql)
	}

	// Update column types
	for _, alterCol := range evolution.AlterColumns {
		sql := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE %s", tableName, alterCol.Name, alterCol.NewType)
		sqls = append(sqls, sql)
	}

	// Execute all schema changes
	for _, sql := range sqls {
		_, err := a.util.ExecuteSQL(ctx, sql)
		if err != nil {
			return &ActionResult{
				ActionType: "schema_evolution",
				Success:    false,
				Message:    fmt.Sprintf("Failed to execute schema change: %s, Error: %v", sql, err),
			}, err
		}
	}

	return &ActionResult{
		ActionType: "schema_evolution",
		Success:    true,
		Message:    fmt.Sprintf("Schema evolution completed with %d changes", len(sqls)),
		Details: map[string]interface{}{
			"changes_applied": len(sqls),
		},
	}, nil
}

// SchemaEvolution defines schema evolution operations
type SchemaEvolution struct {
	AddColumns    []AddColumn
	DropColumns   []string
	RenameColumns map[string]string
	AlterColumns  []AlterColumn
}

// AddColumn defines a column to add
type AddColumn struct {
	Name    string
	Type    string
	Comment string
}

// AlterColumn defines a column type change
type AlterColumn struct {
	Name    string
	NewType string
}

// ExecutePartitionEvolution executes partition evolution operations
func (a *SparkActions) ExecutePartitionEvolution(ctx context.Context, identifier table.Identifier, evolution *PartitionEvolution) (*ActionResult, error) {
	tableName := FormatTableIdentifier(identifier)

	var sqls []string

	// Add partition fields
	for _, addPartition := range evolution.AddPartitions {
		sql := fmt.Sprintf("ALTER TABLE %s ADD PARTITION FIELD %s", tableName, addPartition.Transform)
		sqls = append(sqls, sql)
	}

	// Drop partition fields
	for _, dropPartition := range evolution.DropPartitions {
		sql := fmt.Sprintf("ALTER TABLE %s DROP PARTITION FIELD %s", tableName, dropPartition)
		sqls = append(sqls, sql)
	}

	// Execute all partition changes
	for _, sql := range sqls {
		_, err := a.util.ExecuteSQL(ctx, sql)
		if err != nil {
			return &ActionResult{
				ActionType: "partition_evolution",
				Success:    false,
				Message:    fmt.Sprintf("Failed to execute partition change: %s, Error: %v", sql, err),
			}, err
		}
	}

	return &ActionResult{
		ActionType: "partition_evolution",
		Success:    true,
		Message:    fmt.Sprintf("Partition evolution completed with %d changes", len(sqls)),
		Details: map[string]interface{}{
			"changes_applied": len(sqls),
		},
	}, nil
}

// PartitionEvolution defines partition evolution operations
type PartitionEvolution struct {
	AddPartitions  []AddPartition
	DropPartitions []string
}

// AddPartition defines a partition field to add
type AddPartition struct {
	Transform string
	Name      string
}
