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

package deletes

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
)

// ClusteredPositionDeleteWriter clusters position deletes for optimal I/O and storage efficiency.
// It groups deletes by various clustering strategies and manages multiple underlying writers
// to minimize write amplification and optimize read performance.
type ClusteredPositionDeleteWriter struct {
	config        ClusteredWriterConfig
	clusters      map[string]*deleteCluster
	clustersMutex sync.RWMutex
	totalRecords  int64
	totalBytes    int64
	closed        bool
}

// ClusteredWriterConfig contains configuration for clustered position delete writers.
type ClusteredWriterConfig struct {
	// Base configuration for underlying writers
	BaseConfig PositionDeleteWriterConfig

	// ClusteringStrategy defines how deletes are clustered
	ClusteringStrategy ClusteringStrategy

	// MaxDeletesPerCluster limits the number of deletes per cluster
	MaxDeletesPerCluster int64

	// MaxClustersInMemory limits the number of active clusters in memory
	MaxClustersInMemory int

	// ClusterFlushThreshold triggers a flush when a cluster reaches this size
	ClusterFlushThreshold int64

	// OutputPathPattern for generating cluster output paths
	OutputPathPattern string

	// ClusterNamePrefix for cluster identification
	ClusterNamePrefix string

	// CompressionType for cluster files
	CompressionType string

	// SortStrategy for deletes within each cluster
	SortStrategy SortStrategy

	// AutoFlushInterval for periodic flushing of clusters
	AutoFlushInterval time.Duration

	// EnableMetrics for tracking cluster performance
	EnableMetrics bool
}

// ClusteringStrategy defines how position deletes are clustered.
type ClusteringStrategy int

const (
	// ClusterByFilePrefix clusters deletes by file path prefix
	ClusterByFilePrefix ClusteringStrategy = iota
	// ClusterByPartition clusters deletes by partition
	ClusterByPartition
	// ClusterBySize clusters deletes to maintain target cluster sizes
	ClusterBySize
	// ClusterByTimeWindow clusters deletes by time windows
	ClusterByTimeWindow
	// ClusterByHash clusters deletes using hash-based distribution
	ClusterByHash
)

// String returns the string representation of the ClusteringStrategy.
func (s ClusteringStrategy) String() string {
	switch s {
	case ClusterByFilePrefix:
		return "CLUSTER_BY_FILE_PREFIX"
	case ClusterByPartition:
		return "CLUSTER_BY_PARTITION"
	case ClusterBySize:
		return "CLUSTER_BY_SIZE"
	case ClusterByTimeWindow:
		return "CLUSTER_BY_TIME_WINDOW"
	case ClusterByHash:
		return "CLUSTER_BY_HASH"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", int(s))
	}
}

// deleteCluster represents a cluster of position deletes.
type deleteCluster struct {
	id               string
	deletes          []*PositionDelete
	writer           PositionDeleteWriter
	lastFlushTime    time.Time
	bytesWritten     int64
	recordsWritten   int64
	config           ClusteredWriterConfig
	mutex            sync.RWMutex
}

// ClusterMetrics provides performance metrics for clustering operations.
type ClusterMetrics struct {
	ActiveClusters     int
	TotalClusters      int
	TotalDeletes       int64
	TotalBytes         int64
	AverageClusterSize float64
	FlushCount         int64
	LastFlushTime      time.Time
}

// NewClusteredPositionDeleteWriter creates a new clustered position delete writer.
func NewClusteredPositionDeleteWriter(config ClusteredWriterConfig) (PositionDeleteWriter, error) {
	// Set default values
	if config.MaxDeletesPerCluster <= 0 {
		config.MaxDeletesPerCluster = 50000 // Default max deletes per cluster
	}

	if config.MaxClustersInMemory <= 0 {
		config.MaxClustersInMemory = 100 // Default max clusters in memory
	}

	if config.ClusterFlushThreshold <= 0 {
		config.ClusterFlushThreshold = config.MaxDeletesPerCluster / 2
	}

	if config.OutputPathPattern == "" {
		config.OutputPathPattern = "%s/cluster_%s.parquet"
	}

	if config.ClusterNamePrefix == "" {
		config.ClusterNamePrefix = "pos_delete_cluster"
	}

	if config.AutoFlushInterval <= 0 {
		config.AutoFlushInterval = 5 * time.Minute
	}

	writer := &ClusteredPositionDeleteWriter{
		config:   config,
		clusters: make(map[string]*deleteCluster),
		closed:   false,
	}

	// Start auto-flush routine if enabled
	if config.AutoFlushInterval > 0 {
		go writer.autoFlushRoutine()
	}

	return writer, nil
}

// Write writes a position delete record to the appropriate cluster.
func (w *ClusteredPositionDeleteWriter) Write(ctx context.Context, filePath string, position int64) error {
	if w.closed {
		return fmt.Errorf("writer is closed")
	}

	delete := NewPositionDelete(filePath, position)
	return w.WritePositionDelete(ctx, delete)
}

// WritePositionDelete writes a PositionDelete to the appropriate cluster.
func (w *ClusteredPositionDeleteWriter) WritePositionDelete(ctx context.Context, delete *PositionDelete) error {
	if w.closed {
		return fmt.Errorf("writer is closed")
	}

	if delete == nil {
		return nil
	}

	// Determine the cluster for this delete
	clusterID := w.determineCluster(delete.FilePath)

	// Get or create the cluster
	cluster, err := w.getOrCreateCluster(clusterID)
	if err != nil {
		return fmt.Errorf("failed to get cluster %s: %w", clusterID, err)
	}

	// Add delete to the cluster
	err = w.addToCluster(ctx, cluster, delete)
	if err != nil {
		return fmt.Errorf("failed to add delete to cluster %s: %w", clusterID, err)
	}

	w.totalRecords++
	return nil
}

// WriteBatch writes multiple position deletes to their appropriate clusters.
func (w *ClusteredPositionDeleteWriter) WriteBatch(ctx context.Context, deletes []*PositionDelete) error {
	if w.closed {
		return fmt.Errorf("writer is closed")
	}

	// Group deletes by cluster
	clusterDeletes := make(map[string][]*PositionDelete)
	for _, delete := range deletes {
		if delete != nil {
			clusterID := w.determineCluster(delete.FilePath)
			clusterDeletes[clusterID] = append(clusterDeletes[clusterID], delete)
		}
	}

	// Write to each cluster
	for clusterID, clusteredDeletes := range clusterDeletes {
		cluster, err := w.getOrCreateCluster(clusterID)
		if err != nil {
			return fmt.Errorf("failed to get cluster %s: %w", clusterID, err)
		}

		for _, delete := range clusteredDeletes {
			err = w.addToCluster(ctx, cluster, delete)
			if err != nil {
				return fmt.Errorf("failed to add delete to cluster %s: %w", clusterID, err)
			}
		}

		w.totalRecords += int64(len(clusteredDeletes))
	}

	return nil
}

// WriteIndex writes all position deletes from an index to their appropriate clusters.
func (w *ClusteredPositionDeleteWriter) WriteIndex(ctx context.Context, index PositionDeleteIndex) error {
	if w.closed {
		return fmt.Errorf("writer is closed")
	}

	// Group deletes by cluster
	clusterDeletes := make(map[string][]*PositionDelete)
	for filePath := range index.Files() {
		clusterID := w.determineCluster(filePath)
		for position := range index.Deletes(filePath) {
			delete := NewPositionDelete(filePath, position)
			clusterDeletes[clusterID] = append(clusterDeletes[clusterID], delete)
		}
	}

	// Write to each cluster
	for clusterID, deletes := range clusterDeletes {
		cluster, err := w.getOrCreateCluster(clusterID)
		if err != nil {
			return fmt.Errorf("failed to get cluster %s: %w", clusterID, err)
		}

		for _, delete := range deletes {
			err = w.addToCluster(ctx, cluster, delete)
			if err != nil {
				return fmt.Errorf("failed to add delete to cluster %s: %w", clusterID, err)
			}
		}

		w.totalRecords += int64(len(deletes))
	}

	return nil
}

// determineCluster determines the cluster ID for a given file path.
func (w *ClusteredPositionDeleteWriter) determineCluster(filePath string) string {
	switch w.config.ClusteringStrategy {
	case ClusterByFilePrefix:
		return w.clusterByFilePrefix(filePath)
	case ClusterByPartition:
		return w.clusterByPartition(filePath)
	case ClusterBySize:
		return w.clusterBySize()
	case ClusterByTimeWindow:
		return w.clusterByTimeWindow()
	case ClusterByHash:
		return w.clusterByHash(filePath)
	default:
		return "default"
	}
}

// clusterByFilePrefix clusters deletes by file path prefix.
func (w *ClusteredPositionDeleteWriter) clusterByFilePrefix(filePath string) string {
	// Extract directory path as cluster identifier
	dir := filepath.Dir(filePath)
	// Sanitize the directory path for use as cluster ID
	clusterID := strings.ReplaceAll(dir, "/", "_")
	clusterID = strings.ReplaceAll(clusterID, "\\", "_")
	if len(clusterID) > 100 {
		clusterID = clusterID[:100]
	}
	return clusterID
}

// clusterByPartition clusters deletes by partition information.
func (w *ClusteredPositionDeleteWriter) clusterByPartition(filePath string) string {
	// Extract partition from file path (simplified implementation)
	parts := strings.Split(filePath, "/")
	var partitionParts []string

	for _, part := range parts {
		if strings.Contains(part, "=") {
			partitionParts = append(partitionParts, part)
		}
	}

	if len(partitionParts) > 0 {
		return strings.Join(partitionParts, "_")
	}

	return "no_partition"
}

// clusterBySize clusters deletes to maintain target cluster sizes.
func (w *ClusteredPositionDeleteWriter) clusterBySize() string {
	w.clustersMutex.RLock()
	defer w.clustersMutex.RUnlock()

	// Find cluster with room for more deletes
	for clusterID, cluster := range w.clusters {
		cluster.mutex.RLock()
		size := int64(len(cluster.deletes))
		cluster.mutex.RUnlock()

		if size < w.config.MaxDeletesPerCluster {
			return clusterID
		}
	}

	// Create new cluster
	return fmt.Sprintf("size_cluster_%d", len(w.clusters))
}

// clusterByTimeWindow clusters deletes by time windows.
func (w *ClusteredPositionDeleteWriter) clusterByTimeWindow() string {
	// Use current time window (e.g., hour)
	now := time.Now()
	windowStart := now.Truncate(time.Hour)
	return fmt.Sprintf("time_%d", windowStart.Unix())
}

// clusterByHash clusters deletes using hash-based distribution.
func (w *ClusteredPositionDeleteWriter) clusterByHash(filePath string) string {
	// Simple hash-based clustering
	hash := 0
	for _, c := range filePath {
		hash = hash*31 + int(c)
	}
	if hash < 0 {
		hash = -hash
	}
	clusterCount := w.config.MaxClustersInMemory
	if clusterCount <= 0 {
		clusterCount = 10
	}
	return fmt.Sprintf("hash_%d", hash%clusterCount)
}

// getOrCreateCluster gets an existing cluster or creates a new one.
func (w *ClusteredPositionDeleteWriter) getOrCreateCluster(clusterID string) (*deleteCluster, error) {
	w.clustersMutex.RLock()
	cluster, exists := w.clusters[clusterID]
	w.clustersMutex.RUnlock()

	if exists {
		return cluster, nil
	}

	w.clustersMutex.Lock()
	defer w.clustersMutex.Unlock()

	// Double-check after acquiring write lock
	if cluster, exists := w.clusters[clusterID]; exists {
		return cluster, nil
	}

	// Check if we need to flush some clusters to make room
	if len(w.clusters) >= w.config.MaxClustersInMemory {
		err := w.flushOldestCluster(context.Background())
		if err != nil {
			return nil, fmt.Errorf("failed to flush oldest cluster: %w", err)
		}
	}

	// Create new cluster
	outputPath := fmt.Sprintf(w.config.OutputPathPattern,
		filepath.Dir(w.config.BaseConfig.OutputPath),
		w.config.ClusterNamePrefix+"_"+clusterID)

	writerConfig := w.config.BaseConfig
	writerConfig.OutputPath = outputPath

	if w.config.CompressionType != "" {
		if writerConfig.Properties == nil {
			writerConfig.Properties = make(map[string]string)
		}
		writerConfig.Properties["compression"] = w.config.CompressionType
	}

	var writer PositionDeleteWriter
	var err error

	// Use sorting writer with specified strategy
	sortingConfig := SortingPositionDeleteWriterConfig{
		BaseConfig:   writerConfig,
		SortStrategy: w.config.SortStrategy,
		BatchSize:    int(w.config.MaxDeletesPerCluster),
	}
	writer, err = NewSortingPositionOnlyDeleteWriter(sortingConfig)

	if err != nil {
		return nil, fmt.Errorf("failed to create writer for cluster %s: %w", clusterID, err)
	}

	cluster = &deleteCluster{
		id:            clusterID,
		deletes:       make([]*PositionDelete, 0),
		writer:        writer,
		lastFlushTime: time.Now(),
		config:        w.config,
	}

	w.clusters[clusterID] = cluster
	return cluster, nil
}

// addToCluster adds a delete to the specified cluster.
func (w *ClusteredPositionDeleteWriter) addToCluster(ctx context.Context, cluster *deleteCluster, delete *PositionDelete) error {
	cluster.mutex.Lock()
	defer cluster.mutex.Unlock()

	cluster.deletes = append(cluster.deletes, delete)
	cluster.recordsWritten++

	// Check if we need to flush this cluster
	if int64(len(cluster.deletes)) >= w.config.ClusterFlushThreshold {
		return w.flushCluster(ctx, cluster)
	}

	return nil
}

// flushCluster flushes a specific cluster to storage.
func (w *ClusteredPositionDeleteWriter) flushCluster(ctx context.Context, cluster *deleteCluster) error {
	if len(cluster.deletes) == 0 {
		return nil
	}

	// Write deletes to the cluster's writer
	err := cluster.writer.WriteBatch(ctx, cluster.deletes)
	if err != nil {
		return fmt.Errorf("failed to flush cluster %s: %w", cluster.id, err)
	}

	// Update metrics
	cluster.bytesWritten += cluster.writer.WrittenBytes()
	cluster.lastFlushTime = time.Now()

	// Clear the in-memory deletes
	cluster.deletes = cluster.deletes[:0]

	return nil
}

// flushOldestCluster flushes and removes the oldest cluster from memory.
func (w *ClusteredPositionDeleteWriter) flushOldestCluster(ctx context.Context) error {
	var oldestCluster *deleteCluster
	var oldestClusterID string
	oldestTime := time.Now()

	// Find the oldest cluster
	for clusterID, cluster := range w.clusters {
		cluster.mutex.RLock()
		if cluster.lastFlushTime.Before(oldestTime) {
			oldestTime = cluster.lastFlushTime
			oldestCluster = cluster
			oldestClusterID = clusterID
		}
		cluster.mutex.RUnlock()
	}

	if oldestCluster == nil {
		return nil
	}

	// Flush and close the oldest cluster
	err := w.flushCluster(ctx, oldestCluster)
	if err != nil {
		return err
	}

	err = oldestCluster.writer.Close()
	if err != nil {
		return fmt.Errorf("failed to close cluster %s: %w", oldestClusterID, err)
	}

	// Update total bytes
	w.totalBytes += oldestCluster.bytesWritten

	// Remove from active clusters
	delete(w.clusters, oldestClusterID)

	return nil
}

// autoFlushRoutine periodically flushes clusters based on the configured interval.
func (w *ClusteredPositionDeleteWriter) autoFlushRoutine() {
	ticker := time.NewTicker(w.config.AutoFlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if w.closed {
				return
			}
			w.flushStaleClusters(context.Background())
		}
	}
}

// flushStaleClusters flushes clusters that haven't been flushed recently.
func (w *ClusteredPositionDeleteWriter) flushStaleClusters(ctx context.Context) {
	w.clustersMutex.RLock()
	staleThreshold := time.Now().Add(-w.config.AutoFlushInterval)
	staleClusters := make([]*deleteCluster, 0)

	for _, cluster := range w.clusters {
		cluster.mutex.RLock()
		if cluster.lastFlushTime.Before(staleThreshold) && len(cluster.deletes) > 0 {
			staleClusters = append(staleClusters, cluster)
		}
		cluster.mutex.RUnlock()
	}
	w.clustersMutex.RUnlock()

	// Flush stale clusters
	for _, cluster := range staleClusters {
		w.flushCluster(ctx, cluster)
	}
}

// Close closes all clusters and flushes any remaining data.
func (w *ClusteredPositionDeleteWriter) Close() error {
	if w.closed {
		return nil
	}

	defer func() {
		w.closed = true
	}()

	w.clustersMutex.Lock()
	defer w.clustersMutex.Unlock()

	var errors []string
	for clusterID, cluster := range w.clusters {
		// Flush any remaining deletes
		if len(cluster.deletes) > 0 {
			if err := w.flushCluster(context.Background(), cluster); err != nil {
				errors = append(errors, fmt.Sprintf("flush cluster %s: %v", clusterID, err))
			}
		}

		// Close the cluster writer
		if err := cluster.writer.Close(); err != nil {
			errors = append(errors, fmt.Sprintf("close cluster %s: %v", clusterID, err))
		} else {
			w.totalBytes += cluster.bytesWritten
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("failed to close some clusters: %s", strings.Join(errors, "; "))
	}

	return nil
}

// DataFile returns aggregated data file metadata for all clusters.
func (w *ClusteredPositionDeleteWriter) DataFile() iceberg.DataFile {
	return iceberg.DataFileBuilder{}.
		ContentType(iceberg.EntryContentPosDeletes).
		Path(w.config.BaseConfig.OutputPath).
		Format(w.config.BaseConfig.FileFormat).
		RecordCount(w.totalRecords).
		FileSizeBytes(w.totalBytes).
		PartitionData(make(map[string]any)).
		Build()
}

// WrittenRecords returns the total number of records written across all clusters.
func (w *ClusteredPositionDeleteWriter) WrittenRecords() int64 {
	return w.totalRecords
}

// WrittenBytes returns the total number of bytes written across all clusters.
func (w *ClusteredPositionDeleteWriter) WrittenBytes() int64 {
	return w.totalBytes
}

// GetMetrics returns performance metrics for the clustered writer.
func (w *ClusteredPositionDeleteWriter) GetMetrics() ClusterMetrics {
	w.clustersMutex.RLock()
	defer w.clustersMutex.RUnlock()

	totalClusters := len(w.clusters)
	activeClusters := 0
	totalClusterSize := int64(0)

	for _, cluster := range w.clusters {
		cluster.mutex.RLock()
		if len(cluster.deletes) > 0 {
			activeClusters++
		}
		totalClusterSize += int64(len(cluster.deletes))
		cluster.mutex.RUnlock()
	}

	averageClusterSize := float64(0)
	if totalClusters > 0 {
		averageClusterSize = float64(totalClusterSize) / float64(totalClusters)
	}

	return ClusterMetrics{
		ActiveClusters:     activeClusters,
		TotalClusters:      totalClusters,
		TotalDeletes:       w.totalRecords,
		TotalBytes:         w.totalBytes,
		AverageClusterSize: averageClusterSize,
		LastFlushTime:      time.Now(), // Simplified - could track actual last flush
	}
}

// FlushAllClusters flushes all active clusters.
func (w *ClusteredPositionDeleteWriter) FlushAllClusters(ctx context.Context) error {
	w.clustersMutex.RLock()
	clusters := make([]*deleteCluster, 0, len(w.clusters))
	for _, cluster := range w.clusters {
		clusters = append(clusters, cluster)
	}
	w.clustersMutex.RUnlock()

	var errors []string
	for _, cluster := range clusters {
		if err := w.flushCluster(ctx, cluster); err != nil {
			errors = append(errors, fmt.Sprintf("cluster %s: %v", cluster.id, err))
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("failed to flush some clusters: %s", strings.Join(errors, "; "))
	}

	return nil
} 