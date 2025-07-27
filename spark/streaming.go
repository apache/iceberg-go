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
	"errors"
	"fmt"
	"strconv"

	"github.com/apache/iceberg-go/table"
	// "github.com/apache/spark-connect-go/spark" // Commented out due to library versioning issues
)

// SparkMicroBatchStream provides structured streaming support for Iceberg tables
type SparkMicroBatchStream struct {
	util      *SparkUtil
	converter *SparkValueConverter
}

// NewSparkMicroBatchStream creates a new SparkMicroBatchStream instance
func NewSparkMicroBatchStream(util *SparkUtil) *SparkMicroBatchStream {
	return &SparkMicroBatchStream{
		util:      util,
		converter: NewSparkValueConverter(),
	}
}

// StreamingOptions contains options for streaming operations
type StreamingOptions struct {
	// Checkpoint location for fault tolerance
	CheckpointLocation string
	// Trigger options
	Trigger StreamingTrigger
	// Output mode
	OutputMode StreamingOutputMode
	// Processing time interval
	ProcessingTimeInterval string
	// Maximum files per trigger
	MaxFilesPerTrigger int
	// Schema enforcement
	EnforceSchema bool
	// Starting position for streaming
	StartingPosition string
	// Watermark configuration
	WatermarkColumn string
	WatermarkDelay  string
}

// StreamingTrigger defines trigger types for streaming
type StreamingTrigger string

const (
	TriggerProcessingTime StreamingTrigger = "processingTime"
	TriggerOnce           StreamingTrigger = "once"
	TriggerContinuous     StreamingTrigger = "continuous"
	TriggerAvailableNow   StreamingTrigger = "availableNow"
)

// StreamingOutputMode defines output modes for streaming
type StreamingOutputMode string

const (
	OutputModeAppend   StreamingOutputMode = "append"
	OutputModeUpdate   StreamingOutputMode = "update"
	OutputModeComplete StreamingOutputMode = "complete"
)

// StreamingQueryInfo represents a running streaming query info
type StreamingQueryInfo struct {
	QueryID   string
	Name      string
	Status    StreamingStatus
	DataFrame *DataFrame
	util      *SparkUtil
}

// StreamingStatus represents the status of a streaming query
type StreamingStatus struct {
	IsActive               bool
	InputRowsPerSecond     float64
	ProcessedRowsPerSecond float64
	BatchId                int64
	LastProgress           map[string]interface{}
}

// ReadStreamFromTable creates a streaming DataFrame from an Iceberg table
func (s *SparkMicroBatchStream) ReadStreamFromTable(
	ctx context.Context,
	identifier table.Identifier,
	options *StreamingOptions,
) (*DataFrame, error) {
	tableName := FormatTableIdentifier(identifier)

	// Create streaming read from Iceberg table
	reader := s.util.session.ReadStream().
		Format("iceberg").
		Option("path", tableName)

	// Apply streaming options
	if options.StartingPosition != "" {
		reader = reader.Option("startingVersion", options.StartingPosition)
	}

	if options.MaxFilesPerTrigger > 0 {
		reader = reader.Option("maxFilesPerTrigger", strconv.Itoa(options.MaxFilesPerTrigger))
	}

	if options.EnforceSchema {
		reader = reader.Option("enforceSchema", "true")
	}

	// Load the streaming DataFrame
	df, err := reader.Load()
	if err != nil {
		return nil, fmt.Errorf("failed to create streaming DataFrame: %w", err)
	}

	// Apply watermark if specified
	if options.WatermarkColumn != "" && options.WatermarkDelay != "" {
		df = df.WithWatermark(options.WatermarkColumn, options.WatermarkDelay)
	}

	return df, nil
}

// WriteStreamToTable writes a streaming DataFrame to an Iceberg table
func (s *SparkMicroBatchStream) WriteStreamToTable(
	ctx context.Context,
	df *DataFrame,
	identifier table.Identifier,
	options *StreamingOptions,
) (*StreamingQuery, error) {
	tableName := FormatTableIdentifier(identifier)

	// Create streaming write to Iceberg table
	writer := df.WriteStream().
		Format("iceberg").
		Option("path", tableName)

	// Apply output mode
	switch options.OutputMode {
	case OutputModeAppend:
		writer = writer.OutputMode("append")
	case OutputModeUpdate:
		writer = writer.OutputMode("update")
	case OutputModeComplete:
		writer = writer.OutputMode("complete")
	default:
		writer = writer.OutputMode("append")
	}

	// Set checkpoint location
	if options.CheckpointLocation != "" {
		writer = writer.Option("checkpointLocation", options.CheckpointLocation)
	}

	// Apply trigger
	switch options.Trigger {
	case TriggerProcessingTime:
		if options.ProcessingTimeInterval != "" {
			writer = writer.Trigger(ProcessingTime(options.ProcessingTimeInterval))
		}
	case TriggerOnce:
		writer = writer.Trigger(Once())
	case TriggerContinuous:
		if options.ProcessingTimeInterval != "" {
			writer = writer.Trigger(Continuous(options.ProcessingTimeInterval))
		}
	case TriggerAvailableNow:
		writer = writer.Trigger(AvailableNow())
	}

	// Start the streaming query
	query, err := writer.Start()
	if err != nil {
		return nil, fmt.Errorf("failed to start streaming query: %w", err)
	}

	return query, nil
}

// CreateStreamingAggregation creates a streaming aggregation query
func (s *SparkMicroBatchStream) CreateStreamingAggregation(
	ctx context.Context,
	sourceIdentifier table.Identifier,
	targetIdentifier table.Identifier,
	aggregationSQL string,
	options *StreamingOptions,
) (*StreamingQuery, error) {
	// Read stream from source table
	sourceDF, err := s.ReadStreamFromTable(ctx, sourceIdentifier, options)
	if err != nil {
		return nil, fmt.Errorf("failed to read source stream: %w", err)
	}

	// Create temporary view for aggregation
	viewName := "streaming_source_" + FormatTableIdentifier(sourceIdentifier)
	err = sourceDF.CreateOrReplaceTempView(viewName)
	if err != nil {
		return nil, fmt.Errorf("failed to create temporary view: %w", err)
	}

	// Apply aggregation
	aggregationDF, err := s.util.ExecuteSQL(ctx, aggregationSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to execute aggregation SQL: %w", err)
	}

	// Write aggregated stream to target table
	return s.WriteStreamToTable(ctx, aggregationDF, targetIdentifier, options)
}

// CreateStreamingJoin creates a streaming join query
func (s *SparkMicroBatchStream) CreateStreamingJoin(
	ctx context.Context,
	streamIdentifier table.Identifier,
	staticIdentifier table.Identifier,
	joinCondition string,
	joinType string,
	targetIdentifier table.Identifier,
	options *StreamingOptions,
) (*StreamingQuery, error) {
	// Read streaming table
	streamDF, err := s.ReadStreamFromTable(ctx, streamIdentifier, options)
	if err != nil {
		return nil, fmt.Errorf("failed to read streaming table: %w", err)
	}

	// Read static table
	staticTableName := FormatTableIdentifier(staticIdentifier)
	staticDF, err := s.util.ExecuteSQL(ctx, "SELECT * FROM "+staticTableName)
	if err != nil {
		return nil, fmt.Errorf("failed to read static table: %w", err)
	}

	// Create temporary views
	streamViewName := "stream_" + FormatTableIdentifier(streamIdentifier)
	staticViewName := "static_" + FormatTableIdentifier(staticIdentifier)

	err = streamDF.CreateOrReplaceTempView(streamViewName)
	if err != nil {
		return nil, fmt.Errorf("failed to create stream view: %w", err)
	}

	err = staticDF.CreateOrReplaceTempView(staticViewName)
	if err != nil {
		return nil, fmt.Errorf("failed to create static view: %w", err)
	}

	// Create join query
	joinSQL := fmt.Sprintf(
		"SELECT * FROM %s %s %s %s ON %s",
		streamViewName,
		joinType,
		"JOIN",
		staticViewName,
		joinCondition,
	)

	joinDF, err := s.util.ExecuteSQL(ctx, joinSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to execute join: %w", err)
	}

	// Write joined stream to target table
	return s.WriteStreamToTable(ctx, joinDF, targetIdentifier, options)
}

// GetStreamingQueryStatus gets the status of a streaming query
func (s *SparkMicroBatchStream) GetStreamingQueryStatus(query *StreamingQuery) (*StreamingStatus, error) {
	// This would need to be implemented based on the Spark Connect Go API
	// For now, return a placeholder
	return &StreamingStatus{
		IsActive: true,
		LastProgress: map[string]interface{}{
			"id":   query.ID(),
			"name": query.Name(),
		},
	}, nil
}

// StopStreamingQuery stops a running streaming query
func (s *SparkMicroBatchStream) StopStreamingQuery(ctx context.Context, query *StreamingQuery) error {
	// This would need to be implemented based on the Spark Connect Go API
	// For now, return a placeholder
	return errors.New("streaming query stop not yet implemented")
}

// AwaitTermination waits for a streaming query to terminate
func (s *SparkMicroBatchStream) AwaitTermination(ctx context.Context, query *StreamingQuery, timeoutMs *int64) error {
	// This would need to be implemented based on the Spark Connect Go API
	// For now, return a placeholder
	return errors.New("streaming query await termination not yet implemented")
}

// ListActiveStreams lists all active streaming queries
func (s *SparkMicroBatchStream) ListActiveStreams(ctx context.Context) ([]*StreamingQuery, error) {
	// This would need to be implemented based on the Spark Connect Go API
	// For now, return empty list
	return []*StreamingQuery{}, nil
}

// CreateStreamToStreamJoin creates a stream-to-stream join
func (s *SparkMicroBatchStream) CreateStreamToStreamJoin(
	ctx context.Context,
	leftStreamIdentifier table.Identifier,
	rightStreamIdentifier table.Identifier,
	joinCondition string,
	joinType string,
	watermarkSpec map[string]string, // column -> delay mapping
	targetIdentifier table.Identifier,
	options *StreamingOptions,
) (*StreamingQuery, error) {
	// Read left stream
	leftDF, err := s.ReadStreamFromTable(ctx, leftStreamIdentifier, options)
	if err != nil {
		return nil, fmt.Errorf("failed to read left stream: %w", err)
	}

	// Read right stream
	rightDF, err := s.ReadStreamFromTable(ctx, rightStreamIdentifier, options)
	if err != nil {
		return nil, fmt.Errorf("failed to read right stream: %w", err)
	}

	// Apply watermarks
	for column, delay := range watermarkSpec {
		leftDF = leftDF.WithWatermark(column, delay)
		rightDF = rightDF.WithWatermark(column, delay)
	}

	// Create temporary views
	leftViewName := "left_stream_" + FormatTableIdentifier(leftStreamIdentifier)
	rightViewName := "right_stream_" + FormatTableIdentifier(rightStreamIdentifier)

	err = leftDF.CreateOrReplaceTempView(leftViewName)
	if err != nil {
		return nil, fmt.Errorf("failed to create left stream view: %w", err)
	}

	err = rightDF.CreateOrReplaceTempView(rightViewName)
	if err != nil {
		return nil, fmt.Errorf("failed to create right stream view: %w", err)
	}

	// Create stream-to-stream join query
	joinSQL := fmt.Sprintf(
		"SELECT * FROM %s %s %s %s ON %s",
		leftViewName,
		joinType,
		"JOIN",
		rightViewName,
		joinCondition,
	)

	joinDF, err := s.util.ExecuteSQL(ctx, joinSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to execute stream join: %w", err)
	}

	// Write joined stream to target table
	return s.WriteStreamToTable(ctx, joinDF, targetIdentifier, options)
}
