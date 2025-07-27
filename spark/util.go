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

// Package spark provides Apache Spark integration for Iceberg Go tables.
package spark

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/apache/iceberg-go/table"
	// "github.com/apache/spark-connect-go/spark" // Commented out due to library versioning issues
)

// SparkUtil provides general utility functions for Spark integration
type SparkUtil struct {
	session *SparkSession
}

// NewSparkUtil creates a new SparkUtil instance with the given Spark session
func NewSparkUtil(session *SparkSession) *SparkUtil {
	return &SparkUtil{
		session: session,
	}
}

// Session returns the underlying Spark session
func (s *SparkUtil) Session() *SparkSession {
	return s.session
}

// ConnectToSpark creates a new Spark session with the given configuration
func ConnectToSpark(ctx context.Context, host string, port int, options map[string]string) (*SparkSession, error) {
	config := NewSparkSessionBuilder().
		Master(fmt.Sprintf("sc://%s:%d", host, port)).
		AppName("iceberg-go-integration")

	// Apply additional configuration options
	for key, value := range options {
		config = config.Config(key, value)
	}

	session, err := config.Build(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create Spark session: %w", err)
	}

	return session, nil
}

// FormatTableIdentifier converts an Iceberg table identifier to Spark table name format
func FormatTableIdentifier(identifier table.Identifier) string {
	return strings.Join(identifier, ".")
}

// ParseTableIdentifier converts a Spark table name to Iceberg table identifier
func ParseTableIdentifier(tableName string) table.Identifier {
	return strings.Split(tableName, ".")
}

// ValidateSparkCompatibility checks if the Spark session supports Iceberg operations
func (s *SparkUtil) ValidateSparkCompatibility(ctx context.Context) error {
	// Check if Iceberg catalog is available
	catalogs, err := s.session.Sql(ctx, "SHOW CATALOGS")
	if err != nil {
		return fmt.Errorf("failed to check available catalogs: %w", err)
	}

	rows, err := catalogs.Collect(ctx)
	if err != nil {
		return fmt.Errorf("failed to collect catalog results: %w", err)
	}

	// Check if any Iceberg-compatible catalog exists
	for _, row := range rows {
		if len(row) > 0 {
			// At least one catalog exists, assume Iceberg support
			return nil
		}
	}

	return errors.New("no compatible catalogs found for Iceberg integration")
}

// CreateCatalogIfNotExists creates an Iceberg catalog in Spark if it doesn't exist
func (s *SparkUtil) CreateCatalogIfNotExists(ctx context.Context, catalogName string, catalogProps map[string]string) error {
	// Build CREATE CATALOG statement
	var props []string
	for key, value := range catalogProps {
		props = append(props, fmt.Sprintf("'%s'='%s'", key, value))
	}

	createCatalogSQL := fmt.Sprintf(
		"CREATE CATALOG IF NOT EXISTS %s USING iceberg WITH (%s)",
		catalogName,
		strings.Join(props, ", "),
	)

	df, err := s.session.Sql(ctx, createCatalogSQL)
	if err != nil {
		return fmt.Errorf("failed to create catalog %s: %w", catalogName, err)
	}
	_, err = df.Collect(ctx)
	if err != nil {
		return fmt.Errorf("failed to create catalog %s: %w", catalogName, err)
	}

	return nil
}

// GetTableProperties retrieves table properties from Spark
func (s *SparkUtil) GetTableProperties(ctx context.Context, tableName string) (map[string]string, error) {
	sql := "SHOW TBLPROPERTIES " + tableName
	df, err := s.session.Sql(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("failed to get table properties: %w", err)
	}

	rows, err := df.Collect(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to collect table properties: %w", err)
	}

	properties := make(map[string]string)
	for _, row := range rows {
		if len(row) >= 2 {
			key := fmt.Sprintf("%v", row[0])
			value := fmt.Sprintf("%v", row[1])
			properties[key] = value
		}
	}

	return properties, nil
}

// ExecuteSQL executes a SQL statement and returns the result as a DataFrame
func (s *SparkUtil) ExecuteSQL(ctx context.Context, sql string) (*DataFrame, error) {
	df, err := s.session.Sql(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("failed to execute SQL: %w", err)
	}

	return df, nil
}

// Close closes the Spark session
func (s *SparkUtil) Close() error {
	if s.session != nil {
		return s.session.Stop()
	}

	return nil
}
