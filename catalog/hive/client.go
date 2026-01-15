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

package hive

import (
	"context"
	"fmt"
	"net/url"
	"strconv"

	"github.com/beltran/gohive"
	"github.com/beltran/gohive/hive_metastore"
)

// HiveClient interface for Hive Metastore operations.
// This allows for mocking in tests.
type HiveClient interface {
	Close()

	GetDatabase(ctx context.Context, name string) (*hive_metastore.Database, error)
	CreateDatabase(ctx context.Context, database *hive_metastore.Database) error
	AlterDatabase(ctx context.Context, dbname string, db *hive_metastore.Database) error
	DropDatabase(ctx context.Context, name string, deleteData, cascade bool) error
	GetAllDatabases(ctx context.Context) ([]string, error)

	GetTable(ctx context.Context, dbName, tableName string) (*hive_metastore.Table, error)
	CreateTable(ctx context.Context, tbl *hive_metastore.Table) error
	AlterTable(ctx context.Context, dbName, tableName string, newTbl *hive_metastore.Table) error
	DropTable(ctx context.Context, dbName, tableName string, deleteData bool) error
	GetTables(ctx context.Context, dbName, pattern string) ([]string, error)
}

// thriftClient wraps the gohive HiveMetastoreClient.
type thriftClient struct {
	client *gohive.HiveMetastoreClient
}

// NewHiveClient creates a new Hive Metastore client using gohive.
func NewHiveClient(uri string, opts *HiveOptions) (HiveClient, error) {
	parsed, err := url.Parse(uri)
	if err != nil {
		return nil, fmt.Errorf("invalid URI: %w", err)
	}

	host := parsed.Hostname()
	portStr := parsed.Port()
	if portStr == "" {
		portStr = "9083"
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, fmt.Errorf("invalid port: %w", err)
	}

	// Determine authentication mode
	auth := "NOSASL"
	if opts != nil && opts.KerberosAuth {
		auth = "KERBEROS"
	}

	config := gohive.NewMetastoreConnectConfiguration()
	config.TransportMode = "binary"

	client, err := gohive.ConnectToMetastore(host, port, auth, config)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to metastore: %w", err)
	}

	return &thriftClient{client: client}, nil
}

func (c *thriftClient) Close() {
	if c.client != nil {
		c.client.Close()
	}
}

func (c *thriftClient) GetDatabase(ctx context.Context, name string) (*hive_metastore.Database, error) {
	return c.client.Client.GetDatabase(ctx, name)
}

func (c *thriftClient) CreateDatabase(ctx context.Context, database *hive_metastore.Database) error {
	return c.client.Client.CreateDatabase(ctx, database)
}

func (c *thriftClient) AlterDatabase(ctx context.Context, dbname string, db *hive_metastore.Database) error {
	return c.client.Client.AlterDatabase(ctx, dbname, db)
}

func (c *thriftClient) DropDatabase(ctx context.Context, name string, deleteData, cascade bool) error {
	return c.client.Client.DropDatabase(ctx, name, deleteData, cascade)
}

func (c *thriftClient) GetAllDatabases(ctx context.Context) ([]string, error) {
	return c.client.Client.GetAllDatabases(ctx)
}

func (c *thriftClient) GetTable(ctx context.Context, dbName, tableName string) (*hive_metastore.Table, error) {
	return c.client.Client.GetTable(ctx, dbName, tableName)
}

func (c *thriftClient) CreateTable(ctx context.Context, tbl *hive_metastore.Table) error {
	return c.client.Client.CreateTable(ctx, tbl)
}

func (c *thriftClient) AlterTable(ctx context.Context, dbName, tableName string, newTbl *hive_metastore.Table) error {
	return c.client.Client.AlterTable(ctx, dbName, tableName, newTbl)
}

func (c *thriftClient) DropTable(ctx context.Context, dbName, tableName string, deleteData bool) error {
	return c.client.Client.DropTable(ctx, dbName, tableName, deleteData)
}

func (c *thriftClient) GetTables(ctx context.Context, dbName, pattern string) ([]string, error) {
	return c.client.Client.GetTables(ctx, dbName, pattern)
}
