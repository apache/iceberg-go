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

type HiveClient interface {
	Close() error

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

	Lock(ctx context.Context, request *hive_metastore.LockRequest) (*hive_metastore.LockResponse, error)
	CheckLock(ctx context.Context, lockId int64) (*hive_metastore.LockResponse, error)
	Unlock(ctx context.Context, lockId int64) error
}

type thriftClient struct {
	client *gohive.HiveMetastoreClient
}

func newHiveClient(uri string, opts *HiveOptions) (HiveClient, error) {
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

func (c *thriftClient) Close() error {
	if c.client != nil {
		c.client.Close()
		c.client = nil
	}

	return nil
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

func (c *thriftClient) Lock(ctx context.Context, request *hive_metastore.LockRequest) (*hive_metastore.LockResponse, error) {
	return c.client.Client.Lock(ctx, request)
}

func (c *thriftClient) CheckLock(ctx context.Context, lockId int64) (*hive_metastore.LockResponse, error) {
	return c.client.Client.CheckLock(ctx, &hive_metastore.CheckLockRequest{
		Lockid: lockId,
	})
}

func (c *thriftClient) Unlock(ctx context.Context, lockId int64) error {
	return c.client.Client.Unlock(ctx, &hive_metastore.UnlockRequest{
		Lockid: lockId,
	})
}
