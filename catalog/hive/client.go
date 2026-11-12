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
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
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
	Heartbeat(ctx context.Context, lockId int64) error
	Unlock(ctx context.Context, lockId int64) error
}

// LockClientProvider supplies a separate connection for renewing a metastore
// lock during view updates. The returned client is owned and closed by the caller.
// Its connection must be independent of concurrent catalog RPCs.
type LockClientProvider interface {
	NewLockClient(context.Context) (HiveClient, error)
}

type thriftClient struct {
	Client         *hive_metastore.ThriftHiveMetastoreClient
	closeTransport func() error
	uri            string
	opts           *HiveOptions
	dialContext    func(context.Context, string, string) (net.Conn, error)
}

func (c *thriftClient) NewLockClient(ctx context.Context) (HiveClient, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	host, port, err := parseHiveURI(c.uri)
	if err != nil {
		return nil, err
	}
	dial := c.dialContext
	if dial == nil {
		dial = (&net.Dialer{}).DialContext
	}
	dialCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	conn, err := dial(dialCtx, "tcp", net.JoinHostPort(host, strconv.Itoa(port)))
	if err != nil {
		if conn != nil {
			_ = conn.Close()
		}

		return nil, fmt.Errorf("failed to connect to metastore: %w", err)
	}
	if err := dialCtx.Err(); err != nil {
		_ = conn.Close()

		return nil, err
	}
	transport := thrift.NewTBufferedTransport(thrift.NewTSocketFromConnConf(conn, nil), 4096)
	closeTransport := closeOnce(transport.Close)
	protocols := thrift.NewTBinaryProtocolFactoryConf(nil)
	client := thrift.NewTStandardClient(protocols.GetProtocol(transport), protocols.GetProtocol(transport))
	generated := hive_metastore.NewThriftHiveMetastoreClient(&cancelableLockRPC{
		client: client, close: func() { _ = closeTransport() },
	})

	return &thriftClient{Client: generated, closeTransport: closeTransport, uri: c.uri, opts: c.opts, dialContext: c.dialContext}, nil
}

func parseHiveURI(uri string) (string, int, error) {
	parsed, err := url.Parse(uri)
	if err != nil {
		return "", 0, fmt.Errorf("invalid URI: %w", err)
	}

	host := parsed.Hostname()
	if host == "" {
		return "", 0, errors.New("invalid URI: host is required")
	}
	portStr := parsed.Port()
	if portStr == "" {
		portStr = "9083"
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return "", 0, fmt.Errorf("invalid port: %w", err)
	}
	if port < 1 || port > 65535 {
		return "", 0, fmt.Errorf("invalid port: %d", port)
	}

	return host, port, nil
}

func newHiveClient(uri string, opts *HiveOptions) (HiveClient, error) {
	host, port, err := parseHiveURI(uri)
	if err != nil {
		return nil, err
	}
	config := gohive.NewMetastoreConnectConfiguration()
	config.TransportMode = "binary"

	client, err := gohive.ConnectToMetastore(host, port, "NOSASL", config)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to metastore: %w", err)
	}

	return &thriftClient{
		Client: client.Client, uri: uri, opts: opts,
		closeTransport: closeOnce(func() error {
			client.Close()

			return nil
		}),
	}, nil
}

func (c *thriftClient) Close() error {
	if c.closeTransport != nil {
		return c.closeTransport()
	}

	return nil
}

func closeOnce(closeFn func() error) func() error {
	var once sync.Once
	var err error

	return func() error {
		once.Do(func() { err = closeFn() })

		return err
	}
}

func (c *thriftClient) GetDatabase(ctx context.Context, name string) (*hive_metastore.Database, error) {
	return c.Client.GetDatabase(ctx, name)
}

func (c *thriftClient) CreateDatabase(ctx context.Context, database *hive_metastore.Database) error {
	return c.Client.CreateDatabase(ctx, database)
}

func (c *thriftClient) AlterDatabase(ctx context.Context, dbname string, db *hive_metastore.Database) error {
	return c.Client.AlterDatabase(ctx, dbname, db)
}

func (c *thriftClient) DropDatabase(ctx context.Context, name string, deleteData, cascade bool) error {
	return c.Client.DropDatabase(ctx, name, deleteData, cascade)
}

func (c *thriftClient) GetAllDatabases(ctx context.Context) ([]string, error) {
	return c.Client.GetAllDatabases(ctx)
}

func (c *thriftClient) GetTable(ctx context.Context, dbName, tableName string) (*hive_metastore.Table, error) {
	return c.Client.GetTable(ctx, dbName, tableName)
}

func (c *thriftClient) CreateTable(ctx context.Context, tbl *hive_metastore.Table) error {
	return c.Client.CreateTable(ctx, tbl)
}

func (c *thriftClient) AlterTable(ctx context.Context, dbName, tableName string, newTbl *hive_metastore.Table) error {
	return c.Client.AlterTable(ctx, dbName, tableName, newTbl)
}

func (c *thriftClient) DropTable(ctx context.Context, dbName, tableName string, deleteData bool) error {
	return c.Client.DropTable(ctx, dbName, tableName, deleteData)
}

func (c *thriftClient) GetTables(ctx context.Context, dbName, pattern string) ([]string, error) {
	return c.Client.GetTables(ctx, dbName, pattern)
}

func (c *thriftClient) Lock(ctx context.Context, request *hive_metastore.LockRequest) (*hive_metastore.LockResponse, error) {
	return c.Client.Lock(ctx, request)
}

func (c *thriftClient) CheckLock(ctx context.Context, lockId int64) (*hive_metastore.LockResponse, error) {
	return c.Client.CheckLock(ctx, &hive_metastore.CheckLockRequest{
		Lockid: lockId,
	})
}

func (c *thriftClient) Heartbeat(ctx context.Context, lockId int64) error {
	txnID := int64(0)

	return c.Client.Heartbeat(ctx, &hive_metastore.HeartbeatRequest{
		Lockid: &lockId,
		Txnid:  &txnID,
	})
}

func (c *thriftClient) Unlock(ctx context.Context, lockId int64) error {
	return c.Client.Unlock(ctx, &hive_metastore.UnlockRequest{
		Lockid: lockId,
	})
}

// The buffered Thrift transport does not apply context deadlines to socket
// reads. Closing this dedicated connection interrupts a stalled renewal RPC.
type cancelableLockRPC struct {
	client thrift.TClient
	close  func()
}

func (c *cancelableLockRPC) Call(ctx context.Context, method string, args, result thrift.TStruct) (thrift.ResponseMeta, error) {
	if err := ctx.Err(); err != nil {
		return thrift.ResponseMeta{}, err
	}
	closed := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { c.close(); close(closed) })
	defer func() {
		if !stop() {
			<-closed
		}
	}()
	metadata, err := c.client.Call(ctx, method, args, result)
	if ctx.Err() != nil {
		err = errors.Join(err, ctx.Err())
	}

	return metadata, err
}
