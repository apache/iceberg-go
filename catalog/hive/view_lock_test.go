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
	"io"
	"net"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/beltran/gohive/hive_metastore"
	"github.com/stretchr/testify/require"
)

type viewDialTestClient struct {
	*mockHiveClient
	dialer *thriftClient
}

func (c *viewDialTestClient) NewLockClient(ctx context.Context) (HiveClient, error) {
	return c.dialer.NewLockClient(ctx)
}

func TestHiveUpdateViewReleasesLockIfRenewalDialIsCanceled(t *testing.T) {
	started := make(chan struct{})
	base := &mockHiveClient{}
	expectImmediateTableLock(base, 7)
	client := &viewDialTestClient{mockHiveClient: base, dialer: &thriftClient{
		uri: "thrift://example.invalid", dialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			close(started)
			<-ctx.Done()

			return nil, ctx.Err()
		},
	}}
	cat := NewCatalogWithClient(client, nil)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result := make(chan error, 1)
	go func() { _, err := cat.UpdateView(ctx, table.Identifier{"db", "view"}, nil, nil); result <- err }()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("renewal dial did not start")
	}
	cancel()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("view update did not stop after cancellation")
	}
	base.AssertExpectations(t)
}

func TestViewLockHeartbeatIntervalOptions(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value string
		want  time.Duration
	}{
		{"default", "", 4 * time.Minute},
		{"custom", "75", 75 * time.Millisecond},
		{"minimum", "1", time.Millisecond},
		{"maximum", "9223372036854", time.Duration(9223372036854) * time.Millisecond},
		{"zero", "0", 4 * time.Minute},
		{"negative", "-1", 4 * time.Minute},
		{"malformed", "1s", 4 * time.Minute},
		{"duration overflow", "9223372036855", 4 * time.Minute},
		{"integer overflow", "9223372036854775808", 4 * time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := NewHiveOptions()
			opts.ApplyProperties(iceberg.Properties{LockHeartbeatIntervalMs: tc.value})
			require.Equal(t, tc.want, opts.viewLockHeartbeatInterval())
		})
	}
	cat := NewCatalogWithClient(&mockHiveClient{}, iceberg.Properties{LockHeartbeatIntervalMs: "10"},
		WithProperties(iceberg.Properties{LockHeartbeatIntervalMs: "25"}))
	require.Equal(t, 25*time.Millisecond, cat.opts.viewLockHeartbeatInterval())
}

func TestLockClientDialHonorsCancellation(t *testing.T) {
	started := make(chan string, 1)
	client := &thriftClient{uri: "thrift://[::1]", dialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		started <- network + " " + address
		<-ctx.Done()

		return nil, ctx.Err()
	}}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result := make(chan error, 1)
	go func() { _, err := client.NewLockClient(ctx); result <- err }()
	select {
	case address := <-started:
		require.Equal(t, "tcp [::1]:9083", address)
	case <-time.After(5 * time.Second):
		t.Fatal("lock-client connection did not start")
	}
	cancel()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("cancellation did not interrupt lock-client connection")
	}
}

func TestLockClientDialHasTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := &thriftClient{uri: "thrift://example.invalid", dialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			<-ctx.Done()

			return nil, ctx.Err()
		}}
		start := time.Now()
		_, err := client.NewLockClient(t.Context())
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Equal(t, 5*time.Second, time.Since(start))
	})
}

func TestLockClientClosesConnectionIfDialCompletesAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	conn, peer := net.Pipe()
	defer peer.Close()
	client := &thriftClient{uri: "thrift://example.invalid", dialContext: func(context.Context, string, string) (net.Conn, error) {
		cancel()

		return conn, nil
	}}
	lockClient, err := client.NewLockClient(ctx)
	require.Nil(t, lockClient)
	require.ErrorIs(t, err, context.Canceled)
	readResult := make(chan error, 1)
	go func() { _, err := peer.Read(make([]byte, 1)); readResult <- err }()
	select {
	case err := <-readResult:
		require.ErrorIs(t, err, io.EOF)
	case <-time.After(time.Second):
		t.Fatal("connection was not closed after cancellation")
	}
}

func TestLockClientBinaryRoundTrip(t *testing.T) {
	conn, peer := net.Pipe()
	client := &thriftClient{uri: "thrift://example.invalid", dialContext: func(context.Context, string, string) (net.Conn, error) {
		return conn, nil
	}}
	serverResult := make(chan error, 1)
	go func() {
		serverResult <- func() error {
			transport := thrift.NewTBufferedTransport(thrift.NewTSocketFromConnConf(peer, nil), 4096)
			defer transport.Close()
			protocol := thrift.NewTBinaryProtocolFactoryConf(nil).GetProtocol(transport)
			ctx := t.Context()
			name, messageType, sequence, err := protocol.ReadMessageBegin(ctx)
			if err != nil {
				return err
			}
			args := hive_metastore.NewThriftHiveMetastoreCheckLockArgs()
			if err := args.Read(ctx, protocol); err != nil {
				return err
			}
			if err := protocol.ReadMessageEnd(ctx); err != nil {
				return err
			}
			if name != "check_lock" || messageType != thrift.CALL || args.Rqst == nil || args.Rqst.Lockid != 42 {
				return fmt.Errorf("unexpected check lock request: %s, %v, %v", name, messageType, args.Rqst)
			}
			if err := protocol.WriteMessageBegin(ctx, name, thrift.REPLY, sequence); err != nil {
				return err
			}
			response := &hive_metastore.ThriftHiveMetastoreCheckLockResult{
				Success: &hive_metastore.LockResponse{Lockid: 42, State: hive_metastore.LockState_ACQUIRED},
			}
			if err := response.Write(ctx, protocol); err != nil {
				return err
			}
			if err := protocol.WriteMessageEnd(ctx); err != nil {
				return err
			}

			return transport.Flush(ctx)
		}()
	}()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	lockClient, err := client.NewLockClient(ctx)
	require.NoError(t, err)
	defer lockClient.Close()
	response, err := lockClient.CheckLock(ctx, 42)
	require.NoError(t, err)
	require.Equal(t, hive_metastore.LockState_ACQUIRED, response.State)
	require.EqualValues(t, 42, response.Lockid)
	require.NoError(t, <-serverResult)
}

func TestLockClientHeartbeatBinaryRoundTrip(t *testing.T) {
	conn, peer := net.Pipe()
	client := &thriftClient{uri: "thrift://example.invalid", dialContext: func(context.Context, string, string) (net.Conn, error) {
		return conn, nil
	}}
	serverResult := make(chan error, 1)
	go func() {
		serverResult <- func() error {
			transport := thrift.NewTBufferedTransport(thrift.NewTSocketFromConnConf(peer, nil), 4096)
			defer transport.Close()
			protocol := thrift.NewTBinaryProtocolFactoryConf(nil).GetProtocol(transport)
			ctx := t.Context()
			name, messageType, sequence, err := protocol.ReadMessageBegin(ctx)
			if err != nil {
				return err
			}
			args := hive_metastore.NewThriftHiveMetastoreHeartbeatArgs()
			if err := args.Read(ctx, protocol); err != nil {
				return err
			}
			if err := protocol.ReadMessageEnd(ctx); err != nil {
				return err
			}
			if name != "heartbeat" || messageType != thrift.CALL || args.Ids == nil ||
				args.Ids.Lockid == nil || *args.Ids.Lockid != 42 ||
				args.Ids.Txnid == nil || *args.Ids.Txnid != 0 {
				return fmt.Errorf("unexpected heartbeat request: %s, %v, %v", name, messageType, args.Ids)
			}
			if err := protocol.WriteMessageBegin(ctx, name, thrift.REPLY, sequence); err != nil {
				return err
			}
			if err := hive_metastore.NewThriftHiveMetastoreHeartbeatResult().Write(ctx, protocol); err != nil {
				return err
			}
			if err := protocol.WriteMessageEnd(ctx); err != nil {
				return err
			}

			return transport.Flush(ctx)
		}()
	}()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	lockClient, err := client.NewLockClient(ctx)
	require.NoError(t, err)
	defer lockClient.Close()
	require.NoError(t, lockClient.Heartbeat(ctx, 42))
	require.NoError(t, <-serverResult)
}

func TestViewLockLeaseRenewsAndStops(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var heartbeats atomic.Int32
		client := &viewRenewalTestClient{heartbeat: func(context.Context, int64) error {
			heartbeats.Add(1)

			return nil
		}}
		lease := newViewLockLease(t.Context(), client, 7, time.Second)
		synctest.Wait()
		time.Sleep(3500 * time.Millisecond)
		synctest.Wait()
		require.EqualValues(t, 3, heartbeats.Load())
		require.NoError(t, lease.check(t.Context()))
		lease.stop()
		count := heartbeats.Load()
		time.Sleep(3 * time.Second)
		require.Equal(t, count, heartbeats.Load())
	})
}

func TestViewLockLeaseRemembersRenewalFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var heartbeats atomic.Int32
		cause := errors.New("lock disappeared")
		client := &viewRenewalTestClient{heartbeat: func(context.Context, int64) error {
			heartbeats.Add(1)

			return cause
		}}
		lease := newViewLockLease(t.Context(), client, 7, time.Second)
		synctest.Wait()
		time.Sleep(3500 * time.Millisecond)
		synctest.Wait()
		require.EqualValues(t, 1, heartbeats.Load())
		require.ErrorIs(t, lease.check(t.Context()), cause)
		lease.stop()
	})
}

func TestLockClientCancellationInterruptsStalledMetastore(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	received := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		connection, err := listener.Accept()
		if err != nil {
			return
		}
		defer connection.Close()
		buffer := make([]byte, 4096)
		if _, err := connection.Read(buffer); err != nil {
			return
		}
		close(received)
		io.Copy(io.Discard, connection)
	}()
	original := &thriftClient{uri: "thrift://" + listener.Addr().String(), opts: &HiveOptions{}}
	ctx, cancel := context.WithCancel(t.Context())
	client, err := original.NewLockClient(ctx)
	require.NoError(t, err)
	defer client.Close()
	result := make(chan error, 1)
	go func() { result <- client.Heartbeat(ctx, 1) }()
	select {
	case <-received:
	case <-time.After(5 * time.Second):
		t.Fatal("metastore did not receive lock request")
	}
	cancel()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("cancellation did not interrupt lock RPC")
	}
	<-done
	require.NoError(t, client.Close())
	require.NoError(t, client.Close())
}
