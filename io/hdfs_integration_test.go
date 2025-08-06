//go:build integration

package io_test

import (
	"context"
	"testing"

	icebergio "github.com/apache/iceberg-go/io"
	hdfs "github.com/colinmarc/hdfs/v2"
	"github.com/stretchr/testify/require"
)

func TestHDFSOpenAndReadFile(t *testing.T) {
	addr := "localhost:9000"
	user := "hdfs"
	props := map[string]string{
		icebergio.HDFSNameNode: addr,
		icebergio.HDFSUser:     user,
	}
	ctx := context.Background()
	fs, err := icebergio.LoadFS(ctx, props, "hdfs://"+addr)
	require.NoError(t, err)

	client, err := hdfs.NewClient(hdfs.ClientOptions{Addresses: []string{addr}, User: user})
	require.NoError(t, err)
	const filename = "/tmp/iceberg-go-hdfs-test.txt"
	w, err := client.Create(filename)
	require.NoError(t, err)
	_, err = w.Write([]byte("hello hdfs"))
	require.NoError(t, err)
	require.NoError(t, w.Close())

	f, err := fs.Open("hdfs://" + addr + filename)
	require.NoError(t, err)
	buf := make([]byte, len("hello hdfs"))
	n, err := f.Read(buf)
	require.NoError(t, err)
	require.Equal(t, len(buf), n)
	require.Equal(t, "hello hdfs", string(buf))
	require.NoError(t, f.Close())

	data, err := fs.(icebergio.ReadFileIO).ReadFile("hdfs://" + addr + filename)
	require.NoError(t, err)
	require.Equal(t, "hello hdfs", string(data))

	require.NoError(t, fs.Remove("hdfs://"+addr+filename))
}
