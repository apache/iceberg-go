package io

import (
	"errors"
	"io/fs"
	"net/url"
	"strconv"
	"strings"

	hdfs "github.com/colinmarc/hdfs/v2"
)

// Constants for HDFS configuration options
const (
	HDFSNameNode            = "hdfs.namenode"
	HDFSUser                = "hdfs.user"
	HDFSUseDatanodeHostname = "hdfs.use-datanode-hostname"
)

// HdfsFS is an implementation of IO backed by an HDFS cluster.
type HdfsFS struct{ client *hdfs.Client }

func (h *HdfsFS) preprocess(name string) string {
	if strings.HasPrefix(name, "hdfs://") {
		if u, err := url.Parse(name); err == nil {
			if u.Path != "" {
				return u.Path
			}
		}
		name = strings.TrimPrefix(name, "hdfs://")
		if idx := strings.IndexByte(name, '/'); idx >= 0 {
			name = name[idx:]
		}
	}
	return name
}

// Open opens the named file for reading from HDFS.
func (h *HdfsFS) Open(name string) (File, error) {
	name = h.preprocess(name)
	f, err := h.client.Open(name)
	if err != nil {
		return nil, err
	}
	return hdfsFile{f}, nil
}

// ReadFile reads the named file and returns its contents.
func (h *HdfsFS) ReadFile(name string) ([]byte, error) {
	name = h.preprocess(name)
	return h.client.ReadFile(name)
}

// Remove removes the named file or (empty) directory from HDFS.
func (h *HdfsFS) Remove(name string) error {
	name = h.preprocess(name)
	return h.client.Remove(name)
}

// hdfsFile wraps a FileReader to implement fs.File, io.ReadSeekCloser, and io.ReaderAt.
type hdfsFile struct{ *hdfs.FileReader }

func (f hdfsFile) Stat() (fs.FileInfo, error) { return f.FileReader.Stat(), nil }

// createHDFSFS constructs an HDFS-backed IO from a parsed URL and configuration properties.
func createHDFSFS(parsed *url.URL, props map[string]string) (IO, error) {
	addresses := []string{}
	if nn := props[HDFSNameNode]; nn != "" {
		addresses = append(addresses, nn)
	} else if parsed != nil && parsed.Host != "" {
		addresses = append(addresses, parsed.Host)
	}
	if len(addresses) == 0 {
		return nil, errors.New("hdfs namenode not specified")
	}
	opts := hdfs.ClientOptions{Addresses: addresses}
	if user := props[HDFSUser]; user != "" {
		opts.User = user
	}
	if v, ok := props[HDFSUseDatanodeHostname]; ok {
		if b, err := strconv.ParseBool(v); err == nil {
			opts.UseDatanodeHostname = b
		}
	}
	client, err := hdfs.NewClient(opts)
	if err != nil {
		return nil, err
	}
	return &HdfsFS{client: client}, nil
}
