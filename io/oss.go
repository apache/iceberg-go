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

package io

import (
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
)

// Constants for OSS configuration options
const (
	OSSAccessKey = "client.oss-access-key"
	OSSSecretKey = "client.oss-secret-key"
	OSSEndpoint  = "client.oss-endpoint"
)

func createOSSFileIO(parsed *url.URL, props map[string]string) (IO, error) {
	endpoint, ok := props[OSSEndpoint]
	if !ok {
		endpoint = os.Getenv("OSS_ENDPOINT")
	}
	if endpoint == "" {
		return nil, fmt.Errorf("oss endpoint must be specified")
	}

	accessKey := props[OSSAccessKey]
	secretKey := props[OSSSecretKey]

	client, err := oss.New(endpoint, accessKey, secretKey)
	if err != nil {
		return nil, err
	}

	bucketName := parsed.Host
	bucket, err := client.Bucket(bucketName)
	if err != nil {
		return nil, err
	}

	ossFS := &ossFS{
		bucket: bucket,
	}

	preprocess := func(n string) string {
		_, after, found := strings.Cut(n, "://")
		if found {
			n = after
		}
		return strings.TrimPrefix(n, parsed.Host)
	}

	return FSPreProcName(ossFS, preprocess), nil
}

type ossFS struct {
	bucket *oss.Bucket
}

// Open implements fs.FS
func (o *ossFS) Open(name string) (fs.File, error) {
	if !fs.ValidPath(name) {
		return nil, &os.PathError{Op: "open", Path: name, Err: os.ErrInvalid}
	}
	if name == "." {
		return &ossFile{
			name:   name,
			bucket: o.bucket,
		}, nil
	}
	name = strings.TrimPrefix(name, "/")

	obj, err := o.bucket.GetObject(name)
	if err != nil {
		return nil, err
	}

	return &ossFile{
		reader: obj,
		name:   name,
		bucket: o.bucket,
	}, nil
}

type ossFile struct {
	mutex  sync.Mutex
	reader io.ReadCloser
	bucket *oss.Bucket
	name   string
}

func (f *ossFile) Read(p []byte) (int, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	return f.reader.Read(p)
}

func (f *ossFile) Close() error {
	return f.reader.Close()
}

func (f *ossFile) Stat() (fs.FileInfo, error) {
	header, err := f.bucket.GetObjectMeta(f.name)
	if err != nil {
		return nil, err
	}

	cl, err := strconv.ParseInt(header.Get("Content-Length"), 10, 64)
	if err != nil {
		return nil, err
	}

	return &ossFileInfo{
		name: f.name,
		size: cl,
	}, nil
}

type ossFileInfo struct {
	name string
	size int64
}

func (fi *ossFileInfo) Name() string       { return fi.name }
func (fi *ossFileInfo) Size() int64        { return fi.size }
func (fi *ossFileInfo) Mode() fs.FileMode  { return 0444 }
func (fi *ossFileInfo) ModTime() time.Time { return time.Time{} }
func (fi *ossFileInfo) IsDir() bool        { return false }
func (fi *ossFileInfo) Sys() interface{}   { return nil }
