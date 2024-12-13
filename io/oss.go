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
	"context"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/credentials"
)

// Constants for OSS configuration options
const (
	OSSAccessKey        = "client.oss-access-key"
	OSSSecretKey        = "client.oss-secret-key"
	OSSEndpoint         = "client.oss-endpoint"
	OSSSignatureVersion = "client.oss-signature-version"
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

	provider := credentials.NewStaticCredentialsProvider(accessKey, secretKey)

	cfg := oss.LoadDefaultConfig().
		WithRetryMaxAttempts(3).
		WithCredentialsProvider(provider).
		WithEndpoint(endpoint).
		WithSignatureVersion(parseSignatureVersion(props[OSSSignatureVersion])).
		WithUsePathStyle(true).
		WithConnectTimeout(10 * time.Second).
		WithReadWriteTimeout(time.Minute)

	client := oss.NewClient(cfg)

	ossFS := &ossFS{
		client: client,
		bucket: parsed.Host,
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

// parseSignatureVersion converts string version to oss.SignatureVersionType
// "0" -> 0 (v1)
// "1" -> 1 (v4)
// defaults to 1 (v4) for unknown values
func parseSignatureVersion(version string) oss.SignatureVersionType {
	switch version {
	case "0": // v1
		return 0
	case "1": // v4
		return 1
	default:
		return 1
	}
}

type ossFS struct {
	client *oss.Client
	bucket string
}

// Open implements fs.FS
func (o *ossFS) Open(name string) (fs.File, error) {
	if !fs.ValidPath(name) {
		return nil, &os.PathError{Op: "open", Path: name, Err: os.ErrInvalid}
	}
	if name == "." {
		return &ossFile{
			name: name,
		}, nil
	}
	name = strings.TrimPrefix(name, "/")

	file, err := o.client.OpenFile(context.Background(), o.bucket, name)
	if err != nil {
		return nil, err
	}

	return &ossFile{
		file: file,
		name: name,
	}, nil
}

type ossFile struct {
	mutex sync.Mutex
	file  *oss.ReadOnlyFile
	name  string
}

// Read implements io.Reader
func (f *ossFile) Read(p []byte) (int, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	return f.file.Read(p)
}

// Seek implements io.Seeker
func (f *ossFile) Seek(offset int64, whence int) (int64, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	return f.file.Seek(offset, whence)
}

// Close implements io.Closer
func (f *ossFile) Close() error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	return f.file.Close()
}

// ReadAt implements io.ReaderAt
func (f *ossFile) ReadAt(p []byte, off int64) (n int, err error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	if _, err := f.file.Seek(off, io.SeekStart); err != nil {
		return 0, err
	}
	return f.file.Read(p)
}

func (f *ossFile) Stat() (fs.FileInfo, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	return f.file.Stat()
}
