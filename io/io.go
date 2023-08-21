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
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/wolfeidau/s3iofs"
)

const (
	S3RegionName      = "s3.region-name"
	S3SessionToken    = "s3.session-token"
	S3SecretAccessKey = "s3.secret-access-key"
	S3AccessKeyID     = "s3.access-key-id"
	S3EndpointURL     = "s3.region"
	S3ProxyURI        = "s3.proxy-uri"
)

type IO interface {
	Open(name string) (File, error)
	Remove(name string) error
}

type ReadFileIO interface {
	IO

	ReadFile(name string) ([]byte, error)
}

type File interface {
	fs.File
	io.ReadSeekCloser
	io.ReaderAt
}

func FS(fsys fs.FS) IO {
	if _, ok := fsys.(fs.ReadFileFS); ok {
		return readFileFS{ioFS{fsys, nil}}
	}
	return ioFS{fsys, nil}
}

func FSPreProcName(fsys fs.FS, fn func(string) string) IO {
	if _, ok := fsys.(fs.ReadFileFS); ok {
		return readFileFS{ioFS{fsys, fn}}
	}
	return ioFS{fsys, fn}
}

type readFileFS struct {
	ioFS
}

func (r readFileFS) ReadFile(name string) ([]byte, error) {
	if r.preProcessName != nil {
		name = r.preProcessName(name)
	}

	rfs, ok := r.fsys.(fs.ReadFileFS)
	if !ok {
		return nil, errMissingReadFile
	}
	return rfs.ReadFile(name)
}

type ioFS struct {
	fsys fs.FS

	preProcessName func(string) string
}

func (f ioFS) Open(name string) (File, error) {
	if f.preProcessName != nil {
		name = f.preProcessName(name)
	}

	if name == "/" {
		name = "."
	} else {
		name = strings.TrimPrefix(name, "/")
	}
	file, err := f.fsys.Open(name)
	if err != nil {
		return nil, err
	}

	return ioFile{file}, nil
}

func (f ioFS) Remove(name string) error {
	r, ok := f.fsys.(interface{ Remove(name string) error })
	if !ok {
		return errMissingRemove
	}
	return r.Remove(name)
}

var (
	errMissingReadDir  = errors.New("fs.File directory missing ReadDir method")
	errMissingSeek     = errors.New("fs.File missing Seek method")
	errMissingReadAt   = errors.New("fs.File missing ReadAt")
	errMissingRemove   = errors.New("fs.FS missing Remove method")
	errMissingReadFile = errors.New("fs.FS missing ReadFile method")
)

type ioFile struct {
	file fs.File
}

func (f ioFile) Close() error               { return f.file.Close() }
func (f ioFile) Read(b []byte) (int, error) { return f.file.Read(b) }
func (f ioFile) Stat() (fs.FileInfo, error) { return f.file.Stat() }
func (f ioFile) Seek(offset int64, whence int) (int64, error) {
	s, ok := f.file.(io.Seeker)
	if !ok {
		return 0, errMissingSeek
	}
	return s.Seek(offset, whence)
}

func (f ioFile) ReadAt(p []byte, off int64) (n int, err error) {
	r, ok := f.file.(io.ReaderAt)
	if !ok {
		return 0, errMissingReadAt
	}
	return r.ReadAt(p, off)
}

func (f ioFile) ReadDir(count int) ([]fs.DirEntry, error) {
	d, ok := f.file.(fs.ReadDirFile)
	if !ok {
		return nil, errMissingReadDir
	}

	return d.ReadDir(count)
}

func inferFileIOFromSchema(path string, props map[string]string) (IO, error) {
	parsed, err := url.Parse(path)
	if err != nil {
		return nil, err
	}

	switch parsed.Scheme {
	case "s3", "s3a", "s3n":
		opts := []func(*config.LoadOptions) error{}
		endpoint, ok := props[S3EndpointURL]
		if !ok {
			endpoint = os.Getenv("AWS_S3_ENDPOINT")
		}

		if endpoint != "" {
			opts = append(opts, config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				if service != s3.ServiceID {
					// fallback to default resolution for the service
					return aws.Endpoint{}, &aws.EndpointNotFoundError{}
				}

				return aws.Endpoint{
					URL:               endpoint,
					SigningRegion:     region,
					HostnameImmutable: true,
				}, nil
			})))
		}

		if defaultRegion, ok := props[S3RegionName]; ok {
			opts = append(opts, config.WithDefaultRegion(defaultRegion))
		}

		accessKey, secretKey := props[S3AccessKeyID], props[S3SecretAccessKey]
		token := props[S3SessionToken]
		if accessKey != "" || secretKey != "" || token != "" {
			opts = append(opts, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
				props[S3AccessKeyID], props[S3SecretAccessKey], props[S3SessionToken])))
		}

		if proxy, ok := props[S3ProxyURI]; ok {
			proxyURL, err := url.Parse(proxy)
			if err != nil {
				return nil, fmt.Errorf("invalid s3 proxy url '%s'", proxy)
			}

			opts = append(opts, config.WithHTTPClient(awshttp.NewBuildableClient().WithTransportOptions(
				func(t *http.Transport) {
					t.Proxy = http.ProxyURL(proxyURL)
				},
			)))
		}

		awscfg, err := config.LoadDefaultConfig(context.Background(), opts...)
		if err != nil {
			return nil, err
		}

		preprocess := func(n string) string {
			_, after, found := strings.Cut(n, "://")
			if found {
				n = after
			}

			return strings.TrimPrefix(n, parsed.Host)
		}

		s3fs := s3iofs.New(parsed.Host, awscfg)
		return FSPreProcName(s3fs, preprocess), nil
	case "file":
		return LocalFS{}, nil
	default:
		return nil, nil
	}
}

func LoadFS(props map[string]string, location string) (IO, error) {
	if location != "" {
		iofs, err := inferFileIOFromSchema(location, props)
		if err != nil {
			return nil, err
		}
		if iofs != nil {
			return iofs, nil
		}
	}

	if warehouse, ok := props["warehouse"]; ok {
		iofs, err := inferFileIOFromSchema(warehouse, props)
		if err != nil {
			return nil, err
		}
		if iofs != nil {
			return iofs, nil
		}
	}

	return LocalFS{}, nil
}
