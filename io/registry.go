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
	"maps"
	"net/url"
	"slices"
	"strings"
	"sync"
)

type registry map[string]Registrar

func (r registry) getKeys() []string {
	regMutex.Lock()
	defer regMutex.Unlock()

	return slices.Collect(maps.Keys(r))
}

func (r registry) set(ioType string, reg Registrar) {
	regMutex.Lock()
	defer regMutex.Unlock()
	r[ioType] = reg
}

func (r registry) get(ioType string) (Registrar, bool) {
	regMutex.Lock()
	defer regMutex.Unlock()
	reg, ok := r[ioType]

	return reg, ok
}

func (r registry) remove(ioType string) {
	regMutex.Lock()
	defer regMutex.Unlock()
	delete(r, ioType)
}

var (
	regMutex        sync.Mutex
	defaultRegistry = registry{}
	ErrIONotFound   = errors.New("IO type not registered")
)

// Registrar is a factory for creating IO instances, used for registering to use
// with Load.
type Registrar interface {
	GetIO(ctx context.Context, props map[string]string) (IO, error)
}

type RegistrarFunc func(context.Context, map[string]string) (IO, error)

func (f RegistrarFunc) GetIO(ctx context.Context, props map[string]string) (IO, error) {
	return f(ctx, props)
}

// Register adds the new IO type to the registry. If the IO type is already registered, it will be replaced.
func Register(ioType string, reg Registrar) {
	if reg == nil {
		panic("io: RegisterIO factory is nil")
	}
	defaultRegistry.set(ioType, reg)
}

// Unregister removes the requested IO factory from the registry.
func Unregister(ioType string) {
	defaultRegistry.remove(ioType)
}

// GetRegisteredIOs returns the list of registered IO names that can
// be looked up via Load.
func GetRegisteredIOs() []string {
	return defaultRegistry.getKeys()
}

// Load allows loading a specific IO implementation by scheme and properties.
//
// This is utilized alongside Register/Unregister to not only allow
// easier IO loading but also to allow for custom IO implementations to
// be registered and loaded external to this module.
//
// The scheme parameter is extracted from the URI to determine which IO
// implementation to use. For example, "s3://bucket/path" would use the
// "s3" IO implementation.
//
// Currently, the following IO types are supported by default:
//
//   - "file" or "" for local filesystem
//   - "s3", "s3a", "s3n" for Amazon S3
//   - "gs" for Google Cloud Storage
//   - "abfs", "abfss", "wasb", "wasbs" for Azure Blob Storage
//   - "mem" for in-memory storage
func Load(ctx context.Context, props map[string]string, location string) (IO, error) {
	if location == "" {
		location = props["warehouse"]
	}

	scheme := ""
	if strings.Contains(location, "://") {
		parsed, err := url.Parse(location)
		if err != nil {
			return nil, fmt.Errorf("failed to parse IO location: %w", err)
		}
		scheme = parsed.Scheme
	}

	// Default to local filesystem if no scheme
	if scheme == "" {
		scheme = "file"
	}

	reg, ok := defaultRegistry.get(scheme)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrIONotFound, location)
	}

	return reg.GetIO(ctx, props)
}
