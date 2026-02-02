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
	"maps"
	"net/url"
	"slices"
	"sync"
)

type registry map[string]SchemeFactory

func (r registry) getKeys() []string {
	regMutex.Lock()
	defer regMutex.Unlock()

	return slices.Collect(maps.Keys(r))
}

func (r registry) set(scheme string, factory SchemeFactory) {
	regMutex.Lock()
	defer regMutex.Unlock()
	r[scheme] = factory
}

func (r registry) get(scheme string) (SchemeFactory, bool) {
	regMutex.Lock()
	defer regMutex.Unlock()
	factory, ok := r[scheme]

	return factory, ok
}

func (r registry) remove(scheme string) {
	regMutex.Lock()
	defer regMutex.Unlock()
	delete(r, scheme)
}

var (
	regMutex        sync.Mutex
	defaultRegistry = registry{}
)

// SchemeFactory is a function that creates an IO implementation for a given URI and properties.
type SchemeFactory func(ctx context.Context, parsed *url.URL, props map[string]string) (IO, error)

// Register adds a new scheme factory to the registry. If the scheme is already registered, it will be replaced.
func Register(scheme string, factory SchemeFactory) {
	if factory == nil {
		panic("io: Register factory is nil")
	}
	defaultRegistry.set(scheme, factory)
}

// Unregister removes the requested scheme factory from the registry.
func Unregister(scheme string) {
	defaultRegistry.remove(scheme)
}

// GetRegisteredSchemes returns the list of registered scheme names.
func GetRegisteredSchemes() []string {
	return defaultRegistry.getKeys()
}

func init() {
	// Register local filesystem schemes
	localFSFactory := func(ctx context.Context, parsed *url.URL, props map[string]string) (IO, error) {
		return LocalFS{}, nil
	}
	Register("file", localFSFactory)
	Register("", localFSFactory)
}

func inferFileIOFromScheme(ctx context.Context, path string, props map[string]string) (IO, error) {
	parsed, err := url.Parse(path)
	if err != nil {
		return nil, err
	}

	// Look up the scheme in the registry
	factory, ok := defaultRegistry.get(parsed.Scheme)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrIONotFound, parsed.Scheme)
	}

	return factory(ctx, parsed, props)
}
