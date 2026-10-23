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
	"sync"

	"github.com/apache/iceberg-go/internal/schemes"
)

// SchemeFactory creates an IO implementation for a URI and its properties.
type SchemeFactory func(ctx context.Context, parsed *url.URL, props map[string]string) (IO, error)

var (
	// ErrSchemeAlreadyRegistered indicates that a registry already has a factory for a scheme.
	ErrSchemeAlreadyRegistered = errors.New("io scheme already registered")
	// ErrInvalidSchemeFactory indicates that an attempt was made to register a nil factory.
	ErrInvalidSchemeFactory = errors.New("io scheme factory is nil")
)

// Registry manages FileIO factories for URI schemes. A registry may be used by
// multiple goroutines. Factories run without the registry lock held, so they
// may register other schemes or load additional FileIO instances.
type Registry struct {
	mu        sync.RWMutex
	factories map[string]SchemeFactory
}

func builtInRegistry() *Registry {
	localFSFactory := func(context.Context, *url.URL, map[string]string) (IO, error) {
		return LocalFS{}, nil
	}

	return &Registry{factories: map[string]SchemeFactory{
		"":     localFSFactory,
		"file": localFSFactory,
	}}
}

// defaultRegistry is populated by side-effect imports of the cloud providers.
// Independent registries clone these registrations rather than sharing the map.
var defaultRegistry = builtInRegistry()

// NewRegistry creates an independent snapshot of the globally registered
// schemes, including cloud providers already registered through side-effect
// imports. Future registrations in either registry do not affect the other.
func NewRegistry() *Registry {
	return defaultRegistry.Clone()
}

// Clone returns an independent copy of a registry's factory registrations.
// Factories themselves are shared, but the registration maps and locks are not.
func (r *Registry) Clone() *Registry {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return &Registry{factories: maps.Clone(r.factories)}
}

// Register adds a scheme factory. Unlike MustRegister, it returns errors for
// invalid or duplicate registrations rather than panicking.
func (r *Registry) Register(scheme string, factory SchemeFactory) error {
	if factory == nil {
		return ErrInvalidSchemeFactory
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.factories[scheme]; exists {
		return fmt.Errorf("%w: %q", ErrSchemeAlreadyRegistered, scheme)
	}

	if r.factories == nil {
		r.factories = make(map[string]SchemeFactory)
	}
	r.factories[scheme] = factory

	return nil
}

// MustRegister registers a scheme or panics. It is intended for provider init
// functions where failing to register a required backend is unrecoverable.
func (r *Registry) MustRegister(scheme string, factory SchemeFactory) {
	if err := r.Register(scheme, factory); err != nil {
		panic(err)
	}
}

// Unregister removes a scheme from this registry only.
func (r *Registry) Unregister(scheme string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.factories, scheme)
}

// RegisteredSchemes returns the scheme names in sorted order.
func (r *Registry) RegisteredSchemes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return slices.Sorted(maps.Keys(r.factories))
}

// Register adds a scheme to the global registry and returns an error for nil
// factories or duplicate registrations. Use MustRegister from init functions.
func Register(scheme string, factory SchemeFactory) error {
	return defaultRegistry.Register(scheme, factory)
}

// MustRegister registers a global scheme or panics. Use this from backend init
// functions, and use Register when a caller can handle registration errors.
func MustRegister(scheme string, factory SchemeFactory) {
	defaultRegistry.MustRegister(scheme, factory)
}

// Unregister removes a scheme from the global registry.
func Unregister(scheme string) {
	defaultRegistry.Unregister(scheme)
}

// GetRegisteredSchemes returns the names registered globally in sorted order.
func GetRegisteredSchemes() []string {
	return defaultRegistry.RegisteredSchemes()
}

func schemeImportHint(backend string) string {
	return "hint: import the matching IO module for side-effect registration: " +
		`_ "github.com/apache/iceberg-go/io/gocloud/` + backend + `"`
}

func schemeRegistrationHint(scheme string) string {
	backend := schemes.BackendFor(scheme)
	if backend == "" {
		return ""
	}

	return schemeImportHint(backend)
}

func (r *Registry) inferFileIOFromScheme(ctx context.Context, path string, props map[string]string) (IO, error) {
	parsed, err := url.Parse(path)
	if err != nil {
		return nil, err
	}

	r.mu.RLock()
	factory, ok := r.factories[parsed.Scheme]
	r.mu.RUnlock()

	if !ok {
		hint := schemeRegistrationHint(parsed.Scheme)
		if hint != "" {
			return nil, fmt.Errorf("%w for path %q (scheme: %s)\n%s", ErrIOSchemeNotFound, path, parsed.Scheme, hint)
		}

		return nil, fmt.Errorf("%w for path %q (scheme: %s)", ErrIOSchemeNotFound, path, parsed.Scheme)
	}

	return factory(ctx, parsed, props)
}
