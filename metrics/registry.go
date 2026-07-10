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

package metrics

import (
	"fmt"
	"sync"
)

// ReporterImplKey is the catalog/table property that selects a registered
// reporter by name (e.g. "logging"). It is the Go analogue of Java's
// metrics-reporter-impl; Go uses a name→factory registry rather than
// reflection over a class name.
const ReporterImplKey = "metrics-reporter-impl"

// Built-in reporter names usable as the value of [ReporterImplKey].
const (
	ReporterNameNop     = "nop"
	ReporterNameLogging = "logging"
)

// Factory builds a Reporter from configuration properties. The same property
// map that selected the reporter is passed in, so a factory may read its own
// configuration keys.
type Factory func(props map[string]string) (Reporter, error)

var (
	registryMu sync.RWMutex
	registry   = map[string]Factory{}
)

func init() {
	Register(ReporterNameNop, func(map[string]string) (Reporter, error) { return NopReporter{}, nil })
	Register(ReporterNameLogging, func(map[string]string) (Reporter, error) {
		return NewLoggingReporter(nil), nil
	})
}

// Register makes a reporter factory available under name. It panics if name is
// empty or already registered, mirroring database/sql.Register — registration
// is expected to happen once, from package init.
func Register(name string, factory Factory) {
	if name == "" {
		panic("metrics: Register called with empty name")
	}
	if factory == nil {
		panic("metrics: Register called with nil factory")
	}
	registryMu.Lock()
	defer registryMu.Unlock()
	if _, dup := registry[name]; dup {
		panic("metrics: Register called twice for " + name)
	}
	registry[name] = factory
}

// Deregister removes a previously registered reporter factory. It is a no-op if
// name is not registered. This exists primarily so tests can register a factory
// and undo it via t.Cleanup, keeping the process-global registry re-runnable
// under go test -count=N.
func Deregister(name string) {
	registryMu.Lock()
	defer registryMu.Unlock()
	delete(registry, name)
}

// FromProperties builds the reporter named by props[ReporterImplKey]. An absent
// or empty name yields [NopReporter] (reporting is opt-in), and an unrecognized name is
// an error so misconfiguration surfaces rather than silently disabling metrics.
func FromProperties(props map[string]string) (Reporter, error) {
	name := props[ReporterImplKey]
	if name == "" {
		return NopReporter{}, nil
	}

	registryMu.RLock()
	factory, ok := registry[name]
	registryMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("metrics: unknown reporter %q (set via %q)", name, ReporterImplKey)
	}

	return factory(props)
}
