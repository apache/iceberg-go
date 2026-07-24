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

package encryption_test

import (
	"testing"

	"github.com/apache/iceberg-go/encryption"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadKeyManagementClient_Memory(t *testing.T) {
	kms, err := encryption.LoadKeyManagementClient(map[string]string{
		encryption.KMSTypeKey: "memory",
	})
	require.NoError(t, err)
	assert.IsType(t, &encryption.InMemoryKeyManagementClient{}, kms)
}

func TestLoadKeyManagementClient_NoPropertiesSet(t *testing.T) {
	_, err := encryption.LoadKeyManagementClient(map[string]string{})
	require.ErrorIs(t, err, encryption.ErrKMSTypeNotFound)
}

func TestLoadKeyManagementClient_UnregisteredName(t *testing.T) {
	_, err := encryption.LoadKeyManagementClient(map[string]string{
		encryption.KMSTypeKey: "does-not-exist",
	})
	require.ErrorIs(t, err, encryption.ErrKMSTypeNotFound)
}

func TestRegisterKMS_DuplicatePanics(t *testing.T) {
	const name = "test-duplicate-kms"
	factory := func(_ map[string]string) (encryption.KeyManagementClient, error) {
		return encryption.NewInMemoryKeyManagementClient(), nil
	}

	encryption.RegisterKMS(name, factory)
	defer encryption.UnregisterKMS(name)

	assert.Panics(t, func() {
		encryption.RegisterKMS(name, factory)
	})
}

func TestRegisterKMS_NilFactoryPanics(t *testing.T) {
	assert.Panics(t, func() {
		encryption.RegisterKMS("test-nil-factory-kms", nil)
	})
}

func TestGetRegisteredKMSNames_IncludesMemory(t *testing.T) {
	names := encryption.GetRegisteredKMSNames()
	assert.Contains(t, names, "memory")
}

func TestLoadKeyManagementClient_FactoryError(t *testing.T) {
	const name = "test-error-kms"
	wantErr := assert.AnError
	encryption.RegisterKMS(name, func(_ map[string]string) (encryption.KeyManagementClient, error) {
		return nil, wantErr
	})
	defer encryption.UnregisterKMS(name)

	_, err := encryption.LoadKeyManagementClient(map[string]string{
		encryption.KMSTypeKey: name,
	})
	require.ErrorIs(t, err, wantErr)
}

func TestLoadKeyManagementClient_PassesPropsToFactory(t *testing.T) {
	const name = "test-props-kms"
	wantProps := map[string]string{
		encryption.KMSTypeKey:   name,
		"encryption.kms.region": "us-east-1",
	}

	var gotProps map[string]string
	encryption.RegisterKMS(name, func(props map[string]string) (encryption.KeyManagementClient, error) {
		gotProps = props

		return encryption.NewInMemoryKeyManagementClient(), nil
	})
	defer encryption.UnregisterKMS(name)

	_, err := encryption.LoadKeyManagementClient(wantProps)
	require.NoError(t, err)
	assert.Equal(t, wantProps, gotProps, "the full props map given to LoadKeyManagementClient must reach the factory unmodified")
}
