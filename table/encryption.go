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

package table

import "maps"

// EncryptionKey represents an encryption key stored in table metadata (V3+).
type EncryptionKey struct {
	KeyID                string            `json:"key-id"`
	EncryptedKeyMetadata string            `json:"encrypted-key-metadata"`
	EncryptedByID        *string           `json:"encrypted-by-id,omitempty"`
	Properties           map[string]string `json:"properties,omitempty"`
}

func (e EncryptionKey) Equals(other EncryptionKey) bool {
	if e.KeyID != other.KeyID {
		return false
	}

	if e.EncryptedKeyMetadata != other.EncryptedKeyMetadata {
		return false
	}

	switch {
	case e.EncryptedByID == nil && other.EncryptedByID != nil:
		return false
	case e.EncryptedByID != nil && other.EncryptedByID == nil:
		return false
	case e.EncryptedByID != nil && other.EncryptedByID != nil:
		if *e.EncryptedByID != *other.EncryptedByID {
			return false
		}
	}

	return maps.Equal(e.Properties, other.Properties)
}
