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

// Package encryption defines interfaces and utilities for Iceberg table
// encryption. It provides:
//
//   - [EncryptionManager] — the central coordination interface for per-file
//     envelope encryption and decryption.
//   - [KeyManagementClient] — the interface for KMS backends (wrap/unwrap DEKs).
//   - [EncryptedInputFile] / [EncryptedOutputFile] — file wrappers that carry
//     per-file key metadata alongside the data stream.
//   - [NativeEncryptionInputFile] / [NativeEncryptionOutputFile] — extensions
//     for format-native encryption (e.g., Parquet column-level encryption).
//   - [PlaintextEncryptionManager] — a no-op implementation suitable for
//     unencrypted tables.
package encryption

import (
	"context"

	icebergio "github.com/apache/iceberg-go/io"
)

// EncryptionKeyMetadata is the opaque per-file key metadata blob embedded in
// manifest entries (DataFile.KeyMetadata, ManifestFile.KeyMetadata) and
// statistics files (StatisticsFile.KeyMetadata). The encoding is
// [EncryptionManager]-defined and typically envelopes the wrapped DEK together
// with any associated authenticated additional data (AAD).
type EncryptionKeyMetadata []byte

// EncryptedInputFile is a readable file that carries the per-file encryption
// key metadata required by an [EncryptionManager] to derive the decryption key.
type EncryptedInputFile interface {
	icebergio.File
	// KeyMetadata returns the opaque per-file encryption key metadata.
	KeyMetadata() EncryptionKeyMetadata
}

// EncryptedOutputFile is a writable file whose plaintext content is encrypted
// by the [EncryptionManager]. After the file has been closed, [KeyMetadata]
// returns the finalized per-file key metadata to embed in the corresponding
// manifest entry.
type EncryptedOutputFile interface {
	icebergio.FileWriter
	// KeyMetadata returns the finalized per-file encryption key metadata.
	// The returned value is only guaranteed to be populated after Close has
	// been called.
	KeyMetadata() EncryptionKeyMetadata
}

// NativeEncryptionInputFile extends [EncryptedInputFile] with access to
// format-native decryption properties (e.g., Parquet FileDecryptionProperties).
// An [EncryptionManager] may return a NativeEncryptionInputFile from
// [EncryptionManager.NewDecryptedInputFile] when native-format encryption is
// in use; callers should type-assert to retrieve the concrete properties.
type NativeEncryptionInputFile interface {
	EncryptedInputFile
	// NativeDecryptionProperties returns format-native decryption properties.
	// The concrete type is format-specific; callers should type-assert as
	// needed (e.g., *parquet.FileDecryptionProperties for Parquet files).
	NativeDecryptionProperties(ctx context.Context) (any, error)
}

// NativeEncryptionOutputFile extends [EncryptedOutputFile] with access to
// format-native encryption properties (e.g., Parquet FileEncryptionProperties).
// An [EncryptionManager] may return a NativeEncryptionOutputFile from
// [EncryptionManager.NewEncryptedOutputFile] when native-format encryption is
// in use; callers should type-assert to retrieve the concrete properties.
type NativeEncryptionOutputFile interface {
	EncryptedOutputFile
	// NativeEncryptionProperties returns format-native encryption properties.
	// The concrete type is format-specific; callers should type-assert as
	// needed (e.g., *parquet.FileEncryptionProperties for Parquet files).
	NativeEncryptionProperties(ctx context.Context) (any, error)
}

// EncryptionManager handles envelope key derivation, file-level encryption,
// and decryption. It is the central coordination point between the
// [KeyManagementClient] (key encryption key operations) and the per-file data
// encryption keys (DEKs).
//
// A [PlaintextEncryptionManager] is provided for tables that do not use
// encryption. Vendors and users can supply custom implementations to integrate
// with their KMS solutions.
type EncryptionManager interface {
	// NewEncryptedOutputFile creates a new encrypted output file.
	// keyID is the table property encryption.key-id that identifies the key
	// encryption key (KEK) to use when wrapping the generated DEK.
	NewEncryptedOutputFile(ctx context.Context, writer icebergio.FileWriter, keyID string) (EncryptedOutputFile, error)

	// NewDecryptedInputFile wraps an existing file for transparent decryption.
	// keyMetadata is the per-file opaque blob stored in the manifest entry
	// (DataFile.KeyMetadata or ManifestFile.KeyMetadata).
	NewDecryptedInputFile(ctx context.Context, file icebergio.File, keyMetadata EncryptionKeyMetadata) (EncryptedInputFile, error)
}

// PlaintextEncryptionManager is a no-op [EncryptionManager] that passes all
// data through without any encryption or decryption. It is the default manager
// used when no KMS is configured or when a table carries no encryption-keys
// metadata.
type PlaintextEncryptionManager struct{}

var _ EncryptionManager = PlaintextEncryptionManager{}

// NewEncryptedOutputFile returns a pass-through [EncryptedOutputFile] that
// writes data unmodified and returns nil [EncryptionKeyMetadata]. Any
// non-empty keyID is silently ignored so that callers do not need to detect
// whether encryption is configured before invoking this method.
func (PlaintextEncryptionManager) NewEncryptedOutputFile(_ context.Context, writer icebergio.FileWriter, _ string) (EncryptedOutputFile, error) {
	return &plaintextOutputFile{FileWriter: writer}, nil
}

// NewDecryptedInputFile returns a pass-through [EncryptedInputFile] that reads
// data unmodified and surfaces the provided keyMetadata unchanged. Any
// non-nil keyMetadata is preserved but not interpreted.
func (PlaintextEncryptionManager) NewDecryptedInputFile(_ context.Context, file icebergio.File, keyMetadata EncryptionKeyMetadata) (EncryptedInputFile, error) {
	return &plaintextInputFile{File: file, keyMetadata: keyMetadata}, nil
}

// plaintextInputFile is an [EncryptedInputFile] that performs no decryption.
type plaintextInputFile struct {
	icebergio.File
	keyMetadata EncryptionKeyMetadata
}

var _ EncryptedInputFile = (*plaintextInputFile)(nil)

func (f *plaintextInputFile) KeyMetadata() EncryptionKeyMetadata { return f.keyMetadata }

// plaintextOutputFile is an [EncryptedOutputFile] that performs no encryption.
type plaintextOutputFile struct {
	icebergio.FileWriter
}

var _ EncryptedOutputFile = (*plaintextOutputFile)(nil)

func (f *plaintextOutputFile) KeyMetadata() EncryptionKeyMetadata { return nil }
