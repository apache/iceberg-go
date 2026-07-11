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

package encryption

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"sync"
)

// Sentinel errors returned by [KeyManagementClient] implementations. Callers
// should test with [errors.Is] rather than string matching.
var (
	// ErrUnknownKeyID is returned when a KMS client is asked to wrap or unwrap
	// with a key ID it does not know about.
	ErrUnknownKeyID = errors.New("encryption: unknown key ID")

	// ErrInvalidKeyLength is returned when a key or requested key length is not
	// valid for the underlying cipher (for example, a KEK that is not 16, 24,
	// or 32 bytes for AES, or a DEK request of length <= 0).
	ErrInvalidKeyLength = errors.New("encryption: invalid key length")

	// ErrCiphertextTooShort is returned when a wrapped key or encrypted payload
	// is smaller than the minimum required (e.g. the AES-GCM nonce prefix).
	ErrCiphertextTooShort = errors.New("encryption: ciphertext too short")

	// ErrAuthenticationFailed is returned when an authenticated decryption
	// primitive rejects its input (tampering, wrong key, or corruption).
	ErrAuthenticationFailed = errors.New("encryption: authentication failed")
)

// KeyManagementClient is the interface for a Key Management Service (KMS) that
// wraps and unwraps data encryption keys (DEKs) using key encryption keys
// (KEKs) managed externally by the KMS.
//
// In the standard envelope-encryption model:
//   - A KEK is identified by a key ID (the table property encryption.key-id).
//   - A fresh DEK is generated per-file (or per-rotation interval).
//   - The DEK is wrapped by the KMS and stored in the file's key_metadata field.
//   - On read, the wrapped DEK is unwrapped by the KMS to decrypt the file.
//
// Implementations are responsible for authentication, authorization, and
// network communication with the KMS backend.
type KeyManagementClient interface {
	// WrapKey encrypts (wraps) plaintextKey using the KEK identified by keyID.
	// The returned bytes are an opaque, KMS-specific wrapped key blob.
	WrapKey(ctx context.Context, keyID string, plaintextKey []byte) ([]byte, error)

	// UnwrapKey decrypts (unwraps) wrappedKey using the KEK identified by keyID.
	// It returns the original plaintext DEK.
	UnwrapKey(ctx context.Context, keyID string, wrappedKey []byte) ([]byte, error)

	// SupportsKeyGeneration reports whether this client can generate new DEKs
	// server-side. When false, callers should generate DEKs locally and use
	// [WrapKey] to protect them.
	SupportsKeyGeneration() bool

	// GenerateKey generates a new DEK of the given byte length, returning both
	// the plaintext DEK and its KMS-wrapped form. This is only valid when
	// [SupportsKeyGeneration] returns true.
	GenerateKey(ctx context.Context, keyID string, length int) (plaintext, wrapped []byte, err error)
}

// InMemoryKeyManagementClient is a [KeyManagementClient] backed by an
// in-memory map of master (key-encryption) keys. Key wrapping and unwrapping
// use AES-GCM.
//
// # Warning
//
// InMemoryKeyManagementClient is intended for testing only. All keys are held
// in plaintext in process memory with no persistence, access control, or audit
// logging. Do not use it in production.
type InMemoryKeyManagementClient struct {
	mu   sync.RWMutex
	keys map[string][]byte // keyID -> raw KEK bytes
}

var _ KeyManagementClient = (*InMemoryKeyManagementClient)(nil)

// NewInMemoryKeyManagementClient creates a new [InMemoryKeyManagementClient]
// with no pre-loaded keys. Register KEKs with [AddKey] before use.
func NewInMemoryKeyManagementClient() *InMemoryKeyManagementClient {
	return &InMemoryKeyManagementClient{keys: make(map[string][]byte)}
}

// AddKey registers a master key (KEK) under keyID. masterKey must be 16, 24,
// or 32 bytes for AES-128, AES-192, or AES-256 respectively. If keyID is
// already registered, its KEK is silently replaced.
func (c *InMemoryKeyManagementClient) AddKey(keyID string, masterKey []byte) error {
	switch len(masterKey) {
	case 16, 24, 32: // valid AES-GCM key lengths
	default:
		return fmt.Errorf("%w: master key for %q must be 16, 24, or 32 bytes; got %d", ErrInvalidKeyLength, keyID, len(masterKey))
	}
	keyCopy := make([]byte, len(masterKey))
	copy(keyCopy, masterKey)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.keys[keyID] = keyCopy

	return nil
}

// WrapKey encrypts plaintextKey with the KEK identified by keyID using
// AES-GCM. The returned ciphertext layout is:
//
//	12-byte random nonce || AES-GCM ciphertext || 16-byte GCM authentication tag
func (c *InMemoryKeyManagementClient) WrapKey(_ context.Context, keyID string, plaintextKey []byte) ([]byte, error) {
	kek, err := c.getKey(keyID)
	if err != nil {
		return nil, err
	}

	return aesgcmSeal(kek, plaintextKey)
}

// UnwrapKey decrypts wrappedKey using the KEK identified by keyID. The
// wrappedKey must have been produced by [WrapKey] or [GenerateKey].
func (c *InMemoryKeyManagementClient) UnwrapKey(_ context.Context, keyID string, wrappedKey []byte) ([]byte, error) {
	kek, err := c.getKey(keyID)
	if err != nil {
		return nil, err
	}

	return aesgcmOpen(kek, wrappedKey)
}

// SupportsKeyGeneration returns true; the in-memory client generates DEKs
// locally using crypto/rand.
func (c *InMemoryKeyManagementClient) SupportsKeyGeneration() bool { return true }

// GenerateKey generates a cryptographically random DEK of the given byte
// length, wraps it with the KEK identified by keyID, and returns both the
// plaintext DEK and the wrapped form.
func (c *InMemoryKeyManagementClient) GenerateKey(ctx context.Context, keyID string, length int) (plaintext, wrapped []byte, err error) {
	if length <= 0 {
		return nil, nil, fmt.Errorf("%w: key length must be positive, got %d", ErrInvalidKeyLength, length)
	}
	plaintext = make([]byte, length)
	if _, err = io.ReadFull(rand.Reader, plaintext); err != nil {
		return nil, nil, fmt.Errorf("encryption: failed to generate random key: %w", err)
	}
	wrapped, err = c.WrapKey(ctx, keyID, plaintext)
	if err != nil {
		return nil, nil, err
	}

	return plaintext, wrapped, nil
}

// getKey returns the KEK for keyID, or an error wrapping [ErrUnknownKeyID] if
// not found. The returned slice is the live map entry and must not be mutated
// by callers; it is safe to read concurrently because [AddKey] copies its
// input on ingress and never mutates a previously stored KEK in place.
func (c *InMemoryKeyManagementClient) getKey(keyID string) ([]byte, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	kek, ok := c.keys[keyID]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnknownKeyID, keyID)
	}

	return kek, nil
}

// aesgcmSeal encrypts plaintext with key using AES-GCM with a random 12-byte
// nonce. The nonce is prepended to the returned ciphertext.
func aesgcmSeal(key, plaintext []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("encryption: failed to create AES cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("encryption: failed to create GCM: %w", err)
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err = io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("encryption: failed to generate nonce: %w", err)
	}
	// Seal appends ciphertext+tag to nonce, producing: nonce || ciphertext || tag.
	return gcm.Seal(nonce, nonce, plaintext, nil), nil
}

// aesgcmOpen decrypts AES-GCM ciphertext produced by [aesgcmSeal].
func aesgcmOpen(key, ciphertext []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("encryption: failed to create AES cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("encryption: failed to create GCM: %w", err)
	}
	nonceSize := gcm.NonceSize()
	if len(ciphertext) < nonceSize {
		return nil, fmt.Errorf("%w: need at least %d bytes for AES-GCM nonce, got %d", ErrCiphertextTooShort, nonceSize, len(ciphertext))
	}
	plaintext, err := gcm.Open(nil, ciphertext[:nonceSize], ciphertext[nonceSize:], nil)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrAuthenticationFailed, err)
	}

	return plaintext, nil
}
