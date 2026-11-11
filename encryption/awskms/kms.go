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

// Package awskms implements Iceberg key wrapping with AWS Key Management Service.
// Import this package to register the "aws" KMS provider. The provider uses the
// AWS SDK credential chain and accepts client.region when loaded by the registry.
package awskms

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/apache/iceberg-go/encryption"
	"github.com/apache/iceberg-go/utils"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/kms/types"
)

type kmsAPI interface {
	Encrypt(context.Context, *kms.EncryptInput, ...func(*kms.Options)) (*kms.EncryptOutput, error)
	Decrypt(context.Context, *kms.DecryptInput, ...func(*kms.Options)) (*kms.DecryptOutput, error)
	GenerateDataKey(context.Context, *kms.GenerateDataKeyInput, ...func(*kms.Options)) (*kms.GenerateDataKeyOutput, error)
}

// Client wraps data keys with a symmetric AWS KMS key. Key IDs may be key IDs,
// key ARNs, alias names, or alias ARNs accepted by AWS KMS.
// A Client is safe for concurrent use.
type Client struct {
	client kmsAPI
}

var _ encryption.KeyManagementClient = (*Client)(nil)

// NewClient constructs a KMS client using cfg's credentials, region, and transport.
// Options can configure service-specific settings such as a custom endpoint.
func NewClient(cfg aws.Config, optFns ...func(*kms.Options)) *Client {
	return &Client{client: kms.NewFromConfig(cfg, optFns...)}
}

func init() {
	encryption.MustRegisterKMS("aws", func(ctx context.Context, props map[string]string) (encryption.KeyManagementClient, error) {
		var cfg aws.Config
		if supplied := utils.GetAWSConfig(ctx); supplied != nil {
			cfg = supplied.Copy()
		} else {
			var err error
			cfg, err = config.LoadDefaultConfig(ctx)
			if err != nil {
				return nil, err
			}
		}
		if region := props["client.region"]; region != "" {
			cfg.Region = region
		}

		return NewClient(cfg), nil
	})
}

// WrapKey encrypts plaintextKey with keyID using KMS SYMMETRIC_DEFAULT.
func (c *Client) WrapKey(ctx context.Context, keyID string, plaintextKey []byte) ([]byte, error) {
	if keyID == "" {
		return nil, encryption.ErrUnknownKeyID
	}
	if len(plaintextKey) == 0 || len(plaintextKey) > 4096 {
		return nil, fmt.Errorf("%w: AWS KMS plaintext must contain 1 to 4096 bytes", encryption.ErrInvalidKeyLength)
	}
	output, err := c.client.Encrypt(ctx, &kms.EncryptInput{
		KeyId: aws.String(keyID), Plaintext: plaintextKey,
		EncryptionAlgorithm: types.EncryptionAlgorithmSpecSymmetricDefault,
	})
	if err != nil {
		return nil, fmt.Errorf("AWS KMS wrap key: %w", err)
	}
	if output == nil || len(output.CiphertextBlob) == 0 {
		return nil, errors.New("AWS KMS wrap key: missing ciphertext")
	}

	return slices.Clone(output.CiphertextBlob), nil
}

// UnwrapKey decrypts wrappedKey and requires it to have been encrypted by keyID.
func (c *Client) UnwrapKey(ctx context.Context, keyID string, wrappedKey []byte) ([]byte, error) {
	if keyID == "" {
		return nil, encryption.ErrUnknownKeyID
	}
	if len(wrappedKey) == 0 {
		return nil, encryption.ErrCiphertextTooShort
	}
	output, err := c.client.Decrypt(ctx, &kms.DecryptInput{
		KeyId: aws.String(keyID), CiphertextBlob: wrappedKey,
		EncryptionAlgorithm: types.EncryptionAlgorithmSpecSymmetricDefault,
	})
	if err != nil {
		return nil, fmt.Errorf("AWS KMS unwrap key: %w", err)
	}
	if output == nil || len(output.Plaintext) == 0 {
		return nil, errors.New("AWS KMS unwrap key: missing plaintext")
	}

	return slices.Clone(output.Plaintext), nil
}

// SupportsKeyGeneration reports that KMS can generate and wrap data keys.
func (*Client) SupportsKeyGeneration() bool { return true }

// GenerateKey creates a data key of length bytes. AWS KMS accepts 1 to 1024 bytes.
func (c *Client) GenerateKey(ctx context.Context, keyID string, length int) (plaintext, wrapped []byte, err error) {
	if keyID == "" {
		return nil, nil, encryption.ErrUnknownKeyID
	}
	if length < 1 || length > 1024 {
		return nil, nil, fmt.Errorf("%w: AWS KMS data keys must contain 1 to 1024 bytes", encryption.ErrInvalidKeyLength)
	}
	output, err := c.client.GenerateDataKey(ctx, &kms.GenerateDataKeyInput{
		KeyId: aws.String(keyID), NumberOfBytes: aws.Int32(int32(length)),
	})
	if err != nil {
		return nil, nil, fmt.Errorf("AWS KMS generate data key: %w", err)
	}
	if output == nil || len(output.Plaintext) != length || len(output.CiphertextBlob) == 0 {
		return nil, nil, errors.New("AWS KMS generate data key: incomplete key material")
	}

	return slices.Clone(output.Plaintext), slices.Clone(output.CiphertextBlob), nil
}
