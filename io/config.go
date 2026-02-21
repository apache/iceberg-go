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

// Constants for S3 configuration options
const (
	S3Region                 = "s3.region"
	S3SessionToken           = "s3.session-token"
	S3SecretAccessKey        = "s3.secret-access-key"
	S3AccessKeyID            = "s3.access-key-id"
	S3EndpointURL            = "s3.endpoint"
	S3ProxyURI               = "s3.proxy-uri"
	S3ConnectTimeout         = "s3.connect-timeout"
	S3SignerURI              = "s3.signer.uri"
	S3RemoteSigningEnabled   = "s3.remote-signing-enabled"
	S3ForceVirtualAddressing = "s3.force-virtual-addressing"
)

// Constants for GCS configuration options
const (
	GCSEndpoint   = "gcs.endpoint"
	GCSKeyPath    = "gcs.keypath"
	GCSJSONKey    = "gcs.jsonkey"
	GCSCredType   = "gcs.credtype"
	GCSUseJSONAPI = "gcs.usejsonapi" // set to anything to enable
)

// Constants for Azure configuration options
const (
	ADLSSasTokenPrefix         = "adls.sas-token."
	ADLSConnectionStringPrefix = "adls.connection-string."
	ADLSSharedKeyAccountName   = "adls.auth.shared-key.account.name"
	ADLSSharedKeyAccountKey    = "adls.auth.shared-key.account.key"
	ADLSEndpoint               = "adls.endpoint"
	ADLSProtocol               = "adls.protocol"

	// Not in use yet
	// ADLSReadBlockSize          = "adls.read.block-size-bytes"
	// ADLSWriteBlockSize         = "adls.write.block-size-bytes"
)
