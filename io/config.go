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
	S3SignerUri              = "s3.signer.uri"
	S3ForceVirtualAddressing = "s3.force-virtual-addressing"
)

// Constants for GCS configuration options
const (
	GCSEndpoint   = "gcs.endpoint"
	GCSKeyPath    = "gcs.keypath"
	GCSJSONKey    = "gcs.jsonkey"
	GCSCredType   = "gcs.credtype"
	GCSUseJsonAPI = "gcs.usejsonapi" // set to anything to enable
)

// Constants for Azure configuration options
const (
	AdlsSasTokenPrefix         = "adls.sas-token."
	AdlsConnectionStringPrefix = "adls.connection-string."
	AdlsSharedKeyAccountName   = "adls.auth.shared-key.account.name"
	AdlsSharedKeyAccountKey    = "adls.auth.shared-key.account.key"
	AdlsEndpoint               = "adls.endpoint"
	AdlsProtocol               = "adls.protocol"

	// Not in use yet
	// AdlsReadBlockSize          = "adls.read.block-size-bytes"
	// AdlsWriteBlockSize         = "adls.write.block-size-bytes"
)
