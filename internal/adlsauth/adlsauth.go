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

// Package adlsauth holds the part of the Azure FileIO's authentication order
// that code outside io/gocloud/azure has to agree with. The REST catalog uses it
// to keep a vended adls.token from being shadowed by inherited credentials, and
// the io/gocloud/azure tests pin it against the bucket factory's actual
// selection, so a change to the factory's order fails a test instead of
// silently shadowing the token again.
package adlsauth

import (
	"net/url"
	"strings"

	iceio "github.com/apache/iceberg-go/io"
)

// Account identifies the storage account an ADLS location authenticates
// against, in the two forms the Azure bucket factory keys per-account
// credentials by.
type Account struct {
	// Host keys SAS tokens, for example myaccount.dfs.core.windows.net.
	Host string
	// Name keys connection strings, for example myaccount.
	Name string
}

// AccountFor returns the storage account for location, or false if location
// has no account.
func AccountFor(location string) (Account, bool) {
	parsed, err := url.Parse(location)
	if err != nil {
		return Account{}, false
	}

	return AccountForURL(parsed)
}

// AccountForURL returns the storage account for a parsed location, or false if
// it has no account. The Azure bucket factory derives its account through this
// too, so credentials are cleared under the same keys they are looked up by.
func AccountForURL(parsed *url.URL) (Account, bool) {
	host := parsed.Hostname()
	name, _, _ := strings.Cut(host, ".")
	if name == "" {
		return Account{}, false
	}

	return Account{Host: host, Name: name}, true
}

// ClearTokenShadowingCredentials deletes from props, the properties of an IO
// for acct, the credentials the Azure bucket factory selects ahead of a
// nonempty adls.token: any shared key, the SAS token for acct's host and the
// connection string for acct's name. SAS tokens and connection strings for other
// accounts are keyed by account, never apply to acct, and are kept.
//
// The shared key is cleared whichever account it names, because the factory
// does not scope it: a configured shared key signs requests to every account,
// so one for another account would sign acct's requests with the wrong key.
// Callers pass a copy, so the shared key stays in the configuration other
// accounts' IO is built from.
func ClearTokenShadowingCredentials(props map[string]string, acct Account) {
	delete(props, iceio.ADLSSharedKeyAccountName)
	delete(props, iceio.ADLSSharedKeyAccountKey)
	delete(props, iceio.ADLSSasTokenPrefix+acct.Host)
	delete(props, iceio.ADLSConnectionStringPrefix+acct.Name)
}
