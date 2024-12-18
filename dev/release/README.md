<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Release

## Overview

    1. Test the revision to be released
    2. Prepare RC and vote (detailed later)
    3. Publish (detailed later)

### Prepare RC and vote

Run `dev/release/release_rc.sh` on a working copy of 
`git@github.com:apache/iceberg-go` not from your fork:

```console
$ git clone git@github.com:apache/iceberg-go.git
$ dev/release/release_rc.sh ${VERSION} ${RC}
(Send a vote email to dev@iceberg.apache.org.
 You can use a draft shown by release_rc.sh for the email.)
```

Here is an example to release RC1:

```console
$ GH_TOKEN=${YOUR_GITHUB_TOKEN} dev/release/release_rc.sh 1.0.0 1
```

The arguments of `release_rc.sh` are the version and the RC number. If RC1 has a problem, we'll increment the RC number such as RC2, RC3 and so on.

Requirements to run `release_rc.sh`:

    * You must be an Apache Iceberg committer or PMC member
    * You must prepare your PGP key for signing

If you don't have a PGP key, https://infra.apache.org/release-signing.html#generate
may be helpful.

Your PGP key must be registered to the following:

    * https://downloads.apache.org/iceberg/KEYS

See the header comment of them for how to add a PGP key.

Apache Iceberg committers can update them by Subversion client with their ASF account.
e.g.:

```console
$ svn co https://dist.apache.org/repos/dist/release/iceberg
$ cd iceberg
$ editor KEYS
$ svn ci KEYS
```

### Publish

We need to do the following to publish a new release:

    * Publish to apache.org

Run `dev/release/release.sh` to publish to apache.org:

```console
$ GH_TOKEN=${YOUR_GITHUB_TOKEN} dev/release/release.sh ${VERSION} ${RC}
```

Add the release to ASF's report database via [Apache Committee Report Helper](https://reporter.apache.org/addrelease.html?iceberg)

### Verify

We have a script for verifying a RC.

You must install the following to run the script:

    * `curl`
    * `gpg`
    * `shasum` or `sha512sum`
    * `tar`

You don't need to have Go installed, if it isn't on the system the latest Go will be
automatically downloaded and used only for verification.

To verify a RC, run the following:

```console
$ dev/release/verify_rc.sh ${VERSION} ${RC}
```

If the verification is successful, the message `RC looks good!` is shown.
