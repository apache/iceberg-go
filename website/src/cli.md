<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

# CLI

Run `go build ./cmd/iceberg` from the root of this repository to build the CLI executable, alternately you can run `go install github.com/apache/iceberg-go/cmd/iceberg` to install it to the `bin` directory of your `GOPATH`.

The `iceberg` CLI usage is very similar to [pyiceberg CLI](https://py.iceberg.apache.org/cli/) \
You can pass the catalog URI with `--uri` argument.

Example:
You can start the Iceberg REST API docker image which runs on default in port `8181`
```
docker pull apache/iceberg-rest-fixture:latest
docker run -p 8181:8181 apache/iceberg-rest-fixture:latest
```
and run the `iceberg` CLI pointing to the REST API server.

```
 ./iceberg --uri http://0.0.0.0:8181 list
┌─────┐
| IDs |
| --- |
└─────┘
```
**Create Namespace**
```
./iceberg --uri http://0.0.0.0:8181 create namespace taxitrips
```

**List Namespace**
```
 ./iceberg --uri http://0.0.0.0:8181 list
┌───────────┐
| IDs       |
| --------- |
| taxitrips |
└───────────┘


```
