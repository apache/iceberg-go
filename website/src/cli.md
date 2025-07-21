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

**Create Table**
- Notes: Only identity transform is supported at this moment
```
# Create a simple table with REST catalog and Minio
./iceberg create table default.table-1 
        --properties write.format.default=parquet \
        --partition-spec foo \
        --sort-order foo:desc:nulls-last \
        --schema '[{"id":1,"name":"foo","type":"string","required":false},{"id":2,"name":"bar","type":"int","required":true}]' \
        --catalog rest \
        --uri http://localhost:8181
Table default.table-1 created successfully

# Describe the newly created table
./iceberg describe --catalog rest --uri http://localhost:8181 default.table-1
Table format version | 2                                                                                               
Metadata location    | s3://warehouse/default/table-1/metadata/00000-f0ccaadd-d988-482e-99da-3a37870288fe.metadata.json
Table UUID           | 33fa3fac-e638-4335-a085-343c6d9e7de5                                                            
Last updated         | 1753133512562                                                                                   
Sort Order           | 1: [                                                                                            
                     | 1 desc nulls-last                                                                               
                     | ]                                                                                               
Partition Spec       | [                                                                                               
                     |  1000: foo: identity(1)                                                                          
                     | ]                                                                                               

Current Schema, id=0
├──1: foo: optional string
└──2: bar: required int

Current Snapshot | 

Snapshots

Properties
key                             | value  
-----------------------------------------
write.format.default            | parquet
write.parquet.compression-codec | zstd   

```