<!--
 - Licensed to the Apache Software Foundation (ASF) under one or more
 - contributor license agreements.  See the NOTICE file distributed with
 - this work for additional information regarding copyright ownership.
 - The ASF licenses this file to You under the Apache License, Version 2.0
 - (the "License"); you may not use this file except in compliance with
 - the License.  You may obtain a copy of the License at
 -
 -   http://www.apache.org/licenses/LICENSE-2.0
 -
 - Unless required by applicable law or agreed to in writing, software
 - distributed under the License is distributed on an "AS IS" BASIS,
 - WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 - See the License for the specific language governing permissions and
 - limitations under the License.
 -->

# DynamoDB catalog

Import `github.com/apache/iceberg-go/catalog/dynamodb` to register the catalog.
The AWS SDK default credential chain is used by `catalog.Load`. `NewCatalog`
accepts an `aws.Config` for custom credentials, transports, and retry settings.

| Property | Meaning |
| --- | --- |
| `type` | `dynamodb` |
| `warehouse` | Required default table storage location |
| `dynamodb.table-name` | Backing DynamoDB table, default `iceberg` |
| `dynamodb.endpoint` | Optional service endpoint, including DynamoDB Local |
| `client.region` | AWS region override when using the registry |

The catalog creates the backing table when it is absent. It uses Java's
`identifier`/`namespace` keys, the `namespace-identifier` index, `p.` property
columns, UUID versions, and millisecond timestamps. Separate logical catalogs
must use separate backing tables to keep their contents separate.

Namespace `default_location` controls the default location for its tables.
Namespace components and table names cannot contain dots. Go namespace creation
requires its immediate parent to exist. Lists return direct children.

Metadata commits use conditional transactions and idempotency tokens. Rename
atomically moves the catalog entry and keeps the metadata location. Go writers
also update namespace versions when creating tables or child namespaces, or
registering and renaming tables. This protects namespace deletion against
concurrent Go creation. Java writers do not update these namespace versions;
namespace lifecycle changes shared with Java need external coordination.
These version updates can conflict when many tables or child namespaces are
created in the same namespace, or when its properties are updated concurrently.
Creation retries a limited number of times and can return `ErrCommitFailed`.

Dropping a namespace checks the index first, then scans the backing table with
consistent reads to confirm that it is empty. A successful drop therefore uses
read capacity proportional to the size of the whole catalog table.

Run the local integration suite with:

```sh
docker run --rm -p 8000:8000 amazon/dynamodb-local:latest -jar DynamoDBLocal.jar -inMemory -sharedDb
make integration-dynamodb
```

The shared integration recipe starts DynamoDB Local and runs this suite in CI.
