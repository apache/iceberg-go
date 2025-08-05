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

# Hive Catalog

The Hive catalog integrates with the Hive Metastore.
Below is a small example of creating and loading an Iceberg table
through the Hive catalog.

```go
package main

import (
    "context"

    "github.com/apache/iceberg-go"
    "github.com/apache/iceberg-go/catalog/hive"
    "github.com/apache/iceberg-go/table"
)

func main() {
    ctx := context.Background()
    cat, err := hive.NewHiveCatalog(hive.Config{Host: "metastore", Port: 9083, Auth: "NONE"})
    if err != nil {
        panic(err)
    }

    schema := iceberg.NewSchema(iceberg.NestedFieldMap{
        1: iceberg.PrimitiveField(1, "id", iceberg.IntType{}, false),
    })

    if _, err := cat.CreateTable(ctx, table.Identifier{"db", "tbl"}, schema); err != nil {
        panic(err)
    }

    if _, err := cat.LoadTable(ctx, table.Identifier{"db", "tbl"}, nil); err != nil {
        panic(err)
    }
}
```

The catalog can also be created via the generic factory using connection
properties or a `hive://` URI:

```go
cat, err := catalog.Load(ctx, "hive", iceberg.Properties{
    "type": "hive",
    "uri":  "hive://metastore:9083?auth=NONE",
})
if err != nil {
    panic(err)
}
```

The table metadata location is stored in the Hive table properties under the
`metadata_location` key.
