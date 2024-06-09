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

# AWS integration testing

To validate the glue catalog you will need to create some test resources. 

# Prerequisites

1. An AWS account.
2. [AWS CLI](https://aws.amazon.com/cli/) is installed.
2. Exported environment variables for `AWS_DEFAULT_REGION`, `AWS_REGION` and `AWS_PROFILE`, I use [direnv](https://direnv.net/) to maintain these variables in a `.envrc` file. 
3. Your have logged into an AWS account via the AWS CLI.

The way to deploy this template is using the included cloudformation template is as follows:

```
aws cloudformation deploy --stack-name test-iceberg-glue-catalog --template-file docs/cfn/glue-catalog.yaml
```

Once deployed you can retrieve the outputs of the stack.

```
aws cloudformation describe-stacks --stack-name test-iceberg-glue-catalog --query 'Stacks[0].Outputs'
```

This should output JSON as follows:

```
[
    {
        "OutputKey": "IcebergBucket",
        "OutputValue": "test-iceberg-glue-catalog-icebergbucket-abc123abc123"
    },
    {
        "OutputKey": "GlueDatabase",
        "OutputValue": "iceberg_test"
    }
]
```

Export the required environment variables.

```
# the glue database from the outputs of the stack
export TEST_DATABASE_NAME=iceberg_test

# the s3 bucket name from the outputs of the stack
export TEST_CREATE_TABLE_LOCATION=s3://test-iceberg-glue-catalog-icebergbucket-abc123abc123/testing

# the name of the table you will create in the glue catalog
export TEST_CREATE_TABLE_NAME=records
```

Run the creation integration test to validate the catalog creation, and provide a table which can be used to validate other integration tests.

```
go test -v -run TestGlueCreateTableIntegration ./catalog
```

