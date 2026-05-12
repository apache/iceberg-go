# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import argparse
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .config("spark.sql.catalog.hadoop_test", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.hadoop_test.type", "hadoop")
    .config("spark.sql.catalog.hadoop_test.warehouse", "/tmp/iceberg-hadoop-warehouse")
    .getOrCreate()
)


def runSQL(sql):
    result = spark.sql(sql)
    result.show(truncate=False)

    return result


def runSQLAssert(sql):
    """Execute SQL and assert at least one row is returned."""
    result = runSQL(sql)
    count = result.count()
    assert count > 0, f"Expected at least one row, got {count} for query: {sql}"
    print(f"OK: {count} row(s) returned")

    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--sql", type=str, required=True, help="Validation SQL statement to execute")
    parser.add_argument("--assert-rows", action="store_true",
                        help="Assert that the query returns at least one row")
    args = parser.parse_args()

    if args.sql:
        if args.assert_rows:
            runSQLAssert(args.sql)
        else:
            runSQL(args.sql)