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

spark = SparkSession.builder.getOrCreate()


def testSetProperties():
    spark.sql("SHOW TBLPROPERTIES default.go_test_set_properties").show(truncate=False)


def testAddedFile():
    spark.sql("SELECT COUNT(*) FROM default.test_partitioned_by_days").show(truncate=False)


def testReadDifferentDataTypes():
    spark.sql("DESCRIBE TABLE EXTENDED default.go_test_different_data_types").show(truncate=False)
    spark.sql("SELECT * FROM default.go_test_different_data_types").show(truncate=False)


def testReadSpecUpdate():
    spark.sql("DESCRIBE TABLE EXTENDED default.go_test_update_spec").show(truncate=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--test", type=str, required=True, help="Name of the test to run")
    args = parser.parse_args()

    if args.test == "TestSetProperties":
        testSetProperties()

    if args.test == "TestAddedFile":
        testAddedFile()

    if args.test == "TestReadDifferentDataTypes":
        testReadDifferentDataTypes()

    if args.test == "TestReadSpecUpdate":
        testReadSpecUpdate()
