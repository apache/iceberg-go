import argparse
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


def testSetProperties():
    df = spark.sql("SHOW TBLPROPERTIES default.go_test_set_properties")
    props = {row['key']: row['value'] for row in df.collect()}
    df.show()

    assert props["write.parquet.compression-codec"] == "snappy"
    assert props["write.merge.mode"] == "merge"
    assert props["write.metadata.merge.min-count"] == "1"
    assert props["write.metadata.target-file-size-bytes"] == "1"


def testAddedFile():
    df = spark.sql("SELECT COUNT(*) FROM default.test_partitioned_by_days")
    rows = df.collect()
    df.show()

    assert len(rows) == 3


def testReadDifferentDataTypes():
    df = spark.sql("SELECT * FROM default.go_test_different_data_types")
    rows = df.collect()
    df.show()

    assert len(rows) == 3

    expected_rows = [
        {
            "bool": "false",
            "string": "a",
            "string_long": "a" * 22,
            "int": "1",
            "long": "1",
            "float": "0.0",
            "double": "0.0",
            "timestamp": "2023-01-01 11:25:00",  # Spark adjusts for +08:00 → UTC
            "timestamptz": "2023-01-01 19:25:00",
            "date": "2023-01-01",
            "uuid": "00000000-0000-0000-0000-000000000000",
            "binary": "AQ==",
            "fixed": "AAAAAAAAAAAAAAAAAAAAAA==",
            "small_dec": "123456.78",
            "med_dec": "12345678901234.56",
            "large_dec": "1234567890123456789012.34",
        },
        {col: None for col in df.columns},
        {
            "bool": "true",
            "string": "z",
            "string_long": "z" * 22,
            "int": "9",
            "long": "9",
            "float": "0.9",
            "double": "0.9",
            "timestamp": "2023-03-01 11:25:00",
            "timestamptz": "2023-03-01 19:25:00",
            "date": "2023-03-01",
            "uuid": "11111111-1111-1111-1111-111111111111",
            "binary": "Eg==",
            "fixed": "EREREREREREREREREREREQ==",
            "small_dec": "876543.21",
            "med_dec": "65432109876543.21",
            "large_dec": "4321098765432109876543.21",
        },
    ]
    for actual, expected in zip(rows, expected_rows):
        for k in expected:
            assert actual[k] == expected[k]


def testReadSpecUpdate():
    df = spark.sql("DESCRIBE TABLE EXTENDED default.go_test_update_spec")
    df.show()

    rows = df.collect()

    metadata = {row["col_name"]: row["data_type"] for row in rows if row["col_name"] is not None}

    partitioning = metadata.get("Partitioning")
    assert partitioning is not None, "Missing Partitioning info"

    assert "truncate(5, baz)" in partitioning
    assert "bucket(3, baz)" in partitioning


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
