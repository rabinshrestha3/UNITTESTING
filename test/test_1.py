
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from src.testing import replace_nulls_with_one

@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder.master("local[1]").appName("Test").getOrCreate()
    yield spark
    spark.stop()

def test_replace_nulls_with_one(spark):
    schema = StructType([
        StructField("a", IntegerType(), True),
        StructField("b", StringType(), True)
    ])
    data = [
        (None, "foo"),
        (2, None),
        (None, None)
    ]
    df = spark.createDataFrame(data, schema)
    result_df = replace_nulls_with_one(df)
    result = result_df.collect()
    expected = [('1', 'foo'), ('2', '1'), ('1', '1')]
    print("Test result:", [tuple(row) for row in result])
    print("Expected:", expected)
    assert [tuple(row) for row in result] == expected

def test_replace_nulls_with_one_dataframe_equality(spark):
    schema = StructType([
        StructField("a", IntegerType(), True),
        StructField("b", StringType(), True)
    ])
    data = [
        (None, "foo"),
        (2, None),
        (None, None)
    ]
    df = spark.createDataFrame(data, schema)
    result_df = replace_nulls_with_one(df)

    expected_schema = StructType([
        StructField("a", StringType(), True),
        StructField("b", StringType(), True)
    ])
    expected_data = [
        ("1", "foo"),
        ("2", "1"),
        ("1", "1")
    ]
    expected_df = spark.createDataFrame(expected_data, expected_schema)

    assert result_df.collect() == expected_df.collect()
