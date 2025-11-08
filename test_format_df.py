import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import lit
from pyspark.testing import assertDataFrameEqual
from format_df import format_df
from try_delta_merge import create_initial_delta_table, perform_delta_merge


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("test") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()


def test_format_df_adds_new_col(spark):
    # Create test DataFrame

    create_initial_delta_table(spark, ".")


    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True)
    ])
    data = [(1, "Alice"), (2, "Bob")]
    test_df = spark.createDataFrame(data, schema)
    
    # Create expected DataFrame
    expected_df = test_df.withColumn("new_col", lit("local"))
    
    # Apply format_df function
    result_df = format_df(test_df)
    
    # Use pyspark.testing.assertDataFrameEqual for comparison
    assertDataFrameEqual(result_df, expected_df)