import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import lit
from pyspark.testing import assertDataFrameEqual
from format_df import format_df
from try_delta_merge import create_initial_delta_table, perform_delta_merge


@pytest.fixture(scope="session")
def spark():
    from delta import configure_spark_with_delta_pip
    
    # Initialize the SparkSession builder
    builder = (
        SparkSession.builder
        .appName("test")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    
    # Configure the builder with Delta Lake support using the utility function
    return configure_spark_with_delta_pip(builder).getOrCreate()


def test_format_df_adds_new_col(spark):
    # Create test DataFrame

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


def test_spark_delta(spark):
    import os
    table_path = os.getcwd() + "test_table.delta"
    create_initial_delta_table(spark, table_path)

    result_df = spark.read.format("delta").load(table_path)

    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("value", IntegerType(), True)
    ])
    initial_data = [(1, "Alice", 100), (2, "Bob", 200), (3, "Charlie", 300)]
    expected_df = spark.createDataFrame(initial_data, schema)

    assertDataFrameEqual(result_df, expected_df)

