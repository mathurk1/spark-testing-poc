import pytest
import tempfile
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.testing import assertDataFrameEqual
from try_delta_merge import create_initial_delta_table, perform_delta_merge


@pytest.fixture(scope="session")
def spark():
    from delta import configure_spark_with_delta_pip
    builder = SparkSession.builder.appName("test_delta_merge")
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark


@pytest.fixture
def temp_table_path():
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir)


def test_perform_delta_merge_update_and_insert(spark, temp_table_path):
    # Create initial Delta table
    create_initial_delta_table(spark, temp_table_path)
    
    # Define merge data: update Bob's value, add David
    merge_data = [(2, "Bob", 250), (4, "David", 400)]
    
    # Perform merge
    result_df = perform_delta_merge(spark, temp_table_path, merge_data)
    
    # Create expected DataFrame
    expected_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("value", IntegerType(), True)
    ])
    expected_data = [(1, "Alice", 100), (2, "Bob", 250), (3, "Charlie", 300), (4, "David", 400)]
    expected_df = spark.createDataFrame(expected_data, expected_schema)
    
    # Sort both DataFrames by id for comparison
    result_sorted = result_df.orderBy("id")
    expected_sorted = expected_df.orderBy("id")
    
    # Assert DataFrames are equal
    assertDataFrameEqual(result_sorted, expected_sorted)


def test_perform_delta_merge_only_updates(spark, temp_table_path):
    # Create initial Delta table
    create_initial_delta_table(spark, temp_table_path)
    
    # Define merge data: only updates, no new records
    merge_data = [(1, "Alice", 150), (3, "Charlie", 350)]
    
    # Perform merge
    result_df = perform_delta_merge(spark, temp_table_path, merge_data)
    
    # Create expected DataFrame
    expected_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("value", IntegerType(), True)
    ])
    expected_data = [(1, "Alice", 150), (2, "Bob", 200), (3, "Charlie", 350)]
    expected_df = spark.createDataFrame(expected_data, expected_schema)
    
    # Sort both DataFrames by id for comparison
    result_sorted = result_df.orderBy("id")
    expected_sorted = expected_df.orderBy("id")
    
    # Assert DataFrames are equal
    assertDataFrameEqual(result_sorted, expected_sorted)


def test_perform_delta_merge_only_inserts(spark, temp_table_path):
    # Create initial Delta table
    create_initial_delta_table(spark, temp_table_path)
    
    # Define merge data: only new records, no updates
    merge_data = [(4, "David", 400), (5, "Eve", 500)]
    
    # Perform merge
    result_df = perform_delta_merge(spark, temp_table_path, merge_data)
    
    # Create expected DataFrame
    expected_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("value", IntegerType(), True)
    ])
    expected_data = [(1, "Alice", 100), (2, "Bob", 200), (3, "Charlie", 300), (4, "David", 400), (5, "Eve", 500)]
    expected_df = spark.createDataFrame(expected_data, expected_schema)
    
    # Sort both DataFrames by id for comparison
    result_sorted = result_df.orderBy("id")
    expected_sorted = expected_df.orderBy("id")
    
    # Assert DataFrames are equal
    assertDataFrameEqual(result_sorted, expected_sorted)