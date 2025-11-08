from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from delta.tables import DeltaTable


def create_initial_delta_table(spark: SparkSession, table_path: str) -> None:
    """Create initial Delta table with sample data."""
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("value", IntegerType(), True)
    ])
    
    initial_data = [(1, "Alice", 100), (2, "Bob", 200), (3, "Charlie", 300)]
    initial_df = spark.createDataFrame(initial_data, schema)
    initial_df.write.format("delta").mode("overwrite").save(table_path)


def perform_delta_merge(spark: SparkSession, table_path: str, merge_data: list) -> DataFrame:
    """Perform merge operation on Delta table and return result."""
    # Get schema from existing table
    existing_df = spark.read.format("delta").load(table_path)
    schema = existing_df.schema
    
    # Create merge DataFrame
    merge_df = spark.createDataFrame(merge_data, schema)
    
    # Get Delta table reference
    delta_table = DeltaTable.forPath(spark, table_path)
    
    # Execute merge operation
    delta_table.alias("target").merge(
        merge_df.alias("source"),
        "target.id = source.id"
    ).whenMatchedUpdate(set={
        "value": "source.value"
    }).whenNotMatchedInsert(values={
        "id": "source.id",
        "name": "source.name", 
        "value": "source.value"
    }).execute()
    
    # Return merged result
    return spark.read.format("delta").load(table_path)


def try_delta_merge(spark: SparkSession, table_path: str) -> DataFrame:
    """Main function that creates table, performs merge, and returns result."""
    # Create initial table
    create_initial_delta_table(spark, table_path)
    
    # Define merge data (update Bob's value, add David)
    merge_data = [(2, "Bob", 250), (4, "David", 400)]
    
    # Perform merge and return result
    result_df = perform_delta_merge(spark, table_path, merge_data)
    result_df.show()
    
    return result_df
