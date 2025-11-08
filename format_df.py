from pyspark.sql import DataFrame
from pyspark.sql.functions import lit


def format_df(df: DataFrame) -> DataFrame:
    return df.withColumn("new_col", lit("local"))