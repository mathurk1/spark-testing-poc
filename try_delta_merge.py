def _write_initial_silver(spark, path):
    # Initial current row for id=1
    today = F.current_date()
    data = [
        Row(id=1, val="old", row_hash="oldhash",
            is_current=1, is_deleted=0,
            effective_start_date=None, 
            effective_end_date=None,
            merge_insert_timestamp=None,
            merge_update_timestamp=None)
    ]
    df = spark.createDataFrame(data) \
        .withColumn("effective_start_date", F.current_date()) \
        .withColumn("effective_end_date", F.lit("9999-12-31").cast("date")) \
        .withColumn("merge_insert_timestamp", F.current_timestamp()) \
        .withColumn("merge_update_timestamp", F.current_timestamp())
    df.write.mode("overwrite").format("delta").save(str(path))
