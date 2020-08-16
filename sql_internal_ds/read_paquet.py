from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc


spark = (SparkSession
        .builder
        .appName("SparkSQL")
        .enableHiveSupport()
        .getOrCreate())

# Unmanage table read
spark.sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
    USING parquet
    OPTIONS (
    path "/mnt/d/git/learning_spark/spark-warehouse/us_delay_flights_tbl/" )
""")

# Test
spark.sql("SELECT * FROM us_delay_flights_tbl").show()

