from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc
# https://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html

spark = (SparkSession
        .builder
        .appName("SparkSQL")
        .config("spark.sql.warehouse.dir", "/mnt/d/git/learning_spark/spark-warehouse")
        .enableHiveSupport()
        .getOrCreate())

# Load data
df = spark.read.format("parquet").load("/mnt/d/git/learning_spark/spark-warehouse/us_delay_flights_tbl")
df.createOrReplaceTempView("us_delay_flights_tbl")

# Convert to table
us_flights_df = spark.sql("SELECT * FROM us_delay_flights_tbl")
us_flights_df2 = spark.table("us_delay_flights_tbl")

# Test
(us_flights_df2.select("distance", "origin", "destination")
    .where("distance > 1000")
    .orderBy("distance", ascending=False)
).show(10)