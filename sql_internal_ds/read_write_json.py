from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc


spark = (SparkSession
        .builder
        .appName("SparkSQL")
        .enableHiveSupport()
        .getOrCreate())
# Read to DF
json_path = "/mnt/d/git/learning_spark/data/song_data/A/A/A/*"
df = spark.read.format("json").load(json_path)

# Read to table
spark.sql(
f"""
    CREATE OR REPLACE TEMPORARY VIEW songs
    USING json
    OPTIONS (
    path "{json_path}"
    )
"""
)

spark.sql("SELECT * FROM songs").show()

# Write to json file
(df.write.format("json")
           .mode("overwrite")
        #    .option("compression", "snappy")
           .save("/tmp/data/json/df_json"))