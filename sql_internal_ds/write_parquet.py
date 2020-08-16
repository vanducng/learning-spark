from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc


spark = (SparkSession
        .builder
        .appName("SparkSQL")
        .enableHiveSupport()
        .getOrCreate())

# Create data frame
df = spark.createDataFrame([("Brooke", 20), ("Denny", 31), ("Jules", 30), ("TD", 35), ("Brooke", 25)], ["name", "age"])

# Write to parquet as default
(df.write
.mode("overwrite")
.option("compression", "snappy")
.saveAsTable("ages"))


