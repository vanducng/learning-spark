from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import Row

spark = (
        SparkSession
        .builder
        .appName("SparkApp")
        .getOrCreate()
)

# using DDL String to define a schema
schema = "`Author` STRING, `State` STRING"
rows = [Row("Matei Zaharia", "CA"), Row("Reynold Xin", "CA")]
authors_df = spark.createDataFrame(rows, schema)
authors_df.show()

