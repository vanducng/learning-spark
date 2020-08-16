# Manage table mean spark handle both meta & data 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

csv_file = "/mnt/d/git/learning_spark/data/flight-data/departuredelays.csv"

spark.sql("CREATE DATABASE learn_spark_db")
spark.sql("USE learn_spark_db")

# spark.sql("CREATE TABLE us_delay_flights_tbl (date STRING, delay INT, distance INT, origin STRING, destination STRING)")

# schema as defined in the above example
schema = "date STRING, delay INT, distance INT, origin STRING, destination STRING"
flights_df = spark.read.csv(csv_file, schema=schema)
flights_df.write.saveAsTable("us_delay_flights_tbl")

# Test result
spark.sql("""SELECT distance, origin, destination 
        FROM us_delay_flights_tbl WHERE distance > 1000 
        ORDER BY distance DESC""").show(10)
        