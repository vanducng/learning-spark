# Manage table mean spark handle both meta & data 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

csv_file = "/mnt/d/git/learning_spark/data/flight-data/departuredelays.csv"

# Use SQL
spark.sql(f"""CREATE TABLE us_delay_flights_tbl(date STRING, delay INT, distance INT, origin STRING, destination STRING) 
           USING csv OPTIONS (PATH '{csv_file}')""")

# Use DataFrame API
# (flights_df.write
#            .option("path", csv_file)
#            .saveAsTable("us_delay_flights_tbl"))

# Test result
spark.sql("""SELECT distance, origin, destination 
        FROM us_delay_flights_tbl WHERE distance > 1000 
        ORDER BY distance DESC""").show(10)