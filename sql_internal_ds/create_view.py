from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

# Load data
csv_file = "/mnt/d/git/learning_spark/data/flight-data/departuredelays.csv"
schema = "`date` STRING, `delay`INT, `distance` INT, `origin` STRING, `destination` STRING"
df = (spark.read.format("csv")
    .schema(schema)
    # .option("inferSchema", "true")
    .option("header", "true")
    .load(csv_file))
df.createOrReplaceTempView("us_delay_flights_tbl")

# Create view
# - temporary view is tied to a single SparkSession within a Spark application.
# - global view is visible across multiple SparkSessions within a Spark application
# Yes, you can create multiple SparkSessions within a single Spark application—for some use cases, 
# where you want to access data (and combine them) from two different SparkSessions that don’t share the same Hive metastore configurations.
df_sfo = spark.sql("SELECT date, delay, origin, destination FROM us_delay_flights_tbl WHERE origin = 'SFO'")
df_jfk = spark.sql("SELECT date, delay, origin, destination FROM us_delay_flights_tbl WHERE origin = 'JFK'")

# create a temporary and global temporary view
df_sfo.createOrReplaceGlobalTempView("us_origin_airport_SFO_global_tmp_view")
df_jfk.createOrReplaceTempView("us_origin_airport_JFK_tmp_view")

# Test result
print("global_temp.us_origin_airport_SFO_global_tmp_view")
spark.sql("SELECT * FROM global_temp.us_origin_airport_SFO_global_tmp_view").show(10)

print("us_origin_airport_JFK_tmp_view")
spark.sql("SELECT * FROM us_origin_airport_JFK_tmp_view").show(10)


spark.catalog.listDatabases()
spark.catalog.listTables()
# spark.catalog.listColumns("us_delay_flights_tbl")

# Drop view
spark.catalog.dropGlobalTempView("us_origin_airport_SFO_global_tmp_view")
spark.catalog.dropTempView("us_origin_airport_JFK_tmp_view")