from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

csv_file = "/mnt/d/git/learning_spark/data/flight-data/departuredelays.csv"

# Incase need to specify the schema
schema = "`date` STRING, `delay`INT, `distance` INT, `origin` STRING, `destination` STRING"

# Read and create temporary view
# Infer schema, note that for the larger file you many need to specify schema
df = (spark.read.format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load(csv_file))

df.createOrReplaceTempView("us_delay_flights_tbl")

# show top 10 flight with distance > 1000
spark.sql("""SELECT distance, origin, destination 
        FROM us_delay_flights_tbl WHERE distance > 1000 
        ORDER BY distance DESC""").show(10)

# Above query is equivilent to 
(df.select("distance", "origin", "destination")
    .where(col("distance") > 1000)
    .orderBy(desc("distance"))
).show(10)

# Or
(df.select("distance", "origin", "destination")
    .where("distance > 1000")
    .orderBy("distance", ascending=False)
).show(10)

# 
spark.sql("""
SELECT date, delay, origin, destination
FROM us_delay_flights_tbl WHERE delay > 120 AND origin = 'SFO' AND destination = 'ORD'
ORDER BY delay DESC
""").show(10)

spark.sql("""SELECT delay, origin, destination,
         CASE
         WHEN delay > 360 THEN 'Very Long Delays'
         WHEN delay > 120 AND delay < 360 THEN 'Long Delays'
         WHEN delay > 60 AND delay < 120 THEN 'Short Delays'
         WHEN delay > 0 and delay < 60 THEN 'Tolerable Delays'
         WHEN delay = 0 THEN 'No Delays'
         ELSE 'Early'
         END AS Flight_Delays
         FROM us_delay_flights_tbl
         ORDER BY origin, delay DESC""").show(10)
