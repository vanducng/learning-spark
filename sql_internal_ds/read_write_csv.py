from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

csv_file = "/mnt/d/git/learning_spark/data/flight-data/departuredelays.csv"
schema = "`date` STRING, `delay`INT, `distance` INT, `origin` STRING, `destination` STRING"

df = (spark.read.format("csv")
        .option("header", "true")
        .schema(schema)
        .option("mode", "FAILFAST")     # exit if any errors
        .option("nullValue", "")        # replace any null data field with quotes
        .load(csv_file))


# Read into table
spark.sql(
    f"""
        CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
        USING csv
        OPTIONS (
        path "{csv_file}",
        header "true",
        inferSchema "true",
        mode "FAILFAST"
        )
    """
)
spark.sql("SELECT * FROM us_delay_flights_tbl").show()

# Write CSV
df.write.format("csv").mode("overwrite").save("/tmp/data/csv/df_csv")