import os
import configparser
from datetime import datetime
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, pandas_udf

def create_spark_session():
    """
    Create spark session
    """
    spark = SparkSession \
        .builder \
        .appName("MusicHub") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    """
    Description: This functions is to load the song logs from S3, then processes on EMR or EC2 then store back to S3 for further usage

    Parameters:
        spark       : spark session
        input_data  : where the song json files located
        output_data : where to store the output files after completing process input files
    """
    
    # read song data file
    songs_df = spark.read.json(input_data)

    # Create temp view for querying
    songs_df.createOrReplaceTempView("songs")

    # extract columns to create songs table
    songs_table = spark.sql("""
                            SELECT song_id, 
                            title,
                            artist_id,
                            year,
                            duration
                            FROM songs
                            """) 

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy("year", "artist_id").parquet(output_data + "songs")

    # extract columns to create artists table
    artists_table = spark.sql("""
                            SELECT DISTINCT artist_id,
                            artist_name name,
                            artist_location location,
                            artist_latitude latitude,
                            artist_longitude longitude
                            FROM songs
                            """)
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data + "artists")


def process_log_data(spark, input_data, output_data):
    """
    Description: This functions is to load the user logs from S3, then processes on EMR or EC2 then store back to S3 for further usage

    Parameters:
        spark       : spark session
        input_data  : where the song json files located
        output_data : where to store the output files after completing process input files
    """

    # read log data file
    logs_df = spark.read.json(input_data)
    
    # filter by actions for song plays
    logs_df = logs_df.filter(logs_df["page"] == "NextSong")

    # Create temp view used for SQL query
    logs_df.createOrReplaceTempView("logs")

    # extract columns for users table
    users_table = spark.sql("""
                    SELECT DISTINCT userId user_id,
                    firstName first_name,
                    lastName last_name,
                    gender,
                    level
                    FROM logs
                    WHERE TRIM(userId) <> ''
                """)
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data + "users")

    # create timestamp column from original timestamp column
    get_timestamp = pandas_udf(lambda x: datetime.fromtimestamp(x/1000), TimestampType())
    logs_df = logs_df.withColumn("timestamp", get_timestamp(logs_df.ts))
    
    # Create temp view for timestamp for query
    logs_df.select("timestamp").createOrReplaceTempView("time")

    # extract columns to create time table
    time_table = spark.sql("""
                    SELECT DISTINCT timestamp start_time,
                    HOUR(timestamp) hour,
                    DAYOFMONTH(timestamp) day,
                    WEEKOFYEAR(timestamp) week,
                    MONTH(timestamp) month,
                    YEAR(timestamp) year,
                    DAYOFWEEK(timestamp) weekday
                    FROM time
                    """)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data + "time")

    # Create temp view used for SQL query, this call is updated with timestamp column
    logs_df.createOrReplaceTempView("logs")

    # Load songs table
    songs_df = spark.read.parquet(output_data + "songs")
    songs_df.createOrReplaceTempView("songs")

    # Load artists table
    artists_df = spark.read.parquet(output_data + "artists")
    artists_df.createOrReplaceTempView("artists") 

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
                        SELECT monotonically_increasing_id() songplay_id,
                        l.timestamp start_time,
                        l.userId user_id,
                        s.song_id,
                        a.artist_id,
                        l.sessionId session_id,
                        l.location,
                        l.userAgent user_agent,
                        MONTH(l.timestamp) month,
                        YEAR(l.timestamp) year
                        FROM logs l
                        INNER JOIN songs s ON s.title = l.song AND s.duration = l.length
                        INNER JOIN artists a ON a.artist_id = s.artist_id AND a.name = l.artist
                        """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data + "songplays")

if __name__ == "__main__":
    spark = create_spark_session()
    input_song_data = "/user/duc.nguyenv3/labs/data/song_data/*/*/*/*.json"
    input_log_data = "/user/duc.nguyenv3/labs/data/log_data/*/*/*.json"
    output_data = "/user/duc.nguyenv3/labs/data/output/"
    
    process_song_data(spark, input_song_data, output_data)    
    process_log_data(spark, input_log_data, output_data)
