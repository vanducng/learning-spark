from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType
from pyspark.sql.functions import *
from pyspark.sql import SparkSession

spark = (
    SparkSession
    .builder
    .appName("SparkApp")
    .getOrCreate()
)
sf_fire_file = "/mnt/d/git/learning_spark/data/fire-service/Fire_Department_Calls_for_Service.csv"

fire_schema = StructType([
                StructField('CallNumber', IntegerType(), True),
                StructField('UnitID', StringType(), True),
                StructField('IncidentNumber', IntegerType(), True),
                StructField('CallType', StringType(), True),                  
                StructField('CallDate', StringType(), True),       
                StructField('WatchDate', StringType(), True),       
                StructField('ReceivedDtTm', StringType(), True),       
                StructField('EntryDtTm', StringType(), True),       
                StructField('DispatchDtTm', StringType(), True),       
                StructField('ResponseDtTm', StringType(), True),       
                StructField('OnSceneDtTm', StringType(), True),       
                StructField('TransportDtTm', StringType(), True),                  
                StructField('HospitalDtTm', StringType(), True),       
                StructField('CallFinalDisposition', StringType(), True),       
                StructField('AvailableDtTm', StringType(), True),       
                StructField('Address', StringType(), True),       
                StructField('City', StringType(), True),       
                StructField('ZipcodeofIncident', IntegerType(), True),       
                StructField('Battalion', StringType(), True),                 
                StructField('StationArea', StringType(), True),
                StructField('Box', StringType(), True),
                StructField('OriginalPriority', StringType(), True),
                StructField('Priority', StringType(), True),
                StructField('FinalPriority', IntegerType(), True),       
                StructField('ALSUnit', BooleanType(), True),       
                StructField('CallTypeGroup', StringType(), True),
                StructField('NumberofAlarms', IntegerType(), True),
                StructField('UnitType', StringType(), True),
                StructField('Unitsequenceincalldispatch', IntegerType(), True),
                StructField('FirePreventionDistrict', StringType(), True),
                StructField('SupervisorDistrict', StringType(), True),
                StructField('NeighborhoodDistrict', StringType(), True),
                StructField('Location', StringType(), True),
                StructField('RowID', StringType(), True)
                ])


# read the file using DataFrameReader using format CSV
fire_df = spark.read.csv(sf_fire_file, header=True, schema=fire_schema)
fire_df.show(10)

# Projection equate to the select() method
# Filter can be expressed as filter() or where() methods
few_fire_df = (
                fire_df
                .select("IncidentNumber", "AvailableDtTm", "CallType")
                .where(col("CallType") != "Medical Incident")
            )
few_fire_df.show(5, truncate=False)

# In Python, return the count using the action distinctCount()
calltype_res = fire_df.select("CallType").where(col("CallType") != "null").distinct().count()
print("Number of calltype_res = " + str(calltype_res))

# filter for only distinct non-null CallTypes from all the rows
fire_df.select("CallType").where(col("CallType") != "null").distinct().show(10, False)

# Renaming, Adding or Dropping Columns, and Aggregating
pattern1 = 'MM/dd/yyyy'
pattern2 = 'MM/dd/yyyy hh:mm:ss aa'


fire_ts_df = fire_df \
  .withColumn('CallDateTS', to_timestamp(col('CallDate'), pattern1).cast("timestamp")) \
  .drop('CallDate') \
  .withColumn('WatchDateTS', to_timestamp(col('WatchDate'), pattern1).cast("timestamp")) \
  .drop('WatchDate') \
  .withColumn('ReceivedDtTmTS', to_timestamp(col('ReceivedDtTm'), pattern2).cast("timestamp")) \
  .drop('ReceivedDtTm') \
  .withColumn('EntryDtTmTS', to_timestamp(col('EntryDtTm'), pattern2).cast("timestamp")) \
  .drop('EntryDtTm') \
  .withColumn('DispatchDtTmTS', to_timestamp(col('DispatchDtTm'), pattern2).cast("timestamp")) \
  .drop('DispatchDtTm') \
  .withColumn('ResponseDtTmTS', to_timestamp(col('ResponseDtTm'), pattern2).cast("timestamp")) \
  .drop('ResponseDtTm') \
  .withColumn('OnSceneDtTmTS', to_timestamp(col('OnSceneDtTm'), pattern2).cast("timestamp")) \
  .drop('OnSceneDtTm') \
  .withColumn('TransportDtTmTS', to_timestamp(col('TransportDtTm'), pattern2).cast("timestamp")) \
  .drop('TransportDtTm') \
  .withColumn('HospitalDtTmTS', to_timestamp(col('HospitalDtTm'), pattern2).cast("timestamp")) \
  .drop('HospitalDtTm') \
  .withColumn('AvailableDtTmTS', to_timestamp(col('AvailableDtTm'), pattern2).cast("timestamp")) \
  .drop('AvailableDtTm')

fire_ts_df.printSchema()
# calculate how many distinct years of data is in the CSV file
fire_ts_df.select(year('CallDateTS')).distinct().orderBy('year(CallDateTS)').show()

# Write to file or table
fire_ts_df.write.format("csv").mode("overwrite").save("/tmp/data/csv/fire_call_service")
fire_ts_df.write.saveAsTable("fire_call_service")