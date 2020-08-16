# pandas UDFs (also known as vectorized UDFs) uses Apache Arrow to transfer data and utilizes pandas to work with the data
# Once the data is in Apache Arrow format, there is no longer the need to serialize/pickle the data as it is 
# already in a format consumable by the Python process

# There are three types of pandas UDFs: scalar, grouped map, and grouped aggregate. 

import pandas as pd

# Import various pyspark SQL functions including pandas_udf
from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import LongType
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

# Declare the cubed function 
def cubed(a: pd.Series) -> pd.Series:
    return a * a * a

# Create the pandas UDF for the cubed function 
cubed_udf = pandas_udf(cubed, returnType=LongType())

# Create a Spark DataFrame, 'spark' is an existing SparkSession
df = spark.range(1, 5)

# Execute function as a Spark vectorized UDF
df.select("id", cubed_udf(col("id"))).show()
