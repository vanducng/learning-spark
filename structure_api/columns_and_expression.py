from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, concat


schema = "`Id` INT,`First` STRING,`Last` STRING,`Url` STRING,`Published` STRING,`Hits` INT,`Campaigns` ARRAY<STRING>"
data = [[1, "Jules", "Damji", "https://tinyurl.1", "1/4/2016", 4535, ["twitter", "LinkedIn"]],
        [2, "Brooke","Wenig","https://tinyurl.2", "5/5/2018", 8908, ["twitter", "LinkedIn"]],
        [3, "Denny", "Lee", "https://tinyurl.3","6/7/2019",7659, ["web", "twitter", "FB", "LinkedIn"]],
        [4, "Tathagata", "Das","https://tinyurl.4", "5/12/2018", 10568, ["twitter", "FB"]],
        [5, "Matei","Zaharia", "https://tinyurl.5", "5/14/2014", 40578, ["web", "twitter", "FB", "LinkedIn"]],
        [6, "Reynold", "Xin", "https://tinyurl.6", "3/2/2015", 25568, ["twitter", "LinkedIn"]]
    ]

spark = (SparkSession
    .builder
    .appName("SparkApp")
    .getOrCreate())

blogs_df = spark.createDataFrame(data, schema)

# use an expression to compute a value
blogs_df.select(expr("Id * 2").alias("new_id")).show(2)

# or use col to compute value
blogs_df.select((col("Id")*2).alias("new_id")).show(2)

# use expression to compute big hitters for blogs
# this adds a new column Big Hitters based on the conditional expression
blogs_df.withColumn("Big Hitters", (expr("Hits > 10000"))).show()

# use expression to concatenate three columns, create a new column
# and show the newly created concatenated column
blogs_df.withColumn("AuthorsId", (concat(expr("First"), expr("Last"), expr("Id")))).select(expr("AuthorsId")).show(n=4)


# expr are same as column method call
blogs_df.select(col("Hits")).show(2)
blogs_df.select(expr("Hits")).show(2)
blogs_df.select("Hits").show(2)

# Sort by column "Id" in descending order
blogs_df.orderBy("Id", ascending=False).show()
