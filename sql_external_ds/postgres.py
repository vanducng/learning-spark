# Read Option 1: Loading data from a JDBC source using load format
jdbcDF1 = (spark
  .read
  .format("jdbc") 
  .option("url", "jdbc:postgresql://[DBSERVER]:5740")
  .option("dbtable", "[SCHEMA].[TABLENAME]")
  .option("user", "[USERNAME]")
  .option("password", "[PASSWORD]")
  .load())

# Read Option 2: Loading data from a JDBC source using jdbc methods format
jdbcDF2 = (spark
  .read 
  .jdbc("jdbc:postgresql://[DBSERVER]:5740", "[SCHEMA].[TABLENAME]",
          properties={"user": "[USERNAME]", "password": "[PASSWORD]"}))

# Saving data from a JDBC source
# Write Option 1: Saving data to a JDBC source using save format
(jdbcDF1
  .write
  .format("jdbc")
  .option("url", "jdbc:postgresql://[DBSERVER]:5740")
  .option("dbtable", "[SCHEMA].[TABLENAME]") 
  .option("user", "[USERNAME]")
  .option("password", "[PASSWORD]")
  .save())

# Write Option 2: Saving data to a JDBC source using JDBC methods
(jdbcDF2
  .write 
  .jdbc("jdbc:postgresql:[DBSERVER]:5740", "[SCHEMA].[TABLENAME]",
          properties={"user": "[USERNAME]", "password": "[PASSWORD]"}))