# Write a dictionary or list to hdfs
best_params = {
    "Key1": 123,
    "Key2": "hello"
}

save_path = "hdfs://nameservice1/user/duc.nguyenv3/projects/01_lgbm_modeling/data/output/best_params.json"
sdf = (
        spark
        .sparkContext
        .parallelize([best_params])
        .toDF()
        .coalesce(1)
        .write
        .mode("overwrite")
        .json(save_path)
    )

# Load the dict to SparkDF
