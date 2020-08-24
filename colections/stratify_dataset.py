# Way 1
from pyspark.sql import functions as F 

def stratified_split_train_test(df, frac, label, join_on, seed=42):
    """ stratfied split of a dataframe in train and test set.
    inspiration gotten from:
    https://stackoverflow.com/a/47672336/1771155
    https://stackoverflow.com/a/39889263/1771155"""
    fractions = df.select(label).distinct().withColumn("fraction", F.lit(frac)).rdd.collectAsMap()
    df_frac = df.stat.sampleBy(label, fractions, seed)
    df_remaining = df.join(df_frac, on=join_on, how="left_anti")
    return df_frac, df_remaining


# Way 2
# read in data
df = spark.read.csv(file, header=True)

# split dataframes between 0s and 1s
zeros = df.filter(df["Target"]==0)
ones = df.filter(df["Target"]==1)

# split datasets into training and testing
train0, test0 = zeros.randomSplit([0.8,0.2], seed=1234)
train1, test1 = ones.randomSplit([0.8,0.2], seed=1234)

# stack datasets back together
train = train0.union(train1)
test = test0.union(test1)