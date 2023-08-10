from pyspark.sql import SparkSession

# create spark session
spark = (
    SparkSession.
    builder.
    appName('SparkUnion').
    getOrCreate()
)

# Data
data = [1, 2, 3, 4]

rdd = spark.sparkContext.parallelize(data)
rdd_carte = rdd.cartesian(rdd).collect()
print(rdd_carte)