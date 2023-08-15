from pyspark.sql import SparkSession

spark = (
    SparkSession.
    builder.
    appName('spark-first').
    getOrCreate()
)

data = [x for x in range(11, 1, -1)]

val = spark.sparkContext.parallelize(data)
val.collect()
first_val = val.first()
print(first_val)