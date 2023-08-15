from pyspark.sql import SparkSession

spark = (
    SparkSession.
    builder.
    appName('Spark - reduceBy').
    getOrCreate()
)

data = [
    ('Project', 1),
    ('Gutenberg', 1),
    ('Alice', 1),
    ('Adventures', 1),
    ('in', 1),
    ('Wonderland', 1),
    ('Project', 1),
    ('Gutenberg', 1),
    ('Adventures', 1),
    ('in', 1),
    ('Wonderland', 1),
    ('Project', 1),
    ('Gutenberg', 1)
]

rdd = spark.sparkContext.parallelize(data)
reduced_rdd = rdd.reduceByKey(lambda x,y: x+y)

for key, val in reduced_rdd.collect():
    print(f'| -- {key}: {val} -- |')