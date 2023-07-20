"""Spark Map and Flat Map Transformation"""

from pyspark.sql import SparkSession

spark = (
    SparkSession
    .builder
    .appName('map transformation')
    .getOrCreate()
)

nums_list = [*range(1, 21)]

nums_rdd = spark.sparkContext.parallelize(nums_list)

nums_squared_rdd = nums_rdd.map(lambda x: (x, x*x))

# for element in nums_squared_rdd.collect():
#     print(element)

# Sort by key transformation
countries_list = [('India', 5), ('USA', -99), ('Greece', 13)]
countries_rdd = spark.sparkContext.parallelize(countries_list)

# srtd_countries_list = countries_rdd.sortByKey().collect()
# for val, rnk in srtd_countries_list:
#     print(val, rnk)

# srtd_countries_list = countries_rdd.map(lambda c: (c[1], c[0])).sortByKey(False).collect()
# for val, rnk in srtd_countries_list:
#     print(val, rnk)

# RDD Actions
num_list = [1,5,2,3,4]
result = spark.sparkContext.parallelize(num_list).reduce(lambda x, y: x + y)
print(result)
