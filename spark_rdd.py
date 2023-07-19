"""Spark RDD (Resilient Distributed Data)"""

from pyspark.sql import SparkSession

spark = (
    SparkSession
    .builder
    .appName('Spark RDD tutorial')
    .getOrCreate()
)

WORDS_LIST = "Spark makes life a lot easier and puts me into good Spirits, Spark is to Awesome!".split(' ')

words_rdd = (
    spark
    .sparkContext
    .parallelize(WORDS_LIST)
)

WORDS_DATA = words_rdd.collect()

for word in WORDS_DATA:
    print(word)

print(f'Total Records: {words_rdd.count()}')

print(f'Distinct Records: {words_rdd.distinct().count()}')

words_unique_rdd = words_rdd.distinct()

for word in words_unique_rdd.collect():
    print(word)

# RDD Filter
def word_start_withs(word, letter):
    """get records which starts with letter"""
    return word.startswith(letter)

op = words_rdd.filter(lambda word: word_start_withs(word, "S")).collect()
print(op)
