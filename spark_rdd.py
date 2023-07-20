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
'''
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

words_trd_rdd = words_rdd.map(lambda word: (word, word[0], word_start_withs(word, "S")))

for element in words_trd_rdd.collect():
    print(element)

words_rdd.flatMap(lambda word: list(word)).take(10)
print(words_rdd)
'''

# RDD Actions
def word_length_reducer(leftWord, rightWord):
    if len(leftWord) > len(rightWord):
        return leftWord
    else:
        return rightWord

print(words_rdd.reduce(word_length_reducer))

print(spark.sparkContext.parallelize(range(1, 21)).max())
