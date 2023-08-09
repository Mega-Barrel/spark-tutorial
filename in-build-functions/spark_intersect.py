import pyspark
from pyspark.sql import SparkSession

# create spark session
spark = (
    SparkSession.
    builder.
    appName('SparkUnion').
    getOrCreate()
)

# Data 1
data1 = [
    ("James","Sales","NY",90000,34,10000),
    ("Michael","Sales","NY",86000,56,20000),
    ("Robert","Sales","CA",81000,30,23000),
    ("Maria","Finance","CA",90000,24,23000)
]

# Data 2
data2 = [
    ("James","Sales","NY",90000,34,10000),
    ("Maria","Finance","CA",90000,24,23000),
    ("Jen","Finance","NY",79000,53,15000),
    ("Jeff","Marketing","CA",80000,25,18000),
    ("Kumar","Marketing","NY",91000,50,21000)
]

col1 = ['name', 'department', 'state', 'salary', 'age', 'bonus']
col2 = ['name', 'department', 'state', 'salary', 'age', 'bonus']

# DF
df1 = spark.createDataFrame(data=data1, schema=col1)
df2 = spark.createDataFrame(data=data2, schema=col2)

# Merge 2 df with same value
union_df = df1.intersect(df2)
union_df.show(truncate=False)