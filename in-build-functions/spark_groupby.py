from pyspark.sql import SparkSession

spark = (
    SparkSession.
    builder.
    appName('spark-group-by').
    getOrCreate()
)

data = [
    ("Software Engineer", 10000),
    ("Data Scientist", 41000),
    ("Sales Person", 230000),
    ("CTO", 50000),
    ("Data Analyst", 111000),
    ("CEO", 300000),
    ("Advertising", 250000),
    ("Marketing", 114000),
    ("Data Engineer", 680000),
    ("Advertising", 20000),
    ("Data Engineer", 70000)
]

column = ['Job', 'Salary']

df = spark.createDataFrame(data=data, schema=column)

# Sort the dataframe
grouped_data = (
    df.groupBy('Job')
    .agg(
        {'Salary': 'sum'}
    )
    .withColumnRenamed(
        'sum(Salary)', 'Total Salary'
    )
    .sort(
        'Total Salary',
        ascending=False
    )
)

grouped_data.show()