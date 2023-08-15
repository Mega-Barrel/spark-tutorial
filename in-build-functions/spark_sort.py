from pyspark.sql import SparkSession

spark = (
    SparkSession.
    builder.
    appName('spark-sort').
    getOrCreate()
)

data = [
    ("Sam", "Software Engineer", "IND", 10000),
    ("Raj", "Data Scientist", "US", 41000),
    ("Jonas", "Sales Person", "UK", 230000),
    ("Peter", "CTO", "Ireland", 50000),
    ("Hola", "Data Analyst", "Australia", 111000),
    ("Ram", "CEO", "Iran", 300000),
    ("Lekhana", "Advertising", "UK", 250000),
    ("Thanos", "Marketing", "IND", 114000),
    ("Nick", "Data Engineer", "Ireland", 680000),
    ("Wade", "Data Engineer", "IND", 70000)
]

column = ['Name', 'Job', 'Country', 'Salary']

df = spark.createDataFrame(data=data, schema=column)

# Sort the dataframe
df.sort(['Job'], ascending=[True]).show()