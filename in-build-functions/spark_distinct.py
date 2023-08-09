from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

# Create a Spark session
spark = (
    SparkSession.
    builder.
    appName('SparkDistinct').
    getOrCreate()
)

# Data
data = [
    ("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100)
]

# Create a dataFrame
columns = ['name', 'department', 'salary']
df = spark.createDataFrame(data=data, schema=columns)
df.printSchema()
df.show(truncate=False)

distinctDf = df.distinct()
print(f'Distinct Count: {distinctDf.count()}')