
from pyspark.sql import SparkSession
from pyspark.sql.functions import count

spark = (
    SparkSession
    .builder
    .appName('TotalOrdersCountry')
    .getOrCreate()
)

sales_file = 'data/sales_records.csv'

sales_df = (
    spark
    .read
    .format('csv')
    .option('header', 'true')
    .option('inferSchema', 'true')
    .load(sales_file)
)

sales_df.select(
    "Region",
    "Country",
    "Order ID"
).show(n=10, truncate=False)

count_sales_df = (
    sales_df
    .select(
        "Region",
        "Country",
        "Order ID"
    ).groupBy(
        "Region",
        "Country"
    ).agg(
        count("Order ID")
    .alias(
        "Total Orders"
    )
    )
    .orderBy(
        "Total Orders",
        ascending=False
    )
)

count_sales_df.show(n=10, truncate=False)
print(f"Total Rows: {count_sales_df.count()}")
