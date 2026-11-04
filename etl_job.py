import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, sum as spark_sum

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Extract data from S3
source_path = "s3://sakthi-mini-datawarehouse-2026/sales_data.csv"

df = spark.read.option("header", "true") \
    .option("inferSchema", "true").csv(source_path)

# Transform: Remove zero-dollar sales
df = df.withColumn("amount", col("amount").cast("double"))
df = df.filter(col("amount") > 0)

# Fact table
sales_fact = df.select(
    "sale_id", "product_id", "quantity", "amount"
)

# Product dimension
product_dim = df.select(
    "product_id", "product_name", "category"
).dropDuplicates(["product_id"])

# Aggregated sales report
sales_report = df.groupBy(
    "product_id", "product_name", "category"
).agg(
    spark_sum("amount").alias("total_sales")
)

# Load into S3
output_path = "s3://sakthi-mini-datawarehouse-2026/warehouse/"

sales_fact.write.mode("overwrite").parquet(
    output_path + "sales_fact/"
)

product_dim.write.mode("overwrite").parquet(
    output_path + "product_dim/"
)

sales_report.write.mode("overwrite").option(
    "header", "true"
).csv(output_path + "sales_report/")

job.commit()
