from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Read_Bronze")
    .master("local[*]")
    .getOrCreate()
)

bronze_path = "bronze/ecommerce"

df = spark.read.parquet(bronze_path)

print("\n========== BRONZE DATA ==========")

print("Rows:", df.count())
print("Columns:", len(df.columns))

print("\n========== SCHEMA ==========")
df.printSchema()

print("\n========== SAMPLE ==========")
df.show(5, truncate=False)

spark.stop()