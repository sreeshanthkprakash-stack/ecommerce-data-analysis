from pyspark.sql import SparkSession

# ---------------------------------------
# 1. Create Spark session
# ---------------------------------------

spark = (
    SparkSession.builder
    .appName("Ecommerce_Bronze_Ingestion")
    .master("local[*]")
    .getOrCreate()
)

# ---------------------------------------
# 2. File paths
# ---------------------------------------

input_path = "data/raw/Ecommerce.csv"
output_path = "bronze/ecommerce"

# ---------------------------------------
# 3. Read raw CSV
# ---------------------------------------

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(input_path)
)

# ---------------------------------------
# 4. Display basic information
# ---------------------------------------

print("\n========== BRONZE INGESTION ==========")

print("Number of rows:", df.count())

print("Number of columns:", len(df.columns))

print("\n========== SCHEMA ==========")
df.printSchema()

print("\n========== SAMPLE DATA ==========")
df.show(5, truncate=False)

# ---------------------------------------
# 5. Write Bronze data as Parquet
# ---------------------------------------

(
    df.write
    .mode("overwrite")
    .parquet(output_path)
)

print("\nBronze data successfully written.")

# ---------------------------------------
# 6. Stop Spark
# ---------------------------------------

spark.stop()