from pyspark.sql import SparkSession


# ============================================================
# 1. CREATE SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("Ecommerce_Gold_PostgreSQL_Load")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "org.postgresql:postgresql:42.7.3"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# 2. POSTGRESQL CONNECTION
# ============================================================

jdbc_url = (
    "jdbc:postgresql://localhost:5432/ecommerce_dw"
)

properties = {
    "user": "postgres",
    "password": "YOUR_POSTGRESQL_PASSWORD",
    "driver": "org.postgresql.Driver"
}


# ============================================================
# 3. READ GOLD PARQUET
# ============================================================

gold_base = "gold"

dim_customer = spark.read.parquet(
    f"{gold_base}/dim_customer"
)

dim_product = spark.read.parquet(
    f"{gold_base}/dim_product"
)

dim_date = spark.read.parquet(
    f"{gold_base}/dim_date"
)

dim_device = spark.read.parquet(
    f"{gold_base}/dim_device"
)

dim_marketing = spark.read.parquet(
    f"{gold_base}/dim_marketing"
)

dim_payment = spark.read.parquet(
    f"{gold_base}/dim_payment"
)

fact_session = spark.read.parquet(
    f"{gold_base}/fact_session"
)


# ============================================================
# 4. LOAD DIMENSIONS
# ============================================================

print("\nLoading DimCustomer...")

dim_customer.write \
    .jdbc(
        url=jdbc_url,
        table="gold.dim_customer",
        mode="append",
        properties=properties
    )

print("DimCustomer loaded.")


print("\nLoading DimProduct...")

dim_product.write \
    .jdbc(
        url=jdbc_url,
        table="gold.dim_product",
        mode="append",
        properties=properties
    )

print("DimProduct loaded.")


print("\nLoading DimDate...")

dim_date.write \
    .jdbc(
        url=jdbc_url,
        table="gold.dim_date",
        mode="append",
        properties=properties
    )

print("DimDate loaded.")


print("\nLoading DimDevice...")

dim_device.write \
    .jdbc(
        url=jdbc_url,
        table="gold.dim_device",
        mode="append",
        properties=properties
    )

print("DimDevice loaded.")


print("\nLoading DimMarketing...")

dim_marketing.write \
    .jdbc(
        url=jdbc_url,
        table="gold.dim_marketing",
        mode="append",
        properties=properties
    )

print("DimMarketing loaded.")


print("\nLoading DimPayment...")

dim_payment.write \
    .jdbc(
        url=jdbc_url,
        table="gold.dim_payment",
        mode="append",
        properties=properties
    )

print("DimPayment loaded.")


# ============================================================
# 5. LOAD FACT TABLE
# ============================================================

print("\nLoading FactSession...")

fact_session.write \
    .jdbc(
        url=jdbc_url,
        table="gold.fact_session",
        mode="append",
        properties=properties
    )

print("FactSession loaded.")


# ============================================================
# 6. FINAL MESSAGE
# ============================================================

print("\n" + "=" * 60)
print("GOLD DATA SUCCESSFULLY LOADED INTO POSTGRESQL")
print("=" * 60)


spark.stop()