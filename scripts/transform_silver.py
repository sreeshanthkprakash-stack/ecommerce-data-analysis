from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    to_date,
    when
)

# ============================================================
# 1. CREATE SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("Ecommerce_Silver_ETL")
    .master("local[*]")
    .getOrCreate()
)

print("\n" + "=" * 60)
print("STARTING SILVER ETL")
print("=" * 60)


# ============================================================
# 2. READ BRONZE DATA
# ============================================================

bronze_path = "bronze/ecommerce"

df = spark.read.parquet(bronze_path)

print("\nBronze rows:", df.count())
print("Bronze columns:", len(df.columns))


# ============================================================
# 3. DISPLAY ORIGINAL SCHEMA
# ============================================================

print("\n" + "=" * 60)
print("ORIGINAL SCHEMA")
print("=" * 60)

df.printSchema()


# ============================================================
# 4. STANDARDIZE COLUMN NAMES
# ============================================================

df = df.toDF(
    *[
        column.strip().lower().replace(" ", "_")
        for column in df.columns
    ]
)

print("\n" + "=" * 60)
print("STANDARDIZED COLUMNS")
print("=" * 60)

print(df.columns)


# ============================================================
# 5. STANDARDIZE DATA TYPES
# ============================================================

df = (
    df
    .withColumn("session_id", col("session_id").cast("string"))
    .withColumn("customer_id", col("customer_id").cast("long"))
    .withColumn("product_id", col("product_id").cast("long"))

    .withColumn("user_type", col("user_type").cast("int"))
    .withColumn("location", col("location").cast("int"))

    .withColumn("device_type", col("device_type").cast("int"))
    .withColumn("marketing_channel", col("marketing_channel").cast("int"))
    .withColumn("product_category", col("product_category").cast("int"))
    .withColumn("payment_method", col("payment_method").cast("int"))

    .withColumn("pages_viewed", col("pages_viewed").cast("int"))
    .withColumn("time_on_site_sec", col("time_on_site_sec").cast("int"))

    .withColumn("quantity", col("quantity").cast("int"))

    .withColumn("unit_price", col("unit_price").cast("double"))
    .withColumn("discount_percent", col("discount_percent").cast("double"))
    .withColumn("discount_amount", col("discount_amount").cast("double"))
    .withColumn("revenue", col("revenue").cast("double"))
    .withColumn("revenue_normalized", col("revenue_normalized").cast("double"))

    .withColumn("added_to_cart", col("added_to_cart").cast("int"))
    .withColumn("purchased", col("purchased").cast("int"))
    .withColumn("cart_abandoned", col("cart_abandoned").cast("int"))

    .withColumn("rating", col("rating").cast("double"))
    .withColumn("review_helpful_votes", col("review_helpful_votes").cast("int"))
)

print("\nData types standardized.")


# ============================================================
# 6. CONVERT VISIT DATE
# ============================================================

df = df.withColumn(
    "visit_date",
    to_date(col("visit_date"), "dd-MM-yyyy")
)

print("\nvisit_date converted to DATE.")


# ============================================================
# 7. REMOVE EXACT DUPLICATE ROWS
# ============================================================

before_duplicates = df.count()

df = df.dropDuplicates()

after_duplicates = df.count()

print("\n" + "=" * 60)
print("DUPLICATE CHECK")
print("=" * 60)

print("Rows before:", before_duplicates)
print("Rows after :", after_duplicates)
print("Duplicates removed:", before_duplicates - after_duplicates)


# ============================================================
# 8. NULL CHECK
# ============================================================

print("\n" + "=" * 60)
print("NULL CHECK")
print("=" * 60)

for column in df.columns:
    null_count = df.filter(col(column).isNull()).count()

    if null_count > 0:
        print(column, ":", null_count)


# ============================================================
# 9. FINAL ROW COUNT
# ============================================================

print("\n" + "=" * 60)
print("SILVER DATASET SUMMARY")
print("=" * 60)

print("Rows:", df.count())
print("Columns:", len(df.columns))


# ============================================================
# 10. SHOW SAMPLE
# ============================================================

df.show(5, truncate=False)


# ============================================================
# DATA QUALITY VALIDATION
# ============================================================

print("\n" + "=" * 60)
print("DATA QUALITY VALIDATION")
print("=" * 60)


# Quantity
invalid_quantity = df.filter(
    col("quantity") < 0
).count()

print("Invalid quantity:", invalid_quantity)


# Unit price
invalid_price = df.filter(
    col("unit_price") < 0
).count()

print("Invalid unit price:", invalid_price)


# Discount
invalid_discount = df.filter(
    (col("discount_percent") < 0) |
    (col("discount_percent") > 100)
).count()

print("Invalid discount:", invalid_discount)


# Revenue
invalid_revenue = df.filter(
    col("revenue") < 0
).count()

print("Invalid revenue:", invalid_revenue)


# Purchased
invalid_purchase_flag = df.filter(
    ~col("purchased").isin(0, 1)
).count()

print("Invalid purchased flag:", invalid_purchase_flag)


# Added to cart
invalid_cart_flag = df.filter(
    ~col("added_to_cart").isin(0, 1)
).count()

print("Invalid added_to_cart flag:", invalid_cart_flag)


# Cart abandoned
invalid_abandoned_flag = df.filter(
    ~col("cart_abandoned").isin(0, 1)
).count()

print("Invalid cart_abandoned flag:", invalid_abandoned_flag)

invalid_purchase_revenue = df.filter(
    (
        (col("purchased") == 0) &
        (col("revenue") != 0)
    )
    |
    (
        (col("purchased") == 1) &
        (col("revenue") <= 0)
    )
).count()

print(
    "Purchase/revenue inconsistencies:",
    invalid_purchase_revenue
)


# ============================================================
# 11. CREATE CUSTOMER DATASET
# ============================================================

customers = (
    df
    .select(
        "customer_id",
        "user_type",
        "location"
    )
    .dropDuplicates(["customer_id"])
)

print("\n========== CUSTOMERS ==========")
print("Customer records:", customers.count())

customers.show(5)


# ============================================================
# 12. CREATE PRODUCT DATASET
# ============================================================

products = (
    df
    .select(
        "product_id",
        "product_category"
    )
    .dropDuplicates(["product_id"])
)

print("\n========== PRODUCTS ==========")
print("Product records:", products.count())

products.show(5)


# ============================================================
# 13. CREATE DEVICE DATASET
# ============================================================

devices = (
    df
    .select("device_type")
    .dropDuplicates()
    .orderBy("device_type")
)

print("\n========== DEVICES ==========")
print("Device records:", devices.count())

devices.show()

# ============================================================
# 14. CREATE MARKETING DATASET
# ============================================================

marketing = (
    df
    .select("marketing_channel")
    .dropDuplicates()
    .orderBy("marketing_channel")
)

print("\n========== MARKETING ==========")
print("Marketing records:", marketing.count())

marketing.show()


# ============================================================
# 15. CREATE PAYMENT DATASET
# ============================================================

payments = (
    df
    .select("payment_method")
    .dropDuplicates()
    .orderBy("payment_method")
)

print("\n========== PAYMENT METHODS ==========")
print("Payment records:", payments.count())

payments.show()


# ============================================================
# 16. CREATE DATE DATASET
# ============================================================

dates = (
    df
    .select("visit_date")
    .dropDuplicates()
    .orderBy("visit_date")
)

print("\n========== DATES ==========")
print("Date records:", dates.count())

dates.show(10)


# ============================================================
# 17. CREATE SESSION DATASET
# ============================================================

sessions = df.select(
    "session_id",
    "customer_id",
    "product_id",
    "visit_date",
    "device_type",
    "marketing_channel",
    "payment_method",

    "pages_viewed",
    "time_on_site_sec",
    "session_duration_bucket",

    "quantity",
    "unit_price",
    "discount_percent",
    "discount_amount",

    "revenue",
    "revenue_normalized",

    "added_to_cart",
    "purchased",
    "cart_abandoned",

    "rating",
    "review_text",
    "review_helpful_votes"
)

print("\n========== SESSIONS ==========")
print("Session records:", sessions.count())

sessions.show(5, truncate=False)


# ============================================================
# 18. WRITE SILVER DATASETS
# ============================================================

customers.write.mode("overwrite").parquet(
    "silver/customers"
)

products.write.mode("overwrite").parquet(
    "silver/products"
)

dates.write.mode("overwrite").parquet(
    "silver/dates"
)

devices.write.mode("overwrite").parquet(
    "silver/devices"
)

marketing.write.mode("overwrite").parquet(
    "silver/marketing"
)

payments.write.mode("overwrite").parquet(
    "silver/payments"
)

sessions.write.mode("overwrite").parquet(
    "silver/sessions"
)

print("\n" + "=" * 60)
print("SILVER DATASETS WRITTEN SUCCESSFULLY")
print("=" * 60)

# ============================================================
# 19. STOP SPARK
# ============================================================

spark.stop()