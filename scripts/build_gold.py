from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    row_number,
    date_format,
    dayofmonth,
    month,
    monthname,
    dayofweek,
    quarter,
    year,
    when
)
from pyspark.sql.window import Window


# --------------------------------------------------
# 1. START SPARK
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("Ecommerce_Gold_Layer")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# 2. READ SILVER DATA
# --------------------------------------------------

silver_base = "silver"

customers = spark.read.parquet(f"{silver_base}/customers")
products = spark.read.parquet(f"{silver_base}/products")
dates = spark.read.parquet(f"{silver_base}/dates")
devices = spark.read.parquet(f"{silver_base}/devices")
marketing = spark.read.parquet(f"{silver_base}/marketing")
payments = spark.read.parquet(f"{silver_base}/payments")
sessions = spark.read.parquet(f"{silver_base}/sessions")


# --------------------------------------------------
# 3. DIM CUSTOMER
# --------------------------------------------------

customer_window = Window.orderBy("customer_id")

dim_customer = (
    customers
    .withColumn(
        "customer_key",
        row_number().over(customer_window)
    )
    .select(
        col("customer_key").cast("long"),
        col("customer_id").cast("long"),
        col("user_type").cast("int"),
        col("location").cast("int")
    )
)


# --------------------------------------------------
# 4. DIM PRODUCT
# --------------------------------------------------

product_window = Window.orderBy("product_id")

dim_product = (
    products
    .withColumn(
        "product_key",
        row_number().over(product_window)
    )
    .select(
        col("product_key").cast("long"),
        col("product_id").cast("long"),
        col("product_category").cast("int")
    )
)


# --------------------------------------------------
# 5. DIM DATE
# --------------------------------------------------

dim_date = (
    dates
    .withColumn("full_date", col("visit_date"))
    .withColumn(
        "date_key",
        date_format("visit_date", "yyyyMMdd").cast("int")
    )
    .withColumn("day", dayofmonth("visit_date"))
    .withColumn("month", month("visit_date"))
    .withColumn("month_name", monthname("visit_date"))
    .withColumn("weekday", dayofweek("visit_date"))
    .withColumn(
        "weekday_name",
        date_format("visit_date", "EEEE")
    )
    .withColumn("quarter", quarter("visit_date"))
    .withColumn("year", year("visit_date"))
    .select(
        "date_key",
        "full_date",
        "day",
        "month",
        "month_name",
        "weekday",
        "weekday_name",
        "quarter",
        "year"
    )
)


# --------------------------------------------------
# 6. DIM DEVICE
# --------------------------------------------------

device_window = Window.orderBy("device_type")

dim_device = (
    devices
    .withColumn(
        "device_key",
        row_number().over(device_window)
    )
    .select(
        col("device_key").cast("long"),
        col("device_type").cast("int")
    )
)


# --------------------------------------------------
# 7. DIM MARKETING
# --------------------------------------------------

marketing_window = Window.orderBy("marketing_channel")

dim_marketing = (
    marketing
    .withColumn(
        "marketing_key",
        row_number().over(marketing_window)
    )
    .select(
        col("marketing_key").cast("long"),
        col("marketing_channel").cast("int")
    )
)


# --------------------------------------------------
# 8. DIM PAYMENT
# --------------------------------------------------

payment_window = Window.orderBy("payment_method")

dim_payment = (
    payments
    .withColumn(
        "payment_key",
        row_number().over(payment_window)
    )
    .select(
        col("payment_key").cast("long"),
        col("payment_method").cast("int")
    )
)


# --------------------------------------------------
# 9. CREATE FACT TABLE
# --------------------------------------------------

fact_window = Window.orderBy("session_id")

fact_session = (
    sessions

    # Customer surrogate key
    .join(
        dim_customer.select(
            "customer_key",
            "customer_id"
        ),
        on="customer_id",
        how="left"
    )

    # Product surrogate key
    .join(
        dim_product.select(
            "product_key",
            "product_id"
        ),
        on="product_id",
        how="left"
    )

    # Date surrogate key
    .join(
        dim_date.select(
            "date_key",
            col("full_date").alias("visit_date")
        ),
        on="visit_date",
        how="left"
    )

    # Device surrogate key
    .join(
        dim_device.select(
            "device_key",
            "device_type"
        ),
        on="device_type",
        how="left"
    )

    # Marketing surrogate key
    .join(
        dim_marketing.select(
            "marketing_key",
            "marketing_channel"
        ),
        on="marketing_channel",
        how="left"
    )

    # Payment surrogate key
    .join(
        dim_payment.select(
            "payment_key",
            "payment_method"
        ),
        on="payment_method",
        how="left"
    )

    # Generate fact surrogate key
    .withColumn(
        "session_key",
        row_number().over(fact_window)
    )

    .select(
        col("session_key").cast("long"),
        col("session_id"),

        col("customer_key"),
        col("product_key"),
        col("date_key"),
        col("device_key"),
        col("marketing_key"),
        col("payment_key"),

        col("quantity"),
        col("unit_price"),
        col("discount_percent"),
        col("discount_amount"),
        col("revenue"),
        col("revenue_normalized"),

        col("pages_viewed"),
        col("time_on_site_sec"),
        col("session_duration_bucket"),

        col("added_to_cart"),
        col("purchased"),
        col("cart_abandoned"),

        col("rating"),
        col("review_text"),
        col("review_helpful_votes")
    )
)


# --------------------------------------------------
# 10. WRITE GOLD DATA
# --------------------------------------------------

gold_base = "gold"

dim_customer.write.mode("overwrite").parquet(
    f"{gold_base}/dim_customer"
)

dim_product.write.mode("overwrite").parquet(
    f"{gold_base}/dim_product"
)

dim_date.write.mode("overwrite").parquet(
    f"{gold_base}/dim_date"
)

dim_device.write.mode("overwrite").parquet(
    f"{gold_base}/dim_device"
)

dim_marketing.write.mode("overwrite").parquet(
    f"{gold_base}/dim_marketing"
)

dim_payment.write.mode("overwrite").parquet(
    f"{gold_base}/dim_payment"
)

fact_session.write.mode("overwrite").parquet(
    f"{gold_base}/fact_session"
)


# --------------------------------------------------
# 11. VALIDATION
# --------------------------------------------------

print("\n========== GOLD LAYER VALIDATION ==========")

print("\nDimCustomer:")
print(dim_customer.count())
dim_customer.show(5, truncate=False)

print("\nDimProduct:")
print(dim_product.count())
dim_product.show(5, truncate=False)

print("\nDimDate:")
print(dim_date.count())
dim_date.show(5, truncate=False)

print("\nDimDevice:")
print(dim_device.count())
dim_device.show(5, truncate=False)

print("\nDimMarketing:")
print(dim_marketing.count())
dim_marketing.show(5, truncate=False)

print("\nDimPayment:")
print(dim_payment.count())
dim_payment.show(5, truncate=False)

print("\nFactSession:")
print(fact_session.count())
fact_session.show(5, truncate=False)


# --------------------------------------------------
# 12. CHECK FACT FOREIGN KEYS
# --------------------------------------------------

print("\n========== FOREIGN KEY VALIDATION ==========")

fk_columns = [
    "customer_key",
    "product_key",
    "date_key",
    "device_key",
    "marketing_key",
    "payment_key"
]

for column in fk_columns:
    null_count = fact_session.filter(
        col(column).isNull()
    ).count()

    print(f"{column}: {null_count} NULL values")


print("\nGold layer build completed successfully.")

spark.stop()