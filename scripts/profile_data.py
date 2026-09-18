from pathlib import Path
import pandas as pd

# Project root directory
BASE_DIR = Path(__file__).resolve().parent.parent

# CSV location
file_path = BASE_DIR / "data" / "raw" / "Ecommerce.csv"

df = pd.read_csv(file_path)

# -----------------------------
# 2. Basic information
# -----------------------------

print("\n========== DATASET SHAPE ==========")
print(df.shape)

print("\n========== COLUMN NAMES ==========")
print(df.columns.tolist())

print("\n========== DATA TYPES ==========")
print(df.dtypes)

print("\n========== FIRST 5 ROWS ==========")
print(df.head())

print("\n========== MISSING VALUES ==========")
print(df.isnull().sum())

print("\n========== DUPLICATE ROWS ==========")
print(df.duplicated().sum())

print("\n========== NUMERICAL SUMMARY ==========")
print(df.describe())

print("\n========== UNIQUE VALUES ==========")

for column in df.columns:
    print(f"{column}: {df[column].nunique()} unique values")
    
print("\n========== SESSION ID CHECK ==========")

print("Total rows:", len(df))
print("Unique sessions:", df["session_id"].nunique())

print("\n========== CUSTOMER ID CHECK ==========")

print("Unique customers:", df["customer_id"].nunique())

print("\n========== CUSTOMER SESSION FREQUENCY ==========")

customer_sessions = df.groupby("customer_id")["session_id"].count()

print(customer_sessions.describe())

categorical_columns = [
    "device_type",
    "user_type",
    "marketing_channel",
    "product_category",
    "payment_method",
    "location",
    "visit_season"
]

for column in categorical_columns:

    print(f"\n========== {column.upper()} ==========")

    print(df[column].value_counts().sort_index())
    
business_columns = [
    "quantity",
    "unit_price",
    "discount_percent",
    "discount_amount",
    "revenue",
    "pages_viewed",
    "time_on_site_sec",
    "rating",
    "review_helpful_votes"
]

print("\n========== BUSINESS METRICS ==========")

print(df[business_columns].describe())