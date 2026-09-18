from pathlib import Path
import pandas as pd

# Project root directory
BASE_DIR = Path(__file__).resolve().parent.parent

# CSV location
file_path = BASE_DIR / "data" / "raw" / "Ecommerce.csv"

df = pd.read_csv(file_path)

columns = [
    "user_type",
    "location",
    "device_type",
    "marketing_channel",
    "product_category",
    "payment_method"
]

for column in columns:
    print("\n" + "=" * 50)
    print(column)
    print("=" * 50)

    print("Unique values:")
    print(df[column].unique())

    print("\nNumber of unique values:")
    print(df[column].nunique())

    print("\nFrequency:")
    print(df[column].value_counts().sort_index())