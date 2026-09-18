# 🛒 Indian E-Commerce End-to-End Data Engineering & BI Analytics Platform

[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.x-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-14%2B-336791?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Power BI](https://img.shields.io/badge/Power%20BI-Desktop-F2C811?logo=powerbi&logoColor=black)](https://powerbi.microsoft.com/)
[![Python](https://img.shields.io/badge/Python-3.10%2B-3776AB?logo=python&logoColor=white)](https://www.python.org/)

An enterprise-grade **Medallion Architecture (Bronze ➔ Silver ➔ Gold)** Data Engineering and Business Intelligence pipeline built with **Apache PySpark**, **PostgreSQL Data Warehouse**, and an **8-Page Interactive Power BI Dashboard**.

---

## 📊 Dataset Reference

- **Source**: [Kaggle - Indian E-Commerce Customer Behavior and Purchase Dataset](https://www.kaggle.com/datasets/kundanbedmutha/indian-e-commerce-customer-behavior-and-purchase)
- **Description**: Captures user browsing sessions, customer demographics, marketing channel interactions, cart additions, payment types, purchase quantities, discounts, and customer churn metrics across the Indian e-commerce landscape.

---

## 📌 Architecture & Data Flow

```
Raw Kaggle CSV (data/raw/Ecommerce.csv)
                  │
                  ▼
         [ 🥉 Bronze Layer ]       ➔ Raw PySpark Ingestion (Immutably stored as Parquet)
                  │
                  ▼
         [ 🥈 Silver Layer ]       ➔ Data cleansing, schema standardization & normalization
                  │                  into 7 relational entities (customers, products, sessions, etc.)
                  │
                  ▼
         [ 🥇 Gold Layer ]         ➔ Star Schema Dimensional Model (6 Dimension tables + Fact table)
                  │                  with window-generated surrogate keys
                  │
                  ▼
      [ 🗄️ PostgreSQL DW ]         ➔ Bulk loaded via PySpark JDBC driver to relational `gold` schema
                  │
                  ▼
     [ 📊 Power BI Analytics ]     ➔ 8-Page Interactive BI Dashboard (ecommerce.pbit & ecommerce.pdf)
```

---

## 📂 Project Structure

```bash
Ecommerce-Data-Engineering/
│
├── data/
│   └── raw/                       # Raw dataset from Kaggle (Ecommerce.csv)
│
├── scripts/
│   ├── ingest_bronze.py           # Ingests raw CSV into immutable Bronze Parquet
│   ├── read_bronze.py             # Verifies & inspects Bronze layer
│   ├── profile_data.py            # Summary statistics and exploratory data profiling
│   ├── check_categories.py        # Categorical cardinality and value validation
│   ├── transform_silver.py        # Normalizes & cleans data into Silver entities
│   ├── build_gold.py              # Builds Star Schema (dim_* surrogate keys & fact_session)
│   └── load_gold_postgres.py      # Distributed batch loader into PostgreSQL via JDBC
│
├── bronze/                        # Bronze Parquet storage
├── silver/                        # Silver normalized tables (customers, dates, products, etc.)
├── gold/                          # Gold Star Schema Parquet tables
│
├── ecommerce_dw.sql               # PostgreSQL DDL Schema and Analytical SQL queries
├── ecommerce.pbit                 # Power BI Template connecting to PostgreSQL
├── ecommerce.pdf                  # Full 8-page Power BI Dashboard Report export
├── requirements.txt               # Python package dependencies
├── .gitignore                     # Git rules for virtual environments & temporary files
└── README.md                      # Complete project documentation
```

---

## 📈 Power BI Business Intelligence Report

The repository includes a pre-built Power BI report ([`ecommerce.pbit`](ecommerce.pbit)) and an exported PDF report ([`ecommerce.pdf`](ecommerce.pdf)) covering **8 strategic analytics pages**:

### 🎯 Key Performance Indicators (KPIs)
| KPI Metric | Value | Business Impact |
| :--- | :--- | :--- |
| **Total Revenue** | **₹10.12M** | Net sales volume generated |
| **Total Sessions** | **25,000** | High traffic volume across mobile, desktop & tablet |
| **Total Purchases** | **5,616** | Completed transactions |
| **Overall Conversion Rate** | **22.46%** | Session-to-purchase efficiency |
| **Average Order Value (AOV)** | **₹1,800** | Basket size spending efficiency |
| **Total Quantity Sold** | **62,000+** | Product units fulfilled |
| **Cart Abandonment Rate** | **65.20%** | Identified opportunity for re-targeting campaigns |
| **Repeat Customer Base** | **6,918 (81.9%)** | Strong retention and customer loyalty |

---

### 📑 Dashboard Pages Summary ([ecommerce.pdf](ecommerce.pdf))

1. **Executive Overview**: Executive-level summary of total revenue (₹10.12M), 25K sessions, monthly revenue pacing, product category performance, and funnel breakdown.
2. **Sales & Revenue Analysis**: Deep dive into AOV (₹1.80K), quantity distribution (62K units), payment method breakdown, and revenue by device.
3. **Customer Analytics**: Total vs. repeat customer ratio (8,442 total / 6,918 repeat), revenue per customer (₹1.20K), and user tier spending distribution.
4. **Product Analytics**: Category sales volume, average discount rates (~9.0%), conversion rates across product lines, and revenue-to-quantity correlation.
5. **Time & Trend Analysis**: Daily and monthly revenue trends (Jan–Dec 2024), weekday seasonality (peaking Tuesdays & Wednesdays), and monthly conversion rates.
6. **Marketing & Channel Analytics**: Channel-level attribution, session counts vs. revenue yields, and conversion rates by channel.
7. **Device & Payment Analytics**: Mobile (50.4%) vs. Desktop (39.55%) vs. Tablet (10.05%) share, conversion rate by device, and preferred payment gateways.
8. **Funnel & Conversion Analysis**: Multi-stage funnel analysis (25K Sessions ➔ 16K Cart Additions [64%] ➔ 5.6K Purchases [22.46%]) and category-level cart abandonment tracking.

---

## 🌟 Medallion Architecture Breakdown

### 🥉 Bronze Layer (`scripts/ingest_bronze.py`)
- Ingests raw CSV without schema mutation.
- Stores dataset in snappy-compressed **Parquet** format.
- Guarantees replayability and data lineage.

### 🥈 Silver Layer (`scripts/transform_silver.py`)
- Standardizes schema to `snake_case`.
- Removes duplicates, handles null values, and enforces data types.
- Splits the flat dataset into 7 relational entities:
  - `customers`
  - `products`
  - `dates`
  - `devices`
  - `marketing`
  - `payments`
  - `sessions`

### 🥇 Gold Layer (`scripts/build_gold.py`)
- Models a high-performance **Star Schema**:
  - **Dimension Tables**: `dim_customer`, `dim_product`, `dim_date`, `dim_device`, `dim_marketing`, `dim_payment` with deterministic surrogate keys (`customer_key`, `product_key`, etc.).
  - **Fact Table**: `fact_session` storing metrics (quantity, price, discount, profit, cart abandonment flag, purchase flag).

### 🗄️ PostgreSQL Data Warehouse (`scripts/load_gold_postgres.py` & `ecommerce_dw.sql`)
- PySpark writes Gold tables into PostgreSQL using the official PostgreSQL JDBC driver.
- Schema is isolated under the dedicated `gold` schema namespace.

---

## 🚀 Getting Started

### 1. Prerequisites
- **Python 3.10+**
- **Java 8, 11, or 17** (for Apache Spark)
- **PostgreSQL 14+**
- **Power BI Desktop** (to view `.pbit` template)

### 2. Installation & Setup

```bash
# Clone the repository
git clone https://github.com/sreeshanthkprakash-stack/ecommerce-data-analysis.git
cd ecommerce-data-analysis

# Create and activate virtual environment
python -m venv venv
.\venv\Scripts\Activate.ps1   # Windows
# source venv/bin/activate    # Linux/macOS

# Install dependencies
pip install -r requirements.txt
```

### 3. Pipeline Execution

Run the pipeline scripts in sequential order:

```bash
# Step 1: Ingest raw CSV to Bronze Parquet
python scripts/ingest_bronze.py

# Step 2: Clean & normalize into Silver tables
python scripts/transform_silver.py

# Step 3: Build Gold Star Schema (Dimensions & Fact)
python scripts/build_gold.py

# Step 4: Create tables in PostgreSQL
psql -U postgres -d ecommerce_dw -f ecommerce_dw.sql

# Step 5: Load Gold tables into PostgreSQL via Spark JDBC
python scripts/load_gold_postgres.py
```

---

## 🔍 Sample Analytical Queries ([ecommerce_dw.sql](ecommerce_dw.sql))

```sql
-- 1. Revenue & Units Sold by Product Category
SELECT 
    p.product_category,
    COUNT(f.session_key) AS total_sessions,
    SUM(f.quantity) AS total_units_sold,
    ROUND(SUM(f.price * f.quantity)::numeric, 2) AS total_revenue
FROM gold.fact_session f
JOIN gold.dim_product p ON f.product_key = p.product_key
GROUP BY p.product_category
ORDER BY total_revenue DESC;

-- 2. Marketing Channel Conversion & ROI
SELECT 
    m.marketing_channel,
    COUNT(f.session_key) AS total_sessions,
    SUM(CASE WHEN f.purchase = 1 THEN 1 ELSE 0 END) AS total_conversions,
    ROUND(AVG(CASE WHEN f.purchase = 1 THEN 1.0 ELSE 0.0 END) * 100, 2) AS conversion_rate_pct,
    ROUND(SUM(f.price * f.quantity)::numeric, 2) AS total_revenue
FROM gold.fact_session f
JOIN gold.dim_marketing m ON f.marketing_key = m.marketing_key
GROUP BY m.marketing_channel
ORDER BY total_revenue DESC;
```

---

## 🛠️ Technology Stack

- **Distributed Processing**: Apache PySpark
- **Storage Layer**: Apache Parquet (Snappy compressed)
- **Data Warehousing**: PostgreSQL 15 (Star Schema)
- **Business Intelligence**: Microsoft Power BI Desktop
- **Programming & Scripting**: Python 3.10, SQL
- **Dataset Source**: [Kaggle Indian E-Commerce Dataset](https://www.kaggle.com/datasets/kundanbedmutha/indian-e-commerce-customer-behavior-and-purchase)

---

## 📄 License
This project is licensed under the [MIT License](LICENSE).
