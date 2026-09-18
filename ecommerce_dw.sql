CREATE SCHEMA gold;

SELECT schema_name
FROM information_schema.schemata
WHERE schema_name = 'gold';

CREATE TABLE gold.dim_customer (
    customer_key BIGSERIAL PRIMARY KEY,
    customer_id BIGINT NOT NULL,
    user_type SMALLINT,
    location INTEGER
);

CREATE TABLE gold.dim_product (
    product_key BIGSERIAL PRIMARY KEY,
    product_id BIGINT NOT NULL,
    product_category SMALLINT
);

CREATE TABLE gold.dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL,
    day INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    weekday INTEGER NOT NULL,
    weekday_name VARCHAR(20) NOT NULL,
    quarter INTEGER NOT NULL,
    year INTEGER NOT NULL,
    season VARCHAR(20)
);

CREATE TABLE gold.dim_device (
    device_key BIGSERIAL PRIMARY KEY,
    device_type SMALLINT NOT NULL
);

CREATE TABLE gold.dim_marketing (
    marketing_key BIGSERIAL PRIMARY KEY,
    marketing_channel SMALLINT NOT NULL
);

CREATE TABLE gold.dim_payment (
    payment_key BIGSERIAL PRIMARY KEY,
    payment_method SMALLINT NOT NULL
);

CREATE TABLE gold.fact_session (
    session_key BIGSERIAL PRIMARY KEY,

    session_id VARCHAR(100) NOT NULL,

    customer_key BIGINT NOT NULL,
    product_key BIGINT NOT NULL,
    date_key INTEGER NOT NULL,
    device_key BIGINT NOT NULL,
    marketing_key BIGINT NOT NULL,
    payment_key BIGINT NOT NULL,

    quantity INTEGER,
    unit_price NUMERIC(12,2),
    discount_percent NUMERIC(5,2),
    discount_amount NUMERIC(12,2),
    revenue NUMERIC(14,2),
    revenue_normalized NUMERIC(14,2),

    pages_viewed INTEGER,
    time_on_site_sec INTEGER,

    added_to_cart SMALLINT,
    purchased SMALLINT,
    cart_abandoned SMALLINT,

    rating NUMERIC(3,2),
    review_text TEXT,
    review_helpful_votes INTEGER,

    CONSTRAINT fk_customer
        FOREIGN KEY (customer_key)
        REFERENCES gold.dim_customer(customer_key),

    CONSTRAINT fk_product
        FOREIGN KEY (product_key)
        REFERENCES gold.dim_product(product_key),

    CONSTRAINT fk_date
        FOREIGN KEY (date_key)
        REFERENCES gold.dim_date(date_key),

    CONSTRAINT fk_device
        FOREIGN KEY (device_key)
        REFERENCES gold.dim_device(device_key),

    CONSTRAINT fk_marketing
        FOREIGN KEY (marketing_key)
        REFERENCES gold.dim_marketing(marketing_key),

    CONSTRAINT fk_payment
        FOREIGN KEY (payment_key)
        REFERENCES gold.dim_payment(payment_key)
);


ALTER TABLE gold.fact_session
ADD COLUMN session_duration_bucket VARCHAR(50);

SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'gold'
ORDER BY table_name, ordinal_position;

TRUNCATE TABLE
    gold.fact_session,
    gold.dim_customer,
    gold.dim_product,
    gold.dim_date,
    gold.dim_device,
    gold.dim_marketing,
    gold.dim_payment
RESTART IDENTITY CASCADE;

SELECT COUNT(*) FROM gold.dim_customer;

SELECT COUNT(*) FROM gold.dim_product;

SELECT
    f.session_id,
    c.customer_id,
    p.product_id,
    d.full_date,
    dv.device_type,
    m.marketing_channel,
    pm.payment_method,
    f.revenue,
    f.purchased
FROM gold.fact_session f
JOIN gold.dim_customer c
    ON f.customer_key = c.customer_key
JOIN gold.dim_product p
    ON f.product_key = p.product_key
JOIN gold.dim_date d
    ON f.date_key = d.date_key
JOIN gold.dim_device dv
    ON f.device_key = dv.device_key
JOIN gold.dim_marketing m
    ON f.marketing_key = m.marketing_key
JOIN gold.dim_payment pm
    ON f.payment_key = pm.payment_key
LIMIT 10;


SELECT 'dim_customer' AS table_name, COUNT(*) AS row_count
FROM gold.dim_customer

UNION ALL

SELECT 'dim_product', COUNT(*)
FROM gold.dim_product

UNION ALL

SELECT 'dim_date', COUNT(*)
FROM gold.dim_date

UNION ALL

SELECT 'dim_device', COUNT(*)
FROM gold.dim_device

UNION ALL

SELECT 'dim_marketing', COUNT(*)
FROM gold.dim_marketing

UNION ALL

SELECT 'dim_payment', COUNT(*)
FROM gold.dim_payment

UNION ALL

SELECT 'fact_session', COUNT(*)
FROM gold.fact_session;

SELECT COUNT(*) AS orphan_customer_records
FROM gold.fact_session f
LEFT JOIN gold.dim_customer c
    ON f.customer_key = c.customer_key
WHERE c.customer_key IS NULL;

SELECT
    d.year,
    d.month,
    d.month_name,
    COUNT(*) AS sessions,
    SUM(f.purchased) AS purchases,
    ROUND(SUM(f.revenue), 2) AS revenue
FROM gold.fact_session f
JOIN gold.dim_date d
    ON f.date_key = d.date_key
GROUP BY
    d.year,
    d.month,
    d.month_name
ORDER BY
    d.year,
    d.month;

SELECT
    COUNT(*) AS invalid_purchase_revenue
FROM gold.fact_session
WHERE
    (purchased = 0 AND revenue <> 0)
    OR
    (purchased = 1 AND revenue <= 0);