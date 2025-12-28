/* =========================================================
   STEP 1: CREATE CORE DIMENSION TABLES
   These tables store master data for customers and products
   ========================================================= */

-- Customer dimension: one row per customer
-- Used for customer segmentation and behavior analysis
CREATE TABLE customers (
    customer_id VARCHAR(10) PRIMARY KEY,
    customer_name VARCHAR(100),
    gender VARCHAR(10),
    age INT,
    city VARCHAR(50)
);

-- Product dimension: one row per product
-- Used for category-level and product-level performance analysis
CREATE TABLE products (
    product_id VARCHAR(10) PRIMARY KEY,
    product_name VARCHAR(100),
    category VARCHAR(50),
    unit_price DECIMAL(10,2)
);




/* =========================================================
   STEP 2: CREATE FACT TABLE FOR SALES TRANSACTIONS
   This table captures all transactional sales data
   ========================================================= */

-- Fact table storing transactional retail sales
-- Includes computed column for total_sales to avoid recalculation
CREATE TABLE retail_sales_data (
    order_id VARCHAR(20) PRIMARY KEY,
    order_date DATE,
    customer_id VARCHAR(10),
    product_id VARCHAR(10),
    store_id VARCHAR(10),
    quantity INT,
    unit_price DECIMAL(10,2),

    -- Computed column for revenue per transaction
    total_sales AS (quantity * unit_price) PERSISTED,

    -- Enforcing referential integrity
    CONSTRAINT fk_customer FOREIGN KEY (customer_id)
        REFERENCES customers(customer_id),
    CONSTRAINT fk_product FOREIGN KEY (product_id)
        REFERENCES products(product_id)
);




/* =========================================================
   STEP 3: DATA TYPE ADJUSTMENT FOR CSV IMPORT
   Temporarily convert order_date to VARCHAR for ingestion
   ========================================================= */

-- Change order_date datatype to VARCHAR to allow CSV import
ALTER TABLE retail_sales_data
ALTER COLUMN order_date VARCHAR(20);




/* =========================================================
   STEP 4: ADD CLEAN DATE COLUMN
   This column will store standardized DATE values
   ========================================================= */

-- Add a clean DATE column after import
ALTER TABLE retail_sales_data
ADD order_date_clean DATE;




/* =========================================================
   STEP 5: ATTEMPT DATE CONVERSION (INITIAL PASS)
   ========================================================= */

-- First attempt to convert order_date to DATE
UPDATE retail_sales_data
SET order_date_clean = TRY_CONVERT(DATE, order_date);




/* =========================================================
   STEP 6: DATA QUALITY CHECKS
   Identify rows where date conversion failed
   ========================================================= */

-- Check records where conversion failed
SELECT *
FROM retail_sales_data
WHERE order_date_clean IS NULL;

-- Inspect length and format of raw date values
SELECT TOP 20 order_date, LEN(order_date)
FROM retail_sales_data;

-- Identify invalid date strings
SELECT DISTINCT order_date
FROM retail_sales_data
WHERE ISDATE(order_date) = 0;




/* =========================================================
   STEP 7: HANDLE dd-mm-yyyy FORMAT
   Use style 105 for European date formats
   ========================================================= */

-- Convert dates stored as dd-mm-yyyy to yyyy-mm-dd
UPDATE retail_sales_data
SET order_date_clean = TRY_CONVERT(DATE, order_date, 105);




/* =========================================================
   STEP 8: FINAL DATA VALIDATION
   Ensure majority of records are successfully converted
   ========================================================= */

-- Summary of conversion success vs failure
SELECT 
    COUNT(*) AS total_rows,
    COUNT(order_date_clean) AS converted_rows,
    SUM(CASE WHEN order_date_clean IS NULL THEN 1 ELSE 0 END) AS null_rows
FROM retail_sales_data;




/* =========================================================
   STEP 9: MONTHLY REVENUE TREND ANALYSIS
   Identifies seasonality and revenue growth patterns
   ========================================================= */

SELECT
    FORMAT(order_date_clean, 'yyyy-MM') AS year_month,
    SUM(total_sales) AS revenue
FROM retail_sales_data
GROUP BY FORMAT(order_date_clean, 'yyyy-MM')
ORDER BY year_month;




/* =========================================================
   STEP 10: CATEGORY PERFORMANCE OVER TIME
   Highlights declining or high-performing categories
   ========================================================= */

WITH yearly_sales AS (
    SELECT
        p.category,
        YEAR(order_date_clean) AS sales_year,
        SUM(total_sales) AS revenue
    FROM retail_sales_data s
    JOIN products p 
        ON s.product_id = p.product_id
    GROUP BY p.category, YEAR(order_date_clean)
)
SELECT *
FROM yearly_sales
ORDER BY revenue DESC;




/* =========================================================
   STEP 11: CUSTOMER SEGMENTATION (NEW vs REPEAT)
   Derived dynamically based on purchase behavior
   ========================================================= */

WITH customer_orders AS (
    SELECT
        customer_id,
        COUNT(order_id) AS total_orders
    FROM retail_sales_data
    GROUP BY customer_id
)
SELECT
    CASE 
        WHEN total_orders > 1 THEN 'Repeat'
        ELSE 'New'
    END AS customer_type,
    COUNT(*) AS customers
FROM customer_orders
GROUP BY 
    CASE 
        WHEN total_orders > 1 THEN 'Repeat'
        ELSE 'New'
    END;




/* =========================================================
   STEP 12: TOP 5 PRODUCTS PER STORE
   Uses window function for ranking
   ========================================================= */

SELECT *
FROM (
    SELECT
        store_id,
        product_id,
        SUM(total_sales) AS revenue,
        RANK() OVER (
            PARTITION BY store_id 
            ORDER BY SUM(total_sales) DESC
        ) AS rnk
    FROM retail_sales_data
    GROUP BY store_id, product_id
) ranked
WHERE rnk <= 5;




/* =========================================================
   STEP 13: CUSTOMER LIFETIME VALUE ANALYSIS
   Identifies high-value customers
   ========================================================= */

SELECT
    customer_id,
    SUM(total_sales) AS lifetime_value
FROM retail_sales_data
GROUP BY customer_id
HAVING SUM(total_sales) > (
    -- Compare customer spend against average transaction value
    SELECT AVG(total_sales)
    FROM retail_sales_data
)
ORDER BY lifetime_value DESC;
