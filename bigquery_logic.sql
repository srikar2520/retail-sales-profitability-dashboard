/* ============================================================
   PROJECT: Retail Sales & Profitability Analytics
   LAYER  : Clean / Transformation Layer
   TOOL   : Google BigQuery
   PURPOSE:
     - Clean raw Superstore data
     - Standardize column names
     - Handle inconsistent date formats
     - Create analytics-ready fields for BI tools
   ============================================================ */

CREATE OR REPLACE TABLE retail_analytics.superstore_clean AS

SELECT
    /* --------------------------------------------------------
       ORDER & CUSTOMER IDENTIFIERS
       -------------------------------------------------------- */

    -- Rename Order ID to snake_case for SQL/BI consistency
    `Order ID` AS order_id,

    /* --------------------------------------------------------
       DATE HANDLING
       Problem:
         - Order Date exists in multiple formats:
           1) MM/DD/YYYY
           2) MM-DD-YYYY
       Solution:
         - Use SAFE.PARSE_DATE to avoid query failures
         - Use COALESCE to select the first valid parsed date
       -------------------------------------------------------- */

    COALESCE(
        SAFE.PARSE_DATE('%m/%d/%Y', `Order Date`),
        SAFE.PARSE_DATE('%m-%d-%Y', `Order Date`)
    ) AS order_date,

    -- Create Year-Month field for trend analysis (YYYY-MM)
    FORMAT_DATE(
        '%Y-%m',
        COALESCE(
            SAFE.PARSE_DATE('%m/%d/%Y', `Order Date`),
            SAFE.PARSE_DATE('%m-%d-%Y', `Order Date`)
        )
    ) AS order_month,

    -- Customer identifier
    `Customer ID` AS customer_id,

    /* --------------------------------------------------------
       BUSINESS DIMENSIONS
       -------------------------------------------------------- */

    Segment,
    Region,
    Category,

    -- Rename Sub-Category for SQL friendliness
    `Sub-Category` AS sub_category,

    /* --------------------------------------------------------
       FACT / MEASURE COLUMNS
       -------------------------------------------------------- */

    Sales,
    Profit,
    Discount,

    /* --------------------------------------------------------
       DERIVED METRICS
       -------------------------------------------------------- */

    -- Profit Margin calculation
    -- SAFE_DIVIDE prevents division-by-zero errors
    SAFE_DIVIDE(Profit, Sales) AS profit_margin

FROM retail_analytics.superstore_raw

/* ------------------------------------------------------------
   DATA QUALITY FILTERS
   ------------------------------------------------------------ */

-- Remove invalid records where Sales is zero or negative
WHERE Sales > 0;
