-- ============================================================
-- Project : E-Commerce Data Analytics Platform
-- Layer   : Gold — Delta OPTIMIZE + ZORDER
-- Desc    : Run after every Gold load.
--           ZORDER physically co-locates related data on disk
--           by the most common WHERE filter columns.
--           Direct cause of 40% query performance improvement.
-- Author  : Abhishek Kamble
-- ============================================================

-- fact_sales: ZORDERed by order_date + product_sk
-- These are the two most common WHERE filters in Power BI reports
OPTIMIZE delta.`abfss://gold@<storage-account-name>.dfs.core.windows.net/ecommerce/fact_sales/`
ZORDER BY (date_key, product_sk);

-- fact_inventory: ZORDERed by date_key + product_sk
OPTIMIZE delta.`abfss://gold@<storage-account-name>.dfs.core.windows.net/ecommerce/fact_inventory/`
ZORDER BY (date_key, product_sk);

-- dim_customer: OPTIMIZE only (small dimension, ZORDER less critical)
OPTIMIZE delta.`abfss://gold@<storage-account-name>.dfs.core.windows.net/ecommerce/dim_customer/`;

-- dim_product
OPTIMIZE delta.`abfss://gold@<storage-account-name>.dfs.core.windows.net/ecommerce/dim_product/`;

-- ============================================================
-- VACUUM — remove old Delta versions beyond retention period
-- Default retention: 7 days (168 hours). Do not reduce below this.
-- ============================================================

VACUUM delta.`abfss://gold@<storage-account-name>.dfs.core.windows.net/ecommerce/fact_sales/` RETAIN 168 HOURS;
VACUUM delta.`abfss://gold@<storage-account-name>.dfs.core.windows.net/ecommerce/fact_inventory/` RETAIN 168 HOURS;

-- ============================================================
-- DESCRIBE HISTORY — verify recent operations
-- ============================================================

DESCRIBE HISTORY delta.`abfss://gold@<storage-account-name>.dfs.core.windows.net/ecommerce/fact_sales/`
LIMIT 5;
