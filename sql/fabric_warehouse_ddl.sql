-- ============================================================
-- Project : E-Commerce Data Analytics Platform
-- Layer   : Gold — Fabric Warehouse DDL
-- Desc    : Star schema DDL for all fact and dimension tables
--           loaded into Microsoft Fabric Warehouse.
-- Author  : Abhishek Kamble
-- ============================================================

-- ============================================================
-- DIMENSION TABLES
-- ============================================================

CREATE TABLE dim_customer (
    customer_sk      BIGINT         NOT NULL,   -- surrogate key (SCD Type 2)
    customer_id      VARCHAR(50)    NOT NULL,   -- natural key
    customer_name    VARCHAR(200)   NULL,
    email            VARCHAR(200)   NULL,
    city             VARCHAR(100)   NULL,
    loyalty_tier     VARCHAR(50)    NULL,       -- tracked — changes trigger Type 2
    start_date       DATE           NOT NULL,
    end_date         DATE           NULL,       -- NULL = current record
    is_current       BIT            NOT NULL DEFAULT 1
);

-- SCD Type 1 — simple overwrite on attribute correction
CREATE TABLE dim_product (
    product_sk          BIGINT        NOT NULL,
    product_id          VARCHAR(50)   NOT NULL,
    product_name        VARCHAR(300)  NULL,
    category            VARCHAR(100)  NULL,
    sub_category        VARCHAR(100)  NULL,
    unit_price_standard DECIMAL(10,2) NULL,
    is_active           BIT           NOT NULL DEFAULT 1,
    last_updated        DATE          NULL
);

CREATE TABLE dim_date (
    date_key     INT           NOT NULL,   -- YYYYMMDD format, e.g. 20240115
    full_date    DATE          NOT NULL,
    year         INT           NOT NULL,
    quarter      INT           NOT NULL,
    month        INT           NOT NULL,
    month_name   VARCHAR(20)   NOT NULL,
    week         INT           NOT NULL,
    day_of_week  VARCHAR(15)   NOT NULL,
    is_weekend   BIT           NOT NULL
);

CREATE TABLE dim_warehouse (
    warehouse_sk    BIGINT       NOT NULL,
    warehouse_id    VARCHAR(50)  NOT NULL,
    warehouse_name  VARCHAR(200) NULL,
    city            VARCHAR(100) NULL,
    region          VARCHAR(100) NULL,
    country         VARCHAR(100) NULL,
    is_active       BIT          NOT NULL DEFAULT 1
);

-- ============================================================
-- FACT TABLES
-- ============================================================

CREATE TABLE fact_sales (
    order_id        VARCHAR(50)    NOT NULL,
    customer_sk     BIGINT         NOT NULL,   -- FK -> dim_customer
    product_sk      BIGINT         NOT NULL,   -- FK -> dim_product
    warehouse_sk    BIGINT         NOT NULL,   -- FK -> dim_warehouse
    date_key        INT            NOT NULL,   -- FK -> dim_date
    quantity        INT            NOT NULL,
    unit_price_usd  DECIMAL(10,2)  NOT NULL,
    discount        DECIMAL(5,2)   NOT NULL DEFAULT 0.00,
    gross_revenue   DECIMAL(14,2)  NOT NULL,
    net_revenue     DECIMAL(14,2)  NOT NULL,
    order_status    VARCHAR(50)    NOT NULL,
    load_date       DATE           NOT NULL    -- partition column
);

CREATE TABLE fact_inventory (
    product_sk          BIGINT        NOT NULL,
    warehouse_sk        BIGINT        NOT NULL,
    date_key            INT           NOT NULL,
    stock_on_hand       INT           NOT NULL,
    reorder_threshold   INT           NOT NULL,
    units_received      INT           NOT NULL DEFAULT 0,
    units_dispatched    INT           NOT NULL DEFAULT 0,
    stock_vs_threshold  INT           NOT NULL,   -- stock_on_hand - reorder_threshold
    load_date           DATE          NOT NULL
);

-- ============================================================
-- PRE-AGGREGATED KPI TABLES
-- Power BI hits these — not raw fact tables
-- ============================================================

CREATE TABLE kpi_monthly_revenue_by_category (
    year              INT            NOT NULL,
    month             INT            NOT NULL,
    category          VARCHAR(100)   NOT NULL,
    total_net_revenue DECIMAL(16,2)  NOT NULL,
    total_units_sold  INT            NOT NULL,
    order_count       INT            NOT NULL
);

CREATE TABLE kpi_customer_clv (
    customer_sk     BIGINT         NOT NULL,
    lifetime_value  DECIMAL(14,2)  NOT NULL,
    clv_bucket      VARCHAR(20)    NOT NULL   -- Platinum / Gold / Silver / Bronze
);

CREATE TABLE kpi_inventory_turnover (
    category          VARCHAR(100)  NOT NULL,
    total_dispatched  INT           NOT NULL,
    avg_stock         DECIMAL(12,2) NOT NULL,
    turnover_ratio    DECIMAL(8,4)  NOT NULL
);
