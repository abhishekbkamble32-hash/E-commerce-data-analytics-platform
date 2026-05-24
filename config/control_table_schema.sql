-- ============================================================
-- Project : E-Commerce Data Analytics Platform
-- Config  : Metadata-driven pipeline control table
-- Desc    : ADF reads this table via Lookup Activity.
--           Adding a new source = one INSERT here.
--           No new pipeline build required.
-- Author  : Abhishek Kamble
-- ============================================================

CREATE TABLE dbo.pipeline_control (
    control_id           INT           IDENTITY(1,1) PRIMARY KEY,
    source_system        VARCHAR(100)  NOT NULL,       -- matches folder name in ADLS
    source_type          VARCHAR(100)  NOT NULL,       -- ADF source type string
    source_dataset       VARCHAR(200)  NOT NULL,       -- ADF dataset reference name
    watermark_column     VARCHAR(100)  NULL,           -- NULL = full load
    last_watermark_value VARCHAR(200)  NULL,
    load_frequency       VARCHAR(50)   NOT NULL,       -- daily | weekly | every_4_hours
    execution_order      INT           NOT NULL,       -- lower = runs first in ForEach
    is_active            BIT           NOT NULL DEFAULT 1,
    created_at           DATETIME      NOT NULL DEFAULT GETUTCDATE(),
    updated_at           DATETIME      NOT NULL DEFAULT GETUTCDATE()
);

-- ============================================================
-- Seed data — 4 sources for E-Commerce platform
-- ============================================================

INSERT INTO dbo.pipeline_control
    (source_system, source_type, source_dataset, watermark_column, last_watermark_value, load_frequency, execution_order)
VALUES
    ('orders',
     'RestSource',
     'ds_orders_rest_api',
     'order_updated_at',
     '2020-01-01T00:00:00Z',
     'daily',
     1),

    ('inventory',
     'SftpReadSettings',
     'ds_inventory_sftp',
     NULL,
     NULL,
     'daily',
     2),

    ('crm',
     'AzureSqlSource',
     'ds_crm_azure_sql',
     'updated_at',
     '2020-01-01T00:00:00Z',
     'daily',
     3),

    ('product_catalogue',
     'AzureSqlSource',
     'ds_product_catalogue_azure_sql',
     NULL,
     NULL,
     'weekly',
     4);

-- ============================================================
-- Stored procedure to update watermark after successful copy
-- Called by ADF SqlServerStoredProcedure activity
-- ============================================================

CREATE PROCEDURE dbo.usp_update_watermark
    @source_system      VARCHAR(100),
    @new_watermark      VARCHAR(200)
AS
BEGIN
    UPDATE dbo.pipeline_control
    SET    last_watermark_value = @new_watermark,
           updated_at           = GETUTCDATE()
    WHERE  source_system = @source_system;
END;
GO

-- ============================================================
-- Query: view current watermarks (use in ADF Monitor / debugging)
-- ============================================================

SELECT
    source_system,
    load_frequency,
    last_watermark_value,
    is_active,
    updated_at
FROM dbo.pipeline_control
ORDER BY execution_order;
