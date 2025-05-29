-- dags/sql/create_staging_table.sql

CREATE TABLE IF NOT EXISTS public.pricebook_analytics_staging (
    product_id VARCHAR(255),
    product_name VARCHAR(255),
    pricebook_id VARCHAR(255),
    pricebook_name VARCHAR(255),
    unit_price NUMERIC,
    currency VARCHAR(10),
    effective_date DATE,
    end_date DATE,
    last_updated_at TIMESTAMP
);

-- Truncate for idempotency: ensures the staging table is empty before new data is inserted.
TRUNCATE TABLE public.pricebook_analytics_staging;