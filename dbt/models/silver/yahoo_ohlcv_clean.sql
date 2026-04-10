-- Silver: clean OHLCV data from Bronze yfinance ingestion
-- Incremental merge on (isin, Date) — only processes new Bronze rows each run.
-- Transformations applied:
--   1. Deduplication on (isin, Date) — keeps latest ingestion
--   2. Filters invalid prices (Close <= 0 or Open <= 0)
--   3. Filters incomplete sessions — excludes today if market not yet closed
--   4. Computes last_trading_date per ISIN (MAX(Date) window function)
-- Dividends, Stock Splits, ingested_at are Bronze metadata — not propagated to Silver.

{{ config(
    materialized='incremental',
    unique_key=['isin', 'Date'],
    incremental_strategy='merge',
    on_schema_change='fail',
    partition_by={'field': 'Date', 'data_type': 'date', 'granularity': 'month'},
    cluster_by=['isin'],
) }}

with raw as (
    select
        Date,
        Open,
        High,
        Low,
        Close,
        Volume,
        isin,
        yf_ticker,
        ingested_at
    from {{ source('bronze', 'yfinance_ohlcv') }}
    where
        isin is not null
        and Close > 0
        and Open > 0
        -- Exclude today's session if market is not yet closed (before 18h Paris)
        and Date <= if(
            extract(hour from datetime(current_timestamp(), 'Europe/Paris')) >= 18,
            current_date('Europe/Paris'),
            date_sub(current_date('Europe/Paris'), interval 1 day)
        )
        {% if is_incremental() %}
        and ingested_at > (select max(ingested_at) from {{ this }})
        {% endif %}
),

deduped as (
    select *
    from raw
    qualify row_number() over (
        partition by isin, Date
        order by ingested_at desc
    ) = 1
)

select
    Date,
    Open,
    High,
    Low,
    Close,
    Volume,
    isin,
    yf_ticker,
    max(Date) over (partition by isin) as last_trading_date
from deduped
