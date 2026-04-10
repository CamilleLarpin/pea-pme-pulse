-- Silver: clean OHLCV data from Bronze yfinance ingestion
-- Incremental insert_overwrite on monthly Date partition (rolling month window).
-- Avoids BigQuery MERGE CPU cost — replaces affected partition(s), no JOIN overhead.
-- Transformations applied:
--   1. Deduplication on (isin, Date) — keeps latest ingestion
--   2. Filters invalid prices (Close <= 0 or Open <= 0)
--   3. Filters incomplete sessions — excludes today if market not yet closed
-- Dividends, Stock Splits, ingested_at are Bronze metadata — not propagated to Silver.

{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    on_schema_change='sync_all_columns',
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
        -- Rewrite from the 1st of the rolling month:
        --   days 1–7 of month → also covers previous month (handles late bronze arrivals)
        --   days 8+ of month  → current month only (single partition rewritten)
        and Date >= date_trunc(date_sub(current_date('Europe/Paris'), interval 7 day), month)
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
    yf_ticker
from deduped
