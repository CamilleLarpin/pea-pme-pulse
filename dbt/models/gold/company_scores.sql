-- Gold: composite company score — historised daily snapshot
-- Grain: one row per (isin, score_date). All 571 companies appear on each run.
-- Composite: weighted average of 4 normalised scores (0–10 each), 25% each.
-- Null fallback: mid-point (5.0) applied per missing score dimension.
-- score_news/score_insider: normalised from [1–10] → [0–10] via (x-1)/9*10
-- score_stock/score_financials: already [0–10], used as-is

{{ config(
    materialized='incremental',
    unique_key=['isin', 'score_date']
) }}

with latest_companies as (
    select *
    from {{ ref('companies') }}
    qualify row_number() over (partition by isin order by snapshot_date desc) = 1
),

news as (
    -- Most recent score_news snapshot per isin
    select isin, investment_score
    from {{ ref('score_news') }}
    qualify row_number() over (partition by isin order by score_date desc) = 1
),

stock as (
    -- Most recent trading day score per isin
    select isin, score_technique
    from {{ ref('stocks_score') }}
    where is_latest
),

insider as (
    -- Most recent insider buy signal within last 45 days per isin
    select isin, score_1_10
    from {{ ref('score_insider') }}
    where signal_date >= date_sub(current_date('Europe/Paris'), interval 45 day)
    qualify row_number() over (partition by isin order by signal_date desc) = 1
),

financials as (
    -- Most recent fundamental score per isin
    -- Source: BQ gold layer directly (fallback until PR #51 is merged as a dbt model)
    select isin, score_fondamental
    from {{ source('gold', 'financials_score') }}
    qualify row_number() over (partition by isin order by date_cloture_exercice desc) = 1
)

select
    c.isin,
    c.name,
    c.ticker_bourso,
    c.snapshot_date,

    -- Individual normalised scores (0–10)
    round(coalesce((n.investment_score  - 1) / 9.0 * 10, 5.0), 2) as score_news,
    round(coalesce(st.score_technique,                   5.0), 2) as score_stock,
    round(coalesce((i.score_1_10       - 1) / 9.0 * 10, 5.0), 2) as score_insider,
    round(coalesce(f.score_fondamental,                  5.0), 2) as score_financials,

    -- Composite: weighted average 25% each (0–10)
    round(
          coalesce((n.investment_score  - 1) / 9.0 * 10, 5.0) * 0.25
        + coalesce(st.score_technique,                   5.0) * 0.25
        + coalesce((i.score_1_10       - 1) / 9.0 * 10, 5.0) * 0.25
        + coalesce(f.score_fondamental,                  5.0) * 0.25,
    2) as composite_score,

    current_date('Europe/Paris') as score_date

from latest_companies c
left join news      n  on c.isin = n.isin
left join stock     st on c.isin = st.isin
left join insider   i  on c.isin = i.isin
left join financials f on c.isin = f.isin
