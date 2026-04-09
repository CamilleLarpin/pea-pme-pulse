-- Gold: enriched PEA-PME company list — latest silver.companies snapshot + current news score
-- Grain: one row per ISIN — rebuilt on every pipeline run
-- Left join: all 571 companies appear; score columns are null when no scored articles exist yet

{{ config(materialized='table') }}

with latest as (
    select *
    from {{ ref('companies') }}
    qualify row_number() over (partition by isin order by snapshot_date desc) = 1
),

scores as (
    select
        isin,
        investment_score,
        mention_count_45d,
        avg_sentiment_45d,
        score_date
    from {{ ref('score_news') }}
)

select
    c.isin,
    c.name,
    c.ticker_bourso,
    c.snapshot_date,
    s.investment_score,
    s.mention_count_45d,
    s.avg_sentiment_45d,
    s.score_date
from latest c
left join scores s on c.isin = s.isin
order by coalesce(s.investment_score, 0) desc
