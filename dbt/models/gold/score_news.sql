-- Gold: investment news score per company — daily snapshot
-- grain: 1 row per ISIN, rebuilt each run
-- window: 45-day rolling, anchored on published_at (always non-null in gold.article_sentiment)
-- mention_count: number of scored articles in window
-- avg_sentiment: average Groq sentiment score (0–10) over window
-- investment_score: PERCENT_RANK of avg_sentiment rescaled to 1–10

{{ config(materialized='table') }}

with sentiment_45d as (
    select
        isin,
        ticker_bourso,
        matched_name,
        count(*)                            as mention_count_45d,
        round(avg(sentiment_score), 2)      as avg_sentiment_45d
    from {{ source('gold', 'article_sentiment') }}
    where published_at >= timestamp_sub(current_timestamp(), interval 45 day)
    group by isin, ticker_bourso, matched_name
),

ranked as (
    select
        *,
        percent_rank() over (order by avg_sentiment_45d) as pct_rank
    from sentiment_45d
)

select
    isin,
    ticker_bourso,
    matched_name,
    mention_count_45d,
    avg_sentiment_45d,
    greatest(1, cast(ceil(pct_rank * 10) as int64)) as investment_score,
    current_date('Europe/Paris')                     as score_date
from ranked
