-- Silver: unified RSS articles from all sources
-- Cleans and deduplicates Bronze RSS tables into a single company-matched article feed.
-- published_at: parsed from RFC 2822 string (e.g. "Thu, 02 Apr 2026 15:30:00 +0000")
-- Dedup key: (lower(title), isin) — same article matched to same company across sources

with yahoo as (
    select
        title,
        link,
        summary,
        matched_name,
        cast(match_score as float64) as match_score,
        isin,
        ticker_bourso,
        fetched_at,
        'yahoo_rss' as source,
        safe.parse_timestamp('%a, %d %b %Y %H:%M:%S %Z', published) as published_at
    from {{ source('bronze', 'yahoo_rss') }}
    where isin is not null
),

google_news as (
    select
        title,
        link,
        summary,
        matched_name,
        cast(match_score as float64) as match_score,
        isin,
        ticker_bourso,
        fetched_at,
        'google_news_rss' as source,
        safe.parse_timestamp('%a, %d %b %Y %H:%M:%S %Z', published) as published_at
    from {{ source('bronze', 'google_news_rss') }}
    where isin is not null
),

unioned as (
    select * from yahoo
    union all
    select * from google_news
),

deduped as (
    select *
    from unioned
    qualify row_number() over (
        partition by lower(title), isin
        order by fetched_at desc
    ) = 1
)

select * from deduped
