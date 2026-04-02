-- Silver: unified RSS articles from all sources
-- Incremental merge on row_id — only processes new Bronze rows each run.
-- row_id: surrogate key on (lower(title), isin) — stable dedup key across sources
-- published_at: parsed from RFC 2822 string (e.g. "Thu, 02 Apr 2026 15:30:00 GMT")



with yahoo as (
    select
        to_hex(md5(concat(lower(title), '|', isin))) as row_id,
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
    from `bootcamp-project-pea-pme`.`bronze`.`yahoo_rss`
    where isin is not null
    
    and fetched_at > (select max(fetched_at) from `bootcamp-project-pea-pme`.`silver`.`rss_articles`)
    
),

google_news as (
    select
        to_hex(md5(concat(lower(title), '|', isin))) as row_id,
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
    from `bootcamp-project-pea-pme`.`bronze`.`google_news_rss`
    where isin is not null
    
    and fetched_at > (select max(fetched_at) from `bootcamp-project-pea-pme`.`silver`.`rss_articles`)
    
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
        partition by row_id
        order by fetched_at desc
    ) = 1
)

select * from deduped