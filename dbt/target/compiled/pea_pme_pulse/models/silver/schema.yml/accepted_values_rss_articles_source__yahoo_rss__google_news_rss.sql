
    
    

with all_values as (

    select
        source as value_field,
        count(*) as n_records

    from `bootcamp-project-pea-pme`.`silver`.`rss_articles`
    group by source

)

select *
from all_values
where value_field not in (
    'yahoo_rss','google_news_rss'
)


