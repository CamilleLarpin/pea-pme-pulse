
    
    

with dbt_test__target as (

  select row_id as unique_field
  from `bootcamp-project-pea-pme`.`silver`.`rss_articles`
  where row_id is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


