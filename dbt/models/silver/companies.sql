-- Silver: PEA-PME company referentiel — historised monthly snapshot
-- Grain: one row per (isin, snapshot_date)
-- snapshot_date = date of the dbt run — each monthly pipeline run adds a new snapshot
-- Idempotent: re-running on the same day merges with no net change

{{ config(
    materialized='incremental',
    unique_key=['isin', 'snapshot_date'],
    incremental_strategy='merge',
    on_schema_change='fail',
) }}

with source as (
    select
        trim(name)          as name,
        upper(trim(isin))   as isin,
        trim(ticker_bourso) as ticker_bourso,
        current_date('Europe/Paris') as snapshot_date
    from {{ source('bronze', 'boursorama') }}
    where isin is not null
      and trim(isin) != ''
)

select *
from source
qualify row_number() over (partition by isin order by name) = 1
