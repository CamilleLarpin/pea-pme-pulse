-- Silver: cleaned AMF regulatory documents
-- Source: bronze.amf
-- Cleaning only:
--   - keep valid business rows
--   - standardize column names
--   - deduplicate on record_id
--   - keep latest ingestion per record_id

with source_data as (
    select
        record_id,
        societe,
        isin,
        publication_ts,
        pdf_url,
        titre,
        sous_type,
        type_information,
        source,
        run_id,
        ingestion_ts
    from {{ source('bronze', 'amf') }}
),

filtered as (
    select
        record_id,
        societe,
        upper(trim(isin)) as isin,
        publication_ts,
        pdf_url,
        titre,
        sous_type,
        type_information,
        source,
        run_id,
        ingestion_ts
    from source_data
    where record_id is not null
      and isin is not null
),

deduped as (
    select *
    from filtered
    qualify row_number() over (
        partition by record_id
        order by ingestion_ts desc
    ) = 1
)

select * from deduped