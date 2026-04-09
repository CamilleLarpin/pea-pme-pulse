{{ config(
    materialized='incremental',
    unique_key='record_id',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
) }}

{% set active_prompt_version = var('amf_active_prompt_version', none) %}

with source_rows as (

    select
        record_id,
        isin,
        ticker,
        source_url,
        pdf_gcs_uri,
        document_publication_ts,
        date_cloture_exercice_raw,
        ca_raw,
        ca_growth_raw,
        ebitda_raw,
        marge_op_raw,
        dette_nette_raw,
        fcf_raw,
        parser_used,
        llm_model,
        llm_prompt_version,
        source,
        source_run_id,
        financial_signal_run_id,
        extracted_at,
        extraction_status,

        coalesce(
            safe_cast(regexp_extract(llm_prompt_version, r'v(\d+)') as int64),
            0
        ) as llm_prompt_version_num

    from {{ source('work', 'amf_financial_signal_staging') }}
    where extraction_status = 'success'

    {% if active_prompt_version is not none %}
      and llm_prompt_version = '{{ active_prompt_version }}'
    {% endif %}

    {% if is_incremental() %}
      and extracted_at >= (
        select coalesce(timestamp_sub(max(extracted_at), interval 7 day), timestamp('1970-01-01'))
        from {{ this }}
      )
    {% endif %}

),

deduplicated as (

    select *
    from source_rows
    qualify row_number() over (
        partition by record_id
        order by
            llm_prompt_version_num desc,
            extracted_at desc,
            financial_signal_run_id desc
    ) = 1

),

normalized as (

    select
        record_id,
        isin,
        ticker,
        source_url,
        pdf_gcs_uri,
        document_publication_ts,

        case
            when regexp_contains(date_cloture_exercice_raw, r'^\d{4}-\d{2}-\d{2}$')
                then safe_cast(date_cloture_exercice_raw as date)
            else null
        end as date_cloture_exercice,

        ca_raw,
        ca_growth_raw,
        ebitda_raw,
        marge_op_raw,
        dette_nette_raw,
        fcf_raw,

        parser_used,
        llm_model,
        llm_prompt_version,
        llm_prompt_version_num,
        source,
        source_run_id,
        financial_signal_run_id,
        extracted_at

    from deduplicated

),

final as (

    select
        record_id,
        isin,
        ticker,
        source_url,
        pdf_gcs_uri,
        document_publication_ts,
        date_cloture_exercice,

        case
            when ca_raw is null then null
            when regexp_contains(ca_raw, r'^\s*-?\d+(\.\d+)?\s+EUR\s*$')
                then safe_cast(regexp_extract(ca_raw, r'^\s*(-?\d+(?:\.\d+)?)') as numeric)
            when regexp_contains(ca_raw, r'^\s*-?\d+(\.\d+)?\s+thousand EUR\s*$')
                then safe_cast(regexp_extract(ca_raw, r'^\s*(-?\d+(?:\.\d+)?)') as numeric) * 1000
            when regexp_contains(ca_raw, r'^\s*-?\d+(\.\d+)?\s+million EUR\s*$')
                then safe_cast(regexp_extract(ca_raw, r'^\s*(-?\d+(?:\.\d+)?)') as numeric) * 1000000
            when regexp_contains(ca_raw, r'^\s*-?\d+(\.\d+)?\s+billion EUR\s*$')
                then safe_cast(regexp_extract(ca_raw, r'^\s*(-?\d+(?:\.\d+)?)') as numeric) * 1000000000
            else null
        end as ca,

        case
            when ca_growth_raw is null then null
            else safe_cast(regexp_extract(ca_growth_raw, r'(-?\d+(?:\.\d+)?)') as float64)
        end as ca_growth,

        case
            when ebitda_raw is null then null
            when regexp_contains(ebitda_raw, r'^\s*-?\d+(\.\d+)?\s+EUR\s*$')
                then safe_cast(regexp_extract(ebitda_raw, r'^\s*(-?\d+(?:\.\d+)?)') as numeric)
            when regexp_contains(ebitda_raw, r'^\s*-?\d+(\.\d+)?\s+thousand EUR\s*$')
                then safe_cast(regexp_extract(ebitda_raw, r'^\s*(-?\d+(?:\.\d+)?)') as numeric) * 1000
            when regexp_contains(ebitda_raw, r'^\s*-?\d+(\.\d+)?\s+million EUR\s*$')
                then safe_cast(regexp_extract(ebitda_raw, r'^\s*(-?\d+(?:\.\d+)?)') as numeric) * 1000000
            when regexp_contains(ebitda_raw, r'^\s*-?\d+(\.\d+)?\s+billion EUR\s*$')
                then safe_cast(regexp_extract(ebitda_raw, r'^\s*(-?\d+(?:\.\d+)?)') as numeric) * 1000000000
            else null
        end as ebitda,

        case
            when marge_op_raw is null then null
            else safe_cast(regexp_extract(marge_op_raw, r'(-?\d+(?:\.\d+)?)') as float64)
        end as marge_op,

        case
            when dette_nette_raw is null then null
            when regexp_contains(dette_nette_raw, r'^\s*-?\d+(\.\d+)?\s+EUR\s*$')
                then safe_cast(regexp_extract(dette_nette_raw, r'^\s*(-?\d+(?:\.\d+)?)') as numeric)
            when regexp_contains(dette_nette_raw, r'^\s*-?\d+(\.\d+)?\s+thousand EUR\s*$')
                then safe_cast(regexp_extract(dette_nette_raw, r'^\s*(-?\d+(?:\.\d+)?)') as numeric) * 1000
            when regexp_contains(dette_nette_raw, r'^\s*-?\d+(\.\d+)?\s+million EUR\s*$')
                then safe_cast(regexp_extract(dette_nette_raw, r'^\s*(-?\d+(?:\.\d+)?)') as numeric) * 1000000
            when regexp_contains(dette_nette_raw, r'^\s*-?\d+(\.\d+)?\s+billion EUR\s*$')
                then safe_cast(regexp_extract(dette_nette_raw, r'^\s*(-?\d+(?:\.\d+)?)') as numeric) * 1000000000
            else null
        end as dette_nette,

        case
            when fcf_raw is null then null
            when regexp_contains(fcf_raw, r'^\s*-?\d+(\.\d+)?\s+EUR\s*$')
                then safe_cast(regexp_extract(fcf_raw, r'^\s*(-?\d+(?:\.\d+)?)') as numeric)
            when regexp_contains(fcf_raw, r'^\s*-?\d+(\.\d+)?\s+thousand EUR\s*$')
                then safe_cast(regexp_extract(fcf_raw, r'^\s*(-?\d+(?:\.\d+)?)') as numeric) * 1000
            when regexp_contains(fcf_raw, r'^\s*-?\d+(\.\d+)?\s+million EUR\s*$')
                then safe_cast(regexp_extract(fcf_raw, r'^\s*(-?\d+(?:\.\d+)?)') as numeric) * 1000000
            when regexp_contains(fcf_raw, r'^\s*-?\d+(\.\d+)?\s+billion EUR\s*$')
                then safe_cast(regexp_extract(fcf_raw, r'^\s*(-?\d+(?:\.\d+)?)') as numeric) * 1000000000
            else null
        end as fcf,

        parser_used,
        llm_model,
        llm_prompt_version,
        llm_prompt_version_num,
        source,
        source_run_id,
        financial_signal_run_id,
        extracted_at,
        current_timestamp() as loaded_at

    from normalized

)

select *
from final