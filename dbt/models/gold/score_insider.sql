-- models/gold/score_insider.sql
with prepared_signals as (
    -- Punta al layer Silver corretto
    select 
        *,
        -- PONTE: Rinominiamo la colonna di Groq per il calcolo successivo
        montant as montant_total 
    from {{ source('silver', 'amf_insider_signals') }}
),

daily_agg as (
    select
        isin,
        societe,
        cast(date_signal as date) as signal_date,
        string_agg(distinct dirigeant, ', ') as insider_names,
        count(distinct source_record_id) as num_operations,
        sum(montant_total) as total_amount,
        -- Calcolo del punteggio grezzo usando il nuovo alias
        (
            log10(sum(montant_total) + 1) * (1 + (count(distinct source_record_id) - 1) * {{ var('insider_weights')['cluster_bonus_weight'] }})
        ) as raw_score
    from prepared_signals
    where type_operation = 'Achat'
    group by 1, 2, 3
),

normalized as (
    select
        *,
        least(10.0, greatest(1.0, 
            round((raw_score / {{ var('insider_weights')['max_log_amount'] }}) * 10, 1)
        )) as score_1_10
    from daily_agg
)

select * from normalized