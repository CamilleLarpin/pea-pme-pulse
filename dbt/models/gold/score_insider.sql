/*
Model: score_insider
Layer: Gold
Description: 
    Calculates a technical insider sentiment score (1-10) based on purchase signals.
    The algorithm weights the total transaction amount logarithmically and applies 
    a bonus for "clustering" (multiple distinct operations on the same day).

Input:
    - silver.amf_insider_signals: Processed insider transaction data.
    - dbt Variables: cluster_bonus_weight, max_log_amount.

Output:
    - isin, societe: Company identifiers.
    - signal_date: Execution date of the transaction.
    - score_1_10: Normalized sentiment score.
*/

with prepared_signals as (
    -- Reference the silver source table
    select 
        *,
        -- Bridge: Rename the AI-extracted amount column for consistency in scoring
        montant as montant_total 
    from {{ source('silver', 'amf_insider_signals') }}
),

daily_agg as (
    -- Aggregate data by company and date to identify purchase clusters
    select
        isin,
        societe,
        cast(date_signal as date) as signal_date,
        string_agg(distinct dirigeant, ', ') as insider_names,
        count(distinct source_record_id) as num_operations,
        sum(montant_total) as total_amount,
        
        -- Raw Score Calculation:
        -- Combines log10 of the total amount with a multiplier for multiple distinct operations.
        (
            log10(sum(montant_total) + 1) * (1 + (count(distinct source_record_id) - 1) * {{ var('insider_weights')['cluster_bonus_weight'] }})
        ) as raw_score
    from prepared_signals
    where type_operation = 'Achat' -- Focus strictly on 'Purchase' transactions
    group by 1, 2, 3
),

normalized as (
    -- Normalize the score to a standard 1 to 10 scale
    select
        *,
        least(10.0, greatest(1.0, 
            round((raw_score / {{ var('insider_weights')['max_log_amount'] }}) * 10, 1)
        )) as score_1_10
    from daily_agg
)

-- Final result set for gold consumption layer
select * from normalized