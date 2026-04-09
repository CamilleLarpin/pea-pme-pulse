{{ config(materialized='table') }}

with companies as (
    select isin, name
    from {{ ref('companies') }}
    qualify row_number() over (partition by isin order by snapshot_date desc) = 1
),

scored as (
    select
        isin,
        yf_ticker,
        Date,
        Close,

        case
            when RSI_14 < 35  then 2.0
            when RSI_14 < 65  then 1.0
            when RSI_14 >= 65 then 0.0
            else 1.0
        end as rsi_signal,

        case
            when MACD is null or MACD_signal is null then 1.0
            when MACD > MACD_signal               then 2.0
            else 0.0
        end as macd_signal,

        case
            when SMA_50 is null or SMA_200 is null then 1.0
            when SMA_50 > SMA_200                  then 2.0
            else 0.0
        end as golden_cross_signal,

        case
            when BB_lower is null or BB_upper is null                          then 1.0
            when (Close - BB_lower) / nullif(BB_upper - BB_lower, 0) < 0.2    then 2.0
            when (Close - BB_lower) / nullif(BB_upper - BB_lower, 0) > 0.8    then 0.0
            else 1.0
        end as bollinger_signal,

        case
            when EMA_20 is null  then 1.0
            when Close > EMA_20  then 2.0
            else 0.0
        end as trend_signal

    from {{ source('silver', 'yahoo_ohlcv') }}
    where isin is not null
)

select
    s.isin,
    coalesce(c.name, s.yf_ticker)                                           as company_name,
    s.yf_ticker,
    s.Date                                                                  as date,
    s.Close,

    s.rsi_signal,
    s.macd_signal,
    s.golden_cross_signal,
    s.bollinger_signal,
    s.trend_signal,

    round(
        s.rsi_signal + s.macd_signal + s.golden_cross_signal
        + s.bollinger_signal + s.trend_signal,
    1)                                                                      as score_technique,

    round(
        avg(s.rsi_signal + s.macd_signal + s.golden_cross_signal
            + s.bollinger_signal + s.trend_signal)
        over (
            partition by s.isin
            order by s.Date
            rows between 6 preceding and current row
        ),
    1)                                                                      as score_7d_avg,

    round(
        avg(s.rsi_signal + s.macd_signal + s.golden_cross_signal
            + s.bollinger_signal + s.trend_signal)
        over (
            partition by s.isin
            order by s.Date
            rows between 13 preceding and current row
        ),
    1)                                                                      as score_14d_avg,

    s.Date = max(s.Date) over (partition by s.isin)                         as is_latest

from scored s
left join companies c on s.isin = c.isin
