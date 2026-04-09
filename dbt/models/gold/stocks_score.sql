{{ config(materialized='table') }}

with scored as (
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
    isin,
    yf_ticker,
    Date                                                                    as date,
    Close,

    rsi_signal,
    macd_signal,
    golden_cross_signal,
    bollinger_signal,
    trend_signal,

    round(
        rsi_signal + macd_signal + golden_cross_signal
        + bollinger_signal + trend_signal,
    1)                                                                      as score_technique,

    round(
        avg(rsi_signal + macd_signal + golden_cross_signal
            + bollinger_signal + trend_signal)
        over (
            partition by isin
            order by Date
            rows between 6 preceding and current row
        ),
    1)                                                                      as score_7d_avg,

    Date = max(Date) over (partition by isin)                               as is_latest

from scored
