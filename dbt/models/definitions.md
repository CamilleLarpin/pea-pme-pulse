{% docs isin %}
International Securities Identification Number — 12-character alphanumeric code uniquely identifying a PEA-PME eligible company. Primary key across all layers. Source: Boursorama référentiel (571 companies).
{% enddocs %}

{% docs ticker_bourso %}
Boursorama platform ticker symbol for the company. Used for display and cross-referencing with the Boursorama référentiel. May be null if not listed on Boursorama.
{% enddocs %}

{% docs matched_name %}
Company name from the Boursorama référentiel, resolved via fuzzy matching against the RSS article title. Canonical name — use this for display, grouping, and LLM output.
{% enddocs %}

{% docs row_id %}
Surrogate deduplication key: hex(md5(lower(title) || '|' || isin)). Stable across runs — used to prevent duplicate scoring and duplicate Silver rows.
{% enddocs %}

{% docs published_at %}
Article publish timestamp in UTC, parsed from the RSS feed. Null if the feed did not provide a parseable date. Used as the time anchor for the 45-day rolling sentiment window.
{% enddocs %}

{% docs sentiment_score %}
Investment relevance signal assigned by Groq llama-3.3-70b-versatile. Integer 0–10: 0 = very negative news for investors, 5 = neutral, 10 = very positive news for investors. Scores a single article in isolation.
{% enddocs %}

{% docs sentiment_reason %}
One-sentence explanation produced by Groq alongside the sentiment_score. Describes why the article received that score. Written in the same language as the article (French or English).
{% enddocs %}

{% docs groq_model %}
Identifier of the Groq model used to produce the sentiment_score. Recorded for auditability and model version tracking. Current value: llama-3.3-70b-versatile.
{% enddocs %}

{% docs scored_at %}
UTC timestamp when the Prefect flow wrote this row to gold.article_sentiment. Used to track scoring latency and debug pipeline runs.
{% enddocs %}

{% docs mention_count_45d %}
Number of RSS articles about this company that were scored within the 45-day rolling window anchored on published_at. A proxy for media coverage intensity. Companies with fewer than 1 mention are excluded from score_news.
{% enddocs %}

{% docs avg_sentiment_45d %}
Rolling average of sentiment_score over all scored articles in the 45-day window. Float 0–10. Represents the sustained news signal for the company over the recent period, not a single article snapshot.
{% enddocs %}

{% docs investment_score %}
Normalized ranking score 1–10 derived from avg_sentiment_45d via PERCENT_RANK across all companies in the current snapshot. 1 = weakest news signal in the universe, 10 = strongest. Designed for relative comparison — not an absolute buy/sell signal. Recomputed every time the dbt model runs.
{% enddocs %}

{% docs score_date %}
Calendar date (Europe/Paris timezone) when this snapshot of score_news was computed. Since score_news is materialized as a table and rebuilt each run, score_date reflects the most recent pipeline execution.
{% enddocs %}

{% docs title %}
Article title as returned by the RSS feed, before any cleaning or normalisation. Used as the input to fuzzy company matching and Groq sentiment scoring.
{% enddocs %}

{% docs link %}
URL of the article as provided by the RSS feed. Points to the original publisher page.
{% enddocs %}

{% docs published %}
Raw publish date string from the RSS feed, unparsed (e.g. "Mon, 07 Apr 2025 08:00:00 +0000"). Preserved as-is at Bronze layer. Parsed to TIMESTAMP in Silver as published_at.
{% enddocs %}

{% docs summary %}
Article excerpt or description from the RSS feed. Used alongside title as input to Groq sentiment scoring.
{% enddocs %}

{% docs fetched_at %}
UTC ISO-8601 timestamp when the Prefect flow fetched the RSS feed. One value per flow run — all articles in the same run share the same fetched_at.
{% enddocs %}

{% docs match_score %}
Fuzzy match confidence score (0–100) produced by rapidfuzz when matching the article title against company names in the Boursorama référentiel. Null if no company was matched. Threshold: 80 — articles below threshold are kept with null isin.
{% enddocs %}

{% docs feed_name %}
Identifier of the specific Google News RSS feed the article was fetched from. Values: euronext_growth | pme_bourse_fr. Not present in yahoo_rss (single feed source).
{% enddocs %}

{% docs last_trading_date %}
Most recent trading date available for this ISIN in the Silver table — computed as MAX(Date) OVER (PARTITION BY isin). Identical for all rows of the same ISIN. Use this to filter for the latest closing price without a subquery: WHERE Date = last_trading_date. Also useful to detect stale ISINs (last_trading_date far behind CURRENT_DATE).
{% enddocs %}

{% docs RSI_14 %}
Relative Strength Index over 14 trading days. Momentum oscillator in [0, 100]. Values below 30 indicate oversold conditions (potential buy signal); values above 70 indicate overbought conditions (potential sell signal). NaN for the first 14 rows of each ISIN (insufficient warmup). Computed via ta.momentum.RSIIndicator(close, window=14).
{% enddocs %}

{% docs MACD %}
Moving Average Convergence Divergence — difference between the 12-day EMA and 26-day EMA of the closing price. Positive values indicate short-term bullish momentum; negative values indicate bearish momentum. NaN for the first 26 rows. Computed via ta.trend.MACD(close).
{% enddocs %}

{% docs MACD_signal %}
9-day EMA of the MACD line. A MACD crossover above the signal line is a classic buy signal; below is a sell signal. NaN for the first 34 rows (26-day EMA warmup + 9-day signal EMA).
{% enddocs %}

{% docs BB_upper %}
Upper Bollinger Band: 20-day SMA + 2 standard deviations of closing price. A closing price near or above BB_upper suggests potential short-term overvaluation or strong upward momentum. NaN for the first 20 rows. Computed via ta.volatility.BollingerBands(close, window=20, window_dev=2).
{% enddocs %}

{% docs BB_lower %}
Lower Bollinger Band: 20-day SMA − 2 standard deviations of closing price. A closing price near or below BB_lower suggests potential undervaluation or strong selling pressure. NaN for the first 20 rows.
{% enddocs %}

{% docs SMA_50 %}
50-day Simple Moving Average of closing price. Medium-term trend indicator. A price above SMA_50 is generally interpreted as bullish in the medium term. NaN for the first 50 rows. Computed via ta.trend.SMAIndicator(close, window=50).
{% enddocs %}

{% docs SMA_200 %}
200-day Simple Moving Average of closing price. Long-term trend benchmark — widely used by analysts. A price above SMA_200 signals a long-term bull market. A golden cross (SMA_50 crossing above SMA_200) is a strong bullish signal. NaN for the first 200 rows — longest warmup in the pipeline. Computed via ta.trend.SMAIndicator(close, window=200).
{% enddocs %}

{% docs EMA_20 %}
20-day Exponential Moving Average of closing price. More reactive than SMA as it weights recent prices more heavily. Useful for detecting trend changes earlier than SMA. NaN for the first rows (EMA warmup). Computed via ta.trend.EMAIndicator(close, window=20).
{% enddocs %}

{% docs computed_at %}
UTC timestamp (DATETIME) when the technical indicators were computed by compute_silver.py. Used to audit pipeline freshness and trace Prefect run history. One value per ISIN per pipeline run — all rows of the same run share the same computed_at.
{% enddocs %}

{% docs yf_ticker %}
Yahoo Finance ticker symbol used to fetch OHLCV data (e.g. GFT.PA). Built from the Boursorama ticker by appending the exchange suffix (.PA for Euronext Paris). Primary key for yfinance API calls.
{% enddocs %}

{% docs date_cotation %}
Trading date (DATE, YYYY-MM-DD) for which the OHLCV prices were recorded. Market days only — no entries for weekends or public holidays. Join key for time-series alignment with other sources.
{% enddocs %}

{% docs close_price %}
Adjusted closing price for the session (EUR for Euronext Paris listings), as returned by Yahoo Finance. Used as the base for all Silver technical indicators (RSI, MACD, Bollinger Bands, SMA, EMA).
{% enddocs %}

{% docs score_technique %}
Composite technical attractiveness score for a single trading day, in [0, 10]. Sum of 5 signals (rsi_signal + macd_signal + golden_cross_signal + bollinger_signal + trend_signal), each worth 0 (bearish), 1 (neutral/NaN), or 2 (bullish). Rounded to 1 decimal. A score of 10 means all 5 indicators are bullish; 5 means all are neutral; 0 means all are bearish.
{% enddocs %}

{% docs score_7d_avg %}
7-day rolling average of score_technique, computed over (partition by isin order by date rows between 6 preceding and current row). Rounded to 1 decimal. Smooths day-to-day volatility in the technical score and provides a short-term trend signal. Useful in the Daily Ranking Report to distinguish sustained performers from single-day spikes.
{% enddocs %}

{% docs rsi_signal %}
Technical signal derived from RSI_14. Scoring: RSI_14 < 35 (oversold) → 2.0 (bullish); 35 ≤ RSI_14 < 65 (neutral zone) → 1.0; RSI_14 ≥ 65 (overbought) → 0.0 (bearish); NULL (warmup period) → 1.0 (neutral, no penalty).
Thresholds set at 35/65 (tighter than the classic 30/70) to improve discrimination in normal market conditions.
{% enddocs %}

{% docs macd_signal %}
Technical signal derived from MACD vs MACD_signal line. Scoring: MACD > MACD_signal (bullish crossover) → 2.0; MACD ≤ MACD_signal → 0.0 (bearish); either value NULL (warmup < 34 bars) → 1.0 (neutral).
{% enddocs %}

{% docs golden_cross_signal %}
Technical signal based on the SMA_50 / SMA_200 relationship (SMA regime detection, not an event-based crossover). Scoring: SMA_50 > SMA_200 (bullish SMA regime — traditionally associated with a golden cross setup) → 2.0; SMA_50 ≤ SMA_200 (bearish regime — death cross territory) → 0.0; either SMA NULL (warmup < 200 bars) → 1.0 (neutral). Note: the signal stays at +2 as long as SMA_50 remains above SMA_200, not only on the day of the crossing. This is deliberate — for daily scoring, a sustained bullish regime is more informative than the single crossing event.
{% enddocs %}

{% docs bollinger_signal %}
Technical signal based on Bollinger %B: %B = (Close - BB_lower) / (BB_upper - BB_lower). Measures where within the band the price sits.
Scoring: %B < 0.2 (lower 20% of band — mean-reversion buy zone) → 2.0 (bullish); %B > 0.8 (upper 20% — overextended) → 0.0 (bearish); 0.2 ≤ %B ≤ 0.8 (middle 60%) → 1.0 (neutral); either band NULL (warmup < 20 bars) → 1.0.
Replaces the simpler inside/outside binary: ~95% of prices naturally fall within Bollinger Bands, making the old logic always neutral. %B thresholds at 0.2/0.8 give a signal to ~40% of stocks.
{% enddocs %}

{% docs trend_signal %}
Technical signal based on closing price vs EMA_20. Scoring: Close > EMA_20 (price above short-term trend) → 2.0 (bullish); Close ≤ EMA_20 → 0.0 (bearish); EMA_20 NULL (warmup) → 1.0 (neutral).
{% enddocs %}

{% docs company_name %}
Legal name of the PEA-PME company, sourced from the Boursorama referential (silver.companies, latest snapshot).
Falls back to yf_ticker when the ISIN has no match in the referential.
Use this column for display — do not use isin or yf_ticker as labels in reports or dashboards.
{% enddocs %}

{% docs is_latest %}
Boolean flag: TRUE if date equals the most recent trading date available for this ISIN (MAX(date) OVER (PARTITION BY isin)), FALSE otherwise. Use WHERE is_latest = TRUE to get the current snapshot of one row per ISIN — equivalent to a DISTINCT ON (isin) ORDER BY date DESC. Required for joining stocks_score into the composite pea_pme_pulse model.
{% enddocs %}
