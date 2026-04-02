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

{% docs yf_ticker %}
Yahoo Finance ticker symbol used to fetch OHLCV data (e.g. GFT.PA). Built from the Boursorama ticker by appending the exchange suffix (.PA for Euronext Paris). Primary key for yfinance API calls.
{% enddocs %}

{% docs date_cotation %}
Trading date (DATE, YYYY-MM-DD) for which the OHLCV prices were recorded. Market days only — no entries for weekends or public holidays. Join key for time-series alignment with other sources.
{% enddocs %}

{% docs close_price %}
Adjusted closing price for the session (EUR for Euronext Paris listings), as returned by Yahoo Finance. Used as the base for all Silver technical indicators (RSI, MACD, Bollinger Bands, SMA, EMA).
{% enddocs %}
