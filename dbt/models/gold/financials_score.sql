{{ config(
    materialized='table',
    labels={"layer": "gold", "domain": "financials"}
) }}
/*
    financials_score (gold)
    ========================
    Z-score pondéré sur 4 métriques fondamentales → score normalisé 0–10.

    Métriques & pondérations :
        - ca_growth    : 25%  (croissance du CA)
        - marge_op     : 30%  (rentabilité opérationnelle)
        - levier       : 25%  (dette nette / ebitda — inversé : moins = mieux)
        - fcf_yield    : 20%  (FCF / CA — proxy rendement cash)

    Règles :
        - Winsorisation à 5% / 95% par métrique pour limiter l'effet outliers
        - Score calculé uniquement si >= 2 métriques disponibles
        - Pondération dynamique : les poids sont redistribués sur les métriques disponibles
        - Score final normalisé entre 0 et 10 via PERCENT_RANK → échelle linéaire
        - En cas d'égalité de date_cloture_exercice pour un même ticker,
          on garde l'extraction la plus récente (extracted_at DESC)
        - Période : semestrielle ou annuelle selon date_cloture_exercice
*/

-- ============================================================
-- 1. Déduplication : un seul enregistrement par (ticker, date_cloture_exercice)
-- ============================================================

SELECT * FROM (
    WITH deduplicated AS (
        SELECT *
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY ticker, date_cloture_exercice
                    ORDER BY extracted_at DESC, llm_prompt_version_num DESC
                ) AS rn
            FROM {{ ref('amf_financial_signal') }}
            WHERE date_cloture_exercice IS NOT NULL
                AND ticker IS NOT NULL
        )
        WHERE rn = 1
    ),

    -- ============================================================
    -- 2. Calcul des métriques dérivées
    --    levier  = dette_nette / ebitda  (null si ebitda <= 0 ou manquant)
    --    fcf_yield = fcf / ca            (null si ca <= 0 ou manquant)
    -- ============================================================
    with_derived AS (
        SELECT
            record_id,
            isin,
            ticker,
            date_cloture_exercice,
            document_publication_ts,
            ca,
            ca_growth,
            ebitda,
            marge_op,
            dette_nette,
            fcf,
            source,
            financial_signal_run_id,
            extracted_at,

            -- Levier financier (dette_nette / ebitda) — null si ebitda null, 0 ou négatif
            CASE
                WHEN ebitda IS NOT NULL AND ebitda > 0 AND dette_nette IS NOT NULL
                THEN dette_nette / ebitda
                ELSE NULL
            END AS levier,

            -- FCF yield (fcf / ca) — null si ca null ou 0
            CASE
                WHEN ca IS NOT NULL AND ca > 0 AND fcf IS NOT NULL
                THEN fcf / ca
                ELSE NULL
            END AS fcf_yield

        FROM deduplicated
    ),

    -- ============================================================
    -- 3. Winsorisation à 5% / 95% par métrique
    --    Clip les valeurs extrêmes pour limiter l'effet outliers
    -- ============================================================
    percentiles AS (
        SELECT
            PERCENTILE_CONT(ca_growth, 0.05) OVER () AS p05_ca_growth,
            PERCENTILE_CONT(ca_growth, 0.95) OVER () AS p95_ca_growth,
            PERCENTILE_CONT(marge_op,  0.05) OVER () AS p05_marge_op,
            PERCENTILE_CONT(marge_op,  0.95) OVER () AS p95_marge_op,
            PERCENTILE_CONT(levier,    0.05) OVER () AS p05_levier,
            PERCENTILE_CONT(levier,    0.95) OVER () AS p95_levier,
            PERCENTILE_CONT(fcf_yield, 0.05) OVER () AS p05_fcf_yield,
            PERCENTILE_CONT(fcf_yield, 0.95) OVER () AS p95_fcf_yield
        FROM with_derived
        LIMIT 1
    ),

    winsorized AS (
        SELECT
            d.*,
            -- ca_growth clippé
            GREATEST(p.p05_ca_growth, LEAST(p.p95_ca_growth, d.ca_growth)) AS ca_growth_w,
            -- marge_op clippée
            GREATEST(p.p05_marge_op,  LEAST(p.p95_marge_op,  d.marge_op))  AS marge_op_w,
            -- levier clippé
            GREATEST(p.p05_levier,    LEAST(p.p95_levier,    d.levier))    AS levier_w,
            -- fcf_yield clippé
            GREATEST(p.p05_fcf_yield, LEAST(p.p95_fcf_yield, d.fcf_yield)) AS fcf_yield_w
        FROM with_derived d
        CROSS JOIN percentiles p
    ),

    -- ============================================================
    -- 4. Statistiques population pour Z-scores (sur valeurs winsorisées)
    -- ============================================================
    stats AS (
        SELECT
            AVG(ca_growth_w) AS mu_ca_growth,  STDDEV(ca_growth_w) AS sd_ca_growth,
            AVG(marge_op_w)  AS mu_marge_op,   STDDEV(marge_op_w)  AS sd_marge_op,
            AVG(levier_w)    AS mu_levier,     STDDEV(levier_w)    AS sd_levier,
            AVG(fcf_yield_w) AS mu_fcf_yield,  STDDEV(fcf_yield_w) AS sd_fcf_yield
        FROM winsorized
    ),

    -- ============================================================
    -- 5. Z-scores individuels + comptage des métriques disponibles
    -- ============================================================
    zscores AS (
        SELECT
            w.*,

            -- Z-scores (null si métrique manquante ou écart-type nul)
            SAFE_DIVIDE(w.ca_growth_w - s.mu_ca_growth, NULLIF(s.sd_ca_growth, 0)) AS z_ca_growth,
            SAFE_DIVIDE(w.marge_op_w  - s.mu_marge_op,  NULLIF(s.sd_marge_op,  0)) AS z_marge_op,
            -- Levier inversé : plus la dette est faible, meilleur le score
            -1 * SAFE_DIVIDE(w.levier_w    - s.mu_levier,    NULLIF(s.sd_levier,    0)) AS z_levier,
            SAFE_DIVIDE(w.fcf_yield_w - s.mu_fcf_yield, NULLIF(s.sd_fcf_yield, 0)) AS z_fcf_yield,

            -- Nombre de métriques disponibles (pour pondération dynamique)
            (CASE WHEN w.ca_growth_w IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN w.marge_op_w  IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN w.levier_w    IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN w.fcf_yield_w IS NOT NULL THEN 1 ELSE 0 END) AS nb_metrics

        FROM winsorized w
        CROSS JOIN stats s
    ),

    -- ============================================================
    -- 6. Score composite pondéré dynamiquement
    --    Poids de base : ca_growth=0.25, marge_op=0.30, levier=0.25, fcf_yield=0.20
    --    Si une métrique est null, son poids est redistribué proportionnellement
    --    sur les métriques disponibles.
    -- ============================================================
    weighted AS (
        SELECT
            *,

            -- Poids effectifs : on met 0 si la métrique est null
            CASE WHEN z_ca_growth IS NOT NULL THEN 0.25 ELSE 0 END AS w_ca_growth,
            CASE WHEN z_marge_op  IS NOT NULL THEN 0.30 ELSE 0 END AS w_marge_op,
            CASE WHEN z_levier    IS NOT NULL THEN 0.25 ELSE 0 END AS w_levier,
            CASE WHEN z_fcf_yield IS NOT NULL THEN 0.20 ELSE 0 END AS w_fcf_yield,

            -- Somme des poids effectifs (pour normalisation)
            (CASE WHEN z_ca_growth IS NOT NULL THEN 0.25 ELSE 0 END +
            CASE WHEN z_marge_op  IS NOT NULL THEN 0.30 ELSE 0 END +
            CASE WHEN z_levier    IS NOT NULL THEN 0.25 ELSE 0 END +
            CASE WHEN z_fcf_yield IS NOT NULL THEN 0.20 ELSE 0 END) AS total_weight

        FROM zscores
        WHERE nb_metrics >= 2  -- seuil minimum d'éligibilité
    ),

    composite AS (
        SELECT
            *,
            -- Score composite = moyenne pondérée normalisée des Z-scores disponibles
            SAFE_DIVIDE(
                COALESCE(z_ca_growth * w_ca_growth, 0) +
                COALESCE(z_marge_op  * w_marge_op,  0) +
                COALESCE(z_levier    * w_levier,    0) +
                COALESCE(z_fcf_yield * w_fcf_yield, 0),
                NULLIF(total_weight, 0)
            ) AS z_composite
        FROM weighted
    ),

    -- ============================================================
    -- 7. Normalisation 0–10 via PERCENT_RANK
    --    PERCENT_RANK → [0, 1] → * 10 → [0, 10]
    --    Arrondi à 2 décimales
    -- ============================================================
    ranked AS (
        SELECT
            *,
            ROUND(
                PERCENT_RANK() OVER (ORDER BY z_composite ASC) * 10,
                2
            ) AS score_fondamental
        FROM composite
    )

    -- ============================================================
    -- 8. Sélection finale
    -- ============================================================
    SELECT
        ticker,
        isin,
        date_cloture_exercice,
        score_fondamental,

        -- Métriques sources pour auditabilité
        ca_growth                               AS ca_growth_pct,
        marge_op                                AS marge_op_pct,
        ROUND(levier, 2)                        AS levier_dette_ebitda,
        ROUND(fcf_yield * 100, 2)              AS fcf_yield_pct,

        -- Z-scores intermédiaires pour debug
        ROUND(z_ca_growth, 3)                   AS z_ca_growth,
        ROUND(z_marge_op,  3)                   AS z_marge_op,
        ROUND(z_levier,    3)                   AS z_levier,
        ROUND(z_fcf_yield, 3)                   AS z_fcf_yield,
        ROUND(z_composite, 3)                   AS z_composite,

        -- Metadata
        nb_metrics,
        ROUND(total_weight, 2)                  AS coverage_score,
        source,
        financial_signal_run_id,
        extracted_at,
        CURRENT_TIMESTAMP()                     AS scored_at

    FROM ranked
    ORDER BY score_fondamental DESC, ticker
)
