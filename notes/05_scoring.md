# 05 — Formules de scoring

## Score Technique (`gold.stocks_score`)

**Source :** `dbt/models/gold/stocks_score.sql`  
**Implémentation miroir Python :** `src/gold/stocks_scorer.py` (tests unitaires uniquement)  
**Grain :** (isin, date)  
**Plage :** [0–10]

### 5 signaux × [0 / 1 / 2 points]

| Signal | Colonne BQ | 2 pts (Haussier) | 1 pt (Neutre) | 0 pt (Baissier) |
|---|---|---|---|---|
| **RSI** | `rsi_signal` | RSI_14 < 35 (survente) | 35 ≤ RSI_14 < 65 ou NaN | RSI_14 ≥ 65 (surachat) |
| **MACD** | `macd_signal` | MACD > MACD_signal | NaN | MACD ≤ MACD_signal |
| **Golden Cross** | `golden_cross_signal` | SMA_50 > SMA_200 | NaN | SMA_50 ≤ SMA_200 |
| **Bollinger** | `bollinger_signal` | %B < 0.2 (bas de bande) | 0.2 ≤ %B ≤ 0.8 ou NaN | %B > 0.8 (haut de bande) |
| **EMA Trend** | `trend_signal` | Close > EMA_20 | NaN | Close ≤ EMA_20 |

```
score_technique = rsi_signal + macd_signal + golden_cross_signal + bollinger_signal + trend_signal
               ∈ [0, 10]  (arrondi à 1 décimale)
```

### Colonnes additionnelles

- `score_7d_avg` — Moyenne mobile 7 jours de `score_technique` (window ROWS BETWEEN 6 PRECEDING AND CURRENT ROW, par ISIN)
- `score_14d_avg` — Moyenne mobile 14 jours
- `is_latest` — TRUE pour le dernier jour disponible par ISIN

### Formule %B (Bollinger)
```
%B = (Close - BB_lower) / (BB_upper - BB_lower)
```
Protégé contre division par zéro avec `NULLIF(BB_upper - BB_lower, 0)`.

---

## Score News (`gold.score_news`)

**Source :** `silver.rss_articles` + `gold.article_sentiment`  
**Grain :** (isin, score_date)  
**Plage :** [1–10]

### Processus
1. Articles des 45 derniers jours, liés à un ISIN via fuzzy matching
2. Chaque article scoré par Groq `llama-3.1-8b-instant` :
   - Input : titre + résumé de l'article
   - Output : `{score: int, reason: str}`
3. Agrégation par ISIN :
   - `mention_count_45d` — Nombre d'articles sur 45j
   - `avg_sentiment_45d` — Sentiment moyen [0–10]
   - `investment_score` — **Classement percentile** sur avg_sentiment_45d :
     ```sql
     GREATEST(1, CAST(CEIL(PERCENT_RANK() OVER (ORDER BY avg_sentiment_45d) * 10) AS INT64))
     ```
     → Ce n'est **pas** une transformation directe de avg_sentiment, c'est un rang relatif parmi toutes les sociétés du jour. Une société avec un sentiment modéré mais supérieur à la médiane peut obtenir un score élevé.

---

## Score Insider (`gold.score_insider`)

**Source :** `silver.amf_insider_signals` → `gold.score_insider` (dbt)  
**Fenêtre :** 6 mois  
**Plage :** [1–10] (`score_1_10`)  
**Filtre :** `type_operation = 'Achat'` uniquement (les ventes ne sont pas scorées)

### Processus
1. PDF AMF téléchargé depuis `bronze.amf`
2. Texte extrait via `pdfplumber`
3. Groq `llama-3.1-8b-instant` extrait les transactions :
   ```json
   {
     "signals": [
       {"dirigeant": "...", "type_operation": "Achat|Vente", "montant": 12500.0, "date_signal": "2026-03-15"}
     ]
   }
   ```
4. Validation heuristique (nom 2+ parties, mots interdits)
5. Scoring dbt avec variables (`dbt_project.yml`) :

| Variable dbt | Valeur | Signification |
|---|---|---|
| `cluster_bonus_weight` | 0.5 | Bonus pour plusieurs opérations (même ISIN) |
| `max_log_amount` | 6 | Diviseur pour normalisation (plafond log10) |

6. Formule exacte :
   ```
   raw_score = log10(sum(total_amount) + 1)
             × (1 + (num_operations - 1) × cluster_bonus_weight)

   score_1_10 = LEAST(10.0, GREATEST(1.0,
       ROUND(raw_score / max_log_amount × 10, 1)
   ))
   ```
   → Plus le montant total acheté est élevé et plus les opérations sont groupées (cluster), plus le score est élevé.

**Note :** Il existe aussi `gold.score_insider_signals` (table Python MERGE, via `gold_insider_signals.py`) qui contient les mêmes données dans un format légèrement différent. C'est `gold.score_insider` (dbt) qui est lu par l'API.

---

## Score Financials (`gold.financials_score`)

**Source :** `silver.amf_financial_signal`  
**Grain :** (isin, date_cloture_exercice)  
**Plage :** [0–10] (`score_fondamental`)  
**Minimum requis :** 2 métriques disponibles pour calculer un score

### 4 métriques extraites des rapports AMF

| Métrique | Colonne BQ | Poids | Direction |
|---|---|---|---|
| Croissance CA | `ca_growth_pct` | **25%** | ↑ plus = mieux |
| Marge opérationnelle | `marge_op_pct` | **30%** | ↑ plus = mieux |
| Levier dette/EBITDA | `levier_dette_ebitda` | **25%** | ↓ moins = mieux (score inversé) |
| FCF Yield | `fcf_yield_pct` | **20%** | ↑ plus = mieux |

### Formule de calcul (dbt `financials_score.sql`)
1. **Winsorisation** : chaque métrique clippée au 5e–95e percentile (élimination des outliers)
2. **Z-scores** par métrique : `(valeur - μ) / σ`
   - Le levier est **inversé** : `z_levier = -1 × z_levier` (moins de dette = score positif)
3. **Pondération dynamique** : si une métrique est nulle, son poids est redistribué proportionnellement sur les autres
4. **Score composite** : `z_composite = Σ(z_i × w_i) / Σ(w_i)`
5. **Normalisation finale** : `PERCENT_RANK() OVER (ORDER BY z_composite) × 10`

→ Le score est un **rang percentile** parmi toutes les sociétés de l'univers, pas un score absolu.

### Colonnes additionnelles
- `nb_metrics` — Nombre de métriques renseignées (max 4)
- `coverage_score` — Somme des poids effectifs utilisés
- `z_ca_growth`, `z_marge_op`, `z_levier`, `z_fcf_yield`, `z_composite` — Z-scores intermédiaires (debug)

**Fréquence :** 2 fois par an (1er avril et 1er octobre)

---

## Score Composite (`gold.company_scores`)

**Source :** Les 4 tables gold ci-dessus  
**Grain :** (isin, score_date)  
**Plage :** [0–10]

### Formule (code dbt réel — `company_scores.sql`)

Les scores news et insider sont sur [1–10] alors que stock et financials sont sur [0–10]. Avant pondération, tous sont **ramenés sur [0–10]** :

```sql
composite_score = ROUND(
    COALESCE((investment_score - 1) / 9.0 * 10, 5.0) * 0.25   -- news : [1–10] → [0–10]
  + COALESCE(score_technique, 5.0) * 0.25                      -- stock : déjà [0–10]
  + COALESCE((score_1_10 - 1) / 9.0 * 10, 5.0) * 0.25         -- insider : [1–10] → [0–10]
  + COALESCE(score_fondamental, 5.0) * 0.25,                   -- financials : déjà [0–10]
2)
```

- **Fallback null = 5.0** (point médian) : une société sans score insider ou financials ne pénalise pas son composite
- **Plage finale : [0–10]**

**⚠️ Correction documentation :** Le README mentionne 35%/25%/25%/15%. **Le code dbt utilise 25% pour chaque dimension.** La valeur à utiliser en présentation est **25% × 4**.

### Interprétation

| Score | Signal | Affichage dashboard |
|---|---|---|
| 6–10 | Positif 🟢 | Opportunité |
| 4–6 | Mixte 🟡 | Neutre |
| 0–4 | Négatif 🔴 | Prudence |
