# 07 — Dashboard Streamlit

## Configuration

- **Fichier :** `src/dashboard/app.py`
- **Framework :** Streamlit ≥1.56.0 + Plotly ≥6.6.0
- **Cache :** `@st.cache_data(ttl=3600)` (1 heure) sur toutes les fonctions de chargement
- **API base URL :** `st.secrets.get("api_base_url")` → env `API_BASE_URL` → défaut `http://localhost:8000`
- **Ballons :** `st.balloons()` au premier chargement (`st.session_state["launched"]`)

## Chargement des données

| Fonction | Endpoint API | Cache |
|---|---|---|
| `load_latest_scores()` | `/gold/stocks-score/latest` | 1h |
| `load_score_history()` | `/gold/stocks-score/history` | 1h |
| `load_company_scores()` | `/gold/company-scores/latest` | 1h |
| `load_news_scores()` | `/gold/score-news/latest` | 1h |
| `load_insider_scores()` | `/gold/score-insider/latest` | 1h |
| `load_financials_scores()` | `/gold/financials-score/latest` | 1h |
| `load_article_sentiments()` | `/gold/article-sentiment/latest` | 1h |

---

## Structure globale

```
Titre : 📈 PEA-PME Pulse — Tableau de bord
Sous-titre : Scoring composite quotidien · 4 dimensions · Mise à jour chaque soir en semaine

├── SIDEBAR
│   ├── Comment lire ce dashboard ?
│   │   ├── Légende scores (🟢 6-10, 🟡 4-6, 🔴 0-4)
│   │   ├── 4 dimensions (Actualités 25%, Technique 25%, Insiders 25%, Fondamentaux 25%)
│   │   ├── Glossaire indicateurs techniques
│   │   └── ⚠️ Disclaimer investissement
│   └── 🔄 Bouton "Actualiser les données" (clear cache + rerun)
│
├── FRAÎCHEUR PIPELINE
│   └── Dernière MAJ : Composite | Technique | Actualités | Insiders | Fondamentaux
│
├── TAB A — 🏆 Vue Composite
├── TAB B — 📰 Actualités
├── TAB C — 🏦 Insiders & Fondamentaux
└── TAB D — 📈 Analyse Technique
```

---

## Sidebar (détail)

**Glossaire indicateurs techniques :**
- **RSI :** Sur-vente/sur-achat (RSI < 35 = signal d'achat)
- **MACD :** Accélération de tendance
- **Golden Cross :** Haussier long terme (SMA 50j > SMA 200j)
- **Bollinger :** Zone basse de la bande (%B < 20% = zone d'achat)
- **EMA 20 :** Prix > moyenne mobile exponentielle 20j ?

**Bouton Actualiser :**
```python
if st.button("🔄 Actualiser les données", use_container_width=True):
    st.cache_data.clear()
    st.rerun()
```

---

## Fraîcheur pipeline

| Dimension | Source | Libellé |
|---|---|---|
| Composite | `gold.company_scores` | score_date max |
| Technique | `gold.stocks_score` | date max |
| Actualités | `gold.score_news` | score_date max |
| Insiders | `gold.score_insider` | signal_date max |
| Fondamentaux | `gold.financials_score` | date_cloture_exercice max → "dernière clôture" |

---

## TAB A — 🏆 Vue Composite

### 4 KPIs (colonnes)
1. "Univers analysé" — Nombre total de PME avec scores aujourd'hui
2. "Score composite moyen" — Moyenne des composite_score
3. "Opportunités ≥ 6" — Nombre de sociétés avec score ≥ 6
4. "Top société" — Nom + score de la meilleure société

### Top 25 Rankings
- Colonnes : #, Entreprise, Score global (/10), Actualités, Technique, Insiders, Fondamentaux
- Dégradé vert→rouge sur toutes les colonnes de score

### Visualisations (2 colonnes 50/50)

**Colonne gauche — Distribution des scores composites** (histogramme Plotly, hauteur=380)
- Zones colorées en arrière-plan :
  - 0–4 : rouge "Prudence"
  - 4–6 : jaune "Neutre"
  - 6–10 : vert "Opportunité"
- Caption : description de la distribution

**Colonne droite — Profil Top 5** (radar chart Plotly)
- Dimensions : score_news, score_stock, score_insider, score_financials
- 5 traces colorées (une par société du Top 5)
- Plage axes : 0–10

---

## TAB B — 📰 Actualités

### Top 20 News Rankings
- Colonnes : #, Entreprise, Score invest. (/10), Sentiment moyen, Nb articles (45j)
- Dégradé vert pour investment_score

### Scatter Plot — "Volume médiatique vs Sentiment"
- X : mention_count_45d
- Y : avg_sentiment_45d (0–10)
- Taille + couleur : investment_score (colorscale RdYlGn [1–10])
- Ligne horizontale y=5 "Neutre" (pointillés)
- Zone idéale : droite + haut (visibilité élevée + sentiment positif)

### Expander — "🔍 Articles récents par société"
- Dropdown : sélection d'une société
- Table : 5 derniers articles de la société
- Colonnes : Titre, Date, Score, Raison (sentiment_reason)

---

## TAB C — 🏦 Insiders & Fondamentaux

### Section 1 — Signaux insiders AMF

**Contexte :** "Achats d'actions déclarés à l'AMF par des dirigeants dans les 45 derniers jours."

**Top 20 table :**
- Colonnes : Société, Date signal, Dirigeants, Nb opérations, Montant (€), Score (/10)
- Dégradé violet pour score

**Graphique en barres horizontales — Top 15 signaux insiders**
- X : Montant total déclaré (€)
- Y : Nom de la société
- Couleur : score (colorscale violet)

### Section 2 — Fondamentaux financiers

**Contexte :** "Scores basés sur 4 métriques : croissance CA, marge opérationnelle, levier, FCF yield."

**Table complète :**
- Colonnes : Entreprise, Clôture, Score fond. (/10), CA Growth (%), Marge Op (%), Levier D/EBITDA, FCF Yield (%), Métriques, Couverture
- Dégradé vert pour scores fondamentaux

**Scatter Plot — "CA Growth vs Marge Op"**
- X : ca_growth_pct (croissance CA %)
- Y : marge_op_pct (marge opérationnelle %)
- Taille + couleur : score_fondamental
- Lignes de quadrant x=0, y=0 (pointillés gris)
- Zone idéale : droite + haut (profitable + en croissance)

---

## TAB D — 📈 Analyse Technique

### 4 KPIs (colonnes)
1. "Entreprises analysées" — Total PME scorées
2. "Meilleure opportunité du jour" — Nom + score
3. "Score ≥ 7 (haussier)" — % des sociétés avec score ≥ 7
4. "Régime haussier long terme" — Nombre avec Golden Cross actif (golden_cross_signal=2)

### 3 sous-onglets

#### Sous-onglet D1 — 📊 Classement
**Top 10 table :**
- Colonnes : #, Entreprise, Date du score, Score (/10), Moy. 7j, Cours (€), + 5 colonnes signaux
- Signaux affichés avec emoji : 🟢 (2=haussier), 🟡 (1=neutre), 🔴 (0=baissier)
- Dégradé vert→rouge sur Score + Moy. 7j

#### Sous-onglet D2 — 🔬 Signaux
- Expander : "Comment lire cette heatmap?" avec légende
- Deux sous-tabs : "🏆 Top 10" et "⚠️ Flop 10"
- **Heatmap :** Sociétés (lignes) × 5 signaux (colonnes)
  - Colorscale RdYlGn [0–2]
  - Valeurs cellules : 0 (rouge), 1 (jaune), 2 (vert)

#### Sous-onglet D3 — 📈 Historique
**Sélecteur de période :** radio buttons (30 jours / 3 mois / 6 mois / 1 an)

**Graphique 1 — Moyenne mobile 7j :**
- X : date, Y : score_7d_avg [0–10]
- Lignes colorées par société (Top 5)
- Ligne horizontale y=7 "Seuil haussier" (pointillés)

**Graphique 2 — Score brut quotidien :**
- X : date, Y : score_technique [0–10]
- Mêmes sociétés Top 5
- Ligne horizontale y=6 "Seuil haussier" (pointillés)
