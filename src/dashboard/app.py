import os

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import streamlit as st

SIGNAL_LABELS = {
    "rsi_signal": "RSI (momentum)",
    "macd_signal": "MACD (tendance)",
    "golden_cross_signal": "Golden Cross (long terme)",
    "bollinger_signal": "Bollinger (volatilité)",
    "trend_signal": "Tendance EMA 20j",
}
SIGNAL_COLS = list(SIGNAL_LABELS.keys())

PERIOD_DAYS = {"30 jours": 30, "3 mois": 90, "6 mois": 180, "1 an": 365}

SIGNAL_EMOJI = {0: "🔴", 1: "🟡", 2: "🟢"}

SCORE_DIMS = {
    "score_news": "Actualités",
    "score_stock": "Technique",
    "score_insider": "Insiders",
    "score_financials": "Fondamentaux",
}


# ── API ───────────────────────────────────────────────────────────────────────


def get_api_base_url() -> str:
    try:
        url = st.secrets.get("api_base_url")
    except Exception:
        url = None
    return url or os.environ.get("API_BASE_URL", "http://localhost:8000")


# ── Data loading ──────────────────────────────────────────────────────────────


@st.cache_data(ttl=3600)
def load_latest_scores() -> pd.DataFrame:
    resp = requests.get(f"{get_api_base_url()}/gold/stocks-score/latest", timeout=120)
    resp.raise_for_status()
    df = pd.DataFrame(resp.json())
    df = df.rename(columns={"close": "Close"})
    return df


@st.cache_data(ttl=3600)
def load_score_history(isins: tuple[str, ...], days: int) -> pd.DataFrame:
    resp = requests.get(
        f"{get_api_base_url()}/gold/stocks-score/history",
        params={"isins": list(isins), "days": days},
        timeout=120,
    )
    resp.raise_for_status()
    return pd.DataFrame(resp.json())


@st.cache_data(ttl=3600)
def load_company_scores() -> pd.DataFrame:
    try:
        resp = requests.get(f"{get_api_base_url()}/gold/company-scores/latest", timeout=120)
        resp.raise_for_status()
        return pd.DataFrame(resp.json())
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def load_news_scores() -> pd.DataFrame:
    try:
        resp = requests.get(f"{get_api_base_url()}/gold/score-news/latest", timeout=120)
        resp.raise_for_status()
        return pd.DataFrame(resp.json())
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def load_insider_scores() -> pd.DataFrame:
    try:
        resp = requests.get(f"{get_api_base_url()}/gold/score-insider/latest", timeout=120)
        resp.raise_for_status()
        return pd.DataFrame(resp.json())
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def load_financials_scores() -> pd.DataFrame:
    try:
        resp = requests.get(f"{get_api_base_url()}/gold/financials-score/latest", timeout=120)
        resp.raise_for_status()
        return pd.DataFrame(resp.json())
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def load_article_sentiments() -> pd.DataFrame:
    try:
        resp = requests.get(f"{get_api_base_url()}/gold/article-sentiment/latest", timeout=120)
        resp.raise_for_status()
        return pd.DataFrame(resp.json())
    except Exception:
        return pd.DataFrame()


# ── Sidebar ───────────────────────────────────────────────────────────────────


def render_sidebar() -> None:
    with st.sidebar:
        st.header("📖 Comment lire ce dashboard ?")
        st.markdown(
            """
**Score composite 0 – 10** = moyenne de 4 dimensions à 25% chacune.

🟢 **6 – 10** : Signal positif

🟡 **4 – 6** : Signal mixte

🔴 **0 – 4** : Signal négatif

---
**Les 4 dimensions :**

📰 **Actualités** — Sentiment presse (LLM, 45j glissants)

📈 **Technique** — 5 indicateurs boursiers quotidiens

🏦 **Insiders** — Achats déclarés à l'AMF (dirigeants)

📊 **Fondamentaux** — Croissance CA, marges, bilan

---
**Indicateurs techniques :**

📊 **RSI** — Survendu ou suracheté ?
*(RSI < 35 = signal d'achat potentiel)*

📈 **MACD** — La tendance s'accélère-t-elle ?

✨ **Golden Cross** — Tendance longue haussière ?
*(moyenne 50j > moyenne 200j)*

🎯 **Bollinger** — Cours bas dans sa fourchette ?
*(%B < 20% = zone d'achat)*

📉 **Tendance EMA** — Cours > moyenne 20j ?

---
*⚠️ Informatif uniquement — pas un conseil en investissement.*
"""
        )


# ── Data freshness ────────────────────────────────────────────────────────────


def render_freshness(
    df_composite: pd.DataFrame,
    df_stocks: pd.DataFrame,
    df_news: pd.DataFrame,
    df_insider: pd.DataFrame,
    df_financials: pd.DataFrame,
) -> None:
    parts = []
    if not df_composite.empty and "score_date" in df_composite.columns:
        parts.append(f"**Composite** {df_composite['score_date'].max()}")
    if not df_stocks.empty and "date" in df_stocks.columns:
        parts.append(f"**Technique** {df_stocks['date'].max()}")
    if not df_news.empty and "score_date" in df_news.columns:
        parts.append(f"**Actualités** {df_news['score_date'].max()}")
    if not df_insider.empty and "signal_date" in df_insider.columns:
        parts.append(f"**Insiders** {df_insider['signal_date'].max()}")
    if not df_financials.empty and "date_cloture_exercice" in df_financials.columns:
        parts.append(
            f"**Fondamentaux** {df_financials['date_cloture_exercice'].max()} (dernière clôture)"
        )
    if parts:
        st.caption("Dernière mise à jour — " + " · ".join(parts))


# ── Global KPIs (composite) ───────────────────────────────────────────────────


def render_global_kpis(df: pd.DataFrame) -> None:
    total = len(df)
    top_row = df.iloc[0]
    avg_score = df["composite_score"].mean()
    opportunities = int((df["composite_score"] >= 6).sum())

    c1, c2, c3, c4 = st.columns(4)
    c1.metric(
        "Univers analysé",
        total,
        help="Nombre de PME PEA-PME avec un score composite aujourd'hui",
    )
    c2.metric(
        "Score composite moyen",
        f"{avg_score:.1f} / 10",
        help="Moyenne du score composite sur l'ensemble des PME scorées",
    )
    c3.metric(
        "Opportunités ≥ 6",
        opportunities,
        help="Entreprises avec un score composite ≥ 6 (signal positif)",
    )
    c4.metric(
        "Top société",
        top_row["name"],
        delta=f"{top_row['composite_score']:.1f} / 10",
        help="Entreprise avec le score composite le plus élevé aujourd'hui",
    )


# ── Tab A — Vue Composite ─────────────────────────────────────────────────────


def render_composite_tab(df: pd.DataFrame) -> None:
    st.subheader("Classement composite")
    st.caption("Score moyen pondéré de 4 dimensions · 25% chacune.")

    top25 = df.head(25).copy().reset_index(drop=True)
    top25["#"] = top25.index + 1
    display = top25[
        [
            "#",
            "name",
            "composite_score",
            "score_news",
            "score_stock",
            "score_insider",
            "score_financials",
        ]
    ].rename(
        columns={
            "name": "Entreprise",
            "composite_score": "Score global (/10)",
            "score_news": "Actualités",
            "score_stock": "Technique",
            "score_insider": "Insiders",
            "score_financials": "Fondamentaux",
        }
    )
    score_cols = ["Score global (/10)", "Actualités", "Technique", "Insiders", "Fondamentaux"]
    styled = (
        display.set_index("#")
        .style.background_gradient(subset=score_cols, cmap="RdYlGn", vmin=0, vmax=10)
        .format({col: "{:.1f}" for col in score_cols})
    )
    st.dataframe(styled, use_container_width=True)

    st.divider()
    col_hist, col_radar = st.columns(2)

    with col_hist:
        st.subheader("Distribution des scores composites")
        st.caption("Répartition des PME selon leur score composite du jour.")
        fig_hist = px.histogram(
            df,
            x="composite_score",
            nbins=20,
            color_discrete_sequence=["#1f4e79"],
            labels={"composite_score": "Score composite"},
        )
        fig_hist.add_vrect(
            x0=0,
            x1=4,
            fillcolor="red",
            opacity=0.07,
            line_width=0,
            annotation_text="Prudence",
            annotation_position="top left",
        )
        fig_hist.add_vrect(
            x0=4,
            x1=6,
            fillcolor="yellow",
            opacity=0.07,
            line_width=0,
            annotation_text="Neutre",
            annotation_position="top left",
        )
        fig_hist.add_vrect(
            x0=6,
            x1=10,
            fillcolor="green",
            opacity=0.07,
            line_width=0,
            annotation_text="Opportunité",
            annotation_position="top left",
        )
        fig_hist.update_layout(height=380, bargap=0.05)
        st.plotly_chart(
            fig_hist,
            use_container_width=True,
            config={"displayModeBar": "hover", "displaylogo": False},
        )

    with col_radar:
        st.subheader("Profil — Top 5")
        st.caption("Équilibre des 4 dimensions.")
        top5 = df.head(5)
        fig_radar = go.Figure()
        dims = list(SCORE_DIMS.values())
        dim_cols = list(SCORE_DIMS.keys())
        for _, row in top5.iterrows():
            fig_radar.add_trace(
                go.Scatterpolar(
                    r=[row[c] for c in dim_cols],
                    theta=dims,
                    fill="toself",
                    opacity=0.5,
                    name=str(row["name"])[:20],
                )
            )
        fig_radar.update_layout(
            polar=dict(radialaxis=dict(range=[0, 10], tickfont=dict(size=8))),
            height=300,
            legend=dict(orientation="v", font=dict(size=9)),
            margin=dict(l=10, r=10, t=30, b=10),
        )
        st.plotly_chart(
            fig_radar,
            use_container_width=True,
            config={"displayModeBar": "hover", "displaylogo": False},
        )


# ── Tab B — Actualités ────────────────────────────────────────────────────────


def render_news_tab(df_news: pd.DataFrame, df_articles: pd.DataFrame) -> None:
    st.subheader("Classement actualités")
    st.caption("Score basé sur le sentiment moyen de la presse sur 45 jours glissants.")

    top20 = df_news.head(20).copy().reset_index(drop=True)
    top20["#"] = top20.index + 1
    display = top20[
        ["#", "matched_name", "investment_score", "avg_sentiment_45d", "mention_count_45d"]
    ].rename(
        columns={
            "matched_name": "Entreprise",
            "investment_score": "Score invest. (/10)",
            "avg_sentiment_45d": "Sentiment moyen",
            "mention_count_45d": "Nb articles (45j)",
        }
    )
    styled = (
        display.set_index("#")
        .style.background_gradient(subset=["Score invest. (/10)"], cmap="Greens", vmin=1, vmax=10)
        .format({"Score invest. (/10)": "{:.0f}", "Sentiment moyen": "{:.2f}"})
    )
    st.dataframe(styled, use_container_width=True)

    st.divider()
    st.subheader("Volume médiatique vs Sentiment")
    st.caption("Les candidats idéaux sont en haut à droite : forte visibilité + sentiment positif.")

    fig = px.scatter(
        df_news,
        x="mention_count_45d",
        y="avg_sentiment_45d",
        size="investment_score",
        color="investment_score",
        color_continuous_scale="RdYlGn",
        range_color=[1, 10],
        hover_name="matched_name",
        labels={
            "mention_count_45d": "Volume médiatique (articles 45j)",
            "avg_sentiment_45d": "Sentiment moyen (0–10)",
            "investment_score": "Score",
        },
    )
    fig.add_hline(
        y=5,
        line_dash="dot",
        line_color="grey",
        annotation_text="Neutre (5)",
        annotation_position="bottom right",
    )
    fig.update_layout(height=380)
    st.plotly_chart(
        fig, use_container_width=True, config={"displayModeBar": "hover", "displaylogo": False}
    )

    if not df_articles.empty:
        st.divider()
        with st.expander("🔍 Articles récents par société"):
            companies = sorted(df_articles["matched_name"].dropna().unique())
            selected = st.selectbox("Sélectionner une société", companies)
            subset = df_articles[df_articles["matched_name"] == selected].head(5)
            st.dataframe(
                subset[["title", "published_at", "sentiment_score", "sentiment_reason"]].rename(
                    columns={
                        "title": "Titre",
                        "published_at": "Date",
                        "sentiment_score": "Score",
                        "sentiment_reason": "Raison",
                    }
                ),
                use_container_width=True,
            )


# ── Tab C — Insiders & Fondamentaux ──────────────────────────────────────────


def render_insiders_financials_tab(
    df_insider: pd.DataFrame, df_financials: pd.DataFrame, df_stocks: pd.DataFrame
) -> None:
    # ── Section Insiders ──────────────────────────────────────────────────────
    st.subheader("🏦 Signaux insiders AMF")
    st.caption("Achats d'actions déclarés à l'AMF par des dirigeants dans les 45 derniers jours.")

    if df_insider.empty:
        st.info("Aucun signal insider récent (45 jours).")
    else:
        display_insider = df_insider[
            [
                "societe",
                "signal_date",
                "insider_names",
                "num_operations",
                "total_amount",
                "score_1_10",
            ]
        ].rename(
            columns={
                "societe": "Société",
                "signal_date": "Date signal",
                "insider_names": "Dirigeants",
                "num_operations": "Nb opérations",
                "total_amount": "Montant (€)",
                "score_1_10": "Score (/10)",
            }
        )
        styled_insider = display_insider.style.background_gradient(
            subset=["Score (/10)"], cmap="Purples", vmin=1, vmax=10
        ).format({"Montant (€)": "{:,.0f}", "Score (/10)": "{:.1f}"})
        st.dataframe(styled_insider, use_container_width=True)

        st.divider()
        top15 = df_insider.head(15).copy()
        fig_bar = px.bar(
            top15,
            x="total_amount",
            y="societe",
            orientation="h",
            color="score_1_10",
            color_continuous_scale="Purples",
            range_color=[1, 10],
            labels={
                "total_amount": "Montant total déclaré (€)",
                "societe": "",
                "score_1_10": "Score",
            },
        )
        fig_bar.update_layout(height=400, yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(
            fig_bar,
            use_container_width=True,
            config={"displayModeBar": "hover", "displaylogo": False},
        )

    st.divider()

    # ── Section Fondamentaux ──────────────────────────────────────────────────
    st.subheader("📊 Fondamentaux financiers")
    st.caption(
        "Scores basés sur 4 métriques : croissance CA, marge opérationnelle, levier, FCF yield."
    )

    if df_financials.empty:
        st.info("Aucune donnée fondamentale disponible.")
    else:
        name_map = df_stocks.drop_duplicates("isin").set_index("isin")["company_name"]
        df_fin = df_financials.copy()
        df_fin["Entreprise"] = df_fin["isin"].map(name_map).fillna(df_fin["ticker"])

        display_fin = df_fin[
            [
                "Entreprise",
                "date_cloture_exercice",
                "score_fondamental",
                "ca_growth_pct",
                "marge_op_pct",
                "levier_dette_ebitda",
                "fcf_yield_pct",
                "nb_metrics",
                "coverage_score",
            ]
        ].rename(
            columns={
                "date_cloture_exercice": "Clôture",
                "score_fondamental": "Score fond. (/10)",
                "ca_growth_pct": "CA Growth (%)",
                "marge_op_pct": "Marge Op (%)",
                "levier_dette_ebitda": "Levier D/EBITDA",
                "fcf_yield_pct": "FCF Yield (%)",
                "nb_metrics": "Métriques",
                "coverage_score": "Couverture",
            }
        )
        styled_fin = display_fin.style.background_gradient(
            subset=["Score fond. (/10)"], cmap="Greens", vmin=0, vmax=10
        ).format(
            {
                "Score fond. (/10)": "{:.1f}",
                "CA Growth (%)": "{:.1f}%",
                "Marge Op (%)": "{:.1f}%",
                "FCF Yield (%)": "{:.1f}%",
                "Couverture": "{:.0%}",
                "Clôture": "{}",
            },
            na_rep="—",
        )
        st.dataframe(styled_fin, use_container_width=True)

        st.divider()
        df_scatter = df_financials.dropna(subset=["ca_growth_pct", "marge_op_pct"])
        if not df_scatter.empty:
            fig_scatter = px.scatter(
                df_scatter,
                x="ca_growth_pct",
                y="marge_op_pct",
                size="score_fondamental",
                color="score_fondamental",
                color_continuous_scale="RdYlGn",
                range_color=[0, 10],
                hover_name="ticker",
                labels={
                    "ca_growth_pct": "Croissance CA (%)",
                    "marge_op_pct": "Marge opérationnelle (%)",
                    "score_fondamental": "Score",
                },
            )
            fig_scatter.add_vline(x=0, line_dash="dot", line_color="grey")
            fig_scatter.add_hline(y=0, line_dash="dot", line_color="grey")
            fig_scatter.update_layout(height=380)
            st.caption("Quadrant supérieur droit = croissance rentable = profil PEA-PME idéal.")
            st.plotly_chart(
                fig_scatter,
                use_container_width=True,
                config={"displayModeBar": "hover", "displaylogo": False},
            )


# ── Tab D — Analyse Technique (EXISTANT — inchangé) ──────────────────────────


def render_kpis(df: pd.DataFrame) -> None:
    total = len(df)
    top_row = df.iloc[0]
    pct_strong = (df["score_technique"] >= 7).sum() / total * 100
    golden_cross_count = int((df["golden_cross_signal"] == 2).sum())

    c1, c2, c3, c4 = st.columns(4)
    c1.metric(
        "Entreprises analysées",
        total,
        help="Nombre de PME éligibles PEA-PME scorées aujourd'hui",
    )
    c2.metric(
        "Meilleure opportunité du jour",
        top_row["company_name"],
        delta=f"{top_row['score_technique']:.1f} / 10",
        help="Entreprise avec le score technique le plus élevé ce jour",
    )
    c3.metric(
        "Score ≥ 7 (haussier)",
        f"{pct_strong:.1f}%",
        help="Part des entreprises avec score brut ≥ 7 aujourd'hui",
    )
    c4.metric(
        "Régime haussier long terme",
        golden_cross_count,
        help="Entreprises dont la moyenne mobile 50j > moyenne 200j (Golden Cross actif)",
    )


def render_ranking(df: pd.DataFrame) -> None:
    st.subheader("Classement du jour")
    st.caption("Score de 0 à 10 basé sur 5 indicateurs techniques · En cas d'égalité, départagé par la moyenne glissante 7 jours (Moy. 7j).")

    top10 = df.head(10).copy().reset_index(drop=True)
    top10["#"] = top10.index + 1

    for col in SIGNAL_COLS:
        top10[SIGNAL_LABELS[col]] = top10[col].map(SIGNAL_EMOJI)

    signal_label_cols = list(SIGNAL_LABELS.values())
    display = top10[
        ["#", "company_name", "date", "score_technique", "score_7d_avg", "Close"]
        + signal_label_cols
    ].rename(
        columns={
            "company_name": "Entreprise",
            "date": "Date du score",
            "score_technique": "Score (/10)",
            "score_7d_avg": "Moy. 7j",
            "Close": "Cours (€)",
        }
    )

    styled = (
        display.set_index("#")
        .style.background_gradient(subset=["Score (/10)"], cmap="RdYlGn", vmin=0, vmax=10)
        .background_gradient(subset=["Moy. 7j"], cmap="RdYlGn", vmin=0, vmax=10)
        .format({"Score (/10)": "{:.1f}", "Moy. 7j": "{:.1f}", "Cours (€)": "{:.2f}"}, na_rep="—")
    )
    st.dataframe(styled, use_container_width=True)


def render_distribution(df: pd.DataFrame) -> None:
    st.subheader("Répartition des scores")
    st.caption("Répartition de l'ensemble des entreprises scorées aujourd'hui.")

    fig = px.histogram(
        df,
        x="score_technique",
        nbins=20,
        color_discrete_sequence=["#4C72B0"],
        labels={"score_technique": "Score technique"},
    )
    fig.add_vrect(
        x0=0,
        x1=4,
        fillcolor="red",
        opacity=0.07,
        line_width=0,
        annotation_text="Prudence",
        annotation_position="top left",
    )
    fig.add_vrect(
        x0=4,
        x1=7,
        fillcolor="yellow",
        opacity=0.07,
        line_width=0,
        annotation_text="Neutre",
        annotation_position="top left",
    )
    fig.add_vrect(
        x0=7,
        x1=10,
        fillcolor="green",
        opacity=0.07,
        line_width=0,
        annotation_text="Opportunité",
        annotation_position="top left",
    )
    fig.update_layout(height=300, bargap=0.05)
    st.plotly_chart(
        fig, use_container_width=True, config={"displayModeBar": "hover", "displaylogo": False}
    )
    st.info(
        "La distribution symétrique autour de 5 est attendue : RSI et Bollinger sont neutres "
        "par défaut dans un marché normal."
    )


def render_signals(df: pd.DataFrame) -> None:
    with st.expander("Comment lire cette heatmap ?"):
        st.markdown(
            """
| Valeur | Signification |
|--------|--------------|
| **2** 🟢 | Signal **haussier** — indicateur positif |
| **1** 🟡 | Signal **neutre** — pas de signal clair |
| **0** 🔴 | Signal **baissier** — indicateur négatif |

Un score élevé correspond à beaucoup de 🟢 sur la même ligne.
"""
        )

    tab_top, tab_flop = st.tabs(["🏆 Top 10", "⚠️ Flop 10"])

    def _heatmap(subset: pd.DataFrame) -> None:
        heatmap_df = subset.set_index("company_name")[SIGNAL_COLS].rename(columns=SIGNAL_LABELS)
        fig = px.imshow(
            heatmap_df.T,
            color_continuous_scale="RdYlGn",
            range_color=[0, 2],
            aspect="auto",
            labels={"color": "Signal"},
            text_auto=True,
        )
        fig.update_coloraxes(
            colorbar_tickvals=[0, 1, 2],
            colorbar_ticktext=["Baissier (0)", "Neutre (1)", "Haussier (2)"],
        )
        fig.update_layout(height=280)
        st.plotly_chart(
            fig, use_container_width=True, config={"displayModeBar": "hover", "displaylogo": False}
        )

    with tab_top:
        _heatmap(df.head(10).copy())
    with tab_flop:
        _heatmap(df.tail(10).copy())


def render_history(top_isins: tuple[str, ...]) -> None:
    st.subheader("Évolution du score — Top 5")

    period = st.radio(
        "Période",
        list(PERIOD_DAYS.keys()),
        horizontal=True,
        index=0,
        label_visibility="collapsed",
    )
    days = PERIOD_DAYS[period]

    hist = load_score_history(top_isins, days)
    if hist.empty:
        st.info("Pas de données historiques disponibles.")
        return

    # ── Graphique 1 : moyenne glissante 7 jours ───────────────────────────────
    st.caption("Moyenne glissante 7 jours — lisse les variations quotidiennes.")
    fig_avg = px.line(
        hist,
        x="date",
        y="score_7d_avg",
        color="company_name",
        labels={"score_7d_avg": "Moy. 7j (/10)", "company_name": "Entreprise", "date": "Date"},
    )
    fig_avg.add_hline(
        y=7,
        line_dash="dot",
        line_color="grey",
        annotation_text="Seuil haussier (6)",
        annotation_position="bottom right",
    )
    fig_avg.update_layout(height=300, yaxis={"range": [0, 10]})
    st.plotly_chart(
        fig_avg, use_container_width=True, config={"displayModeBar": "hover", "displaylogo": False}
    )

    # ── Graphique 2 : score brut ───────────────────────────────────────────────
    st.caption("Score brut du jour · La ligne pointillée marque le seuil haussier (7).")
    fig = px.line(
        hist,
        x="date",
        y="score_technique",
        color="company_name",
        labels={"score_technique": "Score (/10)", "company_name": "Entreprise", "date": "Date"},
    )
    fig.add_hline(
        y=6,
        line_dash="dot",
        line_color="grey",
        annotation_text="Seuil haussier (6)",
        annotation_position="bottom right",
    )
    fig.update_layout(height=300, yaxis={"range": [0, 10]})
    st.plotly_chart(
        fig, use_container_width=True, config={"displayModeBar": "hover", "displaylogo": False}
    )


# ── Main ──────────────────────────────────────────────────────────────────────


def main() -> None:
    st.set_page_config(
        page_title="PEA-PME Pulse",
        page_icon="📈",
        layout="wide",
    )

    render_sidebar()

    st.title("📈 PEA-PME Pulse — Tableau de bord")
    st.caption("Scoring composite quotidien · 4 dimensions · Mise à jour chaque soir en semaine")

    with st.spinner("Chargement des données..."):
        df_composite = load_company_scores()
        df_stocks = load_latest_scores()
        df_news = load_news_scores()
        df_insider = load_insider_scores()
        df_financials = load_financials_scores()
        df_articles = load_article_sentiments()

    if not df_stocks.empty:
        df_stocks = df_stocks.sort_values(
            ["score_technique", "score_7d_avg"], ascending=False
        ).reset_index(drop=True)

    if not df_composite.empty:
        render_global_kpis(df_composite)
        render_freshness(df_composite, df_stocks, df_news, df_insider, df_financials)
        st.divider()

    tab_A, tab_B, tab_C, tab_D = st.tabs(
        [
            "🏆 Vue Composite",
            "📰 Actualités",
            "🏦 Insiders & Fondamentaux",
            "📈 Analyse Technique",
        ]
    )

    with tab_A:
        if df_composite.empty:
            st.info("Données composite non disponibles.")
        else:
            render_composite_tab(df_composite)

    with tab_B:
        if df_news.empty:
            st.info("Données actualités non disponibles.")
        else:
            render_news_tab(df_news, df_articles)

    with tab_C:
        render_insiders_financials_tab(df_insider, df_financials, df_stocks)

    with tab_D:
        if df_stocks.empty:
            st.error("Aucune donnée disponible dans gold.stocks_score.")
        else:
            render_kpis(df_stocks)
            st.divider()
            tab_d1, tab_d2, tab_d3 = st.tabs(["📊 Classement", "🔬 Signaux", "📈 Historique"])
            with tab_d1:
                col_left, col_right = st.columns([3, 2])
                with col_left:
                    render_ranking(df_stocks)
                with col_right:
                    render_distribution(df_stocks)
            with tab_d2:
                render_signals(df_stocks)
            with tab_d3:
                render_history(tuple(df_stocks.head(5)["isin"].tolist()))


if __name__ == "__main__":
    main()
