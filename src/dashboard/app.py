import os

import pandas as pd
import plotly.express as px
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


# ── Layout ────────────────────────────────────────────────────────────────────


def render_sidebar() -> None:
    with st.sidebar:
        st.header("📖 Comment lire ce dashboard ?")
        st.markdown(
            """
**Score de 0 à 10** basé sur 5 indicateurs boursiers.
Plus le score est élevé, plus les signaux techniques sont positifs.

🟢 **7 – 10** : Signaux majoritairement haussiers

🟡 **4 – 7** : Signaux mixtes

🔴 **0 – 4** : Signaux majoritairement baissiers

---
**Les 5 indicateurs :**

📊 **RSI** — L'action est-elle survendue ou surachetée ?
*(RSI < 35 = signal d'achat potentiel)*

📈 **MACD** — La tendance s'accélère-t-elle ?

✨ **Golden Cross** — La tendance longue est-elle haussière ?
*(moyenne 50j > moyenne 200j)*

🎯 **Bollinger** — Le cours est-il bas dans sa fourchette habituelle ?
*(%B < 20% de la bande = zone d'achat)*

📉 **Tendance EMA** — Le cours dépasse-t-il sa moyenne des 20 derniers jours ?

---
*⚠️ Informatif uniquement — pas un conseil en investissement.*
"""
        )


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
    st.caption("Score de 0 à 10 basé sur 5 indicateurs techniques.")

    top10 = df.head(10).copy().reset_index(drop=True)
    top10["#"] = top10.index + 1
    top10["Percentile"] = (df["score_technique"].rank(pct=True) * 100).head(10).round(0).astype(int)

    for col in SIGNAL_COLS:
        top10[SIGNAL_LABELS[col]] = top10[col].map(SIGNAL_EMOJI)

    signal_label_cols = list(SIGNAL_LABELS.values())
    display = top10[
        ["#", "company_name", "date", "score_technique", "Percentile", "Close"] + signal_label_cols
    ].rename(
        columns={
            "company_name": "Entreprise",
            "date": "Date du score",
            "score_technique": "Score (/10)",
            "Close": "Cours (€)",
        }
    )

    styled = (
        display.set_index("#")
        .style.background_gradient(subset=["Score (/10)"], cmap="RdYlGn", vmin=0, vmax=10)
        .background_gradient(subset=["Percentile"], cmap="RdYlGn", vmin=0, vmax=100)
        .format({"Score (/10)": "{:.1f}", "Cours (€)": "{:.2f}"})
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
        annotation_text="Seuil haussier (7)",
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
        y=7,
        line_dash="dot",
        line_color="grey",
        annotation_text="Seuil haussier (7)",
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
    st.caption(
        "Scoring technique quotidien · Données BigQuery · Mise à jour chaque soir en semaine"
    )

    with st.spinner("Chargement des données..."):
        df = load_latest_scores()

    if df.empty:
        st.error("Aucune donnée disponible dans gold.stocks_score.")
        return

    render_kpis(df)
    st.divider()

    tab1, tab2, tab3 = st.tabs(["📊 Classement", "🔬 Signaux", "📈 Historique"])

    with tab1:
        col_left, col_right = st.columns([3, 2])
        with col_left:
            render_ranking(df)
        with col_right:
            render_distribution(df)

    with tab2:
        render_signals(df)

    with tab3:
        top5_isins = tuple(df.head(5)["isin"].tolist())
        render_history(top5_isins)


if __name__ == "__main__":
    main()
