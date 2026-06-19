# streamlit_app/utils.py — shared design system for all dashboard pages
import plotly.graph_objects as go
import plotly.io as pio
import streamlit as st

# ── Palette ───────────────────────────────────────────────────────────────────
C = {
    "primary":   "#1A3C2D",   # deep forest green
    "green":     "#2D6A4F",   # medium green
    "amber":     "#C8792A",   # warm amber
    "blue":      "#1E6B8A",   # steel blue
    "purple":    "#7B4E9E",   # violet
    "red":       "#C0392B",   # terracotta red
    "teal":      "#16A085",   # emerald teal
    "slate":     "#455A64",   # slate
    "bg":        "#F4F7F5",   # sage background
    "surface":   "#FFFFFF",   # card surface
    "border":    "#DDE8E2",   # soft border
    "muted":     "#5A7A6A",   # muted text
    "grid":      "#EEF4F0",   # chart gridlines
}

COLORS = [
    C["green"], C["amber"], C["blue"], C["purple"],
    C["red"],   C["teal"],  C["slate"], "#E67E22",
]

# ── Plotly template ───────────────────────────────────────────────────────────
_FONT = "Inter, Be Vietnam Pro, -apple-system, sans-serif"

_yhct = go.layout.Template(
    layout=go.Layout(
        font=dict(family=_FONT, size=12, color="#1A2B22"),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="#FAFCFB",
        colorway=COLORS,
        title=dict(
            font=dict(size=14, color=C["primary"]),
            x=0, xanchor="left",
            pad=dict(b=14),
        ),
        xaxis=dict(
            gridcolor=C["grid"], zeroline=False, showline=False,
            tickfont=dict(size=11, color=C["muted"]),
            title_font=dict(size=11, color=C["muted"]),
        ),
        yaxis=dict(
            gridcolor=C["grid"], zeroline=False, showline=False,
            tickfont=dict(size=11, color=C["muted"]),
            title_font=dict(size=11, color=C["muted"]),
        ),
        legend=dict(
            bgcolor="rgba(0,0,0,0)",
            font=dict(size=11, color="#2A3B32"),
            borderwidth=0,
        ),
        margin=dict(l=8, r=8, t=52, b=8),
        hoverlabel=dict(
            bgcolor=C["primary"], font_size=12, font_family=_FONT,
            font_color="white", bordercolor=C["primary"],
        ),
    )
)
pio.templates["yhct"] = _yhct
pio.templates.default = "yhct"


def fc(fig, height: int = 360) -> go.Figure:
    """Apply consistent final styling to any Plotly figure."""
    fig.update_layout(height=height)
    return fig


# ── CSS ───────────────────────────────────────────────────────────────────────
_CSS = """
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Be+Vietnam+Pro:wght@400;500;600;700;800&family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">

<style>
/* ── Base ──────────────────────────────────────────────────────────────── */
html, body, [class*="css"], .stApp {
    font-family: 'Inter', 'Be Vietnam Pro', -apple-system, sans-serif !important;
}
.stApp { background: #F4F7F5 !important; }
.main .block-container {
    padding: 1.5rem 2.5rem 3rem;
    max-width: 1440px;
}
#MainMenu, footer { visibility: hidden; }
.stDeployButton { display: none !important; }

/* ── Tabs ──────────────────────────────────────────────────────────────── */
.stTabs [data-baseweb="tab-list"] {
    background: #FFFFFF;
    border-radius: 10px;
    padding: 5px;
    gap: 2px;
    box-shadow: 0 1px 4px rgba(0,0,0,0.08);
    border: 1px solid #DDE8E2;
    flex-wrap: wrap;
}
.stTabs [data-baseweb="tab"] {
    border-radius: 7px !important;
    font-weight: 500 !important;
    font-size: 0.83rem !important;
    color: #4A6B5A !important;
    padding: 7px 16px !important;
    background: transparent !important;
    border: none !important;
    transition: all 0.15s ease;
}
.stTabs [aria-selected="true"] {
    background: #1A3C2D !important;
    color: #FFFFFF !important;
    box-shadow: 0 2px 6px rgba(26,60,45,0.25) !important;
}
.stTabs [data-baseweb="tab-highlight"],
.stTabs [data-baseweb="tab-border"] { display: none !important; }

/* ── Metric cards ──────────────────────────────────────────────────────── */
[data-testid="metric-container"] {
    background: #FFFFFF !important;
    border: 1px solid #DDE8E2 !important;
    border-top: 3px solid #2D6A4F !important;
    border-radius: 12px !important;
    padding: 16px 20px !important;
    box-shadow: 0 1px 4px rgba(0,0,0,0.05) !important;
}
[data-testid="metric-container"] label {
    font-size: 0.74rem !important;
    font-weight: 600 !important;
    color: #5A7A6A !important;
    text-transform: uppercase !important;
    letter-spacing: 0.06em !important;
}
[data-testid="stMetricValue"] > div {
    font-size: 1.75rem !important;
    font-weight: 800 !important;
    color: #1A3C2D !important;
    line-height: 1.15 !important;
}
[data-testid="stMetricDelta"] { font-size: 0.8rem !important; }

/* ── Expanders ─────────────────────────────────────────────────────────── */
[data-testid="stExpander"] {
    border: 1px solid #DDE8E2 !important;
    border-radius: 10px !important;
    background: #FFFFFF !important;
    box-shadow: none !important;
}
[data-testid="stExpander"] summary {
    font-weight: 600 !important;
    color: #1A3C2D !important;
    font-size: 0.88rem !important;
}

/* ── Dataframes ────────────────────────────────────────────────────────── */
.stDataFrame { border-radius: 10px !important; border: 1px solid #DDE8E2 !important; overflow: hidden; }
.stDataFrame thead th {
    background: #F0F5F2 !important;
    color: #1A3C2D !important;
    font-weight: 600 !important;
    font-size: 0.82rem !important;
}

/* ── Alerts ────────────────────────────────────────────────────────────── */
[data-testid="stAlertInfo"] {
    border-left: 4px solid #2D6A4F !important;
    border-radius: 8px !important;
    background: #EEF5F1 !important;
}
[data-testid="stAlertWarning"] {
    border-left: 4px solid #C8792A !important;
    border-radius: 8px !important;
}
[data-testid="stAlertError"] {
    border-left: 4px solid #C0392B !important;
    border-radius: 8px !important;
}

/* ── Buttons ────────────────────────────────────────────────────────────── */
[data-testid="baseButton-primary"] {
    background: #1A3C2D !important;
    color: #FFFFFF !important;
    border: none !important;
    border-radius: 8px !important;
    font-weight: 600 !important;
    transition: background 0.15s ease !important;
}
[data-testid="baseButton-primary"]:hover {
    background: #2D6A4F !important;
}
[data-testid="baseButton-secondary"] {
    border-color: #DDE8E2 !important;
    color: #1A3C2D !important;
    border-radius: 8px !important;
}

/* ── Headings ───────────────────────────────────────────────────────────── */
h1 { font-size: 1.55rem !important; font-weight: 800 !important; color: #1A3C2D !important; letter-spacing: -0.02em !important; }
h2, h3 { color: #1A3C2D !important; font-weight: 700 !important; }

/* ── Custom components ──────────────────────────────────────────────────── */
.kpi-grid { display: flex; gap: 14px; flex-wrap: wrap; margin: 4px 0 20px; }
.kpi-card {
    flex: 1; min-width: 130px;
    background: #FFFFFF;
    border-radius: 12px;
    padding: 18px 22px;
    border: 1px solid #DDE8E2;
    border-top: 3px solid #2D6A4F;
    box-shadow: 0 1px 4px rgba(0,0,0,0.06);
}
.kpi-card.c-amber  { border-top-color: #C8792A; }
.kpi-card.c-blue   { border-top-color: #1E6B8A; }
.kpi-card.c-red    { border-top-color: #C0392B; }
.kpi-card.c-teal   { border-top-color: #16A085; }
.kpi-card.c-purple { border-top-color: #7B4E9E; }
.kpi-card.c-slate  { border-top-color: #455A64; }

.kpi-label {
    font-size: 0.7rem; font-weight: 700; color: #5A7A6A;
    text-transform: uppercase; letter-spacing: 0.07em; margin-bottom: 8px;
}
.kpi-value {
    font-size: 2rem; font-weight: 800; color: #1A3C2D; line-height: 1.1;
}
.kpi-sub { font-size: 0.76rem; color: #8A9A90; margin-top: 5px; }

.sect {
    font-size: 1rem; font-weight: 700; color: #1A3C2D;
    padding: 0 0 10px; margin: 24px 0 12px;
    border-bottom: 2px solid #DDE8E2;
    display: flex; align-items: center; gap: 8px;
}

.chart-wrap {
    background: #FFFFFF;
    border-radius: 12px;
    border: 1px solid #DDE8E2;
    padding: 16px;
    box-shadow: 0 1px 4px rgba(0,0,0,0.05);
    margin-bottom: 14px;
}
</style>
"""


def inject_css() -> None:
    st.markdown(_CSS, unsafe_allow_html=True)


def kpis(cards: list[dict]) -> None:
    """
    Render a row of KPI cards.
    Each card: {label, value, sub=None, accent=None}
    accent: None|"amber"|"blue"|"red"|"teal"|"purple"|"slate"
    """
    parts = []
    for c in cards:
        cls = f'kpi-card c-{c["accent"]}' if c.get("accent") else "kpi-card"
        sub = f'<div class="kpi-sub">{c["sub"]}</div>' if c.get("sub") else ""
        parts.append(
            f'<div class="{cls}">'
            f'<div class="kpi-label">{c["label"]}</div>'
            f'<div class="kpi-value">{c["value"]}</div>'
            f'{sub}</div>'
        )
    st.markdown(f'<div class="kpi-grid">{"".join(parts)}</div>', unsafe_allow_html=True)


def section(icon: str, title: str) -> None:
    st.markdown(f'<div class="sect"><span>{icon}</span>{title}</div>', unsafe_allow_html=True)
