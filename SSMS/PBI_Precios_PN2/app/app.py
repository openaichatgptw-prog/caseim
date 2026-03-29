from __future__ import annotations

import hashlib
import html
import io
import math
import re
import sys
import importlib
from datetime import datetime
from pathlib import Path
from typing import Any, Final

import pandas as pd
import streamlit as st

try:
    import plotly.express as px

    _HAS_PLOTLY = True
except ImportError:  # pip install plotly
    px = None  # type: ignore[misc, assignment]
    _HAS_PLOTLY = False

from services.data_access import (
    buscar_referencias,
    ejecutar_sql_laboratorio,
    obtener_auditoria_dashboard,
    obtener_auditoria_referencias,
    obtener_catalogo_bodegas_auditoria,
    obtener_dashboard_ventas,
    obtener_detalle_ventas_filtrado,
    obtener_dimensiones_ventas,
    refrescar_catalogo_bodegas_auditoria,
    obtener_dataset_margenes,
    obtener_existencia_por_bodega_consulta,
    obtener_rango_fechas_ventas_raw,
    obtener_resumen_referencia,
    obtener_ultimas_ventas,
    sync_read_db,
)
from services import data_access as data_access_service
from services.filter_prefs import load_filter_prefs_into_session, render_reset_filters_button, save_tab_filter_prefs
from services.pipeline_runner import ejecutar_pipelines
from services.sql_reports_loader import SQL_001_KEY, SQL_002_KEY, SQL_003_KEY

PIPELINE_BASE_DIR = Path(__file__).resolve().parents[1]
if str(PIPELINE_BASE_DIR) not in sys.path:
    sys.path.insert(0, str(PIPELINE_BASE_DIR))

try:
    from ref_normalization import normalize_reference_text
except ImportError:
    # En recargas de Streamlit puede quedar un módulo parcialmente inicializado
    # en memoria (sys.modules). Lo invalidamos y reintentamos.
    sys.modules.pop("ref_normalization", None)
    normalize_reference_text = importlib.import_module("ref_normalization").normalize_reference_text

PIPELINE_OPCIONES: Final[list[str]] = [
    "01_Mejora_pipeline_precios_chnV21.py",
    "02_ventas_precios_cnhV2.py",
    "03_Maestro_historico.py",
]
SQL_001_OPCION: Final[str] = "SQL 001 — Margen SIESA (margen_siesa_raw)"
SQL_002_OPCION: Final[str] = "SQL 002 — Atributos refs (atributos_referencias_raw)"
SQL_003_OPCION: Final[str] = "SQL 003 — Auditoría refs (auditoria_raw)"
SQL_OPCIONES: Final[list[str]] = [SQL_001_OPCION, SQL_002_OPCION, SQL_003_OPCION]


def _streamlit_fragment_optional():
    """Streamlit ≥1.33: `st.fragment` evita rerun de toda la app al cambiar widgets del bloque decorado."""
    frag = getattr(st, "fragment", None)
    return frag if frag is not None else (lambda fn: fn)


def _todas_opciones_actualizacion() -> list[str]:
    return list(PIPELINE_OPCIONES) + list(SQL_OPCIONES)


def _seleccion_actualizacion_desde_session_state() -> list[str]:
    """
    Reconstruye qué ejecutar solo desde st.session_state.
    Rápida / Completa son atajos fijos; Personalizada usa dos listas (Python vs SQL 00).
    """
    todas = _todas_opciones_actualizacion()
    preset = st.session_state.get("upd_preset", "Completa")
    if preset == "Rápida":
        return list(PIPELINE_OPCIONES)
    if preset == "Completa":
        return list(todas)
    pl = [x for x in (st.session_state.get("upd_multiselect_pipelines") or []) if x in PIPELINE_OPCIONES]
    sq = [x for x in (st.session_state.get("upd_multiselect_sql") or []) if x in SQL_OPCIONES]
    return pl + sq


def _resumen_actualizacion_texto(seleccion: list[str]) -> str:
    """Texto corto para que el usuario vea qué va a correr."""
    pl = [p for p in seleccion if p in PIPELINE_OPCIONES]
    sq = [s for s in seleccion if s in SQL_OPCIONES]
    partes: list[str] = []
    if pl:
        partes.append("Pipelines: " + ", ".join(p.replace(".py", "") for p in pl))
    if sq:
        partes.append("SQL 00: " + ", ".join(s.split("—")[0].strip() for s in sq))
    return " · ".join(partes) if partes else "(nada seleccionado)"


def _init_multiselect_list(key: str, options: list[str]) -> None:
    """Evita default=[] + valor en session_state (p. ej. desde user_filter_prefs.json)."""
    opt_set = set(options)
    if key not in st.session_state:
        st.session_state[key] = []
    else:
        prev = st.session_state[key]
        if not isinstance(prev, list):
            st.session_state[key] = []
        else:
            st.session_state[key] = [x for x in prev if x in opt_set]


st.set_page_config(page_title="Consulta precios CNH", page_icon=":bar_chart:", layout="wide")
load_filter_prefs_into_session()

st.markdown(
    """
    <style>
    :root {
        --ui-surface: #101a31;
        --ui-border: #25314d;
        --ui-radius: 10px;
        --ui-shadow: 0 3px 10px rgba(0, 0, 0, 0.22);
        --ui-text: #e5e7eb;
        --ui-muted: #94a3b8;
        --ui-accent: #38bdf8;
        --ui-best: #14b8a6;
    }
    .stApp {
        background: #0b1220;
        color: #e5e7eb;
    }
    .main .block-container {
        max-width: 1180px;
        padding-top: 1rem;
        padding-bottom: 1.4rem;
    }
    .hero-card {
        border: 1px solid #25314d;
        border-radius: 12px;
        padding: 1rem 1.1rem;
        margin-bottom: 1rem;
        background: #101a31;
        box-shadow: 0 3px 10px rgba(0, 0, 0, 0.22);
    }
    .hero-title {
        color: #f8fafc;
        font-size: 1.28rem;
        font-weight: 700;
        margin: 0;
        letter-spacing: 0.1px;
    }
    .hero-subtitle {
        color: #9ca3af;
        margin-top: 0.35rem;
        margin-bottom: 0;
        font-size: 0.92rem;
    }
    .section-title {
        color: #e5e7eb;
        margin-top: 0.65rem;
        margin-bottom: 0.35rem;
        font-weight: 650;
        font-size: 0.95rem;
    }
    .section-title--first {
        margin-top: 0.1rem;
    }
    div[data-testid="stMetric"] {
        border: 1px solid var(--ui-border, #25314d);
        border-radius: var(--ui-radius, 10px);
        padding: 0.34rem 0.48rem;
        background: var(--ui-surface, #101a31);
        box-shadow: var(--ui-shadow, 0 3px 10px rgba(0, 0, 0, 0.22));
        min-height: auto;
        position: relative;
        overflow: hidden;
    }
    div[data-testid="stMetric"]::before {
        content: "";
        position: absolute;
        left: 0;
        top: 0;
        width: 100%;
        height: 2px;
        background: linear-gradient(90deg, rgba(56, 189, 248, 0.92), rgba(20, 184, 166, 0.8));
        opacity: 0.95;
    }
    /* Menos “aire” vertical dentro de la tarjeta métrica */
    div[data-testid="stMetric"] > div {
        gap: 0.12rem !important;
    }
    div[data-testid="stMetric"] label p {
        font-size: 0.78rem !important;
        line-height: 1.2 !important;
        margin-bottom: 0.1rem !important;
    }
    div[data-testid="stMetric"] [data-testid="stMetricValue"] {
        font-size: 1.05rem !important;
        font-weight: 760 !important;
        color: #f8fafc !important;
        text-shadow: 0 0 10px rgba(56, 189, 248, 0.14);
    }
    div[data-testid="stMetric"] [data-testid="stMetricDelta"] {
        font-size: 0.72rem !important;
    }
    @media (max-width: 680px) {
        div[data-testid="stMetric"] [data-testid="stMetricValue"] {
            font-size: 0.97rem !important;
        }
    }
    /* Fila de KPIs: alinea alturas entre columnas hermanas */
    div[data-testid="column"] > div[data-testid="stVerticalBlock"] {
        gap: 0.45rem;
    }
    .hint-text {
        color: #9ca3af;
        font-size: 0.88rem;
        margin-top: 0.2rem;
    }
    /* Aire entre gráfico Plotly y pie de figura (simetría con márgenes internos del chart) */
    div[data-testid="stPlotlyChart"] {
        margin-bottom: 0.55rem;
    }
    .fig-caption {
        color: #94a3b8;
        font-size: 0.82rem;
        line-height: 1.45;
        margin: 0.85rem 0 0.45rem 0;
        padding: 0.45rem 0.55rem 0.5rem 0.55rem;
        border-left: 2px solid #334155;
        background: rgba(15, 23, 42, 0.45);
        border-radius: 0 8px 8px 0;
    }
    .fig-caption__label {
        color: #cbd5e1;
        font-weight: 600;
        font-size: 0.84rem;
    }
    /* Bloque único — ratio costo/lista (auditoría CAP3) */
    .audit-ratio-unit-header {
        margin: 0 0 0.15rem 0;
        padding: 0 0 0.65rem 0;
        border-bottom: 1px solid #25314d;
    }
    .audit-ratio-unit-header__kicker {
        display: block;
        font-size: 0.7rem;
        font-weight: 700;
        letter-spacing: 0.1em;
        text-transform: uppercase;
        color: #38bdf8;
        margin-bottom: 0.25rem;
    }
    .audit-ratio-unit-header__title {
        font-size: 1.02rem;
        font-weight: 650;
        color: #f8fafc;
        margin: 0;
        letter-spacing: 0.02em;
    }
    .audit-ratio-side-title {
        font-size: 0.74rem;
        font-weight: 650;
        color: #94a3b8;
        margin: 0 0 0.45rem 0;
        letter-spacing: 0.03em;
    }
    /* Resumen ejecutivo (Eje 1 / Eje 2): panel más estrecho + tipografía protagonista */
    div[data-testid="stVerticalBlockBorderWrapper"]:has(.audit-exec-kpi-inner) {
        max-width: 680px !important;
        margin-left: auto !important;
        margin-right: auto !important;
        width: 100% !important;
        box-sizing: border-box !important;
    }
    div[data-testid="stVerticalBlockBorderWrapper"]:has(.audit-exec-kpi-inner) > div[data-testid="stVerticalBlock"] {
        gap: 0.6rem !important;
        padding: 0.4rem 0.5rem 0.5rem 0.5rem !important;
    }
    div[data-testid="stVerticalBlockBorderWrapper"]:has(.audit-exec-kpi-inner) div[data-testid="stHorizontalBlock"] {
        gap: 0.55rem !important;
    }
    div[data-testid="stVerticalBlockBorderWrapper"]:has(.audit-exec-kpi-inner) div[data-testid="stMetric"] {
        padding: 0.42rem 0.5rem 0.48rem 0.5rem !important;
        min-height: 4.35rem;
    }
    div[data-testid="stVerticalBlockBorderWrapper"]:has(.audit-exec-kpi-inner) div[data-testid="stMetric"] label p {
        font-size: 0.82rem !important;
        line-height: 1.22 !important;
        font-weight: 620 !important;
    }
    div[data-testid="stVerticalBlockBorderWrapper"]:has(.audit-exec-kpi-inner) div[data-testid="stMetric"] [data-testid="stMetricValue"] {
        font-size: 1.22rem !important;
        font-weight: 780 !important;
    }
    div[data-testid="stVerticalBlockBorderWrapper"]:has(.audit-exec-kpi-inner) div[data-testid="stMetric"] [data-testid="stMetricDelta"] {
        font-size: 0.76rem !important;
    }
    /* Solo bloque ratio CAP3: tres KPI misma anchura y altura (rejilla 33/33/33) */
    div[data-testid="column"]:has(p.audit-ratio-side-title) div[data-testid="stHorizontalBlock"] {
        display: flex !important;
        align-items: stretch !important;
        width: 100% !important;
        flex-wrap: nowrap !important;
    }
    div[data-testid="column"]:has(p.audit-ratio-side-title) div[data-testid="stHorizontalBlock"] > div[data-testid="column"] {
        flex: 1 1 33.33% !important;
        min-width: 0 !important;
        max-width: 33.33% !important;
        width: 33.33% !important;
    }
    div[data-testid="column"]:has(p.audit-ratio-side-title) div[data-testid="stHorizontalBlock"] > div[data-testid="column"] div[data-testid="stMetric"] {
        min-height: 6.25rem;
        width: 100% !important;
        max-width: 100%;
        box-sizing: border-box;
    }
    div[data-testid="column"]:has(p.audit-ratio-side-title) div[data-testid="stHorizontalBlock"] > div[data-testid="column"] div[data-testid="stMetric"] label p {
        font-size: 0.72rem !important;
        line-height: 1.2 !important;
        white-space: normal !important;
        overflow-wrap: anywhere;
    }
    div[data-testid="column"]:has(p.audit-ratio-side-title) div[data-testid="stHorizontalBlock"] > div[data-testid="column"] div[data-testid="stMetric"] > div {
        height: 100%;
        justify-content: center;
    }
    div[data-testid="stTextInput"] input,
    div[data-testid="stSelectbox"] > div > div {
        background: #101a31 !important;
        border: 1px solid #2d3a58 !important;
        color: #e5e7eb !important;
        border-radius: 10px !important;
    }
    .stButton > button {
        border-radius: 10px !important;
        border: 1px solid #334155 !important;
        background: #1d4ed8 !important;
        color: #f8fafc !important;
        font-weight: 600 !important;
    }
    /* ── Tarjetas / paneles alineados con st.metric ───────────────────── */
    div[data-testid="stVerticalBlockBorderWrapper"] {
        background: var(--ui-surface) !important;
        border: 1px solid var(--ui-border) !important;
        border-radius: var(--ui-radius) !important;
        box-shadow: var(--ui-shadow) !important;
    }
    .ui-card {
        border: 1px solid var(--ui-border);
        border-radius: var(--ui-radius);
        background: var(--ui-surface);
        box-shadow: var(--ui-shadow);
        color: var(--ui-text);
        padding: 0.55rem 0.65rem;
    }
    .ui-card--tight {
        padding: 0.36rem 0.48rem;
    }
    .ui-card-heading {
        font-size: 0.72rem;
        font-weight: 650;
        letter-spacing: 0.06em;
        text-transform: uppercase;
        color: var(--ui-muted);
        margin: 0 0 0.3rem 0;
    }
    .consulta-strip-gap {
        height: 0.28rem;
    }
    /* Hasta 10 alternas: rejilla 5×2 en desktop, se adapta en móvil */
    .alt-chip-grid {
        display: grid;
        grid-template-columns: repeat(5, minmax(0, 1fr));
        gap: 0.28rem;
    }
    @media (max-width: 900px) {
        .alt-chip-grid {
            grid-template-columns: repeat(3, minmax(0, 1fr));
        }
    }
    @media (max-width: 520px) {
        .alt-chip-grid {
            grid-template-columns: repeat(2, minmax(0, 1fr));
        }
    }
    .alt-chip-note {
        margin: 0.4rem 0 0 0;
        font-size: 0.78rem;
        color: var(--ui-muted);
    }
    .alt-chip-row {
        display: flex;
        flex-wrap: nowrap;
        gap: 0.4rem;
        overflow-x: auto;
        padding-bottom: 2px;
        -webkit-overflow-scrolling: touch;
    }
    .alt-chip {
        min-width: 0;
        border: 1px solid #2d3a58;
        border-radius: var(--ui-radius);
        padding: 0.28rem 0.4rem;
        background: #0c1426;
    }
    .alt-chip.selected {
        border-color: rgba(56, 189, 248, 0.55);
        background: rgba(56, 189, 248, 0.08);
        box-shadow: 0 0 0 1px rgba(56, 189, 248, 0.12);
    }
    .alt-chip-role {
        display: block;
        font-size: 0.68rem;
        color: var(--ui-muted);
        text-transform: uppercase;
        letter-spacing: 0.04em;
        margin-bottom: 0.15rem;
    }
    .alt-chip-ref {
        font-size: 0.85rem;
        font-weight: 600;
        color: #f8fafc;
        display: block;
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
    }
    .alt-chip.selected .alt-chip-ref {
        color: var(--ui-accent);
    }
    /* Tres orígenes en una fila (sin bandas full-width vacías) */
    .origin-grid-unified {
        display: grid;
        grid-template-columns: repeat(3, minmax(0, 1fr));
        gap: 0.42rem;
        margin-top: 0.22rem;
        align-items: stretch;
    }
    .origin-grid-unified > .ui-card--origin {
        min-width: 0;
    }
    .origin-grid-unified .ui-origin-col--px {
        font-size: 0.95rem;
    }
    @media (max-width: 768px) {
        .origin-grid-unified {
            grid-template-columns: 1fr;
        }
        .origin-grid-unified .ui-origin-col--px {
            font-size: 1rem;
        }
    }
    /* Tarjeta por región: apilado vertical (evita solapes precio / “Disponibilidad” en rejillas estrechas) */
    .ui-card--origin-stack {
        display: flex;
        flex-direction: column;
        align-items: stretch;
        gap: 0.4rem;
        padding: 0.48rem 0.52rem !important;
        min-width: 0;
    }
    .ui-card--origin-stack .ui-origin-head {
        min-width: 0;
    }
    .ui-origin-name-line {
        display: flex;
        flex-wrap: wrap;
        align-items: center;
        gap: 0.3rem 0.45rem;
        row-gap: 0.28rem;
    }
    .ui-origin-name {
        font-size: 0.8rem;
        font-weight: 650;
        color: var(--ui-muted);
        line-height: 1.25;
    }
    .ui-card--origin-stack .ui-origin-col--px {
        font-size: 0.98rem;
        font-weight: 700;
        color: #f8fafc;
        text-align: left;
        line-height: 1.25;
        padding: 0.12rem 0;
        word-break: break-word;
    }
    .ui-card--origin-stack .ui-origin-col--dp {
        text-align: left;
        display: flex;
        flex-direction: column;
        align-items: flex-start;
        gap: 0.12rem;
        margin-top: auto;
    }
    .ui-origin-disp-lbl {
        font-size: 0.62rem;
        font-weight: 650;
        text-transform: uppercase;
        letter-spacing: 0.06em;
        color: var(--ui-muted);
    }
    .ui-origin-disp-val {
        font-size: 0.86rem;
        font-weight: 600;
        color: #e5e7eb;
    }
    .ui-card--origin.is-best {
        border-color: rgba(20, 184, 166, 0.45);
        box-shadow: 0 0 0 1px rgba(20, 184, 166, 0.18), var(--ui-shadow);
    }
    .ui-card--origin.is-best .ui-origin-col--px {
        color: #5eead4;
    }
    .ui-card--origin.is-best .ui-origin-disp-lbl {
        color: rgba(94, 234, 212, 0.88);
    }
    .ui-card--origin.is-best .ui-origin-disp-val {
        color: #5eead4;
        font-weight: 700;
    }
    .ui-card--origin.is-best .ui-origin-name {
        color: rgba(94, 234, 212, 0.95);
    }
    .ui-best-pill {
        display: inline-block;
        font-size: 0.58rem;
        font-weight: 700;
        letter-spacing: 0.05em;
        text-transform: uppercase;
        color: #0d9488;
        background: rgba(20, 184, 166, 0.14);
        border-radius: 999px;
        padding: 0.14rem 0.45rem;
        white-space: nowrap;
        flex-shrink: 0;
        line-height: 1.2;
    }
    /* Auditoría referencias: texto introductorio del cuadro de mando en un solo bloque */
    .auditoria-dash-blurb {
        font-size: 0.82rem;
        color: #94a3b8;
        line-height: 1.45;
        margin: 0 0 0.55rem 0;
    }
    .auditoria-dash-blurb p {
        margin: 0 0 0.4rem 0;
    }
    .auditoria-dash-blurb p:last-child {
        margin-bottom: 0;
    }
    h4.auditoria-tab-h {
        margin: 0 0 0.35rem 0;
        font-size: 1.05rem;
        font-weight: 650;
        color: #e5e7eb;
    }
    /* ── Consulta referencias: una hoja compacta, sin bloques apilados ─ */
    .consulta-page-lead {
        margin: 0 0 0.25rem 0;
        font-size: 0.8rem;
        font-weight: 650;
        color: #94a3b8;
        text-transform: uppercase;
        letter-spacing: 0.06em;
    }
    .consulta-sheet-wrap {
        width: 100%;
        margin: 0.3rem 0 0 0;
        box-sizing: border-box;
    }
    .consulta-sheet {
        border: 1px solid var(--ui-border);
        border-radius: 12px;
        background: linear-gradient(180deg, #0e1629 0%, #0a1020 100%);
        padding: 0.5rem 0.55rem 0.55rem 0.55rem;
        box-shadow: var(--ui-shadow);
    }
    .consulta-topline {
        display: flex;
        flex-wrap: wrap;
        align-items: flex-end;
        gap: 0.35rem 0.85rem;
        padding-bottom: 0.4rem;
        margin-bottom: 0.4rem;
        border-bottom: 1px solid rgba(37, 49, 77, 0.9);
    }
    .consulta-ref-block {
        font-size: 1.02rem;
        font-weight: 750;
        color: #f8fafc;
        letter-spacing: 0.02em;
        word-break: break-word;
        flex: 1 1 10rem;
        line-height: 1.25;
    }
    .consulta-kpi-inline {
        display: flex;
        flex-wrap: wrap;
        gap: 0.3rem 0.75rem;
        align-items: baseline;
        flex: 2 1 14rem;
    }
    .consulta-kpi-inline-item {
        display: inline-flex;
        align-items: baseline;
        gap: 0.35rem;
        white-space: nowrap;
    }
    .consulta-kpi-inline-item .consulta-kpi-lbl {
        font-size: 0.62rem;
        font-weight: 650;
        text-transform: uppercase;
        letter-spacing: 0.07em;
        color: #94a3b8;
    }
    .consulta-kpi-inline-item .consulta-kpi-val {
        font-size: 0.88rem;
        font-weight: 650;
        color: #e5e7eb;
    }
    .consulta-kpi-lbl-row {
        display: inline-flex;
        align-items: baseline;
        gap: 0.14rem;
    }
    .consulta-kpi-help-icon {
        display: inline-flex;
        align-items: center;
        justify-content: center;
        width: 0.92rem;
        height: 0.92rem;
        margin-bottom: 0.06rem;
        border-radius: 999px;
        border: 1px solid rgba(125, 211, 252, 0.42);
        font-size: 0.58rem;
        font-weight: 750;
        font-style: normal;
        line-height: 1;
        color: #7dd3fc;
        cursor: help;
        flex-shrink: 0;
        user-select: none;
    }
    .consulta-kpi-help-icon:hover {
        border-color: rgba(125, 211, 252, 0.75);
        background: rgba(56, 189, 248, 0.08);
    }
    .consulta-split {
        display: grid;
        grid-template-columns: minmax(0, 1fr) minmax(0, 1.15fr);
        gap: 0.45rem;
        align-items: start;
    }
    @media (max-width: 780px) {
        .consulta-split {
            grid-template-columns: 1fr;
        }
    }
    .consulta-panel-title {
        font-size: 0.64rem;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        color: #94a3b8;
        margin: 0 0 0.28rem 0;
    }
    .consulta-panel {
        border: 1px solid rgba(37, 49, 77, 0.75);
        border-radius: 8px;
        background: rgba(8, 14, 28, 0.55);
        padding: 0.3rem 0.38rem;
        min-width: 0;
        align-self: stretch;
    }
    .consulta-panel--alternas {
        min-height: 0;
    }
    .consulta-panel--alternas .alt-chip-grid {
        grid-template-columns: repeat(auto-fill, minmax(8.5rem, 1fr));
    }
    .consulta-sheet .origin-grid-unified {
        grid-template-columns: 1fr;
        gap: 0.28rem;
        margin-top: 0;
    }
    .consulta-hr {
        height: 1px;
        background: rgba(37, 49, 77, 0.85);
        margin: 0.38rem 0;
        border: 0;
    }
    .consulta-blockhead {
        font-size: 0.64rem;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        color: #94a3b8;
        margin: 0 0 0.3rem 0;
    }
    .consulta-note {
        margin: 0.2rem 0 0 0;
        font-size: 0.75rem;
        color: #94a3b8;
        line-height: 1.35;
    }
    .consulta-note--repo-formulas {
        margin-top: 0.55rem !important;
        line-height: 1.55 !important;
        max-width: 100%;
    }
    .consulta-kpi-cell .consulta-kpi-lbl {
        display: block;
        font-size: 0.62rem;
        font-weight: 650;
        text-transform: uppercase;
        letter-spacing: 0.07em;
        color: #94a3b8;
        margin-bottom: 0.12rem;
    }
    .consulta-kpi-cell .consulta-kpi-lbl-row {
        display: flex;
        align-items: center;
        gap: 0.2rem;
        margin-bottom: 0.12rem;
        flex-wrap: wrap;
    }
    .consulta-kpi-cell .consulta-kpi-lbl-row .consulta-kpi-lbl {
        display: inline;
        margin-bottom: 0;
    }
    .consulta-kpi-cell .consulta-kpi-val {
        font-size: 0.86rem;
        font-weight: 650;
        color: #e5e7eb;
        line-height: 1.25;
        word-break: break-word;
    }
    .consulta-kpi-cell {
        padding: 0.36rem 0.45rem;
        border: 1px solid rgba(42, 58, 92, 0.5);
        border-radius: 8px;
        background: rgba(13, 20, 38, 0.48);
        min-height: 56px;
    }
    .consulta-compra-grid {
        display: grid;
        grid-template-columns: repeat(4, minmax(150px, 1fr));
        gap: 0.38rem 0.45rem;
        margin: 0;
    }
    .consulta-compra-grid--two {
        grid-template-columns: repeat(2, minmax(0, 1fr));
        max-width: 100%;
    }
    @media (max-width: 980px) {
        .consulta-compra-grid {
            grid-template-columns: repeat(2, minmax(150px, 1fr));
        }
    }
    @media (max-width: 520px) {
        .consulta-compra-grid {
            grid-template-columns: 1fr;
        }
    }
    .consulta-ventas-wrap {
        width: 100%;
        margin: 0.35rem 0 0 0;
        box-sizing: border-box;
    }
    .consulta-ventas-head {
        font-size: 0.64rem;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        color: #94a3b8;
        margin: 0 0 0.2rem 0;
    }
    .consulta-ventas-head-row {
        display: inline-flex;
        align-items: center;
        gap: 0.28rem;
        flex-wrap: wrap;
        margin: 0 0 0.2rem 0;
    }
    .consulta-ventas-head-row .consulta-ventas-head {
        margin: 0;
    }
    .consulta-msg-soft {
        margin: 0.15rem 0 0 0;
        padding: 0.4rem 0.5rem;
        font-size: 0.84rem;
        color: #94a3b8;
        line-height: 1.4;
        border: 1px solid rgba(37, 49, 77, 0.65);
        border-radius: 8px;
        background: rgba(12, 20, 38, 0.5);
    }
    /* ── Consulta individual: jerarquía visual y ritmo ─────────────────── */
    .consulta-individual-hero {
        margin: 0 0 1rem 0;
        padding: 1rem 1.15rem 1.05rem 1.15rem;
        border-radius: 14px;
        border: 1px solid rgba(45, 58, 88, 0.95);
        background: linear-gradient(
            125deg,
            rgba(29, 78, 216, 0.14) 0%,
            rgba(14, 22, 41, 0.92) 42%,
            rgba(10, 16, 32, 0.98) 100%
        );
        box-shadow: 0 4px 24px rgba(0, 0, 0, 0.2);
    }
    .consulta-individual-title {
        margin: 0 0 0.4rem 0;
        font-size: 1.38rem;
        font-weight: 780;
        letter-spacing: -0.025em;
        color: #f8fafc;
        line-height: 1.2;
    }
    .consulta-individual-section-title {
        margin: 0 0 0.65rem 0;
        font-size: 0.7rem;
        font-weight: 750;
        text-transform: uppercase;
        letter-spacing: 0.11em;
        color: #7dd3fc;
    }
    .consulta-individual-section-title--muted {
        color: #94a3b8;
        letter-spacing: 0.09em;
    }
    .consulta-individual-disp-hint {
        margin: 0;
        padding: 0.35rem 0 0 0;
        font-size: 0.8rem;
        color: #94a3b8;
        line-height: 1.45;
        max-width: 22rem;
    }
    .consulta-sheet--individual {
        padding: 0.72rem 0.9rem 0.85rem 0.9rem;
        border-radius: 14px;
        border-color: rgba(56, 189, 248, 0.22);
        box-shadow: 0 6px 28px rgba(0, 0, 0, 0.22), 0 0 0 1px rgba(56, 189, 248, 0.06);
    }
    .consulta-sheet--individual .consulta-topline {
        padding-bottom: 0.55rem;
        margin-bottom: 0.5rem;
    }
    .consulta-sheet--individual .consulta-split {
        gap: 0.55rem;
    }
    .consulta-sheet--individual .consulta-panel {
        padding: 0.42rem 0.48rem;
    }
    .consulta-sheet--individual .origin-grid-unified {
        grid-template-columns: repeat(3, minmax(0, 1fr));
        gap: 0.4rem;
        margin-top: 0.22rem;
    }
    @media (max-width: 768px) {
        .consulta-sheet--individual .origin-grid-unified {
            grid-template-columns: 1fr;
        }
    }
    /* Reporte margen: KPIs en franja densa (1 fila en desktop; reflow en tablet/móvil) */
    .margen-kpi-strip {
        display: grid;
        grid-template-columns: repeat(6, minmax(0, 1fr));
        gap: 0.35rem;
        margin: 0.1rem 0 0.55rem 0;
        align-items: stretch;
    }
    @media (max-width: 1180px) {
        .margen-kpi-strip {
            grid-template-columns: repeat(3, minmax(0, 1fr));
        }
    }
    @media (max-width: 520px) {
        .margen-kpi-strip {
            grid-template-columns: repeat(2, minmax(0, 1fr));
        }
    }
    .margen-kpi-mini {
        border: 1px solid var(--ui-border);
        border-radius: var(--ui-radius);
        background: var(--ui-surface);
        box-shadow: var(--ui-shadow);
        padding: 0.34rem 0.45rem;
        min-width: 0;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
        gap: 0.18rem;
        min-height: 3.12rem;
        position: relative;
        overflow: hidden;
    }
    .margen-kpi-mini::before {
        content: "";
        position: absolute;
        left: 0;
        top: 0;
        width: 100%;
        height: 2px;
        background: linear-gradient(90deg, rgba(56, 189, 248, 0.92), rgba(20, 184, 166, 0.8));
        opacity: 0.95;
    }
    .margen-kpi-mini-lbl {
        font-size: 0.68rem;
        font-weight: 650;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        color: var(--ui-muted);
        line-height: 1.15;
    }
    .margen-kpi-mini-val {
        font-size: 1.02rem;
        font-weight: 760;
        color: #f8fafc;
        line-height: 1.15;
        word-break: break-word;
        text-shadow: 0 0 10px rgba(56, 189, 248, 0.14);
    }
    @media (max-width: 680px) {
        .margen-kpi-mini-val {
            font-size: 0.96rem;
        }
    }
    /* ── Estilo global para TODAS las pestañas (consistencia visual) ── */
    .stTabs [data-baseweb="tab-list"] {
        gap: 0.38rem;
        margin-bottom: 0.45rem;
    }
    .stTabs [data-baseweb="tab"] {
        height: 2rem;
        border-radius: 10px 10px 0 0;
        border: 1px solid #25314d;
        border-bottom: 0;
        background: rgba(12, 20, 38, 0.7);
        color: #b8c4d7;
        padding: 0 0.75rem;
        font-size: 0.82rem;
        font-weight: 600;
    }
    .stTabs [aria-selected="true"] {
        background: #101a31;
        color: #f8fafc;
        box-shadow: inset 0 -2px 0 0 rgba(56, 189, 248, 0.85);
    }
    .stTabs [data-baseweb="tab-highlight"] {
        background: transparent !important;
    }
    .stTabs [data-baseweb="tab-panel"] {
        border: 1px solid #25314d;
        border-radius: 0 12px 12px 12px;
        background: rgba(10, 16, 32, 0.62);
        padding: 0.75rem 0.8rem 0.65rem 0.8rem;
    }
    .stMarkdown h4, .stMarkdown h5 {
        margin-top: 0.2rem;
        margin-bottom: 0.32rem;
        color: #e5e7eb;
    }
    [data-testid="stCaptionContainer"] {
        margin-top: -0.1rem;
        margin-bottom: 0.35rem;
    }
    [data-testid="stCaptionContainer"] p {
        color: #94a3b8 !important;
        font-size: 0.81rem !important;
        line-height: 1.35 !important;
    }
    .stSlider {
        padding-top: 0.08rem;
    }
    .stSlider [data-testid="stTickBar"] {
        margin-top: -0.05rem;
    }
    .stSlider [data-baseweb="slider"] > div:first-child {
        height: 4px;
    }
    .stSlider [role="slider"] {
        border: 1px solid #7dd3fc;
        box-shadow: 0 0 0 2px rgba(56, 189, 248, 0.15);
    }
    div[data-testid="stDataFrame"] {
        border: 1px solid #25314d;
        border-radius: 10px;
        overflow: hidden;
        box-shadow: var(--ui-shadow);
        background: #0e1629;
    }
    div[data-testid="stDataFrame"] [role="grid"] {
        border-radius: 10px;
    }
    div[data-testid="stVerticalBlock"] > div:has(> div[data-testid="stAlert"]) {
        margin-top: 0.2rem;
        margin-bottom: 0.25rem;
    }
    hr {
        border-color: rgba(37, 49, 77, 0.8);
        margin-top: 0.6rem;
        margin-bottom: 0.6rem;
    }
    @media (max-width: 980px) {
        .main .block-container {
            padding-left: 0.8rem;
            padding-right: 0.8rem;
        }
        .stTabs [data-baseweb="tab-panel"] {
            padding: 0.65rem 0.6rem 0.55rem 0.6rem;
        }
    }
    </style>
    """,
    unsafe_allow_html=True,
)


def _to_float(value):
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _parse_refs_alternas(raw_refs: str) -> list[str]:
    txt = (raw_refs or "").strip()
    if not txt:
        return []
    if txt.startswith("(") and txt.endswith(")"):
        txt = txt[1:-1]
    refs = []
    for part in txt.split(","):
        ref = part.strip()
        if ref:
            refs.append(ref)
    return refs


_CONSULTA_MAX_ALTERNAS: Final[int] = 10

def _consulta_help_prorrateo() -> str:
    """Texto de ayuda alineado al cálculo del pipeline (incluye rango dinámico)."""
    hoy = datetime.now()
    fecha_desde = f"{hoy.year - 2}-01-01"
    fecha_hasta = hoy.strftime("%Y-%m-%d")
    return (
        "Precio sugerido en dólares (USD) que resume los precios de lista de Brasil, USA y Europa. "
        f"Participación tomada en el periodo {fecha_desde} a {fecha_hasta}. "
        "Si hay participación de ventas por región (partes de Brasil, USA y Europa), se calcula como "
        "precio de cada país × su participación, y se suman —un solo valor que refleja dónde más se ha vendido. "
        "Si no aplica esa mezcla, se usa el precio de la región que quede disponible o un promedio entre regiones."
    )


def _consulta_help_costo_exist_auditoria() -> str:
    """Auditoría (SQL 003): costos extremos y existencia total vs. universo de bodegas."""
    return (
        "Estos valores dependen del conjunto de bodegas consideradas en el reporte de auditoría cargado "
        "en la app (SQL 003). Ese modelo excluye bodegas de repuestos usados y bodegas de emergencia "
        "(por ejemplo BUSA, EPALM), que sí pueden aparecer en el detalle «Existencia por bodega (Siesa)»."
    )


def _consulta_help_origen_ultima_compra() -> str:
    """Contexto país / tipo origen para la ficha de última compra."""
    return (
        "País de la última compra según el cruce con auditoría de referencias (FactPricing / SQL 003) cuando está disponible; "
        "si también hay tipo de origen del maestro de precios (Brasil / USA / Europa u otro), se muestra junto. "
        "Sirve de contexto para importación y factores logísticos; no sustituye el detalle de la OC en lista."
    )


def _consulta_help_ultimas_ventas(fecha_min: str | None, fecha_max: str | None) -> str:
    """Tooltip con rango real de ventas_raw (MIN/MAX Fecha Factura en DuckDB)."""
    if fecha_min and fecha_max:
        return (
            f"Dataset ventas_raw: facturas desde {fecha_min} hasta {fecha_max} "
            "(mínimo y máximo de «Fecha Factura» en la tabla cargada en DuckDB)."
        )
    if fecha_min or fecha_max:
        a = fecha_min or "—"
        b = fecha_max or "—"
        return (
            f"Dataset ventas_raw: «Fecha Factura» entre {a} y {b} "
            "(al menos una fecha disponible en DuckDB)."
        )
    return (
        "Rango de fechas no disponible: ejecuta «Actualizar datos» con el pipeline que carga "
        "`ventas_raw` (ventas) para ver aquí el periodo exacto."
    )


def _consulta_html_topline_kpis(resumen: dict) -> str:
    """Referencia + KPIs en una sola franja horizontal (menos altura que bloques separados)."""
    ref = html.escape(str(resumen.get("Referencia_Original", "-")))
    pp = resumen.get("Precio Prorrateo")
    if pp is not None:
        try:
            pp_s = html.escape(_fmt_money_usd(float(pp), decimals=2))
        except (TypeError, ValueError):
            pp_s = "—"
    else:
        pp_s = "—"
    disp_t = html.escape(f"{resumen.get('_disp_total', 0):,.2f}")
    help_pr = html.escape(_consulta_help_prorrateo(), quote=True)
    items = (
        f'<span class="consulta-kpi-inline-item">'
        f'<span class="consulta-kpi-lbl-row">'
        f'<span class="consulta-kpi-lbl">Prorrateo</span>'
        f'<span class="consulta-kpi-help-icon" title="{help_pr}" '
        f'aria-label="{help_pr}" role="img">i</span>'
        f"</span>"
        f'<span class="consulta-kpi-val">{pp_s}</span></span>'
        f'<span class="consulta-kpi-inline-item"><span class="consulta-kpi-lbl">Disp. total</span>'
        f'<span class="consulta-kpi-val">{disp_t}</span></span>'
        ""
    )
    return (
        f'<div class="consulta-topline">'
        f'<span class="consulta-ref-block">{ref}</span>'
        f'<div class="consulta-kpi-inline">{items}</div>'
        f"</div>"
    )


def _consulta_html_alternas_panel(refs_alternas: str, ref_original: str) -> str:
    if not refs_alternas:
        return (
            '<div class="consulta-panel consulta-panel--alternas">'
            '<div class="consulta-panel-title">Referencias alternas</div>'
            '<p style="margin:0;color:#94a3b8;font-size:0.82rem;">Sin alternas registradas.</p>'
            "</div>"
        )
    refs = _parse_refs_alternas(refs_alternas)
    if not refs:
        return (
            '<div class="consulta-panel consulta-panel--alternas">'
            '<div class="consulta-panel-title">Referencias alternas</div>'
            '<p style="margin:0;color:#94a3b8;font-size:0.82rem;">Sin alternas registradas.</p>'
            "</div>"
        )
    total_alt = len(refs)
    refs_show = refs[:_CONSULTA_MAX_ALTERNAS]
    chips: list[str] = []
    for ref in refs_show:
        is_selected = ref.upper() == ref_original
        cls = "alt-chip selected" if is_selected else "alt-chip"
        role = "Principal" if is_selected else "Alterna"
        title_attr = html.escape(ref, quote=True)
        chips.append(
            f'<div class="{cls}" title="{title_attr}">'
            f'<span class="alt-chip-role">{html.escape(role)}</span>'
            f'<span class="alt-chip-ref">{html.escape(ref)}</span>'
            f"</div>"
        )
    cap_extra = ""
    if total_alt > _CONSULTA_MAX_ALTERNAS:
        cap_extra = (
            f'<p class="alt-chip-note">Mostrando {_CONSULTA_MAX_ALTERNAS} de {total_alt} alternas.</p>'
        )
    inner = (
        '<div class="consulta-panel-title">Referencias alternas</div>'
        f'<div class="alt-chip-grid">{"".join(chips)}</div>{cap_extra}'
    )
    return f'<div class="consulta-panel consulta-panel--alternas">{inner}</div>'


def _consulta_max_disp_para_slider(resumen: dict) -> float:
    """Tope del slider según datos (mín. 50 para UX)."""
    vals: list[float] = []
    for k in ("disp_br", "disp_usa", "disp_eur"):
        v = _to_float(resumen.get(k))
        if v is not None and math.isfinite(v) and v >= 0:
            vals.append(float(v))
    hi = max(vals) if vals else 100.0
    return max(50.0, hi * 1.35)


def _consulta_html_origins_panel(resumen: dict, disp_umbral: float) -> tuple[str, bool]:
    """HTML del panel de orígenes + True si hay un “mejor precio” elegible bajo la regla disp > umbral."""
    candidatos = [
        ("Brasil", _to_float(resumen.get("Precio Brasil")), _to_float(resumen.get("disp_br"))),
        ("USA", _to_float(resumen.get("Precio Usa")), _to_float(resumen.get("disp_usa"))),
        ("Europa", _to_float(resumen.get("Precio Europa")), _to_float(resumen.get("disp_eur"))),
    ]
    flags = {"Brasil": "🇧🇷", "USA": "🇺🇸", "Europa": "🇪🇺"}
    u = float(disp_umbral)
    elegibles = [
        (origen, precio, disp)
        for origen, precio, disp in candidatos
        if precio is not None
        and disp is not None
        and math.isfinite(float(precio))
        and math.isfinite(float(disp))
        and float(disp) > u
    ]
    mejores_origenes: set[str] = set()
    if elegibles:
        best_price = min(float(p) for _, p, _ in elegibles)
        mejores_origenes = {o for o, p, _ in elegibles if float(p) == best_price}

    origin_cells: list[str] = []
    for origen, precio, disp in candidatos:
        is_best_origin = origen in mejores_origenes
        flag = flags.get(origen, "")
        precio_s = _fmt_money_usd(precio, decimals=2) if precio is not None else "—"
        disp_s = f"{disp:,.2f}" if disp is not None else "—"
        cls = "ui-card ui-card--tight ui-card--origin" + (" is-best" if is_best_origin else "")
        pill_html = '<span class="ui-best-pill">Mejor precio</span>' if is_best_origin else ""
        origin_cells.append(
            f'<div class="{cls} ui-card--origin-stack">'
            f'<div class="ui-origin-head"><div class="ui-origin-name-line">'
            f'<span class="ui-origin-name">{html.escape(flag)} {html.escape(origen)}</span>'
            f"{pill_html}"
            f"</div></div>"
            f'<div class="ui-origin-col--px">{html.escape(precio_s)}</div>'
            f'<div class="ui-origin-col--dp">'
            f'<span class="ui-origin-disp-lbl">Disponibilidad</span>'
            f'<span class="ui-origin-disp-val">{html.escape(disp_s)}</span>'
            f"</div>"
            f"</div>"
        )
    grid = f'<div class="origin-grid-unified">{"".join(origin_cells)}</div>'
    body = f'<div class="consulta-panel-title">Comparación por región</div>{grid}'
    return f'<div class="consulta-panel">{body}</div>', bool(mejores_origenes)


def _consulta_html_reposicion_panel(
    df_masivo_one: pd.DataFrame | None,
    *,
    resumen_fallback: dict | None,
    disp_umbral: float,
    factor_usabr: float,
    factor_euro: float,
    trm: float,
    margin_pct: float,
    piso_margin_pct: float,
) -> str:
    """
    Costo reposición = USD base (mejor precio × factor) × TRM.
    Precio reposición = mismo × TRM ÷ (1 − margen % sobre venta).
    Misma definición que el cotizador en lote.
    """
    title = "Reposición importación (COP)"
    df_src = df_masivo_one
    if (df_src is None or df_src.empty) and resumen_fallback:
        df_src = pd.DataFrame([dict(resumen_fallback)])
    if df_src is None or df_src.empty:
        inner = (
            f'<p class="consulta-note">Sin datos para calcular reposición '
            f"(revisa coincidencia o columnas de precio/disponibilidad por región).</p>"
        )
        return f'<div class="consulta-panel"><div class="consulta-panel-title">{title}</div>{inner}</div>'

    d0 = _consulta_masiva_calcular_mejor_origen(
        df_src.copy(),
        disp_umbral=float(disp_umbral),
        factor_usabr=float(factor_usabr),
        factor_euro=float(factor_euro),
    )
    d0 = _consulta_masiva_ajustar_decimales(d0)
    dcot = _consulta_masiva_cotizador_df(
        d0,
        float(margin_pct),
        float(trm),
        piso_margin_pct=float(piso_margin_pct),
        factor_usabr=float(factor_usabr),
        factor_euro=float(factor_euro),
        factor_otros=float(factor_euro),
        disp_umbral=float(disp_umbral),
    )
    r = dcot.iloc[0]
    cr = r.get("Costo_reposicion_COP")
    pr = r.get("Precio_reposicion_COP")
    cr_s = html.escape(_fmt_money_cop_local(cr, decimals=0))
    pr_s = html.escape(_fmt_money_cop_local(pr, decimals=0))
    trm_s = html.escape(_fmt_num_local(float(trm), decimals=0))
    mg_s = html.escape(_fmt_num_local(float(margin_pct), decimals=0))
    cap = (
        f'<p class="consulta-note consulta-note--repo-formulas">Fórmulas (alineadas al cotizador en lote): '
        f"<strong>Costo reposición</strong> = USD base × TRM ({trm_s}); "
        f"<strong>Precio reposición</strong> = USD base × TRM ÷ (1 − {mg_s}% margen sobre venta). "
        f"USD base = mejor precio lista por región × factor BR/USA o EUR según el origen ganador.</p>"
    )
    grid = (
        f'<div class="consulta-compra-grid consulta-compra-grid--two">'
        f'<div class="consulta-kpi-cell"><span class="consulta-kpi-lbl">Costo reposición (COP)</span>'
        f'<span class="consulta-kpi-val">{cr_s}</span></div>'
        f'<div class="consulta-kpi-cell"><span class="consulta-kpi-lbl">Precio reposición (COP)</span>'
        f'<span class="consulta-kpi-val">{pr_s}</span></div>'
        f"</div>"
    )
    return f'<div class="consulta-panel"><div class="consulta-panel-title">{title}</div>{grid}{cap}</div>'


def _fmt_consulta_display(val: object) -> str:
    """Evita NaT / None / nan en textos y métricas de la pestaña Consulta."""
    if val is None:
        return "—"
    try:
        if pd.isna(val):
            return "—"
    except (TypeError, ValueError):
        pass
    if isinstance(val, float):
        try:
            if not math.isfinite(val) or math.isnan(val):
                return "—"
        except Exception:
            pass
    s = str(val).strip()
    if s.lower() in ("nat", "none", "nan", "<na>"):
        return "—"
    return s


def _fmt_consulta_fecha(val: object) -> str:
    if val is None:
        return "—"
    try:
        if pd.isna(val):
            return "—"
    except (TypeError, ValueError):
        pass
    try:
        ts = pd.Timestamp(val)
        if pd.isna(ts):
            return "—"
        return ts.strftime("%Y-%m-%d")
    except Exception:
        return _fmt_consulta_display(val)


def _fmt_consulta_money(val: object) -> str:
    # Backward-compatible wrapper: consulta money por defecto en COP local.
    return _fmt_money_cop_local(val, decimals=0)


def _fmt_num_local(value: float, decimals: int = 0) -> str:
    """Formatea número estilo es-CO: miles con '.' y decimales con ','."""
    s = f"{abs(float(value)):,.{int(decimals)}f}"
    s = s.replace(",", "_").replace(".", ",").replace("_", ".")
    return f"-{s}" if float(value) < 0 else s


def _fmt_money_cop_local(val: object, decimals: int = 0) -> str:
    if _fmt_consulta_display(val) == "—":
        return "—"
    try:
        return f"${_fmt_num_local(float(val), decimals=decimals)}"
    except (TypeError, ValueError):
        return "—"


def _fmt_consulta_fecha_ddmmyyyy(val: object) -> str:
    """Fecha en tablas de consulta: dd/mm/aaaa."""
    if _fmt_consulta_display(val) == "—":
        return "—"
    try:
        ts = pd.Timestamp(val)
        if pd.isna(ts):
            return "—"
        return ts.strftime("%d/%m/%Y")
    except Exception:
        return "—"


def _fmt_consulta_entero(val: object) -> str:
    """Instalación numérica, cantidades: entero sin decimales."""
    if _fmt_consulta_display(val) == "—":
        return "—"
    try:
        x = float(val)
        if not math.isfinite(x):
            return "—"
        return str(int(round(x)))
    except (TypeError, ValueError):
        return "—"


def _fmt_money_usd(val: object, decimals: int = 2) -> str:
    if _fmt_consulta_display(val) == "—":
        return "—"
    try:
        return f"US${float(val):,.{int(decimals)}f}"
    except (TypeError, ValueError):
        return "—"


def _valor_liq_cop_desde_resumen(resumen: dict) -> object:
    for k in ("Valor Liq. (COP)", "Valor Liquido COP", "Valor liquidado COP"):
        if k in resumen and resumen.get(k) is not None:
            return resumen.get(k)
    return None


def _origen_ultima_compra_texto(resumen: dict) -> str:
    """País últ. compra (auditoría) + tipo origen maestro; evita duplicar si son iguales."""
    pais = resumen.get("Pais_Ultima")
    tipo = resumen.get("Tipo_Origen") or resumen.get("Tipo Origen")

    def norm(v: object) -> str:
        if v is None:
            return ""
        try:
            if pd.isna(v):
                return ""
        except (TypeError, ValueError):
            pass
        s = str(v).strip()
        if not s or s.lower() in ("none", "nan", "<na>"):
            return ""
        return s

    ps, ts = norm(pais), norm(tipo)
    if ps and ts:
        if ps.casefold() == ts.casefold():
            return ps
        return f"{ps} · {ts}"
    return ps or ts


def _consulta_html_ultima_compra(resumen: dict) -> str:
    fecha = html.escape(_fmt_consulta_fecha(resumen.get("Ult. Fecha Compra")))
    prov = html.escape(_fmt_consulta_display(resumen.get("Proveedor")))
    usd = html.escape(_fmt_money_usd(resumen.get("Último Valor (USD)"), decimals=2))
    cop = html.escape(_fmt_money_cop_local(_valor_liq_cop_desde_resumen(resumen), decimals=0))
    costo_min = html.escape(_fmt_money_cop_local(resumen.get("Costo_Min"), decimals=0))
    costo_max = html.escape(_fmt_money_cop_local(resumen.get("Costo_Max"), decimals=0))
    ex_total = _to_float(resumen.get("Existencia_Total"))
    ex_total_s = "—" if ex_total is None else _fmt_num_local(ex_total, decimals=0)
    existencias = html.escape(ex_total_s)
    help_aud = html.escape(_consulta_help_costo_exist_auditoria(), quote=True)
    icon_aud = (
        f'<span class="consulta-kpi-help-icon" title="{help_aud}" '
        f'aria-label="{help_aud}" role="img">i</span>'
    )
    lbl_costo_min = (
        f'<span class="consulta-kpi-lbl-row"><span class="consulta-kpi-lbl">Costo mín.</span>{icon_aud}</span>'
    )
    lbl_costo_max = (
        f'<span class="consulta-kpi-lbl-row"><span class="consulta-kpi-lbl">Costo máx.</span>{icon_aud}</span>'
    )
    lbl_ex = (
        f'<span class="consulta-kpi-lbl-row"><span class="consulta-kpi-lbl">Existencias</span>{icon_aud}</span>'
    )
    help_origen = html.escape(_consulta_help_origen_ultima_compra(), quote=True)
    icon_origen = (
        f'<span class="consulta-kpi-help-icon" title="{help_origen}" '
        f'aria-label="{help_origen}" role="img">i</span>'
    )
    lbl_origen = (
        f'<span class="consulta-kpi-lbl-row"><span class="consulta-kpi-lbl">Origen últ. compra</span>{icon_origen}</span>'
    )
    origen_txt = _origen_ultima_compra_texto(resumen)
    origen_uc = html.escape(origen_txt if origen_txt else "—")
    return (
        f'<div class="consulta-compra-grid">'
        f'<div class="consulta-kpi-cell"><span class="consulta-kpi-lbl">Fecha</span>'
        f'<span class="consulta-kpi-val">{fecha}</span></div>'
        f'<div class="consulta-kpi-cell"><span class="consulta-kpi-lbl">Proveedor</span>'
        f'<span class="consulta-kpi-val">{prov}</span></div>'
        f'<div class="consulta-kpi-cell"><span class="consulta-kpi-lbl">Último valor (USD)</span>'
        f'<span class="consulta-kpi-val">{usd}</span></div>'
        f'<div class="consulta-kpi-cell"><span class="consulta-kpi-lbl">Valor Liq. (COP)</span>'
        f'<span class="consulta-kpi-val">{cop}</span></div>'
        f'<div class="consulta-kpi-cell">{lbl_costo_min}'
        f'<span class="consulta-kpi-val">{costo_min}</span></div>'
        f'<div class="consulta-kpi-cell">{lbl_costo_max}'
        f'<span class="consulta-kpi-val">{costo_max}</span></div>'
        f'<div class="consulta-kpi-cell">{lbl_ex}'
        f'<span class="consulta-kpi-val">{existencias}</span></div>'
        f'<div class="consulta-kpi-cell">{lbl_origen}'
        f'<span class="consulta-kpi-val">{origen_uc}</span></div>'
        f"</div>"
    )


def _consulta_build_sheet_html(
    resumen: dict,
    refs_alternas: str,
    ref_original: str,
    disp_umbral: float,
    *,
    df_masivo_one: pd.DataFrame | None = None,
    trm_repo: float = 4100.0,
    margin_repo_pct: float = 40.0,
    piso_margin_repo_pct: float = 40.0,
    factor_usabr_repo: float = 1.35,
    factor_euro_repo: float = 1.55,
    sheet_variant: str = "",
) -> str:
    """Un solo bloque HTML: resumen + alternas/orígenes + reposición COP + última compra (un único st.markdown)."""
    top = _consulta_html_topline_kpis(resumen)
    alt = _consulta_html_alternas_panel(refs_alternas, ref_original)
    orig, hay_mejor = _consulta_html_origins_panel(resumen, disp_umbral)
    split = f'<div class="consulta-split"><div>{alt}</div><div>{orig}</div></div>'
    note = (
        ""
        if hay_mejor
        else (
            '<p class="consulta-note">Ningún origen cumple <strong>disponibilidad &gt; '
            f"{html.escape(f'{float(disp_umbral):,.2f}')}</strong> "
            "; sube el umbral o revisa stock por región.</p>"
        )
    )
    repo = _consulta_html_reposicion_panel(
        df_masivo_one,
        resumen_fallback=resumen,
        disp_umbral=float(disp_umbral),
        factor_usabr=float(factor_usabr_repo),
        factor_euro=float(factor_euro_repo),
        trm=float(trm_repo),
        margin_pct=float(margin_repo_pct),
        piso_margin_pct=float(piso_margin_repo_pct),
    )
    hr = '<div class="consulta-hr"></div>'
    head = '<div class="consulta-blockhead">Última compra / Inventario</div>'
    comp = _consulta_html_ultima_compra(resumen)
    inner = top + split + note + repo + hr + head + comp
    sheet_cls = "consulta-sheet"
    if sheet_variant == "individual":
        sheet_cls = "consulta-sheet consulta-sheet--individual"
    return f'<div class="consulta-sheet-wrap"><div class="{sheet_cls}">{inner}</div></div>'


def _to_percent_text(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value:,.2f}%"


def _fmt_cop_resumido(v: float | None) -> str:
    """Formato compacto en COP para métricas (inventario, montos)."""
    if v is None or (isinstance(v, float) and not math.isfinite(v)):
        return "—"
    av = abs(float(v))
    if av >= 1_000_000_000:
        return f"COP ${_fmt_num_local(v / 1_000_000_000, decimals=2)} mil M"
    if av >= 1_000_000:
        return f"COP ${_fmt_num_local(v / 1_000_000, decimals=1)} M"
    if av >= 1_000:
        return f"COP ${_fmt_num_local(v / 1_000, decimals=1)} mil"
    return f"COP ${_fmt_num_local(v, decimals=0)}"


# Precio lista (COP) en widgets: slider admite preset "dollar".
# number_input: solo printf simple (p. ej. %.2f); «%,» miles no es válido en todas las versiones de Streamlit.
_FMT_PRECIO_LISTA_SLIDER: Final[str] = "dollar"
_FMT_PRECIO_LISTA_NUM_INPUT: Final[str] = "%.2f"


def _valor_inventario_cop_auditoria(df: pd.DataFrame, lower_map: dict[str, str]) -> float | None:
    """
    Costo de inventario expuesto (proxy): Σ existencia × costo en el conjunto filtrado.
    Prioriza columna Valor_Inventario si existe; si no, Existencia_Intermedio × Costo_Intermedio.
    """
    for key in ("valor_inventario", "valor_inventario_cop"):
        col = lower_map.get(key)
        if col and col in df.columns:
            s = pd.to_numeric(df[col], errors="coerce")
            if s.notna().any():
                tot = float(s.sum())
                return tot if math.isfinite(tot) else None
    ex = lower_map.get("existencia_intermedio")
    co = lower_map.get("costo_intermedio")
    if ex and co and ex in df.columns and co in df.columns:
        exs = pd.to_numeric(df[ex], errors="coerce").fillna(0)
        cos = pd.to_numeric(df[co], errors="coerce").fillna(0)
        tot = float((exs * cos).sum())
        return tot if math.isfinite(tot) else None
    return None


def _auditoria_add_existencia_suma_niveles(df: pd.DataFrame, lower_map: dict[str, str]) -> pd.DataFrame:
    """
    Si el SQL ya expone `Existencia_Total`, no duplicar.
    Si no, calcula Existencia_Min + Existencia_Intermedio + Existencia_Max como `_Existencia_suma_niveles`.
    """
    if lower_map.get("existencia_total") and lower_map["existencia_total"] in df.columns:
        return df
    exm = lower_map.get("existencia_min")
    exi = lower_map.get("existencia_intermedio")
    exx = lower_map.get("existencia_max")
    if not exm or not exi or not exx:
        return df
    if not all(c in df.columns for c in (exm, exi, exx)):
        return df
    out = df.copy()
    out["_Existencia_suma_niveles"] = (
        pd.to_numeric(out[exm], errors="coerce").fillna(0)
        + pd.to_numeric(out[exi], errors="coerce").fillna(0)
        + pd.to_numeric(out[exx], errors="coerce").fillna(0)
    )
    return out


def _auditoria_columns_in_order(df: pd.DataFrame, candidates: list[str | None]) -> list[str]:
    """Devuelve columnas presentes en `df` respetando el orden lógico de `candidates` (sin duplicados)."""
    seen: set[str] = set()
    out: list[str] = []
    for c in candidates:
        if c and c in df.columns and c not in seen:
            out.append(c)
            seen.add(c)
    return out


def _auditoria_coerce_display_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fechas → datetime normalizado (solo día, orden temporal + formato corto en grid);
    precios/costos/TRM → float; % y conteos → float (ordenación numérica como otras pestañas).
    """
    if df.empty:
        return df
    out = df.copy()
    for c in out.columns:
        cl = str(c).lower()
        if "fecha" in cl:
            out[c] = pd.to_datetime(out[c], errors="coerce", dayfirst=True).dt.normalize()
            continue
        # Nombre de bodega donde aplica min/max costo (texto), no monto
        if cl in ("bodega_costomin", "bodega_costomax"):
            continue
        if cl.startswith("var_"):
            out[c] = pd.to_numeric(out[c], errors="coerce")
            continue
        if cl.startswith("_var_") and "pct" in cl:
            out[c] = pd.to_numeric(out[c], errors="coerce")
            continue
        if "margen" in cl and ("pct" in cl or "objetivo" in cl):
            out[c] = pd.to_numeric(out[c], errors="coerce")
            continue
        if "pct" in cl and "margen" not in cl:
            out[c] = pd.to_numeric(out[c], errors="coerce")
            continue
        if any(
            p in cl
            for p in (
                "precio_usd",
                "precio_cop",
                "precio_lista",
                "lista_09",
                "lista_04",
                "precio_prom",
                "costo_min",
                "costo_max",
                "costo_intermedio",
                "valor_inventario",
                "absvar_costo",
                "trm_ultima",
                "trm_penultima",
                "costo_prom",
            )
        ) and not cl.startswith("var_"):
            out[c] = pd.to_numeric(out[c], errors="coerce")
            continue
        if cl.startswith("_precio") or cl.startswith("_factor"):
            out[c] = pd.to_numeric(out[c], errors="coerce")
            continue
        if cl.startswith("_abs_var") or cl == "_score_alerta":
            out[c] = pd.to_numeric(out[c], errors="coerce")
            continue
        if any(
            x in cl
            for x in (
                "dias_entre",
                "dias_desde",
                "existencia",
                "disponible",
                "nrobod",
                "numcostos",
                "escostounico",
            )
        ):
            out[c] = pd.to_numeric(out[c], errors="coerce")
            continue
    return out


def _auditoria_column_order_full(df: pd.DataFrame, lower_map: dict[str, str], costo_inv_col: str | None) -> list[str]:
    """Orden BI centrado en dos preguntas: (1) ¿variación última vs penúltima compra? (2) ¿última vs costo prom. inv.?"""
    wanted_keys: list[str] = [
        # 1 Identificación y política
        "referencia",
        "referencias_alternas",
        "descripcion",
        "u.m.",
        "linea_item",
        "sistema_precio_item",
        "equipo_cnh",
        "modelo_cnh",
        "clasificacion_rotacion",
        "margen_objetivo_sistema",
        "semaforo_variacion",
        # 2 Δ entre compras (última vs penúltima)
        "dias_entre_compras",
        "var_preciousd",
        "var_preciocop",
        "var_trm",
        # costo_inv_col se inserta justo después de var_trm (referencia de costo antes de Δ vs costo)
        # 3 Δ vs costo inventario (SQL)
        "var_costomin_preciocop",
        "var_costomax_preciocop",
        "absvar_costo",
        "absvar_costo_pct",
        "numcostosvalidos",
        "escostounico",
        # 4 Costos en bodega, stock y lista (contexto económico)
        "costo_min",
        "bodega_costomin",
        "costo_intermedio",
        "costo_max",
        "bodega_costomax",
        "existencia_min",
        "existencia_intermedio",
        "existencia_max",
        "existencia_total",
        "_existencia_suma_niveles",
        "disponible_min",
        "disponible_intermedio",
        "disponible_max",
        "nrobod_min",
        "nrobod_intermedio",
        "nrobod_max",
        "precio_lista_09",
        "margen_min_pct",
        "margen_intermedio_pct",
        "margen_max_pct",
        # 5 Evidencia: detalle última y penúltima compra
        "fecha_ultima_compra",
        "pais_ultima",
        "proveedor_ultima",
        "comprador_ultima",
        "precio_usd_ultima",
        "precio_cop_ultima",
        "trm_ultima",
        "fecha_penultima_compra",
        "pais_penultima",
        "proveedor_penultima",
        "comprador_penultima",
        "precio_usd_penultima",
        "precio_cop_penultima",
        "trm_penultima",
    ]
    out: list[str] = []
    seen: set[str] = set()
    for k in wanted_keys:
        if k == "_existencia_suma_niveles":
            if k in df.columns and k not in seen:
                out.append(k)
                seen.add(k)
            continue
        col = lower_map.get(k)
        if col and col in df.columns and col not in seen:
            out.append(col)
            seen.add(col)
        if k == "var_trm" and costo_inv_col and costo_inv_col in df.columns and costo_inv_col not in seen:
            out.append(costo_inv_col)
            seen.add(costo_inv_col)
    if costo_inv_col and costo_inv_col in df.columns and costo_inv_col not in seen:
        vcm = lower_map.get("var_costomin_preciocop")
        if vcm and vcm in out:
            out.insert(out.index(vcm), costo_inv_col)
        else:
            out.append(costo_inv_col)
        seen.add(costo_inv_col)
    # Magnitudes |Δ| y score justo después del bloque SQL de variación (antes de costos en bodega).
    esc = lower_map.get("escostounico")
    prioridad_pack = ("_abs_var_compra", "_abs_var_costo", "_score_alerta")
    anchor_col = esc if esc and esc in out else None
    if anchor_col is None:
        for cand in ("absvar_costo_pct", "numcostosvalidos", "escostounico"):
            c = lower_map.get(cand)
            if c and c in out:
                anchor_col = c
                break
    if anchor_col:
        idx_ins = out.index(anchor_col) + 1
        for m in reversed(prioridad_pack):
            if m in df.columns and m not in seen:
                out.insert(idx_ins, m)
                seen.add(m)
    else:
        cm0 = lower_map.get("costo_min")
        if cm0 and cm0 in out:
            idx0 = out.index(cm0)
            for m in reversed(prioridad_pack):
                if m in df.columns and m not in seen:
                    out.insert(idx0, m)
                    seen.add(m)
        else:
            for m in prioridad_pack:
                if m in df.columns and m not in seen:
                    out.append(m)
                    seen.add(m)
    for tail in (
        "_Origen_Ultima_Norm",
        "_Factor_Logistico",
        "_Precio_Ultima_Log_COP",
        "_Precio_Penultima_Log_COP",
        "_Var_Ultima_vs_CostoLog_Pct",
        "_Var_Penultima_vs_CostoLog_Pct",
    ):
        if tail in df.columns and tail not in seen:
            out.append(tail)
            seen.add(tail)
    _omitir_fin: frozenset[str] = frozenset({"cod_linea", "cod_sistema_precio"})
    for c in df.columns:
        if str(c).lower() in _omitir_fin:
            continue
        if c not in seen:
            out.append(c)
            seen.add(c)
    # ABSVar_Costo_Pct y _abs_var_costo comparten etiqueta de negocio; Arrow no admite nombres duplicados tras rename.
    _apct = lower_map.get("absvar_costo_pct")
    if _apct and "_abs_var_costo" in out and _apct in out:
        out = [c for c in out if c != _apct]
    return out


def _auditoria_tactica_columnas_estrategicas_originales(
    df: pd.DataFrame,
    lower_map: dict[str, str],
    costo_inv_col: str | None,
) -> list[str]:
    """
    Subconjunto inicial de la vista táctica: pocas columnas con lectura ejecutiva.
    El usuario añade el resto con el multiselect (orden de la tabla = orden lógico del reporte).
    """
    out: list[str] = []
    seen: set[str] = set()

    def _add_key(k: str) -> None:
        col = lower_map.get(k)
        if col and col in df.columns and col not in seen:
            out.append(col)
            seen.add(col)

    def _add_direct(name: str) -> None:
        if name in df.columns and name not in seen:
            out.append(name)
            seen.add(name)

    for k in (
        "referencia",
        "referencias_alternas",
        "descripcion",
        "semaforo_variacion",
    ):
        _add_key(k)
    _add_direct("_score_alerta")
    for k in (
        "dias_entre_compras",
        "var_preciousd",
        "var_preciocop",
        "var_trm",
    ):
        _add_key(k)
    if costo_inv_col:
        _add_direct(costo_inv_col)
    for k in (
        "var_costomin_preciocop",
        "var_costomax_preciocop",
        "absvar_costo",
    ):
        _add_key(k)
    _add_direct("_abs_var_compra")
    _add_direct("_abs_var_costo")
    for k in (
        "precio_cop_ultima",
        "existencia_total",
        "sistema_precio_item",
        "clasificacion_rotacion",
    ):
        _add_key(k)
    return out


def _auditoria_one_column_config(orig: str, label: str) -> st.column_config.Column:
    """Formato auditoría: columnas de precio con formato moneda (dollar); costos/otros montos $ sin decimales como antes; fechas DD/MM/AAAA."""
    lc = orig.lower()
    if orig == "_Origen_Ultima_Norm":
        return st.column_config.TextColumn(label, width="small")
    # Fechas: tipo fecha en grid + formato corto (ordenación temporal correcta tras coerce)
    if "fecha" in lc:
        return st.column_config.DatetimeColumn(
            label,
            format="DD/MM/YYYY",
            width="small",
        )
    if orig.startswith("_"):
        if orig == "_score_alerta":
            return st.column_config.NumberColumn(
                label,
                format="%.2f",
                help="Priorización: 55% |Δ última vs penúltima compra| + 45% |Δ última vs costo prom. inv.| (magnitudes absolutas %).",
            )
        if orig == "_Factor_Logistico":
            return st.column_config.NumberColumn(label, format="%.4f")
        if "pct" in lc or "_abs_var" in lc or ("_var_" in lc and "costolog" in lc):
            return st.column_config.NumberColumn(label, format="%.2f%%")
        # Precios internos/ajustados: COP local.
        if "precio" in lc or "log" in lc:
            return st.column_config.NumberColumn(label, format="COP $ %,.0f")
        return st.column_config.NumberColumn(label, format="%.2f")
    if lc == "existencia_total":
        return st.column_config.NumberColumn(
            label,
            format="%,.0f",
            help="Suma Min + Intermedio + Max por referencia (columna del reporte SQL).",
        )
    if orig == "_Existencia_suma_niveles":
        return st.column_config.NumberColumn(
            label,
            format="%,.0f",
            help="Suma Min + Intermedio + Max (proxy; validar si aplica a su modelo).",
        )
    if "semaforo" in lc and "variacion" in lc:
        return st.column_config.TextColumn(
            label,
            width="medium",
            help="Alineación de la última compra (COP × factor país) frente a costos mín./máx. de inventario. "
            "Detalle de categorías: desplegable **📖 Cómo se calcula el semáforo** arriba en esta pestaña.",
        )
    if any(
        x in lc
        for x in (
            "referencia",
            "descripcion",
            "pais",
            "proveedor",
            "comprador",
            "bodega",
            "linea",
            "sistema",
            "equipo",
            "modelo",
            "clasificacion",
            "altern",
        )
    ):
        return st.column_config.TextColumn(label, width="medium")
    if "u.m" in lc or lc in ("u.m.",):
        return st.column_config.TextColumn(label, width="small")
    if ("pct" in lc or ("margen_" in lc and "margen_objetivo" not in lc)) and "precio_lista" not in lc:
        return st.column_config.NumberColumn(label, format="%.2f%%")
    if "margen_objetivo" in lc:
        return st.column_config.NumberColumn(label, format="%.2f%%")
    # TRM nivel (no es $): numérico para ordenar
    if "trm" in lc and "var_" not in lc:
        return st.column_config.NumberColumn(label, format="%,.2f")
    # Costo prom. inventario y montos COP/USD (igual que Cruces / detalle margen: $ sin decimales)
    if "costo" in lc and "prom" in lc and "inst" in lc:
        return st.column_config.NumberColumn(label, format="COP $ %,.0f")
    if any(
        x in lc
        for x in (
            "precio_usd",
            "precio_cop",
            "precio_lista",
            "lista_09",
            "lista_04",
        )
    ) and "pct" not in lc:
        if "usd" in lc:
            return st.column_config.NumberColumn(label, format="US$ %,.2f")
        return st.column_config.NumberColumn(label, format="COP $ %,.0f")
    if any(x in lc for x in ("valor_inventario", "absvar_costo")) and "pct" not in lc:
        return st.column_config.NumberColumn(label, format="COP $ %,.0f")
    if any(x in lc for x in ("costo_min", "costo_max", "costo_intermedio")) and "var_" not in lc:
        return st.column_config.NumberColumn(label, format="COP $ %,.0f")
    if lc.startswith("var_"):
        return st.column_config.NumberColumn(label, format="%.2f%%")
    if any(x in lc for x in ("existencia", "disponible", "dias", "nrobod", "numcostos")):
        return st.column_config.NumberColumn(label, format="%,.0f")
    if lc == "escostounico":
        return st.column_config.NumberColumn(label, format="%,.0f")
    # Rescate: cualquier otro campo de precio (no variación %)
    if "precio" in lc and not lc.startswith("var_"):
        return st.column_config.NumberColumn(label, format="US$ %,.2f")
    if "costo" in lc and not lc.startswith("var_") and "bodega" not in lc:
        if "pct" in lc:
            return st.column_config.NumberColumn(label, format="%.2f%%")
        return st.column_config.NumberColumn(label, format="COP $ %,.0f")
    return st.column_config.TextColumn(label, width="medium")


def _auditoria_build_column_config(orig_cols: list[str], alias_cols: dict[str, str]) -> dict[str, st.column_config.Column]:
    out: dict[str, st.column_config.Column] = {}
    for orig in orig_cols:
        lbl = alias_cols.get(orig, orig)
        out[lbl] = _auditoria_one_column_config(orig, lbl)
    return out


def _auditoria_vista_bloques(
    lower_map: dict[str, str],
) -> list[tuple[str, str, list[str]]]:
    """Bloques operativos: (título, contexto para el analista, claves SQL en lower_map).

    El texto de contexto explica qué decisiones o validaciones aporta cada tabla.
    """
    return [
        (
            "Identificación y política",
            "Catálogo y reglas de precio del ítem: unidad, línea, sistema de precio, equipo/modelo CNH, rotación y margen "
            "objetivo. Úsalo para saber si el desvío encaja con la política del segmento antes de escalar.",
            [
                "referencia",
                "referencias_alternas",
                "descripcion",
                "u.m.",
                "linea_item",
                "sistema_precio_item",
                "equipo_cnh",
                "modelo_cnh",
                "clasificacion_rotacion",
                "margen_objetivo_sistema",
            ],
        ),
        (
            "Semáforo y prioridad (score)",
            "Resumen de riesgo: el **semáforo** resume reglas de negocio; el **score** mezcla |Δ compra| (55 %) y "
            "|Δ vs costo inv.| (45 %) como en la vista estratégica. Prioriza filas con score alto y semáforo crítico/moderado.",
            [
                "semaforo_variacion",
                "_score_alerta",
            ],
        ),
        (
            "Problema 1 — variación última vs penúltima compra",
            "Aquí validas **volatilidad entre dos facturas**: días entre compras, variación en USD y en COP, y efecto TRM. "
            "Si COP se mueve pero USD no (o al revés), sospecha de tipo de cambio o redondeos; si ambos saltan, revisa proveedor o condición comercial.",
            [
                "pais_ultima",
                "pais_penultima",
                "precio_cop_ultima",
                "precio_cop_penultima",
                "dias_entre_compras",
                "var_preciousd",
                "var_preciocop",
                "var_trm",
                "_abs_var_compra",
            ],
        ),
        (
            "Problema 2 — variación última vs costo prom. inventario",
            "Desalineación **precio de compra vs costo promedio de inventario** (y variantes SQL). Es la segunda pata del filtro "
            "“variación fuerte” y del score. Valores extremos suelen indicar compras fuera de curva, costo de inventario desactualizado o mezcla de lotes.",
            [
                "var_costomin_preciocop",
                "var_costomax_preciocop",
                "absvar_costo",
                "absvar_costo_pct",
                "numcostosvalidos",
                "escostounico",
                "_var_ultima_vs_costolog_pct",
                "_var_penultima_vs_costolog_pct",
                "_abs_var_costo",
            ],
        ),
        (
            "Costos en bodega, existencias y precio lista",
            "**Magnitud operativa**: costos y existencias por nivel de bodega, disponible, precio lista y márgenes. "
            "Sirve para decidir si el ajuste vale la pena (stock alto + desvío = mayor exposición en COP).",
            [
                "costo_min",
                "bodega_costomin",
                "costo_intermedio",
                "costo_max",
                "bodega_costomax",
                "existencia_min",
                "existencia_intermedio",
                "existencia_max",
                "existencia_total",
                "_existencia_suma_niveles",
                "disponible_min",
                "disponible_intermedio",
                "disponible_max",
                "nrobod_min",
                "nrobod_intermedio",
                "nrobod_max",
                "precio_lista_09",
                "margen_min_pct",
                "margen_intermedio_pct",
                "margen_max_pct",
            ],
        ),
        (
            "Evidencia: última compra",
            "Datos de la **última OC/factura**: fechas, proveedor, comprador, precios unitarios y TRM. Contrastalo con el bloque "
            "anterior para confirmar que el sistema refleja la operación real.",
            [
                "fecha_ultima_compra",
                "pais_ultima",
                "proveedor_ultima",
                "comprador_ultima",
                "precio_usd_ultima",
                "precio_cop_ultima",
                "trm_ultima",
            ],
        ),
        (
            "Evidencia: penúltima compra",
            "Misma foto para la **compra anterior**: permite ver tendencia (sube/baja estable) y detectar cambios de proveedor o país de origen.",
            [
                "fecha_penultima_compra",
                "pais_penultima",
                "proveedor_penultima",
                "comprador_penultima",
                "precio_usd_penultima",
                "precio_cop_penultima",
                "trm_penultima",
            ],
        ),
        (
            "Factores logísticos (precio COP ajustado)",
            "Ajuste **COP con factor por origen** (USA/BR vs otros) usado en la app para comparar con costo de inventario. "
            "Sin este bloque se malinterpreta el |Δ vs costo inv.| respecto al precio COP “en planta”.",
            [
                "_origen_ultima_norm",
                "_factor_logistico",
                "_precio_ultima_log_cop",
                "_precio_penultima_log_cop",
            ],
        ),
    ]


BUSINESS_LABELS: Final[dict[str, str]] = {
    "Referencia": "Ref",
    "Referencia_Cruce": "Ref. cruce",
    "USD_base": "USD base (cotiz.)",
    "USD_base_unidades_disp": "Und. disp. origen USD base",
    "USD_base_fuente": "Fuente USD",
    "Margen_pct_cot": "Margen % (cot.)",
    "TRM_cot": "TRM (cot.)",
    "P_venta_experto_COP": "P. venta experto (COP)",
    "Costo_reposicion_COP": "Costo reposición (COP)",
    "Precio_reposicion_COP": "Precio reposición (COP)",
    "P_piso_inventario_COP": "P. piso inventario (COP)",
    "P_recomendado_COP": "P. recomendado (COP)",
    "Regla_precio": "Regla precio",
    "Estado_cotizacion": "Estado cotización",
    "Alertas_detalle": "Alertas",
    "Ult_venta_guia": "Últ. venta (guía)",
    "Referencia_Original": "Ref original",
    "Referencia_Alternas": "Refs alternas",
    "Descripcion": "Descripción",
    "Bodega": "Bodega",
    "Nom_Bodega": "Nombre bodega",
    "Nom_Instalacion": "Instalación",
    "Rotacion": "Rotación",
    "Sistema_Precio": "Sistema precio",
    "Sistema_Precio_Item": "Sistema precio",
    "Modelo_CNH": "Modelo CNH",
    "Equipo_CNH": "Equipo CNH",
    "Tipo_Origen": "Tipo origen",
    "Costo_Prom_Inst": "Costo prom. inv (COP)",
    "Precio_Lista_09": "Precio lista 09 (COP)",
    "Precio_Lista_04": "Precio lista 04 (COP)",
    "Existencia": "Existencia",
    "Disponible": "Disponible",
    "Valor_Inventario": "Valor inventario (COP)",
    "Dias_Desde_Fecha_Max": "Días desde últ. mov.",
    "Margen09": "Margen 09 (%)",
    "Margen04": "Margen 04 (%)",
    "DNET BRA USD": "DNET BRA (USD)",
    "DNET USA USD": "DNET USA (USD)",
    "DNET EUR EURO": "DNET EUR (EUR)",
    "Margen_Pct": "Margen (%)",
    "Margen09_Max": "Margen 09 máx (%)",
    "Registros": "Registros",
    "Negativos": "Regs. negativos",
    "Pct_Negativos": "% negativos",
    "Margen_Promedio": "Margen prom. (%)",
    "Margen_Mediano": "Margen mediano (%)",
    "Min_Margen": "Margen mín. (%)",
    "Max_Margen": "Margen máx. (%)",
    "Semaforo_Variacion": "Semáforo",
    "ABSVar_Costo_Pct": "Var abs vs costo (%)",
    "_Origen_Ultima_Norm": "Origen últ. compra",
    "_Factor_Logistico": "F.Log",
    "_Precio_Ultima_Log_COP": "Últ compra + F.Log (COP)",
    "_Precio_Penultima_Log_COP": "Penúlt compra + F.Log (COP)",
    "_Var_Ultima_vs_CostoLog_Pct": "Var últ compra vs costo (%)",
    "_Var_Penultima_vs_CostoLog_Pct": "Var penúlt compra vs costo (%)",
    "_abs_var_compra": "Var abs compra (%)",
    "_abs_var_costo": "Var abs vs costo (%)",
    "_score_alerta": "Score alerta",
    "Precio_COP_Ultima": "Últ compra (COP)",
    "Precio_COP_Penultima": "Penúlt compra (COP)",
    "Precio_USD_Ultima": "Últ compra (USD)",
    "Precio_USD_Penultima": "Penúlt compra (USD)",
    "TRM_Ultima": "TRM últ compra",
    "TRM_Penultima": "TRM penúlt compra",
    "Var_PrecioUSD": "Var compra USD (%)",
    "Var_PrecioCOP": "Var compra COP (%)",
    "Var_TRM": "Var TRM (%)",
    "Var_CostoMin_PrecioCOP": "Var vs costo mín (%)",
    "Var_CostoMax_PrecioCOP": "Var vs costo máx (%)",
    "Valor Liquido COP": "Valor liquidado COP",
    "Ult. Precio Venta": "Valor últ. venta",
    "Fecha Ult. Venta": "Fecha últ. venta",
    "Margen_Objetivo_Sistema": "Margen sistema",
    "Valor": "Valor",
    "Pct_sobre_margen": "% sobre margen",
    # Auditoría referencias (SQL 00)
    "U.M.": "U.M.",
    "Costo_Min": "Costo mín.",
    "Costo_Intermedio": "Costo interm.",
    "Costo_Max": "Costo máx.",
    "Existencia_Min": "Exist. mín.",
    "Existencia_Intermedio": "Exist. interm.",
    "Existencia_Max": "Exist. máx.",
    "_Existencia_suma_niveles": "Exist. Σ niveles",
    "Existencia_Total": "Exist. total (ref.)",
    "Disponible_Min": "Disp. mín.",
    "Disponible_Intermedio": "Disp. interm.",
    "Disponible_Max": "Disp. máx.",
    "NroBod_Min": "N° bod. mín.",
    "NroBod_Intermedio": "N° bod. interm.",
    "NroBod_Max": "N° bod. máx.",
    "Margen_Min_Pct": "Margen mín. (%)",
    "Margen_Intermedio_Pct": "Margen interm. (%)",
    "Margen_Max_Pct": "Margen máx. (%)",
    "Fecha_Ultima_Compra": "Fecha últ. compra",
    "Pais_Ultima": "País últ. compra",
    "Proveedor_Ultima": "Proveedor últ.",
    "Comprador_Ultima": "Comprador últ.",
    "Fecha_Penultima_Compra": "Fecha penúlt. compra",
    "Pais_Penultima": "País penúlt.",
    "Proveedor_Penultima": "Proveedor penúlt.",
    "Comprador_Penultima": "Comprador penúlt.",
    "Dias_Entre_Compras": "Días entre compras",
    "ABSVar_Costo": "Var. abs costo ($)",
    "NumCostosValidos": "N° costos válidos",
    "EsCostoUnico": "Costo único",
    "Cod_Linea": "Cód. línea",
    "Linea_Item": "Línea",
    "Clasificacion_Rotacion": "Rotación",
    "Cod_Sistema_Precio": "Cód. sist. precio",
}


def _label_negocio(col: str) -> str:
    c = str(col)
    if c in BUSINESS_LABELS:
        return BUSINESS_LABELS[c]
    if c.startswith("_"):
        c = c[1:]
    return c.replace("_", " ")


def _renombrar_negocio(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    return df.rename(columns={c: _label_negocio(c) for c in df.columns})


def _hacer_columnas_unicas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Streamlit/pyarrow no admite columnas duplicadas.
    Si dos aliases de negocio colisionan, agrega sufijo incremental.
    """
    if df.empty:
        return df
    out = df.copy()
    usados: dict[str, int] = {}
    nuevas: list[str] = []
    for col in [str(c) for c in out.columns]:
        n = usados.get(col, 0)
        if n == 0:
            nuevas.append(col)
        else:
            nuevas.append(f"{col} ({n+1})")
        usados[col] = n + 1
    out.columns = nuevas
    return out


def _coerce_margen_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Asegura tipos numéricos para filtros y KPIs (DuckDB/pandas a veces devuelven object).
    """
    if df.empty:
        return df
    out = df.copy()
    # Enteros / conteos
    for c in ("Existencia", "Disponible", "Dias_Desde_Fecha_Max"):
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")
            if c == "Dias_Desde_Fecha_Max":
                out[c] = out[c].astype("Int64")
            else:
                out[c] = out[c].astype("Int64")
    # Monetarios / porcentajes / derivados
    for c in (
        "Costo_Prom_Inst",
        "Precio_Lista_09",
        "Precio_Lista_04",
        "Valor_Inventario",
        "Margen09",
        "Margen04",
        "Margen_Pct",
    ):
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce").astype("float64")
    return out


@st.cache_data(show_spinner=False, ttl=300)
def _cargar_margenes_para_dashboard(cache_version: str = "20260326_v3") -> tuple:
    # Cargamos un rango muy amplio para que referencias con márgenes extremos
    # (ej. costos/prices sin Precio_Lista) no se “pierdan” antes de filtrar en UI.
    df = obtener_dataset_margenes(
        limite=0,
        margen_min=-1e15,
        margen_max=1e15,
        incluir_margenes_null=True,
    )
    df = _coerce_margen_numeric_columns(df)
    margen_col = None
    for cand in ("Margen09", "Margen_Pct", "Margen04"):
        if cand in df.columns:
            margen_col = cand
            break
    return df, margen_col


@st.cache_data(show_spinner=False, ttl=600, max_entries=3)
def _cargar_referencias_catalogo_masivo_cached(cache_version: str = "20260326_v3") -> list[str]:
    """Catálogo único de referencias para consulta masiva sin CSV."""
    df = obtener_dataset_margenes(
        limite=0,
        margen_min=-1e15,
        margen_max=1e15,
        incluir_margenes_null=True,
    )
    if df is None or df.empty:
        return []
    refs: list[str] = []
    for c in ("Referencia_Original", "Referencia_Normalizada", "Referencia"):
        if c in df.columns:
            vals = (
                df[c]
                .astype(str)
                .str.strip()
                .replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})
                .dropna()
                .tolist()
            )
            refs.extend(vals)
    if not refs:
        return []
    return list(dict.fromkeys(refs))


@st.cache_data(show_spinner=False, ttl=300)
def _cargar_auditoria() -> dict:
    return obtener_auditoria_dashboard()


@st.cache_data(show_spinner=False, ttl=300)
def _cargar_auditoria_referencias() -> pd.DataFrame:
    return obtener_auditoria_referencias(limite=0)


def _render_header_y_actualizacion() -> None:
    st.session_state.setdefault("_log_visible", False)
    st.session_state.setdefault("_log_text", "")
    st.session_state.setdefault("_log_status", "")
    st.session_state.setdefault("upd_preset", "Completa")
    st.session_state.setdefault("upd_multiselect_pipelines", list(PIPELINE_OPCIONES))
    st.session_state.setdefault("upd_multiselect_sql", [])

    st.markdown(
        """
        <div class="hero-card">
            <h1 class="hero-title">Consulta de referencia - Precios CNH</h1>
            <p class="hero-subtitle">
                Visualiza precio, disponibilidad, última compra y ventas recientes por referencia.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    top_left, top_right = st.columns([3, 2])
    with top_left:
        st.markdown(
            '<div class="hint-text">Actualiza la base antes de consultar si necesitas la última información.</div>',
            unsafe_allow_html=True,
        )
    with top_right:
        control_cols = st.columns([1.55, 1.0, 0.55], gap="small")
        with control_cols[0]:
            with st.popover("Actualizar datos", use_container_width=True):
                st.markdown("**Configuración de actualización**")
                st.radio(
                    "Modo",
                    ["Rápida", "Completa", "Personalizada"],
                    horizontal=True,
                    key="upd_preset",
                    help="Rápida y Completa son atajos. Personalizada permite combinar scripts Python y consultas SQL 00 por separado.",
                )
                preset_actual = st.session_state.get("upd_preset", "Completa")
                if preset_actual == "Rápida":
                    st.caption(
                        "**Rápida:** solo scripts **01, 02 y 03** (DuckDB desde Excels/precios). "
                        "No ejecuta SQL 00 desde SQL Server."
                    )
                elif preset_actual == "Completa":
                    st.caption(
                        "**Completa:** **01 + 02 + 03** y además las **tres consultas SQL 00** "
                        "(margen, atributos, auditoría desde SQL Server)."
                    )
                else:
                    st.caption(
                        "**Personalizada:** elige por separado qué **pipelines Python** corren y "
                        "qué **consultas SQL 00** corren. Vacío en ambos = no hay nada que ejecutar."
                    )
                    st.multiselect(
                        "Scripts Python (pipelines)",
                        options=list(PIPELINE_OPCIONES),
                        key="upd_multiselect_pipelines",
                        help="01 = precios lista, 02 = ventas, 03 = maestro histórico.",
                    )
                    st.multiselect(
                        "Consultas SQL 00 (desde SQL Server → DuckDB)",
                        options=list(SQL_OPCIONES),
                        key="upd_multiselect_sql",
                        help="Cada opción carga una tabla raw en DuckDB. SQL 003 puede filtrarse por bodegas abajo.",
                    )
                seleccion = _seleccion_actualizacion_desde_session_state()
                st.caption(f"**Vista previa:** {_resumen_actualizacion_texto(seleccion)}")
                incluye_sql_003 = SQL_003_OPCION in seleccion
                if incluye_sql_003:
                    st.divider()
                    h_bod, btn_bod = st.columns([4, 1], gap="small")
                    with h_bod:
                        st.markdown("##### Bodegas para SQL 003 — Auditoría")
                    with btn_bod:
                        if st.button(
                            "↻ Bodegas",
                            key="btn_solo_refrescar_bodegas_dim",
                            help="Solo reconstruye `bodegas_dim` desde `margen_siesa_raw` (sin ejecutar pipelines).",
                            width="stretch",
                        ):
                            try:
                                ok_b, msg_b = refrescar_catalogo_bodegas_auditoria()
                                if ok_b:
                                    st.toast(msg_b, icon="✅")
                                    st.rerun()
                                else:
                                    st.warning(msg_b)
                            except Exception as exc:
                                st.error(f"No se pudo actualizar bodegas: {exc}")
                    st.caption(
                        "Filtra **#Existencias** por bodega en la consulta de auditoría. "
                        "**Vacío** = incluir todas las bodegas."
                    )
                    try:
                        df_bod = obtener_catalogo_bodegas_auditoria()
                    except Exception:
                        df_bod = pd.DataFrame()
                    if df_bod.empty:
                        st.info(
                            "Aún no hay catálogo `bodegas_dim`. Ejecuta primero **SQL 001** "
                            "para generarlo desde `margen_siesa_raw`."
                        )
                        st.session_state.setdefault("auditoria_bodegas_sel", [])
                    else:
                        df_bod = df_bod.copy()
                        df_bod["Bodega"] = df_bod["Bodega"].astype(str).str.strip()
                        codes = sorted(df_bod["Bodega"].unique().tolist())

                        def _fmt_bodega_aud(c: str) -> str:
                            sub = df_bod[df_bod["Bodega"] == c]
                            if sub.empty:
                                return c
                            r = sub.iloc[0]
                            nb = str(r.get("Nom_Bodega", "") or "").strip()
                            ni = str(r.get("Nom_Instalacion", "") or "").strip()
                            base = f"{c} — {nb}" if nb else c
                            return f"{base} ({ni})" if ni else base

                        st.multiselect(
                            "Bodegas a incluir en auditoría",
                            options=codes,
                            default=[],
                            format_func=_fmt_bodega_aud,
                            key="auditoria_bodegas_sel",
                            help="Sin selección = todas. Con selección, solo esas bodegas entran al cálculo de auditoría.",
                        )
                        if st.session_state.get("auditoria_bodegas_sel"):
                            st.info(
                                "Este filtro de bodegas **solo aplica a SQL 003 (`auditoria_raw`)**. "
                                "SQL 001 y SQL 002 se cargan completas. Los scripts 01, 02 y 03 no se afectan."
                            )
                actualizar = st.button("Ejecutar actualización", type="primary", width="stretch")
        with control_cols[1]:
            mostrar_log = st.checkbox("Ver log de ejecución", value=False)
        with control_cols[2]:
            with st.popover("⚙️", use_container_width=True):
                st.markdown("**Auditoría**")
                audit_subtabs = st.tabs(["Cruces", "Laboratorio SQL"])
                with audit_subtabs[0]:
                    _render_tab_auditoria()
                with audit_subtabs[1]:
                    _render_tab_laboratorio_sql()

    if st.session_state.get("_log_visible"):
        with st.container(border=True):
            log_top_cols = st.columns([4, 1], gap="small")
            with log_top_cols[0]:
                st.markdown(f"**Log de ejecución** {st.session_state.get('_log_status', '')}".strip())
            with log_top_cols[1]:
                if st.button("Cerrar log", width="stretch", key="cerrar_log_actualizacion"):
                    st.session_state["_log_visible"] = False
                    st.session_state["_log_text"] = ""
                    st.session_state["_log_status"] = ""
            if st.session_state.get("_log_visible"):
                st.text_area(
                    "Detalle",
                    value=st.session_state.get("_log_text", ""),
                    height=260,
                    key="log_actualizacion_visible",
                )

    ejecutar_actualizacion = bool(locals().get("actualizar", False))
    if not ejecutar_actualizacion:
        return

    seleccion = _seleccion_actualizacion_desde_session_state()

    if not seleccion:
        st.warning("Selecciona al menos un proceso para actualizar.")
        return

    st.session_state["_actualizando"] = True
    st.session_state["_log_visible"] = True
    st.session_state["_log_text"] = "Iniciando..."
    st.session_state["_log_status"] = " - Ejecución en curso"
    panel_logs = st.empty()
    log_placeholder = None
    if mostrar_log:
        log_live = panel_logs.container()
        with log_live:
            st.markdown("**Ejecución en curso**")
            log_placeholder = st.empty()
            log_placeholder.text_area("Log de ejecución", value="Iniciando...", height=260)

    def _on_log_update(log_txt: str) -> None:
        st.session_state["_log_text"] = log_txt or "Procesando..."
        if log_placeholder is not None:
            log_placeholder.text_area("Log de ejecución", value=log_txt or "Procesando...", height=260)

    ok = False
    try:
        _sql_opcion_map = {
            SQL_001_OPCION: SQL_001_KEY,
            SQL_002_OPCION: SQL_002_KEY,
            SQL_003_OPCION: SQL_003_KEY,
        }
        sql_keys_sel = [_sql_opcion_map[op] for op in SQL_OPCIONES if op in seleccion]
        ejecutar_sql = len(sql_keys_sel) > 0
        pipelines_seleccionados = [p for p in PIPELINE_OPCIONES if p in seleccion]
        bodegas_aud = None
        if SQL_003_OPCION in seleccion:
            bodegas_aud = list(st.session_state.get("auditoria_bodegas_sel") or [])
        with st.spinner("Ejecutando actualización..."):
            ok, _ = ejecutar_pipelines(
                log_callback=_on_log_update,
                ejecutar_reportes_sql=ejecutar_sql,
                pipelines_a_ejecutar=pipelines_seleccionados,
                auditoria_bodegas=bodegas_aud,
                sql_queries=sql_keys_sel if sql_keys_sel else None,
            )
    finally:
        st.session_state["_actualizando"] = False

    sync_error = None
    try:
        sync_read_db(force=True)
        _cargar_margenes_para_dashboard.clear()
        _cargar_auditoria.clear()
        _cargar_auditoria_referencias.clear()
    except Exception as exc:
        sync_error = str(exc)

    st.session_state["_log_status"] = " - Proceso exitoso" if ok else " - Proceso con errores"
    if ok:
        st.success("Pipelines ejecutados correctamente.")
    else:
        st.error("Falló la actualización de datos.")
    if sync_error:
        st.warning(
            "Se actualizó la base principal, pero no se pudo refrescar la copia de lectura. "
            f"Detalle: {sync_error}"
        )
    st.toast("Actualización terminada.")

def _render_tab_consulta() -> None:
    if st.session_state.get("_actualizando"):
        st.info("Consultas pausadas mientras termina la actualización. Puedes cambiar entre pestañas libremente.")
        return

    tab_individual, tab_masiva = st.tabs(["Consulta individual", "Consulta en lote (CSV)"])
    with tab_individual:
        _render_tab_consulta_individual()
    with tab_masiva:
        _render_tab_consulta_masiva()


def _render_tab_consulta_individual() -> None:
    st.markdown(
        '<div class="consulta-individual-hero" role="region" aria-label="Consulta individual">'
        '<h2 class="consulta-individual-title">Consulta individual</h2>'
        "</div>",
        unsafe_allow_html=True,
    )

    disp_umbral: float = float(st.session_state.get("consulta_origen_disp_umbral", 0.0))

    with st.container(border=True):
        st.markdown(
            '<p class="consulta-individual-section-title">Búsqueda y selección</p>',
            unsafe_allow_html=True,
        )
        try:
            col_busq, col_sel = st.columns([1.05, 2.95], vertical_alignment="bottom", gap="medium")
        except TypeError:
            col_busq, col_sel = st.columns([1.05, 2.95], gap="medium")
        with col_busq:
            texto_busqueda = st.text_input(
                "Referencia o descripción",
                placeholder="Código, alterna o palabra en nombre",
                key="consulta_txt_busqueda",
                label_visibility="visible",
                help="Referencia original, normalizada o texto en descripción.",
            )

        try:
            df_refs = buscar_referencias(texto_busqueda) if texto_busqueda else None
        except Exception as exc:
            st.error(f"No fue posible leer datos de DuckDB: {exc}")
            return

        _q = (texto_busqueda or "").strip()
        _sel_key = (
            "consulta_coincidencias_"
            + hashlib.sha256(_q.encode("utf-8")).hexdigest()[:20]
            if _q
            else "consulta_coincidencias__vacío"
        )

        ref_norm: str | None = None
        with col_sel:
            if df_refs is not None and not df_refs.empty:
                opciones = {}
                for _, row in df_refs.iterrows():
                    desc = str(row.get("Descripción", "") or "").strip()
                    if len(desc) > 70:
                        desc = desc[:67] + "..."
                    etiqueta = f"{row['Referencia_Original']} | {row['Referencia_Normalizada']}"
                    if desc:
                        etiqueta = f"{etiqueta} | {desc}"
                    opciones[etiqueta] = row["Referencia_Normalizada"]

                opts_keys = list(opciones.keys())
                if _sel_key in st.session_state:
                    _cur = st.session_state[_sel_key]
                    if _cur not in opciones:
                        del st.session_state[_sel_key]
                seleccionado = st.selectbox(
                    "Coincidencia",
                    opts_keys,
                    key=_sel_key,
                    help="Elige la fila a analizar.",
                )
                ref_norm = opciones[seleccionado]
            elif texto_busqueda:
                st.caption("Sin coincidencias")
            else:
                st.caption("Escribe para ver coincidencias.")

        resumen: dict | None = None
        df_masivo_one: pd.DataFrame | None = None
        if ref_norm:
            try:
                resumen = obtener_resumen_referencia(ref_norm)
                resolver_masivo = getattr(data_access_service, "obtener_resumen_referencias_masivo", None)
                if resolver_masivo is not None:
                    try:
                        df_single = resolver_masivo([ref_norm])
                        if df_single is not None and not df_single.empty:
                            df_masivo_one = df_single
                            row_m = df_single.iloc[0].to_dict()
                            if resumen is None:
                                resumen = row_m
                            else:
                                for k in (
                                    "Costo_Min",
                                    "Costo_Max",
                                    "Existencia_Total",
                                    "_disponible",
                                    "_disp_total",
                                    "Pais_Ultima",
                                    "Tipo_Origen",
                                ):
                                    v = row_m.get(k)
                                    if v is not None and not (isinstance(v, float) and pd.isna(v)):
                                        resumen[k] = v
                                v_tipo_sp = row_m.get("Tipo Origen")
                                if v_tipo_sp is not None and not (
                                    isinstance(v_tipo_sp, float) and pd.isna(v_tipo_sp)
                                ):
                                    if not str(resumen.get("Tipo_Origen") or "").strip():
                                        resumen["Tipo_Origen"] = v_tipo_sp
                    except Exception:
                        pass
            except Exception as exc:
                st.error(f"No fue posible consultar la referencia: {exc}")
                return

        if ref_norm and resumen:
            st.markdown(
                '<p class="consulta-individual-section-title consulta-individual-section-title--muted">'
                "Umbral de disponibilidad (mejor precio por región)</p>",
                unsafe_allow_html=True,
            )
            try:
                c_hint, c_um = st.columns([1.15, 1.85], vertical_alignment="center", gap="large")
            except TypeError:
                c_hint, c_um = st.columns([1.15, 1.85], gap="large")
            with c_hint:
                st.markdown(
                    '<p class="consulta-individual-disp-hint">Solo compiten orígenes con stock '
                    "<strong>estrictamente mayor</strong> al umbral. Si ninguno cumple, no hay «mejor precio» "
                    "válido hasta que subas el umbral o revises disponibilidad.</p>",
                    unsafe_allow_html=True,
                )
            with c_um:
                max_disp = _consulta_max_disp_para_slider(resumen)
                disp_umbral = st.slider(
                    "Umbral (unidades)",
                    min_value=0.0,
                    max_value=float(max_disp),
                    step=0.5,
                    key="consulta_origen_disp_umbral",
                    help=(
                        "Solo se comparan regiones con stock estrictamente mayor a este valor; entre ellas, "
                        "se resalta la de menor precio. Si ninguna supera el umbral, no hay «Mejor precio»."
                    ),
                    label_visibility="visible",
                )

    if texto_busqueda and (df_refs is None or df_refs.empty):
        st.warning("No se encontraron coincidencias por referencia ni por descripción.")

    save_tab_filter_prefs("consulta")

    if not ref_norm:
        return

    if not resumen:
        st.info("La referencia no tiene datos en `resultado_precios_lista`.")
        return

    refs_alternas = str(resumen.get("RefsAlternas", "") or "").strip()
    ref_original = str(resumen.get("Referencia_Original", "") or "").strip().upper()

    with st.container(border=True):
        st.markdown(
            '<p class="consulta-individual-section-title">Parámetros de reposición (COP)</p>',
            unsafe_allow_html=True,
        )
        _render_consulta_individual_reposicion_sliders()

    trm_repo = float(st.session_state.get("consulta_individual_cot_trm", 4200))
    margin_repo = float(st.session_state.get("consulta_individual_cot_margen", 25))
    piso_margin_repo = float(st.session_state.get("consulta_individual_cot_piso_margen", 40))
    factor_usabr_repo = float(st.session_state.get("consulta_individual_factor_usabr", 1.35))
    factor_euro_repo = float(st.session_state.get("consulta_individual_factor_euro", 1.55))

    st.markdown(
        '<p class="consulta-individual-section-title">Ficha de referencia</p>',
        unsafe_allow_html=True,
    )
    st.markdown(
        _consulta_build_sheet_html(
            resumen,
            refs_alternas,
            ref_original,
            float(disp_umbral),
            df_masivo_one=df_masivo_one,
            trm_repo=trm_repo,
            margin_repo_pct=margin_repo,
            piso_margin_repo_pct=piso_margin_repo,
            factor_usabr_repo=factor_usabr_repo,
            factor_euro_repo=factor_euro_repo,
            sheet_variant="individual",
        ),
        unsafe_allow_html=True,
    )
    st.caption(
        "**Umbral de disponibilidad** (primer bloque) define qué orígenes compiten por el USD base. "
        "**TRM, márgenes y factores** están en el bloque de parámetros de reposición."
    )

    st.markdown(
        '<div class="consulta-ventas-wrap">'
        '<p class="consulta-individual-section-title consulta-individual-section-title--muted">'
        "Inventario por bodega (Siesa)</p></div>",
        unsafe_allow_html=True,
    )
    st.caption(
        "Detalle por instalación y bodega desde `margen_siesa_raw` (carga SQL 001). "
        "Incluye precio lista 09 y margen tal como vienen del reporte Siesa; sin sumas ni recálculos en Python."
    )
    try:
        df_bod = obtener_existencia_por_bodega_consulta(ref_norm)
    except Exception as exc:
        st.error(f"No fue posible consultar inventario por bodega: {exc}")
        df_bod = None

    if df_bod is None:
        pass
    elif df_bod.empty:
        st.markdown(
            '<p class="consulta-msg-soft">No hay filas en <code>margen_siesa_raw</code> para esta referencia '
            "o la tabla no existe. Ejecuta <strong>Actualizar datos</strong> incluyendo "
            "<strong>SQL 001 — Margen SIESA</strong>.</p>",
            unsafe_allow_html=True,
        )
    else:
        df_bod_disp = df_bod.copy()
        if "Instalación" in df_bod_disp.columns:
            df_bod_disp["Instalación"] = df_bod_disp["Instalación"].map(_fmt_consulta_entero)
        for _c in ("Existencia", "Cant. disponible"):
            if _c in df_bod_disp.columns:
                df_bod_disp[_c] = pd.to_numeric(df_bod_disp[_c], errors="coerce").map(_fmt_consulta_entero)
        if "Costo prom. unit. (inst.)" in df_bod_disp.columns:
            df_bod_disp["Costo prom. unit. (inst.)"] = pd.to_numeric(
                df_bod_disp["Costo prom. unit. (inst.)"], errors="coerce"
            ).map(_fmt_money_cop_local)
        if "Precio lista 09" in df_bod_disp.columns:
            df_bod_disp["Precio lista 09"] = pd.to_numeric(
                df_bod_disp["Precio lista 09"], errors="coerce"
            ).map(_fmt_money_cop_local)
        _cfg_bod: dict[str, st.column_config.Column] = {}
        if "Margen" in df_bod_disp.columns:
            _mb = pd.to_numeric(df_bod_disp["Margen"], errors="coerce")
            if _mb.notna().any():
                mx = float(_mb.abs().max())
                if mx <= 2.0:
                    _mb = _mb * 100.0
            df_bod_disp["Margen"] = _mb
            _cfg_bod["Margen"] = st.column_config.NumberColumn(
                "Margen",
                format="%.2f%%",
                help="Porcentaje numérico (reporte Siesa); permite ordenar por valor.",
            )
        _bod_kw: dict = {"width": "stretch", "hide_index": True}
        if _cfg_bod:
            _bod_kw["column_config"] = _cfg_bod
        st.dataframe(df_bod_disp, **_bod_kw)

    try:
        ventas = obtener_ultimas_ventas(ref_norm, limite=20)
    except Exception as exc:
        st.error(f"No fue posible consultar ventas: {exc}")
        ventas = None

    try:
        _v_min, _v_max = obtener_rango_fechas_ventas_raw()
    except Exception:
        _v_min, _v_max = None, None
    _help_uv = html.escape(_consulta_help_ultimas_ventas(_v_min, _v_max), quote=True)
    st.markdown(
        '<div class="consulta-ventas-wrap">'
        '<p class="consulta-individual-section-title consulta-individual-section-title--muted">'
        "Últimas ventas</p>"
        '<div class="consulta-ventas-head-row">'
        f'<span class="consulta-kpi-help-icon" title="{_help_uv}" '
        f'aria-label="{_help_uv}" role="img">i</span>'
        "</div></div>",
        unsafe_allow_html=True,
    )
    if ventas is None:
        st.markdown(
            '<p class="consulta-msg-soft">Consulta de ventas no disponible en este momento.</p>',
            unsafe_allow_html=True,
        )
    elif ventas.empty:
        st.markdown(
            '<p class="consulta-msg-soft">No hay ventas en <code>ventas_raw</code> para esta referencia.</p>',
            unsafe_allow_html=True,
        )
    else:
        ventas_show = _renombrar_negocio(ventas)
        v_disp = ventas_show.copy()
        _cfg_ult_ventas: dict[str, st.column_config.Column] = {}
        # Fecha como datetime (no texto): el grid ordena bien asc/desc; el formato dd/mm/aaaa va en column_config.
        if "Fecha Factura" in v_disp.columns:
            v_disp["Fecha Factura"] = pd.to_datetime(v_disp["Fecha Factura"], errors="coerce")
            _cfg_ult_ventas["Fecha Factura"] = st.column_config.DatetimeColumn(
                "Fecha Factura",
                format="DD/MM/YYYY",
            )
        if "Cant." in v_disp.columns:
            v_disp["Cant."] = pd.to_numeric(v_disp["Cant."], errors="coerce").map(_fmt_consulta_entero)
        for _col in ("Precio Unit. Venta", "Valor Venta"):
            if _col in v_disp.columns:
                v_disp[_col] = pd.to_numeric(v_disp[_col], errors="coerce").map(_fmt_money_cop_local)
        if "Margen" in v_disp.columns:
            marg_s = pd.to_numeric(v_disp["Margen"], errors="coerce")
            if marg_s.notna().any():
                mx_m = float(marg_s.abs().max())
                if mx_m <= 2.0:
                    marg_s = marg_s * 100.0
            v_disp["Margen"] = marg_s
            _cfg_ult_ventas["Margen"] = st.column_config.NumberColumn(
                "Margen",
                format="%.2f%%",
                help="Porcentaje numérico; ordenación por valor.",
            )
        _uv_kw: dict = {"width": "stretch", "hide_index": True}
        if _cfg_ult_ventas:
            _uv_kw["column_config"] = _cfg_ult_ventas
        st.dataframe(v_disp, **_uv_kw)

    save_tab_filter_prefs("consulta")


# Columnas opcionales en consulta masiva: orden lógico (inventario/venta → auditoría última compra).
_CONSULTA_MASIVA_COLS_EXTRA_BLOQUE_INV: Final[tuple[str, ...]] = (
    "Costo_Min",
    "Costo_Max",
    "Existencia_Total",
    "Precio_Lista_09",
    "Tipo_Origen",
    "Ult. Precio Venta",
    "Fecha Ult. Venta",
)
_CONSULTA_MASIVA_COLS_EXTRA_BLOQUE_AUD: Final[tuple[str, ...]] = (
    "Fecha_Ultima_Compra",
    "Pais_Ultima",
    "Proveedor_Ultima",
    "Comprador_Ultima",
    "Precio_USD_Ultima",
    "Precio_COP_Ultima",
    "TRM_Ultima",
)
_CONSULTA_MASIVA_COLS_EXTRA_ORDER: Final[tuple[str, ...]] = (
    _CONSULTA_MASIVA_COLS_EXTRA_BLOQUE_INV + _CONSULTA_MASIVA_COLS_EXTRA_BLOQUE_AUD
)
_CONSULTA_MASIVA_COLS_EXTRA: Final[frozenset[str]] = frozenset(_CONSULTA_MASIVA_COLS_EXTRA_ORDER)

# Ocultas solo en pantalla (siguen en el CSV descargado).
_CONSULTA_MASIVA_COLS_OCULTAR_UI: Final[frozenset[str]] = frozenset(
    {"Referencia_Original", "Referencia_Normalizada"}
)

# Orden lógico para «Solo columnas estratégicas» (etiquetas ya renombradas en la tabla de consulta masiva).
_CONSULTA_MASIVA_PRIORIDAD_ESTRATEGICA: Final[tuple[str, ...]] = (
    "Referencia Entrada",
    "Ref. cruce",
    "Ref. alternas",
    "Estado",
    "Tipo Coincidencia",
    "Descripción",
    "Mejor Origen",
    "Mejor Disponibilidad",
    "Mejor Precio Ajustado",
    "Mejor Precio Sin Factor",
    "Precio Brasil",
    "Precio Usa",
    "Precio Europa",
    "disp br",
    "disp usa",
    "disp eur",
    "Fecha — lista (OC)",
    "Prov. — lista",
    "USD — lista",
    "COP liq. — lista",
    "Costo mín.",
    "Costo máx.",
    "Exist. total (ref.)",
    "Precio lista 09 (COP)",
    "Tipo origen",
    "Valor últ. venta",
    "Fecha últ. venta",
    "Fecha — aud.",
    "País — aud.",
    "Prov. — aud.",
    "USD — aud.",
    "COP — aud.",
    "DNET BRA (USD)",
    "DNET USA (USD)",
    "DNET EUR (EUR)",
)


def _consulta_masiva_columnas_estrategicas(cols: list[str]) -> list[str]:
    """Subconjunto de columnas visibles para lectura ejecutiva (respeta orden de prioridad)."""
    presentes = set(cols)
    out: list[str] = []
    for lbl in _CONSULTA_MASIVA_PRIORIDAD_ESTRATEGICA:
        if lbl in presentes and lbl not in out:
            out.append(lbl)
    if not out:
        return cols[: min(12, len(cols))]
    return out


def _consulta_masiva_preparar_vista(df: pd.DataFrame) -> pd.DataFrame:
    """Quita ref. original/normalizada; orden: entrada → ref. cruce → alternas → estado…"""
    cols = [c for c in df.columns if c not in _CONSULTA_MASIVA_COLS_OCULTAR_UI]
    head: list[str] = []
    for key in (
        "Referencia_Entrada",
        "Referencia_Cruce",
        "RefsAlternas",
        "Estado",
        "Tipo_Coincidencia",
        "Descripción",
    ):
        if key in cols:
            head.append(key)
    tail = [c for c in cols if c not in head]
    return df[head + tail]


def _consulta_masiva_etiquetas_lista(df: pd.DataFrame) -> pd.DataFrame:
    """Última compra pipeline lista (OC) — nombres cortos negocio."""
    m = {
        "Ult. Fecha Compra": "Fecha — lista (OC)",
        "Proveedor": "Prov. — lista",
        "Ultimo Valor USD": "USD — lista",
        "Valor liquidado COP": "COP liq. — lista",
    }
    return df.rename(columns={k: v for k, v in m.items() if k in df.columns})


def _consulta_masiva_etiquetas_audit(df: pd.DataFrame) -> pd.DataFrame:
    """Última compra auditoría (SQL 003) — nombres cortos negocio."""
    m = {
        "Fecha últ. compra": "Fecha — aud.",
        "País últ. compra": "País — aud.",
        "Proveedor últ.": "Prov. — aud.",
        "Comprador últ.": "Compr. — aud.",
        "Últ compra (USD)": "USD — aud.",
        "Últ compra (COP)": "COP — aud.",
        "TRM últ compra": "TRM — aud.",
    }
    return df.rename(columns={k: v for k, v in m.items() if k in df.columns})


def _consulta_masiva_encabezados_id(df: pd.DataFrame) -> pd.DataFrame:
    """Encabezados compactos solo en consulta masiva."""
    m = {"RefsAlternas": "Ref. alternas"}
    return df.rename(columns={k: v for k, v in m.items() if k in df.columns})


def _cot_mejor_origen_es_europa(mejor: object) -> bool:
    """True si el ganador regional es Europa (para incluir EU en dispersión USD)."""
    s = str(mejor or "").upper().strip()
    return "EUROPA" in s or "EUROPE" in s or s in ("EUR", "EU")


def _consulta_masiva_origenes_usd_ajustados(
    row: pd.Series,
    factor_usabr: float,
    factor_euro: float,
    disp_umbral: float,
    *,
    incluir_europa: bool = True,
) -> list[float]:
    """Precios USD ajustados por origen (misma lógica que mejor precio) para medir dispersión.

    Por defecto incluye los tres orígenes. Si ``incluir_europa`` es False, solo Brasil y USA
    entran en la lista: EU es apoyo y no debe inflar el score cuando el mejor precio es BR/USA.
    """
    out: list[float] = []
    bloques: list[tuple[str, str, float]] = [
        ("Precio Brasil", "disp_br", float(factor_usabr)),
        ("Precio Usa", "disp_usa", float(factor_usabr)),
    ]
    if incluir_europa:
        bloques.append(("Precio Europa", "disp_eur", float(factor_euro)))
    for pcol, dcol, fac in bloques:
        pv = row.get(pcol)
        dv = row.get(dcol)
        if pv is None or pd.isna(pv) or dv is None or pd.isna(dv):
            continue
        if float(dv) <= float(disp_umbral):
            continue
        out.append(float(pv) * fac)
    return out


def _consulta_masiva_cotizador_alertas(
    row: pd.Series,
    *,
    p_rec: float | None,
    p_expert: float | None,
    p_piso: float | None,
    p_adj: float | None,
    p_repo_para_guias: float | None,
    costo_min: float | None,
    costo_max: float | None,
    pl09: float | None,
    ult_venta: float | None,
    precio_cop_ult_compra: float | None,
    exist: float | None,
    factor_usabr: float,
    factor_euro: float,
    disp_umbral: float,
    pct_umbral_lista_vs_repo: float = 0.35,
    pct_umbral_venta_vs_repo: float = 0.40,
    pct_umbral_compra_vs_repo: float = 0.40,
    umbral_existencia_justa: float = 3.0,
) -> tuple[str, str, bool, float | None, float | None, float | None]:
    """
    Devuelve: estado_cotización, texto alertas, si se anula P recomendado,
    y tres guías internas (lista09, últ. venta, últ. compra) solo para umbrales de score.

    - **Lista 09** y **últ. venta** se comparan con **precio reposición** = USD_base × TRM ÷ (1 − margen cot.)
      (= **P_experto** cuando hay USD base).
    - **Últ. compra (COP)** se compara con **USD_base × TRM** (costo importación, sin margen).
    """
    # Umbrales (negocio: guía + alertas, no ley absoluta)
    pct_spread_origen = 0.35
    pct_spread_origen_crit = 0.55
    inv_justo_max = min(max(float(umbral_existencia_justa), 1.0), 500.0)
    pct_lista_vs_repo = min(max(float(pct_umbral_lista_vs_repo), 0.01), 0.99)
    pct_venta_vs_repo = min(max(float(pct_umbral_venta_vs_repo), 0.01), 0.99)
    pct_compra_vs_repo = min(max(float(pct_umbral_compra_vs_repo), 0.01), 0.99)
    pct_costo_min_vs_max = 0.35
    score_bloqueo = 5

    alertas: list[str] = []
    score = 0
    guia_pl: float | None = None
    guia_vt: float | None = None
    guia_uc: float | None = None

    tiene_usd = p_adj is not None and pd.notna(p_adj)
    tiene_costo = costo_min is not None and pd.notna(costo_min) and float(costo_min) > 0

    if not tiene_usd and not tiene_costo:
        return (
            "Precio no calculable automáticamente",
            "Sin USD base (mejor ajustado ni último USD lista) y sin costo mín. inventario.",
            True,
            None,
            None,
            None,
        )

    if exist is not None and pd.notna(exist) and 0 < float(exist) <= inv_justo_max:
        alertas.append(f"Inventario muy justo (≤{inv_justo_max:.0f} uds.)")
        score += 1

    if (
        costo_min is not None
        and pd.notna(costo_min)
        and float(costo_min) > 0
        and costo_max is not None
        and pd.notna(costo_max)
        and float(costo_max) > 0
    ):
        lo = min(float(costo_min), float(costo_max))
        hi = max(float(costo_min), float(costo_max))
        spr_cm = (hi - lo) / lo if lo > 1e-12 else 0.0
        if spr_cm > pct_costo_min_vs_max:
            alertas.append(f"Costo mín. vs costo máx. inventario muy desalineados (~{spr_cm * 100:.0f}%)")
            score += 2

    incluir_eu_disp = _cot_mejor_origen_es_europa(row.get("Mejor_Origen"))
    adjs = _consulta_masiva_origenes_usd_ajustados(
        row, factor_usabr, factor_euro, disp_umbral, incluir_europa=incluir_eu_disp
    )
    if len(adjs) >= 2:
        mx, mn = max(adjs), min(adjs)
        if mn > 1e-12:
            spr = (mx - mn) / mn
            if spr > pct_spread_origen:
                alertas.append(f"Dispersión alta entre orígenes USD (~{spr * 100:.0f}%)")
                score += 2 if spr <= pct_spread_origen_crit else 4

    if pl09 is not None and pd.notna(pl09) and float(pl09) > 0:
        p_lista_ref = (
            float(p_expert)
            if p_expert is not None and pd.notna(p_expert) and float(p_expert) > 0
            else None
        )
        if p_lista_ref is not None:
            guia_pl = abs(float(pl09) - p_lista_ref) / max(float(pl09), p_lista_ref)
            if guia_pl > pct_lista_vs_repo:
                alertas.append(
                    f"Lista 09 vs precio reposición (USD base×TRM÷(1−margen cot.)) muy distinto (~{guia_pl * 100:.0f}%)"
                )
                score += 2

    if ult_venta is not None and pd.notna(ult_venta) and float(ult_venta) > 0:
        p_venta_ref = (
            float(p_expert)
            if p_expert is not None and pd.notna(p_expert) and float(p_expert) > 0
            else None
        )
        if p_venta_ref is not None:
            guia_vt = abs(float(ult_venta) - p_venta_ref) / max(float(ult_venta), p_venta_ref)
            if guia_vt > pct_venta_vs_repo:
                alertas.append(
                    f"Últ. venta vs precio reposición (USD base×TRM÷(1−margen cot.)) muy distinto (~{guia_vt * 100:.0f}%)"
                )
                score += 1

    prg = p_repo_para_guias
    if prg is not None and pd.notna(prg) and float(prg) > 0:
        prf = float(prg)
        if precio_cop_ult_compra is not None and pd.notna(precio_cop_ult_compra) and float(precio_cop_ult_compra) > 0:
            uc = float(precio_cop_ult_compra)
            guia_uc = abs(uc - prf) / max(uc, prf)
            if guia_uc > pct_compra_vs_repo:
                alertas.append(
                    f"Últ. compra (COP) vs reposición importación (USD base×TRM) muy distinto (~{guia_uc * 100:.0f}%)"
                )
                score += 1

    if (
        p_expert is not None
        and p_piso is not None
        and p_rec == p_piso
        and float(p_expert) < float(p_piso) * 0.5
    ):
        alertas.append("Piso inventario domina; experto muy por debajo (revisar USD/margen/TRM)")
        score += 1

    texto = " · ".join(alertas) if alertas else ""

    if score >= score_bloqueo:
        return (
            "Precio no calculable automáticamente",
            texto or "Demasiadas señales de riesgo.",
            True,
            guia_pl,
            guia_vt,
            guia_uc,
        )
    if score >= 2 or len(alertas) >= 2:
        return "Revisar manual", texto, False, guia_pl, guia_vt, guia_uc
    if alertas:
        return "OK (con observaciones)", texto, False, guia_pl, guia_vt, guia_uc
    return "OK", "", False, guia_pl, guia_vt, guia_uc


def _consulta_masiva_cotizador_df(
    df: pd.DataFrame,
    margin_pct: float,
    trm: float,
    *,
    piso_margin_pct: float = 40.0,
    factor_usabr: float = 1.35,
    factor_euro: float = 1.55,
    factor_otros: float | None = None,
    disp_umbral: float = 0.0,
    pct_umbral_lista_vs_repo: float = 0.35,
    pct_umbral_venta_vs_repo: float = 0.40,
    pct_umbral_compra_vs_repo: float = 0.40,
    umbral_existencia_justa: float = 3.0,
) -> pd.DataFrame:
    """
    Cotización COP a partir del mejor USD (ya lleva factor BR/USA/EUR de la consulta masiva).
    - Costo_reposicion_COP = USD_base × TRM (sin margen de venta).
    - Precio_reposicion_COP = USD_base × TRM / (1 − margen) (= P_venta_experto_COP cuando hay USD base).
    - P_experto = USD_base × TRM / (1 − margen), margen en % sobre precio de venta.
    - P_piso inventario = Costo_Min / (1 - X%), con X configurable.
    - P_recomendado = max(P_experto, P_piso) cuando ambos existen (salvo bloqueo por alertas).
    Alertas **lista 09** y **últ. venta** vs **precio reposición** (= P_experto). **Últ. compra (COP)** vs **USD_base × TRM**
    (costo importación sin margen).
    """
    m = float(margin_pct) / 100.0
    m = min(max(m, 0.01), 0.95)
    denom = max(1e-12, 1.0 - m)
    trm_f = float(trm)
    factor_otros_f = float(factor_euro if factor_otros is None else factor_otros)
    piso_m = float(piso_margin_pct) / 100.0
    piso_m = min(max(piso_m, 5.0 / 100.0), 80.0 / 100.0)
    denom_piso = max(1e-12, 1.0 - piso_m)

    def _factor_por_pais_ultima_compra(pais: object) -> float:
        s = str(pais or "").upper().strip()
        # Variantes comunes para evitar dejar compras sin factor comparable.
        if (
            "USA" in s
            or s == "US"
            or "ESTADOS UNIDOS" in s
            or "UNITED STATES" in s
            or "BRASIL" in s
            or "BRAZIL" in s
            or s == "BR"
        ):
            return float(factor_usabr)
        if "EUROPA" in s or "EUROPE" in s or "EUR" in s or "FRANCIA" in s or "ITALIA" in s:
            return float(factor_euro)
        return factor_otros_f

    rows: list[dict] = []
    for _, row in df.iterrows():
        p_adj = row.get("Mejor_Precio_Ajustado")
        fuente_usd = "Mejor_Precio_Ajustado"
        factor_ultima_aplicado: float | None = None
        if pd.isna(p_adj) or p_adj is None:
            ult_usd = row.get("Ultimo Valor USD")
            if ult_usd is not None and pd.notna(ult_usd):
                factor_ultima_aplicado = _factor_por_pais_ultima_compra(row.get("Pais_Ultima"))
                p_adj = float(ult_usd) * float(factor_ultima_aplicado)
                fuente_usd = "Ult. Fecha Compra / lista (USD, ajustado)"
            else:
                p_adj = None
                fuente_usd = ""

        costo_min = row.get("Costo_Min")
        costo_max = row.get("Costo_Max")
        exist = row.get("Existencia_Total")
        pl09 = row.get("Precio_Lista_09")
        ult_venta = row.get("Ult. Precio Venta")
        precio_uc: float | None = None
        for _k in ("Precio_COP_Ultima", "Precio COP Ultima"):
            _v = row.get(_k)
            if _v is not None and pd.notna(_v):
                try:
                    _f = float(_v)
                    if _f > 0:
                        precio_uc = _f
                        break
                except (TypeError, ValueError):
                    continue

        p_expert: float | None = None
        if p_adj is not None and pd.notna(p_adj):
            p_expert = float(p_adj) * trm_f / denom

        p_piso: float | None = None
        if costo_min is not None and pd.notna(costo_min) and float(costo_min) > 0:
            p_piso = float(costo_min) / denom_piso

        p_rec: float | None = None
        regla = ""
        if p_expert is not None and p_piso is not None:
            p_rec = max(p_expert, p_piso)
            if abs(p_expert - p_piso) < 1e-6:
                regla = "Experto = piso inventario"
            elif p_rec == p_piso:
                regla = f"Piso inventario (costo min. / (1-{piso_margin_pct:.0f}%))"
            else:
                regla = "Experto (USD×TRM/(1−margen))"
        elif p_expert is not None:
            p_rec = p_expert
            regla = "Experto (sin costo min. para piso)"
        elif p_piso is not None:
            p_rec = p_piso
            regla = "Solo piso inventario (sin USD base)"

        p_adj_for_alert = p_adj if p_adj is not None and pd.notna(p_adj) else None
        p_repo_para_guias: float | None = None
        if p_adj_for_alert is not None and float(p_adj_for_alert) > 0:
            p_repo_para_guias = float(p_adj_for_alert) * trm_f

        estado_cot, alertas_txt, anular_rec, _, _, _ = _consulta_masiva_cotizador_alertas(
            row,
            p_rec=p_rec,
            p_expert=p_expert,
            p_piso=p_piso,
            p_adj=p_adj_for_alert,
            p_repo_para_guias=p_repo_para_guias,
            costo_min=float(costo_min) if costo_min is not None and pd.notna(costo_min) else None,
            costo_max=float(costo_max) if costo_max is not None and pd.notna(costo_max) else None,
            pl09=float(pl09) if pl09 is not None and pd.notna(pl09) else None,
            ult_venta=float(ult_venta) if ult_venta is not None and pd.notna(ult_venta) else None,
            precio_cop_ult_compra=precio_uc,
            exist=float(exist) if exist is not None and pd.notna(exist) else None,
            factor_usabr=factor_usabr,
            factor_euro=factor_euro,
            disp_umbral=disp_umbral,
            pct_umbral_lista_vs_repo=pct_umbral_lista_vs_repo,
            pct_umbral_venta_vs_repo=pct_umbral_venta_vs_repo,
            pct_umbral_compra_vs_repo=pct_umbral_compra_vs_repo,
            umbral_existencia_justa=umbral_existencia_justa,
        )

        p_rec_final = None if anular_rec else p_rec

        costo_repo_cop: float | None = None
        precio_repo_cop: float | None = None
        if p_adj is not None and pd.notna(p_adj) and float(p_adj) > 0:
            costo_repo_cop = float(p_adj) * trm_f
            if p_expert is not None:
                precio_repo_cop = float(p_expert)
            else:
                precio_repo_cop = float(p_adj) * trm_f / denom

        rows.append(
            {
                "Referencia_Entrada": row.get("Referencia_Entrada"),
                "Referencia_Cruce": row.get("Referencia_Cruce"),
                "Estado": row.get("Estado"),
                "Mejor_Origen": row.get("Mejor_Origen"),
                "USD_base": float(p_adj) if p_adj is not None and pd.notna(p_adj) else None,
                "USD_base_unidades_disp": (
                    float(row.get("Mejor_Disponibilidad"))
                    if row.get("Mejor_Disponibilidad") is not None and pd.notna(row.get("Mejor_Disponibilidad"))
                    else None
                ),
                "USD_base_fuente": fuente_usd or None,
                "Factor_ultima_compra_aplicado": factor_ultima_aplicado,
                "Costo_Min": float(costo_min) if costo_min is not None and pd.notna(costo_min) else None,
                "Costo_Max": float(costo_max) if costo_max is not None and pd.notna(costo_max) else None,
                "Existencia_Total": float(exist) if exist is not None and pd.notna(exist) else None,
                "Precio_Lista_09": float(pl09) if pl09 is not None and pd.notna(pl09) else None,
                "Ult_venta_guia": float(ult_venta) if ult_venta is not None and pd.notna(ult_venta) else None,
                "Margen_pct_cot": margin_pct,
                "TRM_cot": trm_f,
                "Costo_reposicion_COP": costo_repo_cop,
                "Precio_reposicion_COP": precio_repo_cop,
                "P_venta_experto_COP": p_expert,
                "P_piso_inventario_COP": p_piso,
                "P_recomendado_COP": p_rec_final,
                "Regla_precio": regla or None,
                "Estado_cotizacion": estado_cot,
                "Alertas_detalle": alertas_txt or None,
            }
        )
    return pd.DataFrame(rows)


def _consulta_masiva_cotizador_format_map(df: pd.DataFrame) -> dict[str, str]:
    """Formato tras `_renombrar_negocio` en el cotizador.
    Regla UI: todas las columnas numéricas con máximo 2 decimales.
    """
    fmt: dict[str, str] = {}
    for c in df.columns:
        if pd.api.types.is_numeric_dtype(df[c]):
            fmt[str(c)] = "{:,.2f}"
    return fmt


def _cot_margen_slider_to_txt() -> None:
    st.session_state["consulta_masiva_cot_margen_txt"] = str(int(st.session_state["consulta_masiva_cot_margen"]))


def _cot_margen_txt_to_slider() -> None:
    try:
        v = float(str(st.session_state["consulta_masiva_cot_margen_txt"]).replace(",", ".").strip())
        v = min(max(v, 10.0), 50.0)
        st.session_state["consulta_masiva_cot_margen"] = int(round(v))
    except (ValueError, TypeError):
        st.session_state["consulta_masiva_cot_margen_txt"] = str(int(st.session_state["consulta_masiva_cot_margen"]))


def _cot_trm_slider_to_txt() -> None:
    st.session_state["consulta_masiva_cot_trm_txt"] = str(int(st.session_state["consulta_masiva_cot_trm"]))


def _cot_trm_txt_to_slider() -> None:
    try:
        v = float(str(st.session_state["consulta_masiva_cot_trm_txt"]).replace(",", ".").strip())
        v = min(max(v, 3500.0), 6000.0)
        v = round(v / 10.0) * 10.0
        st.session_state["consulta_masiva_cot_trm"] = int(v)
    except (ValueError, TypeError):
        st.session_state["consulta_masiva_cot_trm_txt"] = str(int(st.session_state["consulta_masiva_cot_trm"]))


def _cot_piso_slider_to_txt() -> None:
    st.session_state["consulta_masiva_cot_piso_margen_txt"] = str(int(st.session_state["consulta_masiva_cot_piso_margen"]))


def _cot_piso_txt_to_slider() -> None:
    try:
        v = float(str(st.session_state["consulta_masiva_cot_piso_margen_txt"]).replace(",", ".").strip())
        v = min(max(v, 5.0), 80.0)
        st.session_state["consulta_masiva_cot_piso_margen"] = int(round(v))
    except (ValueError, TypeError):
        st.session_state["consulta_masiva_cot_piso_margen_txt"] = str(int(st.session_state["consulta_masiva_cot_piso_margen"]))


def _cot_umbral_lista_slider_to_txt() -> None:
    st.session_state["consulta_masiva_cot_umbral_lista_repo_txt"] = str(
        int(st.session_state["consulta_masiva_cot_umbral_lista_repo"])
    )


def _cot_umbral_lista_txt_to_slider() -> None:
    try:
        v = float(str(st.session_state["consulta_masiva_cot_umbral_lista_repo_txt"]).replace(",", ".").strip())
        v = min(max(v, 5.0), 90.0)
        st.session_state["consulta_masiva_cot_umbral_lista_repo"] = int(round(v))
    except (ValueError, TypeError):
        st.session_state["consulta_masiva_cot_umbral_lista_repo_txt"] = str(
            int(st.session_state["consulta_masiva_cot_umbral_lista_repo"])
        )


def _cot_umbral_venta_slider_to_txt() -> None:
    st.session_state["consulta_masiva_cot_umbral_venta_repo_txt"] = str(
        int(st.session_state["consulta_masiva_cot_umbral_venta_repo"])
    )


def _cot_umbral_venta_txt_to_slider() -> None:
    try:
        v = float(str(st.session_state["consulta_masiva_cot_umbral_venta_repo_txt"]).replace(",", ".").strip())
        v = min(max(v, 5.0), 90.0)
        st.session_state["consulta_masiva_cot_umbral_venta_repo"] = int(round(v))
    except (ValueError, TypeError):
        st.session_state["consulta_masiva_cot_umbral_venta_repo_txt"] = str(
            int(st.session_state["consulta_masiva_cot_umbral_venta_repo"])
        )


def _cot_umbral_compra_slider_to_txt() -> None:
    st.session_state["consulta_masiva_cot_umbral_compra_repo_txt"] = str(
        int(st.session_state["consulta_masiva_cot_umbral_compra_repo"])
    )


def _cot_umbral_compra_txt_to_slider() -> None:
    try:
        v = float(str(st.session_state["consulta_masiva_cot_umbral_compra_repo_txt"]).replace(",", ".").strip())
        v = min(max(v, 5.0), 90.0)
        st.session_state["consulta_masiva_cot_umbral_compra_repo"] = int(round(v))
    except (ValueError, TypeError):
        st.session_state["consulta_masiva_cot_umbral_compra_repo_txt"] = str(
            int(st.session_state["consulta_masiva_cot_umbral_compra_repo"])
        )


def _cot_inv_justo_slider_to_txt() -> None:
    st.session_state["consulta_masiva_cot_inv_justo_txt"] = str(
        int(st.session_state["consulta_masiva_cot_inv_justo"])
    )


def _cot_inv_justo_txt_to_slider() -> None:
    try:
        v = float(str(st.session_state["consulta_masiva_cot_inv_justo_txt"]).replace(",", ".").strip())
        v = min(max(v, 1.0), 50.0)
        st.session_state["consulta_masiva_cot_inv_justo"] = int(round(v))
    except (ValueError, TypeError):
        st.session_state["consulta_masiva_cot_inv_justo_txt"] = str(
            int(st.session_state["consulta_masiva_cot_inv_justo"])
        )


def _consulta_individual_seed_repo_session_state() -> None:
    """Claves propias de la pestaña individual (evita colisión de widgets con consulta masiva en el mismo rerun)."""
    _defaults: tuple[tuple[str, object], ...] = (
        ("consulta_individual_cot_margen", 25),
        ("consulta_individual_cot_margen_txt", "25"),
        ("consulta_individual_cot_trm", 4200),
        ("consulta_individual_cot_trm_txt", "4200"),
        ("consulta_individual_cot_piso_margen", 40),
        ("consulta_individual_cot_piso_margen_txt", "40"),
        ("consulta_individual_factor_usabr", 1.35),
        ("consulta_individual_factor_usabr_num", 1.35),
        ("consulta_individual_factor_euro", 1.55),
        ("consulta_individual_factor_euro_num", 1.55),
    )
    for k, v in _defaults:
        if k not in st.session_state:
            st.session_state[k] = v


def _cot_ind_margen_slider_to_txt() -> None:
    st.session_state["consulta_individual_cot_margen_txt"] = str(
        int(st.session_state["consulta_individual_cot_margen"])
    )


def _cot_ind_margen_txt_to_slider() -> None:
    try:
        v = float(str(st.session_state["consulta_individual_cot_margen_txt"]).replace(",", ".").strip())
        v = min(max(v, 10.0), 50.0)
        st.session_state["consulta_individual_cot_margen"] = int(round(v))
    except (ValueError, TypeError):
        st.session_state["consulta_individual_cot_margen_txt"] = str(
            int(st.session_state["consulta_individual_cot_margen"])
        )


def _cot_ind_trm_slider_to_txt() -> None:
    st.session_state["consulta_individual_cot_trm_txt"] = str(int(st.session_state["consulta_individual_cot_trm"]))


def _cot_ind_trm_txt_to_slider() -> None:
    try:
        v = float(str(st.session_state["consulta_individual_cot_trm_txt"]).replace(",", ".").strip())
        v = min(max(v, 3500.0), 6000.0)
        v = round(v / 10.0) * 10.0
        st.session_state["consulta_individual_cot_trm"] = int(v)
    except (ValueError, TypeError):
        st.session_state["consulta_individual_cot_trm_txt"] = str(int(st.session_state["consulta_individual_cot_trm"]))


def _cot_ind_piso_slider_to_txt() -> None:
    st.session_state["consulta_individual_cot_piso_margen_txt"] = str(
        int(st.session_state["consulta_individual_cot_piso_margen"])
    )


def _cot_ind_piso_txt_to_slider() -> None:
    try:
        v = float(str(st.session_state["consulta_individual_cot_piso_margen_txt"]).replace(",", ".").strip())
        v = min(max(v, 5.0), 80.0)
        st.session_state["consulta_individual_cot_piso_margen"] = int(round(v))
    except (ValueError, TypeError):
        st.session_state["consulta_individual_cot_piso_margen_txt"] = str(
            int(st.session_state["consulta_individual_cot_piso_margen"])
        )


def _render_consulta_individual_reposicion_sliders() -> None:
    """Sliders de reposición solo en consulta individual (claves distintas a la pestaña en lote)."""
    _consulta_individual_seed_repo_session_state()

    def _sync_usabr_slider_to_num_ci() -> None:
        st.session_state["consulta_individual_factor_usabr_num"] = float(
            st.session_state["consulta_individual_factor_usabr"]
        )

    def _sync_usabr_num_to_slider_ci() -> None:
        st.session_state["consulta_individual_factor_usabr"] = float(
            st.session_state["consulta_individual_factor_usabr_num"]
        )

    def _sync_euro_slider_to_num_ci() -> None:
        st.session_state["consulta_individual_factor_euro_num"] = float(
            st.session_state["consulta_individual_factor_euro"]
        )

    def _sync_euro_num_to_slider_ci() -> None:
        st.session_state["consulta_individual_factor_euro"] = float(
            st.session_state["consulta_individual_factor_euro_num"]
        )

    with st.expander("Sliders y entradas manuales (TRM, márgenes, factores)", expanded=True):
        st.caption(
            "Definen **costo reposición** (USD base × TRM) y **precio reposición** (USD base × TRM ÷ (1 − margen %)). "
            "Son independientes del cotizador en **Consulta en lote** (allí tienes sus propios controles)."
        )
        c_mg, c_trm, c_piso = st.columns(3)
        with c_mg:
            st.caption("**Margen objetivo**")
            st.slider(
                "Margen objetivo sobre venta (%)",
                min_value=10,
                max_value=50,
                step=1,
                key="consulta_individual_cot_margen",
                help="Sobre precio de venta; divisor = (1 − margen/100).",
                on_change=_cot_ind_margen_slider_to_txt,
            )
            st.text_input(
                "Margen objetivo (%) manual",
                key="consulta_individual_cot_margen_txt",
                on_change=_cot_ind_margen_txt_to_slider,
            )
        with c_trm:
            st.caption("**TRM de cálculo**")
            st.slider(
                "TRM (COP por USD)",
                min_value=3500,
                max_value=6000,
                step=10,
                key="consulta_individual_cot_trm",
                on_change=_cot_ind_trm_slider_to_txt,
            )
            st.text_input(
                "TRM manual",
                key="consulta_individual_cot_trm_txt",
                on_change=_cot_ind_trm_txt_to_slider,
            )
        with c_piso:
            st.caption("**Piso por inventario**")
            st.slider(
                "Margen piso inventario X (%)",
                min_value=5,
                max_value=80,
                step=1,
                key="consulta_individual_cot_piso_margen",
                help="Usado en el cálculo interno de cotización (P_piso = Costo_Min / (1 − X/100)).",
                on_change=_cot_ind_piso_slider_to_txt,
            )
            st.text_input(
                "Margen piso inventario X (%) manual",
                key="consulta_individual_cot_piso_margen_txt",
                on_change=_cot_ind_piso_txt_to_slider,
            )

        c_f1, c_f2 = st.columns(2)
        with c_f1:
            st.caption("**Factor importación USA/BR**")
            st.slider(
                "Factor importación USA/BR",
                min_value=1.2,
                max_value=1.5,
                step=0.01,
                key="consulta_individual_factor_usabr",
                on_change=_sync_usabr_slider_to_num_ci,
                help="Multiplica Precio Brasil y Precio USA para elegir mejor origen y USD base.",
            )
            st.number_input(
                "Valor (USA/BR)",
                min_value=1.2,
                max_value=1.5,
                step=0.01,
                key="consulta_individual_factor_usabr_num",
                on_change=_sync_usabr_num_to_slider_ci,
            )
        with c_f2:
            st.caption("**Factor importación EURO**")
            st.slider(
                "Factor importación EURO",
                min_value=1.4,
                max_value=1.7,
                step=0.01,
                key="consulta_individual_factor_euro",
                on_change=_sync_euro_slider_to_num_ci,
                help="Multiplica Precio Europa.",
            )
            st.number_input(
                "Valor (EURO)",
                min_value=1.4,
                max_value=1.7,
                step=0.01,
                key="consulta_individual_factor_euro_num",
                on_change=_sync_euro_num_to_slider_ci,
            )


def _render_tab_consulta_masiva() -> None:
    st.markdown('<p class="consulta-page-lead">Consulta en lote (CSV) + consulta rápida</p>', unsafe_allow_html=True)
    st.caption("Carga un CSV, consulta 1 referencia o analiza todo el catálogo.")

    analizar_todas = st.toggle(
        "Analizar todas las referencias (catálogo completo)",
        value=False,
        key="consulta_masiva_all_refs",
        help="Usa todas las referencias disponibles en el dataset de márgenes para análisis masivo.",
    )
    limite_all_refs = 0
    if analizar_todas:
        limite_all_refs = int(
            st.number_input(
                "Límite de referencias para procesar (0 = todas)",
                min_value=0,
                max_value=500_000,
                step=1_000,
                value=0,
                key="consulta_masiva_all_refs_limit",
            )
        )

    c_ref, c_btn = st.columns([3.0, 1.0], gap="small", vertical_alignment="bottom")
    with c_ref:
        ref_rapida = st.text_input(
            "Consulta rápida (1 referencia)",
            placeholder="Referencia principal, normalizada o alterna",
            key="consulta_masiva_ref_rapida",
        )
    with c_btn:
        # Botón alineado y más compacto.
        run_rapida = st.button(
            "Consultar",
            key="consulta_masiva_ref_rapida_run",
            type="primary",
            use_container_width=True,
        )

    uploaded = st.file_uploader(
        "Archivo CSV",
        type=["csv"],
        key="consulta_masiva_csv",
        help="El archivo debe contener al menos una columna con códigos de referencia.",
    )
    # Persistir modo rápido: si Streamlit rerunea (p.ej. al dar "Procesar"),
    # el click del botón se pierde; guardamos la referencia en session_state.
    if run_rapida and str(ref_rapida or "").strip():
        st.session_state["consulta_masiva_modo_rapido"] = True
        st.session_state["consulta_masiva_modo_rapido_ref"] = str(ref_rapida).strip()
    if uploaded is not None:
        st.session_state["consulta_masiva_modo_rapido"] = False

    modo_rapido = bool(st.session_state.get("consulta_masiva_modo_rapido", False))
    ref_rapida_persist = str(st.session_state.get("consulta_masiva_modo_rapido_ref", "") or "").strip()

    if not analizar_todas and uploaded is None and not (modo_rapido and ref_rapida_persist):
        st.info("Sube un CSV o usa la consulta rápida.")
        return

    if analizar_todas:
        refs_unicas = _cargar_referencias_catalogo_masivo_cached()
        if limite_all_refs > 0:
            refs_unicas = refs_unicas[:limite_all_refs]
        if not refs_unicas:
            st.warning("No se encontraron referencias en el catálogo para procesar.")
            return
        col_ref = "Catálogo completo"
        raw = f"ALL_REFS:{len(refs_unicas)}:{limite_all_refs}".encode("utf-8")
        st.caption(f"Modo catálogo completo: {len(refs_unicas):,} referencias a procesar.")
    elif modo_rapido and ref_rapida_persist:
        refs_unicas = [ref_rapida_persist]
        col_ref = "Consulta rápida"
        raw = ref_rapida_persist.encode("utf-8", errors="ignore")
        st.caption("Modo consulta rápida: 1 referencia.")
    else:
        raw = uploaded.getvalue()
        if not raw:
            st.warning("El archivo está vacío.")
            return

        try:
            text_csv = raw.decode("utf-8-sig")
        except UnicodeDecodeError:
            text_csv = raw.decode("latin-1")

    if not analizar_todas and not (modo_rapido and ref_rapida_persist):
        # Referencias se manejan siempre como texto para evitar conversiones tipo 12345 -> 12345.0
        # Evitamos `sep=None` porque en archivos de 1 sola columna puede inferir separadores erróneos.
        non_empty_lines = [ln.strip() for ln in text_csv.splitlines() if ln.strip()]
        if not non_empty_lines:
            st.warning("El CSV no contiene datos.")
            return

        sample = non_empty_lines[0]
        has_common_delim = any(d in sample for d in [",", ";", "\t", "|"])
        try:
            if has_common_delim:
                df_csv = pd.read_csv(
                    io.StringIO(text_csv),
                    sep=None,
                    engine="python",
                    dtype=str,
                    keep_default_na=False,
                )
            else:
                # CSV de una sola columna y sin encabezado.
                df_csv = pd.DataFrame({"Referencia": non_empty_lines})
        except Exception as exc:
            st.error(f"No fue posible leer el CSV: {exc}")
            return

        if df_csv.empty or len(df_csv.columns) == 0:
            st.warning("El CSV no contiene datos.")
            return

        cols = [str(c) for c in df_csv.columns]
        default_idx = 0
        for i, c in enumerate(cols):
            if str(c).strip().lower() in {"referencia", "referencias", "ref", "codigo", "código"}:
                default_idx = i
                break
        col_ref = st.selectbox(
            "Columna con referencias",
            options=cols,
            index=default_idx,
            key="consulta_masiva_col_ref",
        )

        serie = df_csv[col_ref].astype(str).str.strip()

        def _normalizar_ref_csv(val: str) -> str:
            s = normalize_reference_text(val)
            if not s:
                return ""
            # Limpia ruido común de Excel cuando un código texto se guarda/carga como float.
            if re.fullmatch(r"\d+\.0+", s):
                return s.split(".", 1)[0]
            return s

        refs = [_normalizar_ref_csv(x) for x in serie.tolist()]
        refs = [x for x in refs if x]
        if not refs:
            st.warning("No se encontraron referencias válidas en la columna seleccionada.")
            return

        refs_unicas = list(dict.fromkeys(refs))
        st.caption(f"Referencias detectadas: {len(refs_unicas):,}")

    c1, c2, c3 = st.columns(3, gap="small")
    with c1:
        if "consulta_masiva_disp_umbral_num" not in st.session_state:
            st.session_state["consulta_masiva_disp_umbral_num"] = float(
                st.session_state.get("consulta_masiva_disp_umbral", 0.0)
            )

        def _sync_umbral_slider_to_num() -> None:
            st.session_state["consulta_masiva_disp_umbral_num"] = float(
                st.session_state["consulta_masiva_disp_umbral"]
            )

        def _sync_umbral_num_to_slider() -> None:
            st.session_state["consulta_masiva_disp_umbral"] = float(
                st.session_state["consulta_masiva_disp_umbral_num"]
            )

        disp_umbral_masivo = st.slider(
            "Umbral de disponibilidad (mejor precio)",
            min_value=0.0,
            max_value=5000.0,
            value=float(st.session_state.get("consulta_masiva_disp_umbral", 0.0)),
            step=0.5,
            key="consulta_masiva_disp_umbral",
            on_change=_sync_umbral_slider_to_num,
            help="Solo compite un origen si su disponibilidad es estrictamente mayor al umbral.",
        )
        disp_umbral_masivo = st.number_input(
            "Valor (umbral)",
            min_value=0.0,
            max_value=5000.0,
            step=0.5,
            key="consulta_masiva_disp_umbral_num",
            on_change=_sync_umbral_num_to_slider,
        )

    with c2:
        if "consulta_masiva_factor_usabr_num" not in st.session_state:
            st.session_state["consulta_masiva_factor_usabr_num"] = float(
                st.session_state.get("consulta_masiva_factor_usabr", 1.35)
            )

        def _sync_usabr_slider_to_num() -> None:
            st.session_state["consulta_masiva_factor_usabr_num"] = float(
                st.session_state["consulta_masiva_factor_usabr"]
            )

        def _sync_usabr_num_to_slider() -> None:
            st.session_state["consulta_masiva_factor_usabr"] = float(
                st.session_state["consulta_masiva_factor_usabr_num"]
            )

        factor_usabr = st.slider(
            "Factor importación USA/BR",
            min_value=1.2,
            max_value=1.5,
            value=float(st.session_state.get("consulta_masiva_factor_usabr", 1.35)),
            step=0.01,
            key="consulta_masiva_factor_usabr",
            on_change=_sync_usabr_slider_to_num,
            help="Multiplica Precio Brasil y Precio USA para comparar mejor origen.",
        )
        factor_usabr = st.number_input(
            "Valor (USA/BR)",
            min_value=1.2,
            max_value=1.5,
            step=0.01,
            key="consulta_masiva_factor_usabr_num",
            on_change=_sync_usabr_num_to_slider,
        )

    with c3:
        if "consulta_masiva_factor_euro_num" not in st.session_state:
            st.session_state["consulta_masiva_factor_euro_num"] = float(
                st.session_state.get("consulta_masiva_factor_euro", 1.55)
            )

        def _sync_euro_slider_to_num() -> None:
            st.session_state["consulta_masiva_factor_euro_num"] = float(
                st.session_state["consulta_masiva_factor_euro"]
            )

        def _sync_euro_num_to_slider() -> None:
            st.session_state["consulta_masiva_factor_euro"] = float(
                st.session_state["consulta_masiva_factor_euro_num"]
            )

        factor_euro = st.slider(
            "Factor importación EURO",
            min_value=1.4,
            max_value=1.7,
            value=float(st.session_state.get("consulta_masiva_factor_euro", 1.55)),
            step=0.01,
            key="consulta_masiva_factor_euro",
            on_change=_sync_euro_slider_to_num,
            help="Multiplica Precio Europa para comparar mejor origen.",
        )
        factor_euro = st.number_input(
            "Valor (EURO)",
            min_value=1.4,
            max_value=1.7,
            step=0.01,
            key="consulta_masiva_factor_euro_num",
            on_change=_sync_euro_num_to_slider,
        )

    # Firma estable: mismo archivo + columna + lista de refs. Así, al togglear/slider sin reconsultar
    # se reutiliza el resultado SQL y solo se recalcula mejor origen con los factores actuales.
    _consulta_masiva_sig = (
        tuple(refs_unicas),
        col_ref,
        hashlib.md5(raw).hexdigest(),
    )

    run_masiva = st.button("Procesar consulta masiva", key="consulta_masiva_run", type="primary")

    if run_masiva:
        try:
            resolver_masivo = getattr(data_access_service, "obtener_resumen_referencias_masivo", None)
            if analizar_todas:
                # Progreso por bloques para catálogos grandes.
                chunk_size = 2_000
                total_refs = len(refs_unicas)
                total_chunks = max(1, (total_refs + chunk_size - 1) // chunk_size)
                pbar = st.progress(0.0, text=f"Explorando referencias... 0/{total_refs:,}")
                estado = st.empty()
                frames: list[pd.DataFrame] = []
                for i in range(total_chunks):
                    a = i * chunk_size
                    b = min(total_refs, (i + 1) * chunk_size)
                    lote = refs_unicas[a:b]
                    if resolver_masivo is None:
                        df_lote = _consulta_masiva_fallback(lote)
                    else:
                        df_lote = resolver_masivo(lote)
                    if isinstance(df_lote, pd.DataFrame) and not df_lote.empty:
                        frames.append(df_lote)
                    avance = float(b) / float(max(1, total_refs))
                    pbar.progress(avance, text=f"Explorando referencias... {b:,}/{total_refs:,}")
                    estado.caption(f"Lote {i+1}/{total_chunks} procesado ({b-a:,} refs).")
                pbar.empty()
                estado.empty()
                if not frames:
                    df_sql = pd.DataFrame()
                else:
                    df_sql = pd.concat(frames, ignore_index=True)
                    if "Referencia_Entrada" in df_sql.columns:
                        df_sql = df_sql.drop_duplicates(subset=["Referencia_Entrada"], keep="first")
                if resolver_masivo is None:
                    st.info("Se usó modo compatibilidad por lotes (sin método masivo en data_access).")
            else:
                if resolver_masivo is None:
                    df_sql = _consulta_masiva_fallback(refs_unicas)
                    st.info(
                        "Se usó modo compatibilidad para consulta masiva (sin método masivo en data_access)."
                    )
                else:
                    df_sql = resolver_masivo(refs_unicas)
        except Exception as exc:
            st.error(f"No fue posible ejecutar la consulta masiva: {exc}")
            return

        if df_sql.empty:
            st.warning("No se obtuvieron resultados.")
            return

        st.session_state["consulta_masiva_df_sql"] = df_sql
        st.session_state["consulta_masiva_cache_sig"] = _consulta_masiva_sig

    df_sql = st.session_state.get("consulta_masiva_df_sql")
    if st.session_state.get("consulta_masiva_cache_sig") != _consulta_masiva_sig:
        df_sql = None

    if df_sql is None:
        return

    df_out = _consulta_masiva_calcular_mejor_origen(
        df_sql.copy(),
        disp_umbral=float(disp_umbral_masivo),
        factor_usabr=float(factor_usabr),
        factor_euro=float(factor_euro),
    )
    df_out = _consulta_masiva_ajustar_decimales(df_out)
    if "Mejor_Origen" in df_out.columns and "Mejor_Precio_Sin_Factor" in df_out.columns:
        cols = [c for c in df_out.columns if c != "Mejor_Precio_Sin_Factor"]
        idx = cols.index("Mejor_Origen") + 1
        cols = cols[:idx] + ["Mejor_Precio_Sin_Factor"] + cols[idx:]
        df_out = df_out[cols]
    if "Precio Prorrateo" in df_out.columns:
        orden_cols = [c for c in df_out.columns if c != "Precio Prorrateo"] + ["Precio Prorrateo"]
        df_out = df_out[orden_cols]

    if "Referencia_Cruce" not in df_out.columns:
        df_out["Referencia_Cruce"] = None

    n_ok = int((df_out["Estado"] == "OK").sum()) if "Estado" in df_out.columns else 0
    n_no = int((df_out["Estado"] != "OK").sum()) if "Estado" in df_out.columns else 0
    m1, m2, m3 = st.columns(3)
    m1.metric("Total referencias", f"{len(df_out):,}")
    m2.metric("Con coincidencia", f"{n_ok:,}")
    m3.metric("Sin coincidencia", f"{n_no:,}")

    df_vista = _consulta_masiva_preparar_vista(df_out)
    main_cols = [c for c in df_vista.columns if c not in _CONSULTA_MASIVA_COLS_EXTRA]
    extra_ordered = [c for c in _CONSULTA_MASIVA_COLS_EXTRA_ORDER if c in df_vista.columns]

    ver_panel = st.toggle(
        "➕ Todas las columnas (inventario + auditoría)",
        value=False,
        key="consulta_masiva_panel_extra",
        help="Una sola tabla: añade inventario, lista 09, tipo origen, venta y última compra (auditoría SQL 003), "
        "ordenadas al final (bloque inventario/venta y luego auditoría).",
    )

    if ver_panel:
        df_src = df_vista[main_cols + extra_ordered] if extra_ordered else df_vista
        df_show = _consulta_masiva_encabezados_id(
            _consulta_masiva_etiquetas_audit(
                _consulta_masiva_etiquetas_lista(_renombrar_negocio(df_src))
            )
        )
        st.caption(
            "**Lista (OC):** fecha / prov. / USD — importación OC. **Inventario y venta** a continuación del bloque "
            "principal. **Aud. (SQL 003):** última compra contable (no es el mismo documento que la lista OC)."
        )
    else:
        df_src = df_vista[main_cols] if main_cols else df_vista
        df_show = _consulta_masiva_encabezados_id(
            _consulta_masiva_etiquetas_lista(_renombrar_negocio(df_src))
        )
        if extra_ordered and main_cols:
            st.caption(
                "Activa **➕ Todas las columnas** para incluir inventario, lista 09, tipo origen, venta y "
                "auditoría en la misma tabla (orden lógico)."
            )

    @_streamlit_fragment_optional()
    def _consulta_masiva_cols_y_tabla(df_show_full: pd.DataFrame) -> None:
        personalizar_cols = st.toggle(
            "🎛️ Seleccionar columnas de consulta masiva",
            value=False,
            key="consulta_masiva_cols_custom_toggle",
            help="Elige exactamente qué columnas mostrar en la tabla principal de consulta masiva. "
            "Con Streamlit ≥1.33, solo este bloque se redibuja al cambiar la selección.",
        )
        df_tab = df_show_full
        if personalizar_cols and not df_show_full.empty:
            cols_default = list(df_show_full.columns)
            cols_est = _consulta_masiva_columnas_estrategicas(cols_default)
            r_es, r_ms = st.columns([1, 2], gap="small")
            with r_es:
                if st.button(
                    "↺ Solo columnas estratégicas",
                    key="consulta_masiva_reset_strategic_cols",
                    help="Restaura un conjunto corto: identificación, estado, mejores orígenes/precios y señales clave de lista/auditoría.",
                ):
                    st.session_state["consulta_masiva_cols_custom_sel"] = list(cols_est)
            with r_ms:
                st.caption(
                    "Vista compacta al reset; añade o quita columnas en la lista siguiente."
                )
            cols_sel = st.multiselect(
                "Columnas visibles (consulta masiva)",
                options=cols_default,
                default=cols_default,
                key="consulta_masiva_cols_custom_sel",
            )
            if cols_sel:
                df_tab = df_show_full[cols_sel]
            else:
                st.warning("Debes seleccionar al menos una columna para mostrar la tabla.")
                return

        fmt_map = _consulta_masiva_build_format_map(df_tab)
        st.dataframe(
            df_tab.style.format(fmt_map).apply(_consulta_masiva_style_mejor_origen, axis=1),
            width="stretch",
            hide_index=True,
        )

    _consulta_masiva_cols_y_tabla(df_show)

    st.markdown("---")
    st.markdown(
        """
<div style="border:1px solid #25314d;border-radius:12px;padding:10px 12px;background:#0f1a30;margin-bottom:8px;">
  <div style="font-weight:700;color:#e5e7eb;">Cotizador automático (oferta al negocio)</div>
  <div style="color:#9fb0cc;font-size:.9rem;">Calcula precio recomendado y alerta de riesgo con parámetros comerciales.</div>
</div>
""",
        unsafe_allow_html=True,
    )
    with st.container(border=True):
        with st.expander("Metodología de precio", expanded=False):
            st.markdown(
                """
**Base en USD (importación)**  
Se usa el **Mejor precio ajustado (USD)** de la tabla: menor precio entre orígenes Brasil / USA / Europa,
tras los **factores de importación** (sliders de esta pantalla) y el umbral de disponibilidad.  
Si no hay origen válido, respaldo: **Último valor USD** de la lista de precios (última compra OC).
                """.strip()
            )
            st.divider()
            st.markdown("**Precio venta experto (COP)**")
            st.latex(
                r"P_{\mathrm{experto}} = P_{\mathrm{USD}} \times \frac{\mathrm{TRM}}{1 - m}"
            )
            st.caption(
                "Donde *m* es el margen objetivo **sobre el precio de venta** "
                "(ej. 25 % → el divisor es 1 − 0,25 = 0,75)."
            )
            st.divider()
            st.markdown("**Piso por inventario (COP)**")
            st.latex(r"P_{\mathrm{piso}} = \frac{C_{\min}}{1 - X}")
            st.caption(
                "**C_min** = costo mínimo de inventario (misma columna **Costo_Min** que en consulta masiva). "
                "**X** = margen piso inventario (slider de esta pantalla)."
            )
            st.divider()
            st.markdown("**Precio recomendado al negocio**")
            st.latex(
                r"P_{\mathrm{rec}} = \max\left(P_{\mathrm{experto}},\; P_{\mathrm{piso}}\right)"
            )
            st.markdown(
                """
Cuando existen ambos términos. En **Alertas**, **lista 09** y **última venta** se comparan con el **precio reposición** (USD base × TRM ÷ (1 − margen cot.), igual que *P experto*). **Última compra (COP)** se compara con **USD base × TRM** (importación en COP sin margen de venta).
                """.strip()
            )
            st.divider()
            st.markdown(
                """
**Alertas y “precio no calculable automáticamente”**  
Se señala si: existencia total en **(0, N]** unidades (**N** = slider «inventario justo»); dispersión alta entre **Brasil y USA** en USD ajustado (Europa solo cuenta en esa dispersión si **Mejor_Origen** es Europa); **costo mín. vs costo máx.** inventario muy distintos; **lista 09** o **últ. venta** muy lejos del **precio reposición** (USD×TRM÷(1−margen)); **últ. compra (COP)** muy lejos de **USD base × TRM** (umbrales % en sliders). El **costo mín.** alimenta la cotización (piso y contexto), sin comparar contra costo prom.  

Si el **score de riesgo** es alto o **no hay ni USD base ni costo mín.**, el estado pasa a **“Precio no calculable automáticamente”** y se **anula** el precio recomendado (revisión manual obligatoria).
                """.strip()
            )

    # Slider ↔ caja: mismo valor en session_state (callbacks on_change)
    _cot_defaults: tuple[tuple[str, int | str], ...] = (
        ("consulta_masiva_cot_margen", 25),
        ("consulta_masiva_cot_margen_txt", "25"),
        ("consulta_masiva_cot_trm", 4200),
        ("consulta_masiva_cot_trm_txt", "4200"),
        ("consulta_masiva_cot_piso_margen", 40),
        ("consulta_masiva_cot_piso_margen_txt", "40"),
        ("consulta_masiva_cot_umbral_lista_repo", 35),
        ("consulta_masiva_cot_umbral_venta_repo", 40),
        ("consulta_masiva_cot_umbral_compra_repo", 40),
        ("consulta_masiva_cot_inv_justo", 3),
    )
    for _k, _v in _cot_defaults:
        if _k not in st.session_state:
            st.session_state[_k] = _v
    if "consulta_masiva_cot_umbral_lista_repo_txt" not in st.session_state:
        st.session_state["consulta_masiva_cot_umbral_lista_repo_txt"] = str(
            int(st.session_state.get("consulta_masiva_cot_umbral_lista_repo", 35))
        )
    if "consulta_masiva_cot_umbral_venta_repo_txt" not in st.session_state:
        st.session_state["consulta_masiva_cot_umbral_venta_repo_txt"] = str(
            int(st.session_state.get("consulta_masiva_cot_umbral_venta_repo", 40))
        )
    if "consulta_masiva_cot_umbral_compra_repo_txt" not in st.session_state:
        st.session_state["consulta_masiva_cot_umbral_compra_repo_txt"] = str(
            int(st.session_state.get("consulta_masiva_cot_umbral_compra_repo", 40))
        )
    if "consulta_masiva_cot_inv_justo_txt" not in st.session_state:
        st.session_state["consulta_masiva_cot_inv_justo_txt"] = str(
            int(st.session_state.get("consulta_masiva_cot_inv_justo", 3))
        )

    c_mg, c_trm, c_piso = st.columns(3)
    with c_mg:
        st.caption("**Margen objetivo**")
        st.slider(
            "Margen objetivo sobre venta (%)",
            min_value=10,
            max_value=50,
            value=25,
            step=1,
            key="consulta_masiva_cot_margen",
            help="Porcentaje del precio de venta; el divisor de la fórmula es (1 − margen/100).",
            on_change=_cot_margen_slider_to_txt,
        )
        st.text_input(
            "Margen objetivo (%) manual",
            key="consulta_masiva_cot_margen_txt",
            on_change=_cot_margen_txt_to_slider,
        )
    with c_trm:
        st.caption("**TRM de cálculo**")
        st.slider(
            "TRM (COP por USD)",
            min_value=3500,
            max_value=6000,
            value=4200,
            step=10,
            key="consulta_masiva_cot_trm",
            on_change=_cot_trm_slider_to_txt,
        )
        st.text_input(
            "TRM manual",
            key="consulta_masiva_cot_trm_txt",
            on_change=_cot_trm_txt_to_slider,
        )
    with c_piso:
        st.caption("**Piso por inventario**")
        st.slider(
            "Margen piso inventario X (%)",
            min_value=5,
            max_value=80,
            value=40,
            step=1,
            key="consulta_masiva_cot_piso_margen",
            help="Se usa en P_piso = Costo_Min / (1 - X/100).",
            on_change=_cot_piso_slider_to_txt,
        )
        st.text_input(
            "Margen piso inventario X (%) manual",
            key="consulta_masiva_cot_piso_margen_txt",
            on_change=_cot_piso_txt_to_slider,
        )

    st.caption(
        "**Alertas:** **lista 09** y **últ. venta** vs **precio reposición** (USD base × TRM ÷ (1 − margen cot.)); "
        "**últ. compra (COP)** vs **USD base × TRM** (sin margen). El USD base ya lleva factor por origen o última compra lista."
    )
    c_ul, c_uv, c_uc = st.columns(3)
    _h_repo = (
        "Referencia **P_repo** = USD base (cotizador) × TRM de la pantalla. "
        "El USD base es **Mejor precio ajustado** o **Último USD lista × factor país**. "
        "No usa el margen m ni el piso X."
    )
    _h_lista_precio_repo = (
        "**Precio reposición** = USD base × TRM ÷ (1 − margen % cot.), misma base que *P. venta experto* cuando hay USD base. "
        "Brecha |lista 09 − precio reposición| ÷ max(lista, precio reposición). Superar el umbral suma +2 al score."
    )
    _h_venta_precio_repo = (
        "**Precio reposición** = USD base × TRM ÷ (1 − margen % cot.), igual que *P. venta experto*. "
        "Brecha |últ. venta − precio reposición| ÷ max(últ. venta, precio reposición). +1 al score si supera el umbral."
    )
    with c_ul:
        st.slider(
            "Umbral: lista 09 vs precio reposición (%)",
            min_value=5,
            max_value=90,
            value=35,
            step=1,
            key="consulta_masiva_cot_umbral_lista_repo",
            help=_h_lista_precio_repo,
            on_change=_cot_umbral_lista_slider_to_txt,
        )
        st.text_input(
            "Umbral lista vs precio reposición (%) manual",
            key="consulta_masiva_cot_umbral_lista_repo_txt",
            on_change=_cot_umbral_lista_txt_to_slider,
            help="Rango 5–90 %. Acepta coma o punto decimal.",
        )
    with c_uv:
        st.slider(
            "Umbral: últ. venta vs precio reposición (%)",
            min_value=5,
            max_value=90,
            value=40,
            step=1,
            key="consulta_masiva_cot_umbral_venta_repo",
            help=_h_venta_precio_repo,
            on_change=_cot_umbral_venta_slider_to_txt,
        )
        st.text_input(
            "Umbral venta vs precio reposición (%) manual",
            key="consulta_masiva_cot_umbral_venta_repo_txt",
            on_change=_cot_umbral_venta_txt_to_slider,
            help="Rango 5–90 %. Acepta coma o punto decimal.",
        )
    with c_uc:
        st.slider(
            "Umbral: últ. compra (COP) vs USD base×TRM (%)",
            min_value=5,
            max_value=90,
            value=40,
            step=1,
            key="consulta_masiva_cot_umbral_compra_repo",
            help=_h_repo
            + " Usa **Precio_COP_Ultima** (auditoría / cruce masivo). +1 al score si supera el umbral.",
            on_change=_cot_umbral_compra_slider_to_txt,
        )
        st.text_input(
            "Umbral compra vs P_repo (%) manual",
            key="consulta_masiva_cot_umbral_compra_repo_txt",
            on_change=_cot_umbral_compra_txt_to_slider,
            help="Rango 5–90 %. Acepta coma o punto decimal.",
        )

    st.caption("**Inventario justo (alerta de stock bajo)**")
    c_inv, _ = st.columns([1, 2])
    with c_inv:
        st.slider(
            "Tope unidades «inventario muy justo»",
            min_value=1,
            max_value=50,
            value=3,
            step=1,
            key="consulta_masiva_cot_inv_justo",
            help=(
                "Si **Existencia_Total** está entre 0 y este número (excl. 0), suma +1 al score y muestra alerta. "
                "No afecta la fórmula del precio recomendado."
            ),
            on_change=_cot_inv_justo_slider_to_txt,
        )
        st.text_input(
            "Tope inventario justo (uds.) manual",
            key="consulta_masiva_cot_inv_justo_txt",
            on_change=_cot_inv_justo_txt_to_slider,
            help="Entero 1–50.",
        )

    margen_cot = float(st.session_state["consulta_masiva_cot_margen"])
    trm_cot = float(st.session_state["consulta_masiva_cot_trm"])
    piso_margen = float(st.session_state["consulta_masiva_cot_piso_margen"])

    df_cot = _consulta_masiva_cotizador_df(
        df_out,
        float(margen_cot),
        float(trm_cot),
        piso_margin_pct=float(piso_margen),
        factor_usabr=float(factor_usabr),
        factor_euro=float(factor_euro),
        factor_otros=float(factor_euro),
        disp_umbral=float(disp_umbral_masivo),
        pct_umbral_lista_vs_repo=float(st.session_state["consulta_masiva_cot_umbral_lista_repo"]) / 100.0,
        pct_umbral_venta_vs_repo=float(st.session_state["consulta_masiva_cot_umbral_venta_repo"]) / 100.0,
        pct_umbral_compra_vs_repo=float(st.session_state["consulta_masiva_cot_umbral_compra_repo"]) / 100.0,
        umbral_existencia_justa=float(st.session_state["consulta_masiva_cot_inv_justo"]),
    )
    if "Estado_cotizacion" in df_cot.columns:
        _estado = df_cot["Estado_cotizacion"].astype(str)
        k_ok, k_rev, k_bloq = st.columns(3, gap="small")
        k_ok.metric("OK / observaciones", f"{int((_estado == 'OK').sum() + (_estado == 'OK (con observaciones)').sum()):,}")
        k_rev.metric("Revisar manual", f"{int((_estado == 'Revisar manual').sum()):,}")
        k_bloq.metric("No calculable", f"{int((_estado == 'Precio no calculable automáticamente').sum()):,}")
    with st.container(border=True):
        st.markdown("##### Filtro rápido de riesgo")
        r1, r2, r3 = st.columns([1.3, 1.0, 1.9], gap="medium")
        with r1:
            riesgo_x = float(
                st.number_input(
                    "X existencias (riesgo)",
                    min_value=0.0,
                    max_value=50_000.0,
                    value=float(st.session_state.get("consulta_masiva_riesgo_x", 2.0)),
                    step=0.5,
                    key="consulta_masiva_riesgo_x",
                    help="Umbral de inventario para riesgo. Se filtra cuando `Existencia_Total <= X` junto con estado crítico de cotización.",
                )
            )
        with r2:
            st.markdown("<div style='height:1.35rem'></div>", unsafe_allow_html=True)
            only_risk = st.toggle(
                "Mostrar solo riesgo",
                value=bool(st.session_state.get("consulta_masiva_only_risk", False)),
                key="consulta_masiva_only_risk",
                help="Activa para mostrar solo referencias en riesgo según existencia y estado de cotización.",
            )
        with r3:
            estado_riesgo_ui = "ACTIVO" if bool(st.session_state.get("consulta_masiva_only_risk", False)) else "INACTIVO"
            st.markdown("<div style='height:1.25rem'></div>", unsafe_allow_html=True)
            color_estado = "#22c55e" if estado_riesgo_ui == "ACTIVO" else "#94a3b8"
            st.markdown(
                f"<div style='line-height:1.35;'>"
                f"<span style='font-weight:700;color:{color_estado};'>{estado_riesgo_ui}</span>"
                f"<span style='color:#9fb0cc;'> · condición: </span>"
                f"<code style='color:#86efac;'>Existencia_Total &lt;= {riesgo_x:g}</code>"
                f"<span style='color:#9fb0cc;'> y </span>"
                f"<code style='color:#86efac;'>Estado_cotizacion en [Revisar manual, Precio no calculable automáticamente]</code>"
                f"</div>",
                unsafe_allow_html=True,
            )
    if only_risk:
        if "Existencia_Total" in df_cot.columns and "Estado_cotizacion" in df_cot.columns:
            estados_riesgo = {"Revisar manual", "Precio no calculable automáticamente"}
            ex = pd.to_numeric(df_cot["Existencia_Total"], errors="coerce")
            st_cot = df_cot["Estado_cotizacion"].astype(str).str.strip()
            mask_riesgo = (ex <= float(riesgo_x)) & st_cot.isin(estados_riesgo)
            df_cot = df_cot[mask_riesgo].copy()
            st.caption(f"Referencias en riesgo encontradas: {len(df_cot):,}")
        else:
            st.warning("No se pudo aplicar filtro de riesgo: faltan columnas requeridas.")

    df_cot_base = df_cot.copy()

    @_streamlit_fragment_optional()
    def _consulta_masiva_cotizador_vista_y_descargas() -> None:
        # Vista ON/OFF del cotizador + columnas extra desde consulta masiva (solo este bloque se redibuja al cambiar toggles/multiselect).
        base_keys = ("Referencia_Entrada", "Referencia_Cruce")
        cols_base_disponibles = [
            c for c in df_out.columns if c not in base_keys and c not in df_cot_base.columns
        ]
        cols_base_default = [
            c
            for c in (
                "Tipo_Coincidencia",
                "Mejor_Origen",
                "Mejor_Disponibilidad",
                "Precio Brasil",
                "Precio Usa",
                "Precio Europa",
                "Precio Prorrateo",
            )
            if c in cols_base_disponibles
        ]
        traer_datos_extra = st.toggle(
            "➕ Vista analítica del cotizador (más campos)",
            value=False,
            key="consulta_masiva_cot_extra_toggle",
            help="OFF: solo columnas núcleo del cotizador. ON: añade el bloque de diagnóstico (lista 09, últ. venta, alertas, TRM/margen usados…; las alertas usan también últ. compra COP si existe en el cruce) y permite traer columnas extra de la tabla de consulta masiva.",
        )
        cols_extra_sel: list[str] = []
        if traer_datos_extra and cols_base_disponibles:
            key_cols_extra = "consulta_masiva_cot_extra_cols"
            opt_set = set(cols_base_disponibles)
            _def = [c for c in cols_base_default if c in opt_set]
            if key_cols_extra not in st.session_state:
                st.session_state[key_cols_extra] = list(_def)
            else:
                prev = st.session_state[key_cols_extra]
                if not isinstance(prev, list):
                    st.session_state[key_cols_extra] = list(_def)
                else:
                    cleaned = [c for c in prev if c in opt_set]
                    # Dataset cambió y la selección guardada ya no es válida → default estratégico
                    if not cleaned and prev:
                        st.session_state[key_cols_extra] = list(_def)
                    else:
                        st.session_state[key_cols_extra] = cleaned

            c_solo, c_todas = st.columns(2, gap="small")
            with c_solo:
                if st.button(
                    "↺ Solo columnas estratégicas",
                    key="consulta_masiva_cot_reset_strategic",
                    help="Deja solo el subconjunto recomendado (coincidencia, orígenes y precios clave).",
                ):
                    st.session_state[key_cols_extra] = list(_def)
            with c_todas:
                if st.button(
                    "⇢ Todas las columnas de consulta",
                    key="consulta_masiva_cot_select_all_cols",
                    help="Añade al cotizador todas las columnas disponibles de la consulta masiva (además del núcleo y del bloque analítico).",
                ):
                    st.session_state[key_cols_extra] = list(cols_base_disponibles)

            cols_extra_sel = st.multiselect(
                "Columnas adicionales desde la consulta masiva",
                options=cols_base_disponibles,
                default=_def,
                key=key_cols_extra,
                help="Una sola fuente de verdad: lo que marques aquí se fusiona a la tabla del cotizador. "
                "Usa los dos botones de arriba para atajos sin desincronizar el selector.",
            )
        extra_cols_added: list[str] = []
        df_cot = df_cot_base.copy()
        if cols_extra_sel:
            extras = df_out[list(base_keys) + cols_extra_sel].copy()
            # Evita colisiones por columnas repetidas en el merge.
            rename_map = {c: f"_extra_{c}" for c in cols_extra_sel if c in df_cot.columns}
            extras = extras.rename(columns=rename_map)
            extra_cols_added = [rename_map.get(c, c) for c in cols_extra_sel]
            df_cot = df_cot.merge(extras, on=list(base_keys), how="left")

        # Orden profesional: identificación -> decisión -> fundamentos -> control.
        cot_core_pref = [
            "Referencia_Entrada",
            "Referencia_Cruce",
            "Mejor_Origen",
            "Estado_cotizacion",
            "P_recomendado_COP",
            "P_venta_experto_COP",
            "P_piso_inventario_COP",
            "USD_base",
            "Costo_reposicion_COP",
            "Precio_reposicion_COP",
            "USD_base_unidades_disp",
            "Costo_Min",
            "Existencia_Total",
        ]
        cot_analitica_pref = [
            "Estado",
            "Costo_Max",
            "Precio_Lista_09",
            "Ult_venta_guia",
            "Margen_pct_cot",
            "TRM_cot",
            "USD_base_fuente",
            "Regla_precio",
            "Alertas_detalle",
        ]
        cot_core = [c for c in cot_core_pref if c in df_cot.columns]
        cot_analitica = [c for c in cot_analitica_pref if c in df_cot.columns]
        cot_order = cot_core + cot_analitica + [c for c in df_cot.columns if c not in (cot_core + cot_analitica)]
        df_cot = df_cot[cot_order]

        cot_view_cols = cot_core if not traer_datos_extra else (cot_core + cot_analitica)
        cot_view_cols = [c for c in cot_view_cols if c in df_cot.columns]
        extras_visibles = [c for c in extra_cols_added if c in df_cot.columns]
        cot_view_cols = cot_view_cols + [c for c in df_cot.columns if c.startswith("_extra_") and c not in extras_visibles]
        cot_view_cols = cot_view_cols + [c for c in extras_visibles if c not in cot_view_cols]
        df_cot_view = df_cot[cot_view_cols] if cot_view_cols else df_cot

        df_cot_show = _renombrar_negocio(df_cot_view)
        fmt_cot = _consulta_masiva_cotizador_format_map(df_cot_show)
        st.dataframe(
            df_cot_show.style.format(fmt_cot, na_rep="—"),
            width="stretch",
            hide_index=True,
        )
        st.caption(
            "`Fuente USD` indica de dónde salió el `USD base (cotiz.)`: "
            "`Mejor_Precio_Ajustado` (origen BR/USA/EUR con factor) o, si no hubo origen válido, "
            "respaldo `Ult. Fecha Compra / lista (USD, ajustado)` según `País últ. compra`. "
            "**Costo reposición (COP)** = USD base × TRM; **Precio reposición (COP)** = USD base × TRM ÷ (1 − margen %)."
        )
        csv_out = df_out.to_csv(index=False).encode("utf-8-sig")
        csv_cot_bytes = df_cot.to_csv(index=False).encode("utf-8-sig")
        dl1, dl2 = st.columns(2)
        with dl1:
            st.download_button(
                "Descargar resultado consulta (CSV)",
                data=csv_out,
                file_name="consulta_masiva_resultado.csv",
                mime="text/csv",
                key="consulta_masiva_download",
                help="Exporta la salida base de consulta masiva: cruce de referencias, orígenes/precios, disponibilidad y estado de coincidencia.",
            )
        with dl2:
            st.download_button(
                "Descargar cotización (CSV)",
                data=csv_cot_bytes,
                file_name="consulta_masiva_cotizacion.csv",
                mime="text/csv",
                key="consulta_masiva_download_cot",
                help="Exporta la salida del cotizador: precio recomendado, precio experto, piso por inventario, estado de cotización y alertas.",
            )

    _consulta_masiva_cotizador_vista_y_descargas()


def _consulta_masiva_calcular_mejor_origen(
    df_out: pd.DataFrame,
    disp_umbral: float,
    factor_usabr: float,
    factor_euro: float,
) -> pd.DataFrame:
    out = df_out.copy()
    for c in ("Precio Brasil", "Precio Usa", "Precio Europa", "disp_br", "disp_usa", "disp_eur"):
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")

    def _resolver_fila(row: pd.Series) -> tuple[str | None, float | None, float | None, float | None]:
        candidatos: list[tuple[str, float, float, float]] = []
        p_br = row.get("Precio Brasil")
        d_br = row.get("disp_br")
        p_usa = row.get("Precio Usa")
        d_usa = row.get("disp_usa")
        p_eur = row.get("Precio Europa")
        d_eur = row.get("disp_eur")

        if pd.notna(p_br) and pd.notna(d_br) and float(d_br) > disp_umbral:
            candidatos.append(("Brasil", float(d_br), float(p_br), float(p_br) * factor_usabr))
        if pd.notna(p_usa) and pd.notna(d_usa) and float(d_usa) > disp_umbral:
            candidatos.append(("USA", float(d_usa), float(p_usa), float(p_usa) * factor_usabr))
        if pd.notna(p_eur) and pd.notna(d_eur) and float(d_eur) > disp_umbral:
            candidatos.append(("Europa", float(d_eur), float(p_eur), float(p_eur) * factor_euro))

        if not candidatos:
            return None, None, None, None
        best = min(candidatos, key=lambda x: x[3])  # menor precio ajustado
        return best[0], best[1], best[2], best[3]

    resultados = out.apply(_resolver_fila, axis=1)
    out["Mejor_Origen"] = resultados.apply(lambda x: x[0])
    out["Mejor_Disponibilidad"] = resultados.apply(lambda x: x[1])
    out["Mejor_Precio_Sin_Factor"] = resultados.apply(lambda x: x[2])
    out["Mejor_Precio_Ajustado"] = resultados.apply(lambda x: x[3])
    return out


def _consulta_masiva_ajustar_decimales(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    price_cols = [
        "Precio Brasil",
        "Precio Usa",
        "Precio Europa",
        "Ultimo Valor USD",
        "Valor Liquido COP",
        "Valor Liq. (COP)",
        "Costo_Min",
        "Costo_Prom_Inst",
        "Costo_Max",
        "Precio_Lista_09",
        "Ult. Precio Venta",
        "Precio_USD_Ultima",
        "Precio_COP_Ultima",
        "TRM_Ultima",
        "Mejor_Precio_Sin_Factor",
        "Mejor_Precio_Ajustado",
        "Precio Prorrateo",
    ]
    avail_cols = [
        "disp_br",
        "disp_usa",
        "disp_eur",
        "_disp_total",
        "Mejor_Disponibilidad",
        "Existencia_Total",
    ]

    for c in price_cols:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce").round(2)
    for c in avail_cols:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce").round(0).astype("Int64")
    return out


def _consulta_masiva_build_format_map(df_show: pd.DataFrame) -> dict[str, str]:
    # En pantalla: precios en USD con 2 decimales, disponibilidades enteras.
    price_like = {
        "Precio Brasil",
        "Precio Usa",
        "Precio Europa",
        "Ultimo Valor USD",
        "USD — lista",
        "Valor liquidado COP",
        "COP liq. — lista",
        "Valor Liq. (COP)",
        "Costo mín.",
        "Costo máx.",
        "DNET BRA USD",
        "DNET USA USD",
        "DNET EUR EURO",
        "DNET BRA (USD)",
        "DNET USA (USD)",
        "DNET EUR (EUR)",
        "Precio lista 09 (COP)",
        "Valor últ. venta",
        "Últ compra (USD)",
        "Últ compra (COP)",
        "USD — aud.",
        "COP — aud.",
        "TRM últ compra",
        "TRM — aud.",
        "Mejor Precio Sin Factor",
        "Mejor Precio Ajustado",
        "Precio Prorrateo",
    }
    avail_like = {
        "disp br",
        "disp usa",
        "disp eur",
        "disp total",
        "Mejor Disponibilidad",
        "Exist. total (ref.)",
    }
    fmt: dict[str, str] = {}
    for c in df_show.columns:
        name = str(c)
        if name in price_like:
            fmt[name] = "{:,.2f}"
        elif name in avail_like:
            fmt[name] = "{:,.0f}"
    return fmt


def _consulta_masiva_style_mejor_origen(row: pd.Series) -> list[str]:
    origen = str(row.get("Mejor_Origen", row.get("Mejor Origen", "")) or "").strip().lower()
    if origen == "brasil":
        style = "background-color: rgba(20, 184, 166, 0.26); color: #e5e7eb;"
    elif origen == "usa":
        style = "background-color: rgba(56, 189, 248, 0.24); color: #e5e7eb;"
    elif origen == "europa":
        style = "background-color: rgba(99, 102, 241, 0.24); color: #e5e7eb;"
    else:
        style = ""
    target_cols = {
        "Mejor_Origen",
        "Mejor_Disponibilidad",
        "Mejor_Precio_Sin_Factor",
        "Mejor_Precio_Ajustado",
        "Mejor Origen",
        "Mejor Disponibilidad",
        "Mejor Precio Sin Factor",
        "Mejor Precio Ajustado",
    }
    return [style if str(c) in target_cols else "" for c in row.index]


def _consulta_masiva_fallback(referencias: list[str]) -> pd.DataFrame:
    """
    Fallback en app.py para ambientes con cache de módulos.
    Resuelve cada referencia por búsqueda (principal/normalizada/alterna) y trae resumen.
    """
    cols = [
        "Referencia_Entrada",
        "Referencia_Cruce",
        "Estado",
        "Tipo_Coincidencia",
        "Referencia_Original",
        "Referencia_Normalizada",
        "Descripción",
        "RefsAlternas",
        "Precio Prorrateo",
        "Precio Brasil",
        "Precio Usa",
        "Precio Europa",
        "disp_br",
        "disp_usa",
        "disp_eur",
        "_disp_total",
        "_disponible",
        "Ult. Fecha Compra",
        "Proveedor",
        "Ultimo Valor USD",
        "Valor Liquido COP",
        "Costo_Min",
        "Costo_Max",
        "Existencia_Total",
        "Tipo_Origen",
        "Precio_Lista_09",
        "Ult. Precio Venta",
        "Fecha Ult. Venta",
        "Fecha_Ultima_Compra",
        "Pais_Ultima",
        "Proveedor_Ultima",
        "Comprador_Ultima",
        "Precio_USD_Ultima",
        "Precio_COP_Ultima",
        "TRM_Ultima",
    ]
    rows: list[dict] = []
    for ref in referencias:
        entrada = str(ref or "").strip()
        if not entrada:
            continue
        item = {
            "Referencia_Entrada": entrada,
            "Referencia_Cruce": None,
            "Estado": "Sin coincidencia",
            "Tipo_Coincidencia": None,
            "Referencia_Original": None,
            "Referencia_Normalizada": None,
            "Descripción": None,
            "RefsAlternas": None,
            "Precio Prorrateo": None,
            "Precio Brasil": None,
            "Precio Usa": None,
            "Precio Europa": None,
            "disp_br": None,
            "disp_usa": None,
            "disp_eur": None,
            "_disp_total": None,
            "_disponible": None,
            "Ult. Fecha Compra": None,
            "Proveedor": None,
            "Ultimo Valor USD": None,
            "Valor Liquido COP": None,
            "Costo_Min": None,
            "Costo_Max": None,
            "Existencia_Total": None,
            "Tipo_Origen": None,
            "Precio_Lista_09": None,
            "Ult. Precio Venta": None,
            "Fecha Ult. Venta": None,
            "Fecha_Ultima_Compra": None,
            "Pais_Ultima": None,
            "Proveedor_Ultima": None,
            "Comprador_Ultima": None,
            "Precio_USD_Ultima": None,
            "Precio_COP_Ultima": None,
            "TRM_Ultima": None,
        }
        try:
            df_hit = buscar_referencias(entrada, limite=1)
            if df_hit is not None and not df_hit.empty:
                ref_norm = str(df_hit.iloc[0].get("Referencia_Normalizada", "") or "").strip()
                ref_orig = str(df_hit.iloc[0].get("Referencia_Original", "") or "").strip()
                if ref_norm:
                    resumen = obtener_resumen_referencia(ref_norm)
                    if resumen:
                        item["Estado"] = "OK"
                        item["Referencia_Original"] = resumen.get("Referencia_Original")
                        item["Referencia_Normalizada"] = resumen.get("Referencia_Normalizada")
                        item["Descripción"] = resumen.get("Descripción")
                        item["RefsAlternas"] = resumen.get("RefsAlternas")
                        item["Precio Prorrateo"] = resumen.get("Precio Prorrateo")
                        item["Precio Brasil"] = resumen.get("Precio Brasil")
                        item["Precio Usa"] = resumen.get("Precio Usa")
                        item["Precio Europa"] = resumen.get("Precio Europa")
                        item["disp_br"] = resumen.get("disp_br")
                        item["disp_usa"] = resumen.get("disp_usa")
                        item["disp_eur"] = resumen.get("disp_eur")
                        item["_disp_total"] = resumen.get("_disp_total")
                        item["_disponible"] = resumen.get("_disponible")
                        item["Ult. Fecha Compra"] = resumen.get("Ult. Fecha Compra")
                        item["Proveedor"] = resumen.get("Proveedor")
                        item["Ultimo Valor USD"] = resumen.get("Ultimo Valor USD")
                        item["Valor Liquido COP"] = _valor_liq_cop_desde_resumen(resumen)
                        item["Costo_Min"] = resumen.get("Costo_Min")
                        item["Costo_Max"] = resumen.get("Costo_Max")
                        item["Existencia_Total"] = resumen.get("Existencia_Total")
                        item["Tipo_Origen"] = resumen.get("Tipo_Origen") or resumen.get("Tipo Origen")
                        item["Precio_Lista_09"] = resumen.get("Precio_Lista_09")
                        item["Ult. Precio Venta"] = resumen.get("Ult. Precio Venta") or resumen.get("Ultimo Valor Venta")
                        item["Fecha Ult. Venta"] = resumen.get("Fecha Ult. Venta")
                        ent_up = entrada.upper()
                        if ent_up == ref_orig.upper():
                            item["Tipo_Coincidencia"] = "Principal"
                            item["Referencia_Cruce"] = ref_orig
                        elif ent_up == ref_norm.upper():
                            item["Tipo_Coincidencia"] = "Normalizada"
                            item["Referencia_Cruce"] = ref_norm
                        else:
                            item["Tipo_Coincidencia"] = "Alterna"
                            item["Referencia_Cruce"] = ref_norm
        except Exception:
            pass
        rows.append(item)

    out = pd.DataFrame(rows)
    for c in cols:
        if c not in out.columns:
            out[c] = None
    return out[cols]


def _margen_df_con_codigo_referencia(df: pd.DataFrame, margen_col: str, precio_col: str) -> pd.DataFrame:
    """Copia segura + columnas auxiliares + `_ref_codigo` y coerción numérica de margen/precio lista."""
    df_margen = df.copy()
    if "Referencia_Original" not in df_margen.columns:
        df_margen["Referencia_Original"] = ""
    if "Referencia_Alternas" not in df_margen.columns:
        df_margen["Referencia_Alternas"] = ""
    if "Descripcion" not in df_margen.columns:
        df_margen["Descripcion"] = ""
    if "Rotacion" not in df_margen.columns:
        df_margen["Rotacion"] = ""

    ref_original = df_margen["Referencia_Original"].fillna("").astype(str).str.strip()
    ref_alternas = df_margen["Referencia_Alternas"].fillna("").astype(str).str.strip()

    def _primera_alterna(txt: str) -> str:
        clean = (txt or "").strip().strip("()")
        if not clean:
            return ""
        return clean.split(",")[0].strip()

    df_margen["_ref_codigo"] = [
        ro if ro else (_primera_alterna(ra) if _primera_alterna(ra) else "SIN_REFERENCIA")
        for ro, ra in zip(ref_original, ref_alternas)
    ]

    # Cuando incluimos filas con márgenes/precios NULL (referencias sin Precio_Lista),
    # algunos JOINs de puente pueden duplicar registros en UI.
    # Base 00 normalmente trae 1 fila por (Referencia, Bodega); deduplicamos por llaves
    # disponibles para mantener consistencia en KPIs y detalle filtrado.
    dedup_keys: list[str] = []
    if "Referencia_Original" in df_margen.columns:
        dedup_keys.append("Referencia_Original")
    if "Bodega" in df_margen.columns:
        dedup_keys.append("Bodega")
    for extra in ("Nom_Instalacion", "Rotacion"):
        if extra in df_margen.columns:
            dedup_keys.append(extra)
    if len(dedup_keys) >= 2:
        # Si hay duplicados por la ruta de JOIN (común al incluir NULLs),
        # conservar la fila "más informativa" en vez de la primera arbitraria.
        prefer_exist_col = "Existencia" if "Existencia" in df_margen.columns else ("Disponible" if "Disponible" in df_margen.columns else None)
        prefer_cols: list[str] = []
        if prefer_exist_col and prefer_exist_col in df_margen.columns:
            prefer_cols.append(prefer_exist_col)
        if "Costo_Prom_Inst" in df_margen.columns:
            prefer_cols.append("Costo_Prom_Inst")
        if prefer_cols:
            df_margen = df_margen.sort_values(
                by=prefer_cols,
                ascending=[False] * len(prefer_cols),
                kind="mergesort",
            )
        df_margen = df_margen.drop_duplicates(subset=dedup_keys, keep="first")

    if margen_col in df_margen.columns:
        df_margen[margen_col] = pd.to_numeric(df_margen[margen_col], errors="coerce")
    if precio_col in df_margen.columns:
        df_margen[precio_col] = pd.to_numeric(df_margen[precio_col], errors="coerce")
    return df_margen


def _margen_ui_filtros_completos(df_margen: pd.DataFrame, margen_col: str, precio_col: str) -> pd.DataFrame | None:
    """Misma UI y lógica de filtros que **Reporte margen SIESA**."""
    _mh, _mr = st.columns([5, 1], gap="small")
    with _mh:
        st.caption("Análisis de margen con filtros dinámicos para decisiones comerciales.")
    with _mr:
        render_reset_filters_button("margen")
    filtros1, filtros2, filtros3 = st.columns([2.0, 1.0, 1.0], gap="medium")
    with filtros1:
        sub_m1, sub_m2 = st.columns([1.35, 1.0], gap="medium")
        with sub_m1:
            txt_busqueda = st.text_input(
                "Buscar referencia / alterna / descripción / rotación / equipo",
                placeholder="Ej: 12345 o texto",
                key="margen_filtro_busqueda",
            ).strip()
            # Si el usuario busca una referencia que no tenga precio/margen (NULL),
            # permitir que esos registros se mantengan en el filtrado.
            st.session_state.setdefault("margen_incluir_nulls", bool(txt_busqueda))
            incluir_nulls = st.checkbox(
                "Incluir sin precio/margen (NULL)",
                key="margen_incluir_nulls",
                help="Mantiene filas donde `Margen*` o `Precio_Lista_*` vienen en NULL/NaN.",
            )
        with sub_m2:
            modelo_txt = st.text_input(
                "Modelo (palabra clave)",
                placeholder="Ej: 9900, MAGNUM",
                key="margen_filtro_modelo",
                help="Solo columna **Modelo_CNH** (no busca en referencia ni en el cuadro de la izquierda).",
        ).strip()
    with filtros2:
        bodegas = (
            sorted([str(x) for x in df_margen["Bodega"].dropna().unique()])
            if "Bodega" in df_margen.columns
            else []
        )
        _init_multiselect_list("margen_filtro_bodega", bodegas)
        bodega_sel = st.multiselect("Bodega", options=bodegas, key="margen_filtro_bodega")
    with filtros3:
        rotaciones = (
            sorted([str(x) for x in df_margen["Rotacion"].dropna().unique()])
            if "Rotacion" in df_margen.columns
            else []
        )
        _init_multiselect_list("margen_filtro_rotacion", rotaciones)
        rot_sel = st.multiselect("Rotación", options=rotaciones, key="margen_filtro_rotacion")

    filtros5, filtros6, filtros7, filtros8 = st.columns([1, 1, 1, 1], gap="medium")
    with filtros5:
        max_precio_sel = float(df_margen[precio_col].fillna(0).max()) if precio_col in df_margen.columns else 0.0
        max_precio_sel = max(0.0, max_precio_sel)
        max_precio_slider = max(1.0, max_precio_sel)

        def _sync_precio_from_slider() -> None:
            lo, hi = st.session_state["margen_precio_range"]
            st.session_state["margen_precio_desde"] = float(lo)
            st.session_state["margen_precio_hasta"] = float(hi)

        def _sync_precio_from_inputs() -> None:
            lo = float(st.session_state.get("margen_precio_desde", 0.0))
            hi = float(st.session_state.get("margen_precio_hasta", max_precio_slider))
            lo = max(0.0, min(lo, max_precio_slider))
            hi = max(0.0, min(hi, max_precio_slider))
            if lo > hi:
                lo, hi = hi, lo
            st.session_state["margen_precio_desde"] = lo
            st.session_state["margen_precio_hasta"] = hi
            st.session_state["margen_precio_range"] = (lo, hi)

        if "margen_precio_col" not in st.session_state or st.session_state["margen_precio_col"] != precio_col:
            st.session_state["margen_precio_col"] = precio_col
            st.session_state["margen_precio_range"] = (0.0, max_precio_slider)
            st.session_state["margen_precio_desde"] = 0.0
            st.session_state["margen_precio_hasta"] = max_precio_slider
        st.session_state.setdefault("margen_precio_range", (0.0, max_precio_slider))
        st.session_state.setdefault("margen_precio_desde", float(st.session_state["margen_precio_range"][0]))
        st.session_state.setdefault("margen_precio_hasta", float(st.session_state["margen_precio_range"][1]))
        _sync_precio_from_inputs()

        st.slider(
            f"{precio_col} (rango COP)",
            min_value=0.0,
            max_value=max_precio_slider,
            step=1.0,
            format=_FMT_PRECIO_LISTA_SLIDER,
            key="margen_precio_range",
            on_change=_sync_precio_from_slider,
        )
        p1, p2 = st.columns(2, gap="small")
        with p1:
            st.number_input(
                f"Desde {precio_col} (COP)",
                min_value=0.0,
                max_value=max_precio_slider,
                step=1.0,
                format=_FMT_PRECIO_LISTA_NUM_INPUT,
                key="margen_precio_desde",
                on_change=_sync_precio_from_inputs,
            )
        with p2:
            st.number_input(
                f"Hasta {precio_col} (COP)",
                min_value=0.0,
                max_value=max_precio_slider,
                step=1.0,
                format=_FMT_PRECIO_LISTA_NUM_INPUT,
                key="margen_precio_hasta",
                on_change=_sync_precio_from_inputs,
            )
        precio_09_desde = float(st.session_state["margen_precio_desde"])
        precio_09_hasta = float(st.session_state["margen_precio_hasta"])
    with filtros6:
        existencia_col_global = "Existencia" if "Existencia" in df_margen.columns else ("Disponible" if "Disponible" in df_margen.columns else None)
        max_exist = float(df_margen[existencia_col_global].fillna(0).max()) if existencia_col_global else 0.0
        max_exist = max(0.0, max_exist)
        max_exist_slider = max(1.0, max_exist)

        def _sync_exist_from_slider() -> None:
            lo, hi = st.session_state["margen_exist_range"]
            st.session_state["margen_exist_desde"] = float(lo)
            st.session_state["margen_exist_hasta"] = float(hi)

        def _sync_exist_from_inputs() -> None:
            lo = float(st.session_state.get("margen_exist_desde", 0.0))
            hi = float(st.session_state.get("margen_exist_hasta", max_exist_slider))
            lo = max(0.0, min(lo, max_exist_slider))
            hi = max(0.0, min(hi, max_exist_slider))
            if lo > hi:
                lo, hi = hi, lo
            st.session_state["margen_exist_desde"] = lo
            st.session_state["margen_exist_hasta"] = hi
            st.session_state["margen_exist_range"] = (lo, hi)

        st.session_state.setdefault("margen_exist_range", (0.0, max_exist_slider))
        st.session_state.setdefault("margen_exist_desde", float(st.session_state["margen_exist_range"][0]))
        st.session_state.setdefault("margen_exist_hasta", float(st.session_state["margen_exist_range"][1]))
        _sync_exist_from_inputs()

        st.slider(
            "Existencia (rango)",
            min_value=0.0,
            max_value=max_exist_slider,
            step=1.0,
            key="margen_exist_range",
            on_change=_sync_exist_from_slider,
        )
        e1, e2 = st.columns(2, gap="small")
        with e1:
            st.number_input(
                "Desde Existencia",
                min_value=0.0,
                max_value=max_exist_slider,
                step=1.0,
                key="margen_exist_desde",
                on_change=_sync_exist_from_inputs,
            )
        with e2:
            st.number_input(
                "Hasta Existencia",
                min_value=0.0,
                max_value=max_exist_slider,
                step=1.0,
                key="margen_exist_hasta",
                on_change=_sync_exist_from_inputs,
            )
        existencia_desde = float(st.session_state["margen_exist_desde"])
        existencia_hasta = float(st.session_state["margen_exist_hasta"])
    with filtros7:
        nom_inst = (
            sorted([str(x) for x in df_margen["Nom_Instalacion"].dropna().unique()])
            if "Nom_Instalacion" in df_margen.columns
            else []
        )
        _init_multiselect_list("margen_filtro_instalacion", nom_inst)
        inst_sel = st.multiselect("Nom. instalación", options=nom_inst, key="margen_filtro_instalacion")

        # Rango manual de margen (único modo).
        _marg_series = pd.to_numeric(df_margen[margen_col], errors="coerce")
        _marg_valid = _marg_series.dropna()
        margen_min_base = float(_marg_valid.min()) if not _marg_valid.empty else -300.0
        margen_max_base = float(_marg_valid.max()) if not _marg_valid.empty else 300.0
        margen_min_ui = float(min(-300.0, margen_min_base))
        margen_max_ui = float(max(300.0, margen_max_base))
        if margen_min_ui >= margen_max_ui:
            margen_max_ui = margen_min_ui + 1.0

        def _sync_margen_from_slider() -> None:
            lo, hi = st.session_state["margen_pct_range"]
            st.session_state["margen_pct_desde"] = float(lo)
            st.session_state["margen_pct_hasta"] = float(hi)

        def _sync_margen_from_inputs() -> None:
            lo = float(st.session_state.get("margen_pct_desde", -100.0))
            hi = float(st.session_state.get("margen_pct_hasta", 100.0))
            lo = max(margen_min_ui, min(lo, margen_max_ui))
            hi = max(margen_min_ui, min(hi, margen_max_ui))
            if lo > hi:
                lo, hi = hi, lo
            st.session_state["margen_pct_desde"] = lo
            st.session_state["margen_pct_hasta"] = hi
            st.session_state["margen_pct_range"] = (lo, hi)

        st.session_state.setdefault("margen_pct_range", (margen_min_ui, margen_max_ui))
        st.session_state.setdefault("margen_pct_desde", float(st.session_state["margen_pct_range"][0]))
        st.session_state.setdefault("margen_pct_hasta", float(st.session_state["margen_pct_range"][1]))
        _sync_margen_from_inputs()

        st.slider(
            "Rango de margen (%)",
            min_value=float(margen_min_ui),
            max_value=float(margen_max_ui),
            step=1.0,
            key="margen_pct_range",
            on_change=_sync_margen_from_slider,
        )
        st.caption(f"Mínimo de margen detectado en base: {margen_min_base:,.2f}%")
    with filtros8:
        sistemas = (
            sorted([str(x) for x in df_margen["Sistema_Precio"].dropna().unique()])
            if "Sistema_Precio" in df_margen.columns
            else []
        )
        _init_multiselect_list("margen_filtro_sistema", sistemas)
        sistema_sel = st.multiselect("Sistema precio", options=sistemas, key="margen_filtro_sistema")
        m1, m2 = st.columns(2, gap="small")
        with m1:
            st.number_input(
                "Desde %",
                min_value=float(margen_min_ui),
                max_value=float(margen_max_ui),
                step=1.0,
                key="margen_pct_desde",
                on_change=_sync_margen_from_inputs,
            )
        with m2:
            st.number_input(
                "Hasta %",
                min_value=float(margen_min_ui),
                max_value=float(margen_max_ui),
                step=1.0,
                key="margen_pct_hasta",
                on_change=_sync_margen_from_inputs,
            )
    margen_desde = float(st.session_state["margen_pct_desde"])
    margen_hasta = float(st.session_state["margen_pct_hasta"])

    df_filtrado = df_margen.copy()
    df_filtrado = df_filtrado[df_filtrado["_ref_codigo"] != "SIN_REFERENCIA"]
    if bodega_sel and "Bodega" in df_filtrado.columns:
        df_filtrado = df_filtrado[df_filtrado["Bodega"].astype(str).isin(bodega_sel)]
    if rot_sel and "Rotacion" in df_filtrado.columns:
        df_filtrado = df_filtrado[df_filtrado["Rotacion"].fillna("").astype(str).isin(rot_sel)]
    if inst_sel and "Nom_Instalacion" in df_filtrado.columns:
        df_filtrado = df_filtrado[df_filtrado["Nom_Instalacion"].fillna("").astype(str).isin(inst_sel)]
    if sistema_sel and "Sistema_Precio" in df_filtrado.columns:
        df_filtrado = df_filtrado[df_filtrado["Sistema_Precio"].fillna("").astype(str).isin(sistema_sel)]
    if txt_busqueda:
        q = txt_busqueda.upper()
        mask = (
            df_filtrado["_ref_codigo"].str.upper().str.contains(q, regex=False)
            | df_filtrado["Referencia_Alternas"].fillna("").astype(str).str.upper().str.contains(q, regex=False)
            | df_filtrado["Descripcion"].fillna("").astype(str).str.upper().str.contains(q, regex=False)
            | df_filtrado["Rotacion"].fillna("").astype(str).str.upper().str.contains(q, regex=False)
        )
        if "Equipo_CNH" in df_filtrado.columns:
            mask = mask | df_filtrado["Equipo_CNH"].fillna("").astype(str).str.upper().str.contains(q, regex=False)
        df_filtrado = df_filtrado[mask]
    if modelo_txt and "Modelo_CNH" in df_filtrado.columns:
        df_filtrado = df_filtrado[
            df_filtrado["Modelo_CNH"].fillna("").astype(str).str.upper().str.contains(modelo_txt.upper(), regex=False)
        ]
    if margen_desde > margen_hasta:
        margen_desde, margen_hasta = margen_hasta, margen_desde
    effective_incluir_nulls = incluir_nulls or bool(txt_busqueda)
    margen_s = df_filtrado[margen_col]
    if effective_incluir_nulls:
        df_filtrado = df_filtrado[
            ((margen_s >= float(margen_desde)) & (margen_s <= float(margen_hasta)))
            | margen_s.isna()
        ]
    else:
        df_filtrado = df_filtrado[
            (margen_s >= float(margen_desde))
            & (margen_s <= float(margen_hasta))
        ]
    if precio_09_desde > precio_09_hasta:
        precio_09_desde, precio_09_hasta = precio_09_hasta, precio_09_desde
    if existencia_desde > existencia_hasta:
        existencia_desde, existencia_hasta = existencia_hasta, existencia_desde

    if precio_col in df_filtrado.columns:
        precio_s = df_filtrado[precio_col]
        price_mask = (
            (precio_s.fillna(0) >= float(precio_09_desde))
            & (precio_s.fillna(0) <= float(precio_09_hasta))
        )
        if effective_incluir_nulls:
            price_mask = price_mask | precio_s.isna()
        df_filtrado = df_filtrado[price_mask]
    if existencia_col_global:
        df_filtrado = df_filtrado[
            (df_filtrado[existencia_col_global].fillna(0) >= float(existencia_desde))
            & (df_filtrado[existencia_col_global].fillna(0) <= float(existencia_hasta))
        ]
    if df_filtrado.empty:
        st.info("No hay datos para los filtros seleccionados.")
        save_tab_filter_prefs("margen")
        return None
    return df_filtrado


def _margen_html_kpi_strip(
    *,
    total_refs: int,
    inv_s: str,
    exist_s: str,
    margen_pct_s: str,
    margen_dol_s: str,
    costo_bod_s: str,
) -> str:
    """Franja compacta de KPIs del reporte margen (alineada con la estética de Consulta)."""
    pairs: tuple[tuple[str, str], ...] = (
        ("Refs", f"{total_refs:,.0f}"),
        ("Inv", inv_s),
        ("Exist", exist_s),
        ("Margen %", margen_pct_s),
        ("Margen $", margen_dol_s),
        ("Costo Bodega", costo_bod_s),
    )
    cells: list[str] = []
    for lbl, val in pairs:
        cells.append(
            '<div class="margen-kpi-mini">'
            f'<span class="margen-kpi-mini-lbl">{html.escape(lbl)}</span>'
            f'<span class="margen-kpi-mini-val">{html.escape(val)}</span>'
            "</div>"
        )
    return f'<div class="margen-kpi-strip">{"".join(cells)}</div>'


def _render_tab_margen() -> None:
    if st.session_state.get("_actualizando"):
        st.info("Consultas pausadas mientras termina la actualización. Puedes cambiar entre pestañas libremente.")
        return

    try:
        df_margen, margen_col = _cargar_margenes_para_dashboard()
    except Exception as exc:
        msg = str(exc)
        if "No existe `margen_siesa_raw`" in msg:
            st.warning(
                "No existe `margen_siesa_raw` en la base de lectura. "
                "Usa el botón `Forzar actualización (cerrar lecturas)` para generarla."
            )
        else:
            st.error(f"No fue posible consultar `margen_siesa_raw`: {exc}")
        return

    if df_margen.empty:
        st.info("No hay filas en `margen_siesa_raw`. Ejecuta 'Actualizar datos'.")
        return
    if not margen_col:
        st.warning("No se encontró columna de margen esperada (Margen09, Margen_Pct o Margen04).")
        return

    margenes_disponibles = [c for c in ["Margen09", "Margen04"] if c in df_margen.columns]
    if not margenes_disponibles:
        st.warning("No hay columnas de margen disponibles para analizar.")
        return
    _mk = "margen_filtro_margen_col"
    _fallback = [margen_col if margen_col in margenes_disponibles else margenes_disponibles[0]]
    if _mk not in st.session_state:
        st.session_state[_mk] = _fallback
    else:
        prev = st.session_state[_mk]
        if not isinstance(prev, list):
            st.session_state[_mk] = _fallback
        else:
            ok = [x for x in prev if x in margenes_disponibles][:1]
            st.session_state[_mk] = ok if ok else _fallback
    margen_sel = st.multiselect(
        "Margen a analizar",
        options=margenes_disponibles,
        max_selections=1,
        help="Selecciona el margen que quieres mover en filtros y KPIs.",
        key=_mk,
    )
    if not margen_sel:
        st.info("Selecciona un margen para continuar.")
        return
    margen_col = margen_sel[0]
    precio_col = "Precio_Lista_04" if margen_col == "Margen04" else "Precio_Lista_09"

    df_margen = _margen_df_con_codigo_referencia(df_margen, margen_col, precio_col)
    df_filtrado = _margen_ui_filtros_completos(df_margen, margen_col, precio_col)
    if df_filtrado is None:
        return

    marg_tab_pri, marg_tab_graf = st.tabs(["Vista principal", "Reporte gráfico"])
    with marg_tab_pri:
        existencia_col = "Existencia" if "Existencia" in df_filtrado.columns else ("Disponible" if "Disponible" in df_filtrado.columns else None)
        if existencia_col:
            df_filtrado["_existencia_pos"] = df_filtrado[existencia_col].fillna(0) > 0
        else:
            df_filtrado["_existencia_pos"] = False
        if "Costo_Prom_Inst" in df_filtrado.columns and existencia_col:
            df_filtrado["_valor_inventario"] = (
                df_filtrado["Costo_Prom_Inst"].fillna(0).astype(float)
                * df_filtrado[existencia_col].fillna(0).astype(float)
            )
        else:
            df_filtrado["_valor_inventario"] = 0.0

        total_refs = int(df_filtrado["_ref_codigo"].nunique())
        existencia_total = float(df_filtrado[existencia_col].fillna(0).sum()) if existencia_col else 0.0
        valor_inventario = float(df_filtrado["_valor_inventario"].sum())

        # Equivalente DAX: Diferencia Costo Lista_Precio (margen nominal en $)
        if existencia_col and precio_col in df_filtrado.columns and "Costo_Prom_Inst" in df_filtrado.columns:
            margen_nominal = float(
                (
                    df_filtrado[existencia_col].fillna(0).astype(float)
                    * df_filtrado[precio_col].fillna(0).astype(float)
                ).sum()
                - (
                    df_filtrado[existencia_col].fillna(0).astype(float)
                    * df_filtrado["Costo_Prom_Inst"].fillna(0).astype(float)
                ).sum()
            )
            den_total = float(
                (
                    df_filtrado[existencia_col].fillna(0).astype(float)
                    * df_filtrado[precio_col].fillna(0).astype(float)
                ).sum()
            )
            num_total = float(
                (
                    df_filtrado[existencia_col].fillna(0).astype(float)
                    * df_filtrado["Costo_Prom_Inst"].fillna(0).astype(float)
                ).sum()
            )
            margen_total = ((1.0 - (num_total / den_total)) * 100.0) if den_total else None
            costo_prom_bod = (num_total / existencia_total) if existencia_total else None
        else:
            margen_nominal = 0.0
            margen_total = None
            costo_prom_bod = None

        def _fmt_short_money(v: float | None) -> str:
            return _fmt_cop_resumido(v)

        st.markdown(
            _margen_html_kpi_strip(
                total_refs=total_refs,
                inv_s=_fmt_short_money(valor_inventario),
                exist_s=f"{existencia_total:,.0f}",
                margen_pct_s=_to_percent_text(margen_total),
                margen_dol_s=_fmt_short_money(margen_nominal),
                costo_bod_s=_fmt_short_money(costo_prom_bod),
            ),
            unsafe_allow_html=True,
        )

        subtabs = st.tabs(["Detalle filtrado", "Segmentación"])
        with subtabs[0]:
            st.markdown("**Detalle de referencias según filtros aplicados**")
            # Equivalente DAX:
            # CALCULATE(MAX(Margen09), Existencia > 0, ALLEXCEPT(tabla, Referencia))
            # -> max por referencia sobre el dataset base (no sobre el subset filtrado por
            # bodega/rotacion/etc), manteniendo solo la llave de referencia.
            if "Margen09" in df_margen.columns:
                existencia_col_base = (
                    "Existencia"
                    if "Existencia" in df_margen.columns
                    else ("Disponible" if "Disponible" in df_margen.columns else None)
                )
                if existencia_col_base:
                    base_m09 = df_margen[df_margen[existencia_col_base].fillna(0) > 0]
                else:
                    base_m09 = df_margen
                max_margen09_map = (
                    base_m09[base_m09["_ref_codigo"] != "SIN_REFERENCIA"]
                    .groupby("_ref_codigo")["Margen09"]
                    .max()
                    .to_dict()
                )
                df_filtrado["Margen09_Max"] = pd.to_numeric(
                    df_filtrado["_ref_codigo"].map(max_margen09_map),
                    errors="coerce",
                ).astype("float64")

            detalle_cols = [
                c
                for c in [
                    "_ref_codigo",
                    "Referencia_Alternas",
                    "Descripcion",
                    "Nom_Instalacion",
                    "Bodega",
                    "Nom_Bodega",
                    "Existencia",
                    "Costo_Prom_Inst",
                    "_valor_inventario",
                    precio_col,
                                "Margen_Objetivo_Sistema",
                    margen_col,
                    "Margen09_Max",
                    "Tipo_Origen",
                    "Dias_Desde_Fecha_Max",
                ]
                if c in df_filtrado.columns
            ]
            df_crit = df_filtrado.sort_values(by=[margen_col, "_valor_inventario"], ascending=[True, False])
            df_show = df_crit[detalle_cols].rename(
                columns={
                    "_ref_codigo": "Referencia",
                    "_valor_inventario": "Valor_Inventario",
                    precio_col: "Precio_Lista_09" if precio_col == "Precio_Lista_09" else "Precio_Lista_04",
                }
            ).copy()
            df_show = _renombrar_negocio(df_show)

            def _fmt_referencia_display(v) -> str:
                """Quita artefacto float (.0) si la ref llego como numero desde DuckDB/pandas."""
                if v is None or pd.isna(v):
                    return ""
                if isinstance(v, float) and math.isnan(v):
                    return ""
                if isinstance(v, (int,)) and not isinstance(v, bool):
                    return str(int(v))
                if isinstance(v, float) and float(v).is_integer():
                    return str(int(v))
                s = str(v).strip()
                if re.fullmatch(r"-?\d+\.0+", s):
                    return s.split(".")[0]
                return s

            if "Referencia" in df_show.columns:
                df_show["Referencia"] = df_show["Referencia"].map(_fmt_referencia_display)

            # Mantener dtypes numéricos para que el sort del grid sea numérico real.
            column_config: dict[str, st.column_config.Column] = {}
            for col in ["Costo_Prom_Inst", "Valor_Inventario"]:
                label = _label_negocio(col)
                if label in df_show.columns:
                    column_config[label] = st.column_config.NumberColumn(label, format="COP $ %,.0f")
            for col in ["Precio_Lista_09", "Precio_Lista_04"]:
                label = _label_negocio(col)
                if label in df_show.columns:
                    column_config[label] = st.column_config.NumberColumn(label, format="COP $ %,.0f")
            for col in ["Existencia", "Dias_Desde_Fecha_Max"]:
                label = _label_negocio(col)
                if label in df_show.columns:
                    column_config[label] = st.column_config.NumberColumn(label, format="%,.0f")
            for col in ["Margen09", "Margen04", "Margen09_Max", "Margen_Pct", "Margen_Objetivo_Sistema"]:
                label = _label_negocio(col)
                if label in df_show.columns:
                    column_config[label] = st.column_config.NumberColumn(label, format="%.2f%%")

            st.dataframe(
                df_show,
                column_config=column_config,
                width="stretch",
                hide_index=True,
            )

        with subtabs[1]:
            st.markdown("**Cruces por atributo**")
            dims_disponibles = [
                    c for c in ["Sistema_Precio", "Equipo_CNH", "Modelo_CNH", "Bodega", "Rotacion"] if c in df_filtrado.columns
            ]
            if not dims_disponibles:
                st.caption("No hay dimensiones disponibles para segmentación.")
            else:
                col_dim, col_topn = st.columns([2, 1], gap="large")
                with col_dim:
                    dim_sel = st.selectbox("Dimensión de análisis", dims_disponibles, index=0)
                with col_topn:
                    top_n = st.number_input("Top grupos", min_value=5, max_value=50, value=15, step=5)
                df_attr = df_filtrado.copy()
                df_attr[dim_sel] = df_attr[dim_sel].fillna("SIN_DATO").astype(str)
                agg = (
                    df_attr.groupby(dim_sel, dropna=False)
                    .agg(
                        Registros=(margen_col, "count"),
                        Margen_Promedio=(margen_col, "mean"),
                        Margen_Mediano=(margen_col, "median"),
                        Min_Margen=(margen_col, "min"),
                        Max_Margen=(margen_col, "max"),
                        Negativos=(margen_col, lambda s: int((s < 0).sum())),
                        Valor_Inventario=("_valor_inventario", "sum"),
                    )
                    .reset_index()
                )
                agg["Pct_Negativos"] = (agg["Negativos"] / agg["Registros"]) * 100.0
                agg = agg.sort_values(by="Valor_Inventario", ascending=False).head(int(top_n))
                agg_show = _renombrar_negocio(agg)
                st.dataframe(
                    agg_show,
                    width="stretch",
                    hide_index=True,
                    column_config={
                        _label_negocio("Registros"): st.column_config.NumberColumn(
                            _label_negocio("Registros"), format="%,.0f"
                        ),
                        _label_negocio("Margen_Promedio"): st.column_config.NumberColumn(
                            _label_negocio("Margen_Promedio"), format="%.2f%%"
                        ),
                        _label_negocio("Margen_Mediano"): st.column_config.NumberColumn(
                            _label_negocio("Margen_Mediano"), format="%.2f%%"
                        ),
                        _label_negocio("Min_Margen"): st.column_config.NumberColumn(
                            _label_negocio("Min_Margen"), format="%.2f%%"
                        ),
                        _label_negocio("Max_Margen"): st.column_config.NumberColumn(
                            _label_negocio("Max_Margen"), format="%.2f%%"
                        ),
                        _label_negocio("Negativos"): st.column_config.NumberColumn(
                            _label_negocio("Negativos"), format="%,.0f"
                        ),
                        _label_negocio("Valor_Inventario"): st.column_config.NumberColumn(
                            _label_negocio("Valor_Inventario"), format="COP $ %,.0f"
                        ),
                        _label_negocio("Pct_Negativos"): st.column_config.NumberColumn(
                            _label_negocio("Pct_Negativos"), format="%.2f%%"
                        ),
                    },
                )
    with marg_tab_graf:
        st.markdown("##### Reporte gráfico")
        st.caption("Mismos filtros de arriba; gráficos sobre el subconjunto filtrado actual.")
        _margen_plotly_charts(df_filtrado, margen_col, precio_col)

    save_tab_filter_prefs("margen")


def _render_tab_auditoria() -> None:
    if st.session_state.get("_actualizando"):
        st.info("Consultas pausadas mientras termina la actualización. Puedes cambiar entre pestañas libremente.")
        return

    st.markdown("**Auditoría de cargas y cruces**")
    try:
        audit = _cargar_auditoria()
    except Exception as exc:
        st.error(f"No fue posible cargar auditoría: {exc}")
        return

    err = str(audit.get("error") or "").strip()
    if err:
        st.warning(err)

    flags = audit.get("flags") or {}
    f1, f2, f3, f4, f5 = st.columns(5, gap="small")
    f1.metric("Puente RPL/Alternas", "Sí" if flags.get("puente_rpl_alternas") else "No")
    f2.metric("Origen completo", "Sí" if flags.get("origen_tablero_completo") else "No")
    f3.metric("Tabla atributos", "Sí" if flags.get("tabla_atributos") else "No")
    f4.metric("Ref. alternas", "Sí" if flags.get("referencias_alternas") else "No")
    f5.metric("Auditoría refs", "Sí" if flags.get("auditoria_referencias") else "No")

    df_arch = audit.get("archivos")
    if isinstance(df_arch, pd.DataFrame) and not df_arch.empty:
        st.markdown("**Archivos DuckDB**")
        st.dataframe(_renombrar_negocio(df_arch), width="stretch", hide_index=True)

    df_tablas = audit.get("tablas")
    if isinstance(df_tablas, pd.DataFrame) and not df_tablas.empty:
        st.markdown("**Estado de tablas**")
        st.dataframe(_renombrar_negocio(df_tablas), width="stretch", hide_index=True)

    df_cruces = audit.get("cruces")
    if isinstance(df_cruces, pd.DataFrame) and not df_cruces.empty:
        st.markdown("**Cobertura de cruces**")
        cruces_show = _renombrar_negocio(df_cruces)
        st.dataframe(
            cruces_show,
            width="stretch",
            hide_index=True,
            column_config={
                _label_negocio("Valor"): st.column_config.NumberColumn(_label_negocio("Valor"), format="%,.0f"),
                _label_negocio("Pct_sobre_margen"): st.column_config.NumberColumn(_label_negocio("Pct_sobre_margen"), format="%.2f%%"),
            },
        )


def _auditoria_doc_semaforo_markdown() -> str:
    """Texto de ayuda: lógica del semáforo en SQL (misma corrida / mismos filtros)."""
    return """
**¿Qué pregunta responde el semáforo?**  
Si el **precio de la última compra** (en COP y **multiplicado por el factor logístico** según país: Brasil / USA / otros→EUR, definido en `config.ini`) está **alineado** con los **costos de inventario extremos** de esa referencia: **costo mínimo** y **costo máximo** en bodega.

**Dos números que calcula el sistema (columnas en la tabla):**

1. **`ABSVar_Costo`** — En pesos (COP): la **mayor** de estas dos brechas en valor absoluto:
   - diferencia frente al **costo mínimo**, y  
   - diferencia frente al **costo máximo**.  
   Así se mide “cuántos pesos” separan la compra ajustada del inventario en el peor de los dos extremos.

2. **`ABSVar_Costo_Pct`** — La **mayor** desviación **relativa** (en proporción; en pantalla suele verse como %) entre:
   - variación vs costo mínimo, y  
   - variación vs costo máximo.

**Reglas del color (se evalúan en este orden; la primera que cumpla gana):**

| Categoría (SQL) | En pantalla suele verse como | Condición (resumida) |
|-----------------|------------------------------|------------------------|
| Sin datos | Sin dato | No hay precio de última compra → no se puede comparar. |
| **CRÍTICO** | Crítico | Brecha en pesos **muy alta** respecto al resto del reporte **y** desviación % **≥ 30 %** (umbrales fijos del SQL). |
| **MODERADO ALTO** | Moderado alto | Brecha en pesos **alta** (por encima del tercer cuartil del lote) **y** desviación % **≥ 20 %**. |
| **MODERADO BAJO** | Moderado bajo | Brecha en pesos **media-alta** (por encima de la mediana del lote) **y** desviación % **≥ 10 %**. |
| **NO CRÍTICO** | Alineado | No entra en las anteriores. |

Los **cuartiles** (Q1, Q2, Q3) e **IQR** se calculan sobre **todas las referencias** del **mismo resultado** del reporte (misma corrida y filtros). Por eso, si cambias filtros o recargas datos, **puede cambiar** el semáforo de una misma referencia.

**Importante:** el semáforo **no** es lo mismo que “¿subió mucho entre penúltima y última compra?” Eso lo miden **`Var_PrecioCOP`** y los umbrales **|Δ compra|** más arriba en la pantalla.
"""


def _auditoria_normalizar_etiqueta_semaforo(val: object) -> str:
    """Normaliza `Semaforo_Variacion` para comparar con etiquetas SQL (CRÍTICO → CRITICO)."""
    return str(val or "").upper().strip().replace("Í", "I")


def _auditoria_es_semaforo_critico(val: object) -> bool:
    """True solo para la categoría **CRÍTICO**; **no** para *NO CRÍTICO* (evita `in 'CRITICO'`)."""
    return _auditoria_normalizar_etiqueta_semaforo(val) == "CRITICO"


def _auditoria_es_semaforo_mod_alto(val: object) -> bool:
    """MODERADO ALTO en datos SQL (comparación exacta normalizada)."""
    return _auditoria_normalizar_etiqueta_semaforo(val) == "MODERADO ALTO"


def _auditoria_color_discreto_semaforo(val: object) -> str:
    """Color por categoría exacta del semáforo (gráficos Plotly)."""
    s = _auditoria_normalizar_etiqueta_semaforo(val)
    if s == "CRITICO":
        return "#ef4444"
    if s == "MODERADO ALTO":
        return "#f59e0b"
    if s == "MODERADO BAJO":
        return "#eab308"
    if s == "NO CRITICO":
        return "#22c55e"
    if s.startswith("SIN ") or s in ("SIN_DATO", ""):
        return "#64748b"
    return "#64748b"


# Etiquetas de negocio para UI (evita "NO CRÍTICO": confuso y contiene "CRITICO" como subcadena).
_SEMAFORO_NORM_A_UI: Final[dict[str, str]] = {
    "CRITICO": "Crítico",
    "MODERADO ALTO": "Moderado alto",
    "MODERADO BAJO": "Moderado bajo",
    "NO CRITICO": "Alineado",
    "SIN_DATO": "Sin dato",
}


def _auditoria_etiqueta_semaforo_ui(val: object) -> str:
    """Etiqueta legible para pantallas y gráficos (valor SQL → texto de negocio)."""
    s = _auditoria_normalizar_etiqueta_semaforo(val)
    if s.startswith("SIN ") or s in ("SIN_DATO", ""):
        return "Sin dato"
    if s in _SEMAFORO_NORM_A_UI:
        return _SEMAFORO_NORM_A_UI[s]
    return str(val).strip() if val is not None and str(val).strip() else "Sin dato"


def _auditoria_color_discreto_semaforo_ui(val: object) -> str:
    """Color cuando el dato ya es etiqueta UI o sigue siendo valor crudo SQL."""
    s = str(val or "").strip()
    inv = {v: k for k, v in _SEMAFORO_NORM_A_UI.items()}
    if s in inv:
        return _auditoria_color_discreto_semaforo(inv[s])
    return _auditoria_color_discreto_semaforo(val)


def _auditoria_df_map_semaforo_ui(df: pd.DataFrame, sem_col: str | None, alias_cols: dict[str, str]) -> None:
    """Sustituye valores de semáforo por etiquetas UI en la columna ya renombrada (in-place)."""
    if not sem_col:
        return
    disp = alias_cols.get(sem_col, sem_col)
    if disp in df.columns:
        df[disp] = df[disp].map(_auditoria_etiqueta_semaforo_ui)


def _auditoria_inicializar_dataframe(df: pd.DataFrame) -> dict:
    """Prepara `auditoria_raw`: mapas de columnas, numéricos, existencia sumada y límites para sliders."""
    lower_map = {str(c).lower(): c for c in df.columns}
    sem_col = lower_map.get("semaforo_variacion")
    ref_col = lower_map.get("referencia")
    desc_col = lower_map.get("descripcion") or lower_map.get("descripción")
    sistema_precio_col = lower_map.get("sistema_precio_item") or lower_map.get("sistema_precio")
    modelo_col = lower_map.get("modelo_cnh")
    equipo_col = lower_map.get("equipo_cnh")
    pais_ult_col = lower_map.get("pais_ultima")
    dias_col = lower_map.get("dias_entre_compras")
    var_compra_col = lower_map.get("var_preciocop") or lower_map.get("var_preciousd")
    var_costo_base_col = lower_map.get("absvar_costo_pct")
    precio_ult_cop_col = lower_map.get("precio_cop_ultima")
    precio_pen_cop_col = lower_map.get("precio_cop_penultima")
    costo_inv_col = next(
        (c for c in df.columns if "costo" in str(c).lower() and "prom" in str(c).lower() and "inv" in str(c).lower()),
        None,
    )
    precio_lista_col = lower_map.get("precio_lista_09")

    for c in [
        var_compra_col,
        var_costo_base_col,
        dias_col,
        precio_ult_cop_col,
        precio_pen_cop_col,
        costo_inv_col,
        precio_lista_col,
    ]:
        if c and c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    df = _auditoria_add_existencia_suma_niveles(df, lower_map)
    lower_map = {str(c).lower(): c for c in df.columns}
    precio_lista_col = lower_map.get("precio_lista_09")

    ex_tot_col = (
        lower_map.get("existencia_total")
        if lower_map.get("existencia_total") in df.columns
        else ("_Existencia_suma_niveles" if "_Existencia_suma_niveles" in df.columns else None)
    )
    rot_col = lower_map.get("clasificacion_rotacion")

    dias_min_data = dias_max_data = None
    if dias_col and dias_col in df.columns:
        s_dias = pd.to_numeric(df[dias_col], errors="coerce").dropna()
        if not s_dias.empty:
            dias_min_data = int(s_dias.min())
            dias_max_data = int(s_dias.max())
    if dias_min_data is not None and dias_max_data is not None and dias_min_data > dias_max_data:
        dias_min_data, dias_max_data = dias_max_data, dias_min_data

    return {
        "df": df,
        "lower_map": lower_map,
        "sem_col": sem_col,
        "ref_col": ref_col,
        "desc_col": desc_col,
        "sistema_precio_col": sistema_precio_col,
        "modelo_col": modelo_col,
        "equipo_col": equipo_col,
        "pais_ult_col": pais_ult_col,
        "dias_col": dias_col,
        "var_compra_col": var_compra_col,
        "var_costo_base_col": var_costo_base_col,
        "precio_ult_cop_col": precio_ult_cop_col,
        "precio_pen_cop_col": precio_pen_cop_col,
        "costo_inv_col": costo_inv_col,
        "precio_lista_col": precio_lista_col,
        "ex_tot_col": ex_tot_col,
        "rot_col": rot_col,
        "dias_min_data": dias_min_data,
        "dias_max_data": dias_max_data,
    }



def _auditoria_ui_filtros_y_df_filtrado(ctx: dict) -> pd.DataFrame | None:
    """Misma UI + pipeline que **Auditoría referencias** (keys compartidas)."""
    df = ctx["df"]
    lower_map = ctx["lower_map"]
    sem_col = ctx["sem_col"]
    ref_col = ctx["ref_col"]
    desc_col = ctx["desc_col"]
    sistema_precio_col = ctx["sistema_precio_col"]
    modelo_col = ctx["modelo_col"]
    equipo_col = ctx["equipo_col"]
    pais_ult_col = ctx["pais_ult_col"]
    dias_col = ctx["dias_col"]
    var_compra_col = ctx["var_compra_col"]
    var_costo_base_col = ctx["var_costo_base_col"]
    precio_ult_cop_col = ctx["precio_ult_cop_col"]
    precio_pen_cop_col = ctx["precio_pen_cop_col"]
    costo_inv_col = ctx["costo_inv_col"]
    precio_lista_col = ctx["precio_lista_col"]
    ex_tot_col = ctx["ex_tot_col"]
    rot_col = ctx["rot_col"]
    dias_min_data = ctx["dias_min_data"]
    dias_max_data = ctx["dias_max_data"]
    _ah, _ar = st.columns([5, 1], gap="small")
    with _ah:
        st.caption(
            "Auditoría de variaciones de compra con filtros dinámicos para decisiones comerciales "
            "(mismo patrón de layout que **Reporte margen**: búsqueda, rangos con slider + desde/hasta, KPIs abajo)."
        )
    with _ar:
        render_reset_filters_button("auditoria_refs")

    filtros1, filtros2, filtros3, filtros4 = st.columns([1.75, 1.0, 1.0, 1.0], gap="medium")
    with filtros1:
        sub_a1, sub_a2 = st.columns([1.35, 1.0], gap="medium")
        with sub_a1:
            txt = st.text_input(
                "Buscar referencia / alterna / descripción / sistema / equipo",
                placeholder="Ej: 12345 o texto",
                key="aud_refs_txt_busqueda",
            ).strip()
        with sub_a2:
            modelo_txt = st.text_input(
                "Modelo (palabra clave)",
                placeholder="Ej: 9900, MAGNUM",
                key="aud_refs_modelo_txt",
                help="Solo columna de **modelo** en datos (p. ej. Modelo_CNH); no usa referencia ni el buscador general.",
            ).strip()
    with filtros2:
        if sem_col:
            sem_opts = sorted([str(x) for x in df[sem_col].dropna().astype(str).str.strip().unique() if str(x).strip()])
            _init_multiselect_list("aud_refs_sem_sel", sem_opts)
            sem_sel = st.multiselect(
                "Semáforo",
                options=sem_opts,
                key="aud_refs_sem_sel",
                format_func=_auditoria_etiqueta_semaforo_ui,
                help="Regla SQL sobre **alineación precio última compra vs costos extremos** (min/max) y cuartiles; "
                "no es lo mismo que la variación penúltima→última compra. "
                "Las etiquetas muestran nombres de negocio (p. ej. **Alineado** en lugar de *NO CRÍTICO*).",
            )
        else:
            sem_sel = []
            st.caption("Sin columna semáforo.")
    with filtros3:
        if rot_col and rot_col in df.columns:
            rot_opts = sorted([str(x) for x in df[rot_col].dropna().astype(str).str.strip().unique() if str(x).strip()])
            _init_multiselect_list("aud_refs_rot_sel", rot_opts)
            rot_sel = st.multiselect("Rotación / clasificación", options=rot_opts, key="aud_refs_rot_sel")
        else:
            rot_sel = []
            st.caption("Sin clasificación rotación.")
    with filtros4:
        if sistema_precio_col and sistema_precio_col in df.columns:
            sistemas = sorted([str(x) for x in df[sistema_precio_col].dropna().unique()])
            _init_multiselect_list("aud_refs_sistema_sel", sistemas)
            sistema_sel = st.multiselect("Sistema precio", options=sistemas, key="aud_refs_sistema_sel")
        else:
            sistema_sel = []
            st.caption("Sin sistema precio.")

    precio_lo = precio_hi = None
    ex_tot_lo = ex_tot_hi = None

    filtros5, filtros6, filtros7 = st.columns([1, 1, 1], gap="medium")
    with filtros5:
        if precio_lista_col and precio_lista_col in df.columns:
            s_pl = pd.to_numeric(df[precio_lista_col], errors="coerce").dropna()
            if not s_pl.empty:
                pl_min = float(s_pl.min())
                pl_max = float(s_pl.max())
                if pl_min > pl_max:
                    pl_min, pl_max = pl_max, pl_min
                max_precio_slider = max(1.0, pl_max)

                def _sync_aud_precio_from_slider() -> None:
                    lo, hi = st.session_state["aud_precio_range"]
                    st.session_state["aud_precio_desde"] = float(lo)
                    st.session_state["aud_precio_hasta"] = float(hi)

                def _sync_aud_precio_from_inputs() -> None:
                    lo = float(st.session_state.get("aud_precio_desde", 0.0))
                    hi = float(st.session_state.get("aud_precio_hasta", max_precio_slider))
                    lo = max(0.0, min(lo, max_precio_slider))
                    hi = max(0.0, min(hi, max_precio_slider))
                    if lo > hi:
                        lo, hi = hi, lo
                    st.session_state["aud_precio_desde"] = lo
                    st.session_state["aud_precio_hasta"] = hi
                    st.session_state["aud_precio_range"] = (lo, hi)

                if "aud_precio_col" not in st.session_state or st.session_state["aud_precio_col"] != precio_lista_col:
                    st.session_state["aud_precio_col"] = precio_lista_col
                    st.session_state["aud_precio_range"] = (0.0, max_precio_slider)
                    st.session_state["aud_precio_desde"] = 0.0
                    st.session_state["aud_precio_hasta"] = max_precio_slider
                st.session_state.setdefault("aud_precio_range", (0.0, max_precio_slider))
                st.session_state.setdefault("aud_precio_desde", float(st.session_state["aud_precio_range"][0]))
                st.session_state.setdefault("aud_precio_hasta", float(st.session_state["aud_precio_range"][1]))
                _sync_aud_precio_from_inputs()

                st.slider(
                    f"{precio_lista_col} (rango COP)",
                    min_value=0.0,
                    max_value=max_precio_slider,
                    step=1.0,
                    format=_FMT_PRECIO_LISTA_SLIDER,
                    key="aud_precio_range",
                    on_change=_sync_aud_precio_from_slider,
                )
                p1, p2 = st.columns(2, gap="small")
                with p1:
                    st.number_input(
                        "Desde (COP)",
                        min_value=0.0,
                        max_value=max_precio_slider,
                        step=1.0,
                        format=_FMT_PRECIO_LISTA_NUM_INPUT,
                        key="aud_precio_desde",
                        on_change=_sync_aud_precio_from_inputs,
                        help=f"Límite inferior del rango ({precio_lista_col}).",
                    )
                with p2:
                    st.number_input(
                        "Hasta (COP)",
                        min_value=0.0,
                        max_value=max_precio_slider,
                        step=1.0,
                        format=_FMT_PRECIO_LISTA_NUM_INPUT,
                        key="aud_precio_hasta",
                        on_change=_sync_aud_precio_from_inputs,
                        help=f"Límite superior del rango ({precio_lista_col}).",
                    )
                precio_lo = float(st.session_state["aud_precio_desde"])
                precio_hi = float(st.session_state["aud_precio_hasta"])
            else:
                st.caption("Sin valores numéricos en precio lista.")
        else:
            st.caption("Sin precio lista 09.")

    with filtros6:
        if ex_tot_col and ex_tot_col in df.columns:
            s_ex = pd.to_numeric(df[ex_tot_col], errors="coerce").dropna()
            if not s_ex.empty:
                ex_min = float(s_ex.min())
                ex_max = float(s_ex.max())
                if ex_min > ex_max:
                    ex_min, ex_max = ex_max, ex_min
                max_exist_slider = max(1.0, ex_max)

                def _sync_aud_exist_from_slider() -> None:
                    lo, hi = st.session_state["aud_exist_range"]
                    st.session_state["aud_exist_desde"] = float(lo)
                    st.session_state["aud_exist_hasta"] = float(hi)

                def _sync_aud_exist_from_inputs() -> None:
                    lo = float(st.session_state.get("aud_exist_desde", 0.0))
                    hi = float(st.session_state.get("aud_exist_hasta", max_exist_slider))
                    lo = max(0.0, min(lo, max_exist_slider))
                    hi = max(0.0, min(hi, max_exist_slider))
                    if lo > hi:
                        lo, hi = hi, lo
                    st.session_state["aud_exist_desde"] = lo
                    st.session_state["aud_exist_hasta"] = hi
                    st.session_state["aud_exist_range"] = (lo, hi)

                if "aud_exist_col" not in st.session_state or st.session_state["aud_exist_col"] != ex_tot_col:
                    st.session_state["aud_exist_col"] = ex_tot_col
                    st.session_state["aud_exist_range"] = (0.0, max_exist_slider)
                    st.session_state["aud_exist_desde"] = 0.0
                    st.session_state["aud_exist_hasta"] = max_exist_slider
                st.session_state.setdefault("aud_exist_range", (0.0, max_exist_slider))
                st.session_state.setdefault("aud_exist_desde", float(st.session_state["aud_exist_range"][0]))
                st.session_state.setdefault("aud_exist_hasta", float(st.session_state["aud_exist_range"][1]))
                _sync_aud_exist_from_inputs()

                lbl_ex = "Existencia total (ref.)" if lower_map.get("existencia_total") == ex_tot_col else "Σ niveles existencia"
                st.slider(
                    f"{lbl_ex} (rango)",
                    min_value=0.0,
                    max_value=max_exist_slider,
                    step=1.0,
                    key="aud_exist_range",
                    on_change=_sync_aud_exist_from_slider,
                )
                e1, e2 = st.columns(2, gap="small")
                with e1:
                    st.number_input(
                        "Desde existencia",
                        min_value=0.0,
                        max_value=max_exist_slider,
                        step=1.0,
                        key="aud_exist_desde",
                        on_change=_sync_aud_exist_from_inputs,
                    )
                with e2:
                    st.number_input(
                        "Hasta existencia",
                        min_value=0.0,
                        max_value=max_exist_slider,
                        step=1.0,
                        key="aud_exist_hasta",
                        on_change=_sync_aud_exist_from_inputs,
                    )
                ex_tot_lo = float(st.session_state["aud_exist_desde"])
                ex_tot_hi = float(st.session_state["aud_exist_hasta"])
            else:
                st.caption("Sin valores en existencia total.")
        else:
            st.caption("Sin existencia total.")

    dias_lo = dias_hi = None
    with filtros7:
        if dias_min_data is not None and dias_max_data is not None:
            if dias_min_data == dias_max_data:
                dias_lo = dias_hi = dias_min_data
                st.caption(f"Días entre compras fijos en datos: **{dias_min_data}**.")
            else:

                def _sync_aud_dias_from_slider() -> None:
                    lo, hi = st.session_state["aud_dias_range"]
                    st.session_state["aud_dias_desde"] = float(lo)
                    st.session_state["aud_dias_hasta"] = float(hi)

                def _sync_aud_dias_from_inputs() -> None:
                    lo = float(st.session_state.get("aud_dias_desde", float(dias_min_data)))
                    hi = float(st.session_state.get("aud_dias_hasta", float(dias_max_data)))
                    lo = max(float(dias_min_data), min(lo, float(dias_max_data)))
                    hi = max(float(dias_min_data), min(hi, float(dias_max_data)))
                    if lo > hi:
                        lo, hi = hi, lo
                    st.session_state["aud_dias_desde"] = lo
                    st.session_state["aud_dias_hasta"] = hi
                    st.session_state["aud_dias_range"] = (lo, hi)

                if (
                    "aud_dias_sig" not in st.session_state
                    or st.session_state["aud_dias_sig"] != (dias_min_data, dias_max_data)
                ):
                    st.session_state["aud_dias_sig"] = (dias_min_data, dias_max_data)
                    st.session_state["aud_dias_range"] = (float(dias_min_data), float(dias_max_data))
                    st.session_state["aud_dias_desde"] = float(dias_min_data)
                    st.session_state["aud_dias_hasta"] = float(dias_max_data)
                st.session_state.setdefault("aud_dias_range", (float(dias_min_data), float(dias_max_data)))
                st.session_state.setdefault("aud_dias_desde", float(st.session_state["aud_dias_range"][0]))
                st.session_state.setdefault("aud_dias_hasta", float(st.session_state["aud_dias_range"][1]))
                _sync_aud_dias_from_inputs()

                st.slider(
                    "Días entre compras (rango)",
                    min_value=float(dias_min_data),
                    max_value=float(dias_max_data),
                    step=1.0,
                    key="aud_dias_range",
                    on_change=_sync_aud_dias_from_slider,
                )
                d1, d2 = st.columns(2)
                with d1:
                    st.number_input(
                        "Desde días",
                        min_value=dias_min_data,
                        max_value=dias_max_data,
                        step=1,
                        key="aud_dias_desde",
                        on_change=_sync_aud_dias_from_inputs,
                    )
                with d2:
                    st.number_input(
                        "Hasta días",
                        min_value=dias_min_data,
                        max_value=dias_max_data,
                        step=1,
                        key="aud_dias_hasta",
                        on_change=_sync_aud_dias_from_inputs,
                    )
                dias_lo = int(st.session_state["aud_dias_desde"])
                dias_hi = int(st.session_state["aud_dias_hasta"])
        else:
            st.caption("Sin datos de días entre compras.")

    st.divider()
    with st.container():
        st.caption("Variación fuerte (Eje 2) — umbrales; el checkbox activa el filtro.")
        r_um = st.columns([1.05, 1.35, 1.35, 0.85], gap="medium")
        with r_um[0]:
            solo_significativas = st.checkbox(
                "Solo variación fuerte (Eje 2)",
                value=False,
                key="aud_solo_significativas",
                help="Filtra refs para tablas y Eje 2. El Eje 1 (Semáforo) siempre ve todas las refs.",
            )
        sliders_activos = solo_significativas
        u1, u2, u3 = r_um[1], r_um[2], r_um[3]
        with u1:
            st.session_state.setdefault("aud_umbral_var_compra", 20.0)
            st.session_state.setdefault(
                "aud_umbral_var_compra_num",
                float(st.session_state.get("aud_umbral_var_compra", 20.0)),
            )

            def _sync_aud_umbral_compra_from_slider() -> None:
                st.session_state["aud_umbral_var_compra_num"] = float(st.session_state["aud_umbral_var_compra"])

            def _sync_aud_umbral_compra_from_input() -> None:
                v = float(st.session_state["aud_umbral_var_compra_num"])
                v = max(0.0, min(300.0, v))
                st.session_state["aud_umbral_var_compra_num"] = v
                st.session_state["aud_umbral_var_compra"] = v

            st.slider(
                "Umbral |Δ compra| (%)",
                min_value=0.0,
                max_value=300.0,
                step=1.0,
                key="aud_umbral_var_compra",
                disabled=not sliders_activos,
                on_change=_sync_aud_umbral_compra_from_slider,
                help="Valor absoluto de variación % entre última y penúltima compra. "
                + ("Activo." if sliders_activos else "Desactivado — marca «Solo variación fuerte» para filtrar."),
            )
            st.number_input(
                "Escribir |Δ compra| (%)",
                min_value=0.0,
                max_value=300.0,
                step=1.0,
                format="%.2f",
                key="aud_umbral_var_compra_num",
                disabled=not sliders_activos,
                on_change=_sync_aud_umbral_compra_from_input,
                help="Ajuste numérico; se sincroniza con el slider.",
            )
        with u2:
            st.session_state.setdefault("aud_umbral_var_costo", 15.0)
            st.session_state.setdefault(
                "aud_umbral_var_costo_num",
                float(st.session_state.get("aud_umbral_var_costo", 15.0)),
            )

            def _sync_aud_umbral_costo_from_slider() -> None:
                st.session_state["aud_umbral_var_costo_num"] = float(st.session_state["aud_umbral_var_costo"])

            def _sync_aud_umbral_costo_from_input() -> None:
                v = float(st.session_state["aud_umbral_var_costo_num"])
                v = max(0.0, min(300.0, v))
                st.session_state["aud_umbral_var_costo_num"] = v
                st.session_state["aud_umbral_var_costo"] = v

            st.slider(
                "Umbral |Δ vs costo inv.| (%)",
                min_value=0.0,
                max_value=300.0,
                step=1.0,
                key="aud_umbral_var_costo",
                disabled=not sliders_activos,
                on_change=_sync_aud_umbral_costo_from_slider,
                help="Variación % de última compra frente a costo prom. inventario. "
                + ("Activo." if sliders_activos else "Desactivado — marca «Solo variación fuerte» para filtrar."),
            )
            st.number_input(
                "Escribir |Δ costo| (%)",
                min_value=0.0,
                max_value=300.0,
                step=1.0,
                format="%.2f",
                key="aud_umbral_var_costo_num",
                disabled=not sliders_activos,
                on_change=_sync_aud_umbral_costo_from_input,
                help="Ajuste numérico; se sincroniza con el slider.",
            )
        with u3:
            if not sliders_activos:
                st.caption("Los umbrales no filtran.")
            else:
                st.caption("Filtran refs que superan al menos uno.")
        with st.expander("¿Cómo funcionan los umbrales y el checkbox?", expanded=True):
            st.markdown(
                "**Los dos sliders** definen qué se considera \"variación fuerte\":\n\n"
                "• **Umbral |Δ compra|** — Cambio % entre última y penúltima compra. "
                "Ej: 20 % → refs cuyo precio de compra subió o bajó ≥20 % entre compras.\n\n"
                "• **Umbral |Δ vs costo inv.|** — Desalineación entre precio de compra y costo prom. inventario. "
                "Ej: 15 % → refs donde el precio se aleja ≥15 % del costo.\n\n"
                "**El checkbox «Solo variación fuerte»:**\n\n"
                "• **Marcado** — Aplica el filtro: solo ves refs que superan *al menos uno* de los umbrales. "
                "Si una ref tiene |Δ compra| = 25 % y |Δ costo| = 5 %, pasa (supera el primero). "
                "Si tiene 10 % y 8 %, no pasa (ninguno supera 20 % ni 15 %).\n\n"
                "• **Desmarcado** — Los umbrales no filtran. Ves todas las refs (sliders grises, sin efecto).\n\n"
                "**Eje 1 (Semáforo) siempre ve todas las refs**, sin importar este checkbox. "
                "El filtro de umbrales solo afecta a las **tablas** y al **Eje 2 (Variación compra)**.\n\n"
                "**Truco:** Subir un slider al máximo (300 %) lo desactiva — p. ej. costo al 300 % → "
                "filtras solo por variación entre compras; compra al 300 % → solo por desalineación vs inventario."
            )

    umbral_var_compra = float(st.session_state.get("aud_umbral_var_compra", 20.0))
    umbral_var_costo = float(st.session_state.get("aud_umbral_var_costo", 15.0))

    with st.expander("Factores logísticos (última compra)", expanded=False):
        st.caption(
            "USA/BR u otros según país de la última compra; el factor ajusta el precio COP antes de comparar con costo inventario."
        )
        fx1, fx2 = st.columns(2, gap="medium")
        with fx1:
            factor_usa_br = st.slider(
                "Factor USA/BR",
                min_value=1.20,
                max_value=1.30,
                value=1.25,
                step=0.01,
                key="aud_refs_factor_usa_br",
                help="Aplica cuando el origen de la última compra sea USA o Brasil.",
            )
        with fx2:
            factor_otros = st.slider(
                "Factor otros orígenes",
                min_value=1.50,
                max_value=1.70,
                value=1.60,
                step=0.01,
                key="aud_refs_factor_otros",
                help="Aplica para orígenes distintos de USA/BR.",
            )

    st.divider()

    df_fil = df.copy()

    if pais_ult_col and pais_ult_col in df_fil.columns:
        pais_norm = df_fil[pais_ult_col].fillna("").astype(str).str.upper().str.strip()
        es_usa = (
            pais_norm.str.contains("USA", regex=False)
            | pais_norm.str.contains("US", regex=False)
            | pais_norm.str.contains("ESTADOS UNIDOS", regex=False)
            | pais_norm.str.contains("UNITED STATES", regex=False)
        )
        es_br = (
            pais_norm.str.contains("BRAZIL", regex=False)
            | pais_norm.str.contains("BRASIL", regex=False)
            | (pais_norm == "BR")
        )
        df_fil["_Origen_Ultima_Norm"] = "OTROS"
        df_fil.loc[es_usa, "_Origen_Ultima_Norm"] = "USA"
        df_fil.loc[es_br, "_Origen_Ultima_Norm"] = "BR"
        df_fil["_Factor_Logistico"] = float(factor_otros)
        df_fil.loc[df_fil["_Origen_Ultima_Norm"].isin(["USA", "BR"]), "_Factor_Logistico"] = float(factor_usa_br)
    else:
        df_fil["_Origen_Ultima_Norm"] = "SIN_DATO"
        df_fil["_Factor_Logistico"] = float(factor_otros)

    if costo_inv_col and precio_ult_cop_col:
        # Regla de negocio: el factor logístico afecta el precio de compra en COP,
        # no el costo promedio de inventario. (Las Var vs costo en SQL usan precio×factor desde 00_Reportes_SQL.)
        df_fil["_Precio_Ultima_Log_COP"] = df_fil[precio_ult_cop_col] * df_fil["_Factor_Logistico"]
        den_ult = df_fil[costo_inv_col].replace(0, pd.NA)
        df_fil["_Var_Ultima_vs_CostoLog_Pct"] = ((df_fil["_Precio_Ultima_Log_COP"] - den_ult) / den_ult) * 100.0
        df_fil["_ABS_Ultima_vs_CostoLog_Pct"] = df_fil["_Var_Ultima_vs_CostoLog_Pct"].abs()
        if precio_pen_cop_col:
            df_fil["_Precio_Penultima_Log_COP"] = df_fil[precio_pen_cop_col] * df_fil["_Factor_Logistico"]
            df_fil["_Var_Penultima_vs_CostoLog_Pct"] = ((df_fil["_Precio_Penultima_Log_COP"] - den_ult) / den_ult) * 100.0
            df_fil["_ABS_Penultima_vs_CostoLog_Pct"] = df_fil["_Var_Penultima_vs_CostoLog_Pct"].abs()

    if sem_sel and sem_col:
        df_fil = df_fil[df_fil[sem_col].fillna("").astype(str).isin(sem_sel)]
    if txt:
        q = txt.upper()
        refs_alt_col = None
        for cand in (
            "Referencias_Alternas",
            "Referencia_Alternas",
            "RefsAlternas",
            "Refs_Alternas",
        ):
            if cand in df_fil.columns:
                refs_alt_col = cand
                break
        search_cols = [c for c in [ref_col, refs_alt_col, desc_col, sistema_precio_col, equipo_col] if c]
        if search_cols:
            mask = pd.Series(False, index=df_fil.index)
            for c in search_cols:
                mask = mask | df_fil[c].fillna("").astype(str).str.upper().str.contains(q, regex=False)
            df_fil = df_fil[mask]
    if rot_sel and rot_col and rot_col in df_fil.columns:
        df_fil = df_fil[df_fil[rot_col].fillna("").astype(str).isin(rot_sel)]
    if sistema_sel and sistema_precio_col and sistema_precio_col in df_fil.columns:
        df_fil = df_fil[df_fil[sistema_precio_col].fillna("").astype(str).isin(sistema_sel)]
    if modelo_txt and modelo_col and modelo_col in df_fil.columns:
        df_fil = df_fil[
            df_fil[modelo_col].fillna("").astype(str).str.upper().str.contains(modelo_txt.upper(), regex=False)
        ]
    if dias_col and dias_lo is not None and dias_hi is not None and not df_fil.empty:
        lo_d, hi_d = int(dias_lo), int(dias_hi)
        if lo_d > hi_d:
            lo_d, hi_d = hi_d, lo_d
        df_fil = df_fil[(df_fil[dias_col] >= lo_d) & (df_fil[dias_col] <= hi_d)]

    if (
        precio_lista_col
        and precio_lo is not None
        and precio_hi is not None
        and precio_lista_col in df_fil.columns
        and not df_fil.empty
    ):
        lo_p, hi_p = float(precio_lo), float(precio_hi)
        if lo_p > hi_p:
            lo_p, hi_p = hi_p, lo_p
        vpl = pd.to_numeric(df_fil[precio_lista_col], errors="coerce")
        df_fil = df_fil[vpl.notna() & (vpl >= lo_p) & (vpl <= hi_p)]

    if (
        ex_tot_col
        and ex_tot_lo is not None
        and ex_tot_hi is not None
        and ex_tot_col in df_fil.columns
        and not df_fil.empty
    ):
        lo_e, hi_e = float(ex_tot_lo), float(ex_tot_hi)
        if lo_e > hi_e:
            lo_e, hi_e = hi_e, lo_e
        vex = pd.to_numeric(df_fil[ex_tot_col], errors="coerce")
        df_fil = df_fil[vex.notna() & (vex >= lo_e) & (vex <= hi_e)]

    if var_compra_col:
        df_fil["_abs_var_compra"] = df_fil[var_compra_col].abs()
    else:
        df_fil["_abs_var_compra"] = pd.NA
    if "_ABS_Ultima_vs_CostoLog_Pct" in df_fil.columns:
        df_fil["_abs_var_costo"] = df_fil["_ABS_Ultima_vs_CostoLog_Pct"]
    elif var_costo_base_col:
        df_fil["_abs_var_costo"] = df_fil[var_costo_base_col].abs()
    else:
        df_fil["_abs_var_costo"] = pd.NA

    df_base = df_fil.copy()

    if solo_significativas and not df_fil.empty:
        cond_compra = df_fil["_abs_var_compra"].fillna(-1) >= float(umbral_var_compra)
        cond_costo = df_fil["_abs_var_costo"].fillna(-1) >= float(umbral_var_costo)
        df_fil = df_fil[cond_compra | cond_costo]

    if df_base.empty:
        st.info("No hay registros con los filtros actuales.")
        save_tab_filter_prefs("auditoria_refs")
        return None, None
    return df_fil, df_base


def _render_tab_auditoria_referencias() -> None:
    if st.session_state.get("_actualizando"):
        st.info("Consultas pausadas mientras termina la actualización. Puedes cambiar entre pestañas libremente.")
        return

    try:
        df = _cargar_auditoria_referencias()
    except Exception as exc:
        st.error(f"No fue posible cargar `auditoria_raw`: {exc}")
        return

    if df.empty:
        st.info(
            "No hay filas en `auditoria_raw`. En **Actualizar datos** elige modo **Completa** "
            "(o **Personalizada** e incluye **SQL 003 — Auditoría**). Luego vuelve a abrir esta pestaña o pulsa **Rerun**."
        )
        return

    st.markdown('<h4 class="auditoria-tab-h">Auditoría referencias</h4>', unsafe_allow_html=True)
    with st.expander("📖 Cómo se calcula el semáforo (y qué significa cada categoría)", expanded=False):
        st.markdown(_auditoria_doc_semaforo_markdown())

    ctx = _auditoria_inicializar_dataframe(df)
    _result = _auditoria_ui_filtros_y_df_filtrado(ctx)
    if _result is None or _result[0] is None:
        return
    df_fil, df_base = _result
    lower_map = ctx["lower_map"]
    ref_col = ctx["ref_col"]
    sem_col = ctx["sem_col"]
    desc_col = ctx["desc_col"]
    sistema_precio_col = ctx["sistema_precio_col"]
    modelo_col = ctx["modelo_col"]
    dias_col = ctx["dias_col"]
    var_compra_col = ctx["var_compra_col"]
    var_costo_base_col = ctx["var_costo_base_col"]
    precio_ult_cop_col = ctx["precio_ult_cop_col"]
    precio_pen_cop_col = ctx["precio_pen_cop_col"]
    costo_inv_col = ctx["costo_inv_col"]
    precio_lista_col = ctx["precio_lista_col"]
    ex_tot_col = ctx["ex_tot_col"]
    rot_col = ctx["rot_col"]
    pais_ult_col = ctx["pais_ult_col"]
    umbral_var_compra = float(st.session_state.get("aud_umbral_var_compra", 20.0))
    umbral_var_costo = float(st.session_state.get("aud_umbral_var_costo", 15.0))
    total_refs = int(df_fil[ref_col].nunique()) if ref_col else 0
    if sem_col:
        criticos = int(df_fil[sem_col].map(_auditoria_es_semaforo_critico).sum())
    else:
        criticos = 0
    # % del subconjunto actual que supera cada umbral (tras filtros y opcionalmente "solo variación fuerte").
    pct_umbral_compra = float((df_fil["_abs_var_compra"].fillna(-1) >= float(umbral_var_compra)).mean() * 100.0)
    pct_umbral_costo = float((df_fil["_abs_var_costo"].fillna(-1) >= float(umbral_var_costo)).mean() * 100.0)
    valor_inv_cop = _valor_inventario_cop_auditoria(df_fil, lower_map)

    st.markdown(
        '<div class="auditoria-dash-blurb">'
        "<p><strong>Cuadro de mando</strong> del conjunto filtrado: cuántas filas superan cada umbral en los "
        "<strong>dos problemas</strong> (variación <strong>última vs penúltima compra</strong> y "
        "<strong>última vs costo prom. inv.</strong>). <strong>Valor inv.</strong> ≈ impacto económico del slice en inventario.</p>"
        "<p><strong>Semáforo:</strong> compara la <strong>última compra en COP × factor país</strong> (Brasil / USA / EUR según <code>config.ini</code>) "
        "con los <strong>costos mín. y máx.</strong> de inventario; usa las columnas <code>ABSVar_Costo</code> (pesos) y "
        "<code>ABSVar_Costo_Pct</code> (%) más los cuartiles del mismo reporte. "
        "<strong>Abre el desplegable 📖 arriba</strong> para ver la lógica completa y las categorías.</p>"
        "<p>Es <strong>distinto</strong> de «subió mucho entre penúltima y última compra» "
        "(<code>Var_PrecioCOP</code> y los umbrales |Δ compra|).</p>"
        "</div>",
        unsafe_allow_html=True,
    )
    k1, k2, k3, k4, k5 = st.columns(5, gap="small")
    k1.metric(
        "Refs. distintas",
        f"{total_refs:,.0f}",
        help="Referencias únicas (número de valores distintos de Referencia) en el conjunto filtrado.",
    )
    k2.metric(
        "Semáforo crítico",
        f"{criticos:,.0f}",
        help="Cuenta referencias con categoría **Crítico** (última compra ajustada muy lejos del costo min/max vs el resto del lote, y % ≥ 30 %). "
        "Detalle de reglas: desplegable **📖 Cómo se calcula el semáforo** arriba.",
    )
    k3.metric(
        "% ≥ umbral |Δ compra|",
        f"{pct_umbral_compra:,.1f}%",
        help=f"Porcentaje de filas con |variación compra| ≥ {umbral_var_compra:g} % (configurado arriba).",
    )
    k4.metric(
        "% ≥ umbral |Δ vs costo|",
        f"{pct_umbral_costo:,.1f}%",
        help=f"Porcentaje de filas con |variación vs costo inv.| ≥ {umbral_var_costo:g} % (configurado arriba).",
    )
    k5.metric(
        "Valor inv. expuesto (COP)",
        _fmt_cop_resumido(valor_inv_cop) if valor_inv_cop is not None else "N/D",
        help="Suma del costo de inventario en el conjunto filtrado: Σ(Existencia_Intermedio × Costo_Intermedio), "
        "o columna Valor_Inventario si existiera. Indica cuánto ‘dinero en costo’ hay detrás de las alertas actuales.",
    )

    df_fil["_score_alerta"] = (
        0.55 * pd.to_numeric(df_fil["_abs_var_compra"], errors="coerce").fillna(0)
        + 0.45 * pd.to_numeric(df_fil["_abs_var_costo"], errors="coerce").fillna(0)
    )

    # Etiquetas de negocio para columnas visibles en tablas.
    alias_cols = BUSINESS_LABELS

    # Vista única ordenada por riesgo (reutilizada en táctica y operativa).
    df_vista = df_fil.sort_values("_score_alerta", ascending=False)

    aud_tab_pri, aud_tab_graf = st.tabs(["Vista principal", "Reporte gráfico"])
    with aud_tab_pri:
        subtabs = st.tabs(
            ["Vista estratégica", "Vista táctica", "Vista operativa", "Segmentación"]
        )
        with subtabs[0]:
            st.markdown("##### Vista estratégica — identificar y priorizar")
            st.caption(
                "**Objetivo:** localizar referencias con **dos tipos de problema de precio**: (1) salto entre **última y penúltima** compra, "
                "(2) desalineación de la **última compra** frente al **costo prom. inventario**. "
                "El **score** combina ambos (ver ayuda en columna). **Top N** por score descendente."
            )
            top_n = st.number_input(
                "Top N",
                min_value=10,
                max_value=300,
                value=50,
                step=10,
                key="aud_refs_top_n",
            )
            ref_alt_col = lower_map.get("referencias_alternas")
            precio_lista_col = lower_map.get("precio_lista_09")
            costo_min_c = lower_map.get("costo_min")
            costo_max_c = lower_map.get("costo_max")
            ex_tot_resumen = (
                lower_map.get("existencia_total")
                if lower_map.get("existencia_total") in df_fil.columns
                else (
                    "_Existencia_suma_niveles"
                    if "_Existencia_suma_niveles" in df_fil.columns
                    else None
                )
            )
            vpc_sql = lower_map.get("var_preciocop")
            vusd_sql = lower_map.get("var_preciousd")
            # Vista estratégica: identificar → priorizar (score) → problema 1 (última vs penúltima) → problema 2 (vs costo inv.) → magnitud.
            cols_resumen = _auditoria_columns_in_order(
                df_fil,
                [
                    ref_col,
                    ref_alt_col,
                    desc_col,
                    sem_col,
                    "_score_alerta",
                    dias_col,
                    var_compra_col,
                    vusd_sql if vusd_sql and vusd_sql != var_compra_col else None,
                    vpc_sql if vpc_sql and vpc_sql != var_compra_col else None,
                    "_abs_var_compra",
                    costo_inv_col,
                    # No incluir absvar_costo_pct: mismo alias que _abs_var_costo en BUSINESS_LABELS → duplicado en Arrow.
                    "_abs_var_costo",
                    lower_map.get("var_costomin_preciocop"),
                    precio_lista_col,
                    ex_tot_resumen,
                    costo_min_c,
                    costo_max_c,
                    sistema_precio_col,
                    modelo_col,
                    lower_map.get("margen_objetivo_sistema"),
                ],
            )
            df_top = df_fil.sort_values("_score_alerta", ascending=False).head(int(top_n))[cols_resumen].copy()
            df_top = _auditoria_coerce_display_dtypes(df_top)
            df_top = df_top.rename(columns=alias_cols)
            df_top = _hacer_columnas_unicas(df_top)
            _auditoria_df_map_semaforo_ui(df_top, sem_col, alias_cols)
            top_config = _auditoria_build_column_config(
                [c for c in cols_resumen if c in df_fil.columns],
                alias_cols,
            )
            top_config = {k: v for k, v in top_config.items() if k in df_top.columns}
    
            st.caption(
                "**Orden:** identificación → **score** → **problema 1** (días, var. precios entre compras, |Δ compra|) → "
                "**problema 2** (costo prom. inv., |Δ vs costo|) → magnitud (lista, stock, costos) → contexto."
            )
            st.dataframe(
                df_top,
                width="stretch",
                height=420,
                hide_index=True,
                column_config=top_config,
            )
    
        with subtabs[1]:
            st.markdown("##### Vista táctica — diagnosticar cada referencia")
            st.caption(
                "**Una fila = una referencia**. Por defecto ves un **bloque corto** (identificación, **semáforo**, **score**, "
                "variación entre compras, **costo prom. inv.**, Δ vs costo SQL, |Δ| de la app, **última compra COP** y **stock**). "
                "Usa **Solo columnas estratégicas** para reset rápido o la lista para añadir/quitar sin recargar toda la pestaña (requiere Streamlit ≥1.33)."
            )
            orden_cols = _auditoria_column_order_full(df_vista, lower_map, costo_inv_col)
            df_full = df_vista[orden_cols].copy()
            df_full = _auditoria_coerce_display_dtypes(df_full)
            df_full = df_full.rename(columns=alias_cols)
            df_full = _hacer_columnas_unicas(df_full)
            _auditoria_df_map_semaforo_ui(df_full, sem_col, alias_cols)
            cfg_full = _auditoria_build_column_config(orden_cols, alias_cols)
            cfg_full = {k: v for k, v in cfg_full.items() if k in df_full.columns}

            all_lbl = list(df_full.columns)
            orig_est = _auditoria_tactica_columnas_estrategicas_originales(
                df_vista, lower_map, costo_inv_col
            )
            default_lbl: list[str] = []
            for o in orig_est:
                lbl = alias_cols.get(o, o)
                if lbl in df_full.columns:
                    default_lbl.append(lbl)
            if not default_lbl:
                default_lbl = all_lbl[: min(12, len(all_lbl))]

            @_streamlit_fragment_optional()
            def _aud_vista_tactica_selector_y_tabla() -> None:
                if st.button(
                    "↺ Solo columnas estratégicas",
                    key="aud_tac_reset_strategic",
                    help="Restaura el conjunto corto inicial (sin abrir el listado completo de columnas).",
                ):
                    st.session_state["aud_tactica_cols_multiselect_v2"] = list(default_lbl)

                sel = st.multiselect(
                    "Columnas visibles (marca para añadir o quitar)",
                    options=all_lbl,
                    default=default_lbl,
                    key="aud_tactica_cols_multiselect_v2",
                    help="Con Streamlit 1.33+, solo este bloque se vuelve a dibujar al cambiar la selección.",
                )
                if not sel:
                    st.warning("Selecciona al menos una columna; se restaura la vista estratégica por defecto.")
                    sel = list(default_lbl)
                orden_sel = [c for c in df_full.columns if c in sel]
                df_tactica_show = df_full[orden_sel]
                cfg_tactica = {k: v for k, v in cfg_full.items() if k in sel}

                st.dataframe(
                    df_tactica_show,
                    width="stretch",
                    height=680,
                    hide_index=True,
                    column_config=cfg_tactica,
                )

            _aud_vista_tactica_selector_y_tabla()
    
        with subtabs[2]:
            st.markdown("##### Vista operativa — revisar y exportar")
            st.caption(
                "**Misma priorización** que la vista táctica (orden por **score**), pero en **tablas angostas** para trabajar sin "
                "tanto scroll. Flujo recomendado para el analista: **política del ítem** → **riesgo (semáforo/score)** → "
                "**salto entre compras** → **vs inventario** → **exposición en stock y márgenes** → **evidencia de facturas** → "
                "**ajuste logístico COP**. Al final, **CSV** para Excel o compras."
            )
            with st.expander("Guía rápida — qué mirar en cada bloque", expanded=False):
                st.markdown(
                    """
1. **Identificación** — ¿A qué segmento pertenece la ref. y qué margen objetivo tiene?
2. **Semáforo / score** — ¿Qué tan urgente es frente al resto del filtro?
3. **Problema 1** — ¿El precio saltó entre las dos últimas compras (USD, COP, TRM)?
4. **Problema 2** — ¿La última compra está lejos del costo promedio de inventario?
5. **Bodegas y lista** — ¿Cuánto stock y margen hay en juego?
6. **Evidencia última / penúltima** — ¿Quién vendió, a qué precio y en qué fecha?
7. **Factores logísticos** — ¿El COP ajustado por origen explica parte del desvío?
"""
                )
            st.caption(
                "En **cada tabla**, las primeras columnas son **Ref.**, **Refs alternas** y **Descripción** cuando existan; "
                "luego las columnas específicas del bloque."
            )
            bloques = _auditoria_vista_bloques(lower_map)
            ref_alt_col = lower_map.get("referencias_alternas")
            bloque_visible_idx = 0
            for bi, (titulo, ctx_bloque, keys) in enumerate(bloques):
                cols_b: list[str] = []
                for ky in keys:
                    if ky == "_existencia_suma_niveles":
                        coln = "_Existencia_suma_niveles"
                    else:
                        coln = lower_map.get(ky)
                    if coln and coln in df_vista.columns and coln not in cols_b:
                        cols_b.append(coln)
                # Cada tabla operativa: Ref + Refs alternas + Descripción al inicio (contexto fijo).
                id_parts = [c for c in (ref_col, ref_alt_col, desc_col) if c and c in df_vista.columns]
                p2_front: list[str] = []
                if titulo.startswith("Problema 2") and costo_inv_col and costo_inv_col in df_vista.columns:
                    p2_front = [costo_inv_col]
                body = [c for c in cols_b if c not in id_parts and c not in p2_front]
                cols_b = id_parts + p2_front + body
                if not cols_b:
                    continue
                bloque_visible_idx += 1
                df_b = df_vista[cols_b].copy()
                df_b = _auditoria_coerce_display_dtypes(df_b)
                df_b = df_b.rename(columns=alias_cols)
                df_b = _hacer_columnas_unicas(df_b)
                _auditoria_df_map_semaforo_ui(df_b, sem_col, alias_cols)
                cfg_b = _auditoria_build_column_config(cols_b, alias_cols)
                cfg_b = {k: v for k, v in cfg_b.items() if k in df_b.columns}
                st.markdown(f"**Bloque {bloque_visible_idx} — {titulo}**")
                st.caption(ctx_bloque)
                st.dataframe(
                    df_b,
                    width="stretch",
                    height=320,
                    hide_index=True,
                    column_config=cfg_b,
                )
                if bi < len(bloques) - 1:
                    st.divider()
    
            st.divider()
            st.markdown("##### Exportar")
            st.caption("Descarga el conjunto **filtrado** con columnas en orden BI (UTF-8 con BOM para Excel).")
            cols_exp = _auditoria_column_order_full(df_vista, lower_map, costo_inv_col)
            df_exp = df_vista[cols_exp].copy()
            df_exp = _auditoria_coerce_display_dtypes(df_exp)
            df_exp = df_exp.rename(columns=alias_cols)
            df_exp = _hacer_columnas_unicas(df_exp)
            _auditoria_df_map_semaforo_ui(df_exp, sem_col, alias_cols)
            buf = io.StringIO()
            df_exp.to_csv(buf, index=False, encoding="utf-8-sig")
            st.download_button(
                label="Descargar CSV",
                data=buf.getvalue(),
                file_name="auditoria_referencias_filtrada.csv",
                mime="text/csv; charset=utf-8",
                type="primary",
                key="aud_refs_download_csv",
            )
            st.metric("Filas a exportar", f"{len(df_exp):,}")
            st.caption(f"Columnas: **{len(df_exp.columns)}**")
    
        with subtabs[3]:
            st.markdown("##### Segmentación — dónde se concentran los problemas")
            st.caption(
                "**Corte transversal** del mismo filtro: agrupa por semáforo, sistema o modelo para ver **en qué segmentos** "
                "hay más filas afectadas y **mayor score máximo** (prioridad de revisión por categoría)."
            )
            dims = [c for c in [sem_col, sistema_precio_col, modelo_col] if c]
            if not dims:
                st.caption("No hay dimensiones disponibles para segmentar.")
            else:
                d1, d2 = st.columns([2, 1], gap="large")
                with d1:
                    dim_sel = st.selectbox("Dimensión", options=dims)
                with d2:
                    top_seg = st.number_input(
                        "Top grupos",
                        min_value=5,
                        max_value=50,
                        value=15,
                        step=5,
                        key="aud_refs_top_grupos",
                    )
                df_seg = df_fil.copy()
                df_seg[dim_sel] = df_seg[dim_sel].fillna("SIN_DATO").astype(str)
                agg = (
                    df_seg.groupby(dim_sel, dropna=False)
                    .agg(
                        Registros=(dim_sel, "count"),
                        Referencias=(ref_col, "nunique") if ref_col else (dim_sel, "count"),
                        Promedio_variacion_abs_compra_pct=("_abs_var_compra", "mean"),
                        Promedio_variacion_abs_vs_costo_pct=("_abs_var_costo", "mean"),
                        Score_alerta_maximo=("_score_alerta", "max"),
                    )
                    .reset_index()
                    .sort_values(by="Score_alerta_maximo", ascending=False)
                    .head(int(top_seg))
                )
                # Orden: dimensión → volumen → riesgo (score máx.) → intensidad media de variaciones.
                _seg_order = [
                    dim_sel,
                    "Registros",
                    "Referencias",
                    "Score_alerta_maximo",
                    "Promedio_variacion_abs_compra_pct",
                    "Promedio_variacion_abs_vs_costo_pct",
                ]
                agg = agg[[c for c in _seg_order if c in agg.columns]]
                if sem_col and dim_sel == sem_col:
                    agg[dim_sel] = agg[dim_sel].map(_auditoria_etiqueta_semaforo_ui)
                st.dataframe(
                    agg,
                    width="stretch",
                    hide_index=True,
                    column_config={
                        dim_sel: st.column_config.TextColumn(_label_negocio(dim_sel), width="medium"),
                        "Registros": st.column_config.NumberColumn("Registros (filas)", format="%,d"),
                        "Referencias": st.column_config.NumberColumn("Referencias únicas", format="%,d"),
                        "Score_alerta_maximo": st.column_config.NumberColumn("Score alerta máx.", format="%.2f"),
                        "Promedio_variacion_abs_compra_pct": st.column_config.NumberColumn(
                            "Prom. |Δ compra| (%)", format="%.2f%%"
                        ),
                        "Promedio_variacion_abs_vs_costo_pct": st.column_config.NumberColumn(
                            "Prom. |Δ vs costo| (%)", format="%.2f%%"
                        ),
                    },
                )

    with aud_tab_graf:
        st.markdown("##### Reporte gráfico")
        graf_subtabs = st.tabs(
            [
                "Eje 1 — Semáforo (alineación vs costos)",
                "Eje 2 — Variación compra (última vs penúltima)",
            ]
        )
        with graf_subtabs[0]:
            st.caption(
                f"Todas las refs filtradas ({len(df_base):,}) — **sin** filtro de umbrales. "
                "El semáforo evalúa la distribución completa."
            )
            _auditoria_charts_semaforo_st(df_base, ctx)
        with graf_subtabs[1]:
            if df_fil.empty:
                st.info("No hay refs que superen los umbrales. Ajusta los sliders o desmarca «Solo variación fuerte».")
            else:
                solo_activo = bool(st.session_state.get("aud_solo_significativas", False))
                if solo_activo:
                    st.caption(
                        f"**{len(df_fil):,}** refs que superan al menos un umbral "
                        f"(|Δ compra| ≥ {float(st.session_state.get('aud_umbral_var_compra', 20)):.0f}% "
                        f"o |Δ costo| ≥ {float(st.session_state.get('aud_umbral_var_costo', 15)):.0f}%) "
                        f"de {len(df_base):,} filtradas."
                    )
                else:
                    st.caption(
                        f"Todas las refs filtradas ({len(df_fil):,}) — «Solo variación fuerte» desactivado. "
                        "Actívalo para enfocarte en refs con saltos de precio."
                    )
                _auditoria_charts_variacion_st(df_fil, ctx)

    save_tab_filter_prefs("auditoria_refs")


def _render_tab_laboratorio_sql() -> None:
    st.caption("Ejecuta consultas SQL de lectura sobre DuckDB (`pipeline_read.duckdb`).")
    st.caption("Permitido: SELECT / WITH / SHOW / DESCRIBE / EXPLAIN.")
    default_sql = (
        "SELECT Referencia, Descripcion, Semaforo_Variacion, ABSVar_Costo_Pct\n"
        "FROM auditoria_raw\n"
        "ORDER BY ABSVar_Costo_Pct DESC\n"
        "LIMIT 200"
    )
    st.session_state.setdefault("lab_sql_query", default_sql)
    st.session_state.setdefault("lab_sql_limit", 2000)
    st.session_state.setdefault("lab_sql_result", None)

    sql_txt = st.text_area(
        "Consulta SQL",
        key="lab_sql_query",
        height=180,
        placeholder="Ej: SELECT * FROM auditoria_raw LIMIT 100",
    )
    c1, c2, c3 = st.columns([1.1, 1.2, 2.7], gap="small")
    with c1:
        run_sql = st.button("Ejecutar SQL", type="primary", width="stretch")
    with c2:
        st.number_input("Máx filas", min_value=50, max_value=10000, step=50, key="lab_sql_limit")
    with c3:
        st.caption(
            "Tablas sugeridas: `auditoria_raw`, `margen_siesa_raw`, "
            "`atributos_referencias_raw`, `resultado_precios_lista`, `origen_precios_tablero`."
        )

    if run_sql:
        try:
            out = ejecutar_sql_laboratorio(sql_txt, limite=int(st.session_state["lab_sql_limit"]))
            st.session_state["lab_sql_result"] = out
            st.success(f"Consulta ejecutada. Filas devueltas: {len(out):,}.")
        except Exception as exc:
            st.session_state["lab_sql_result"] = None
            st.error(f"Error SQL: {exc}")

    df_res = st.session_state.get("lab_sql_result")
    if isinstance(df_res, pd.DataFrame):
        if df_res.empty:
            st.info("La consulta no devolvió filas.")
        else:
            st.dataframe(_renombrar_negocio(df_res), width="stretch", hide_index=True)


@st.cache_data(show_spinner=False, ttl=300)
def _cargar_dimensiones_ventas_cached() -> dict:
    return obtener_dimensiones_ventas()


@st.cache_data(show_spinner=False, ttl=180, max_entries=30)
def _cargar_dashboard_ventas_cached(
    sedes: tuple[str, ...],
    sistemas: tuple[str, ...],
    rotaciones: tuple[str, ...],
    unidades: tuple[str, ...],
    clientes: tuple[str, ...],
    vendedores: tuple[str, ...],
    lineas: tuple[str, ...],
    modelos: tuple[str, ...],
    anios: tuple[int, ...],
    meses: tuple[int, ...],
) -> dict:
    return obtener_dashboard_ventas(
        sedes=list(sedes),
        sistemas_precio=list(sistemas),
        rotaciones=list(rotaciones),
        unidades_negocio=list(unidades),
        clientes=list(clientes),
        vendedores=list(vendedores),
        lineas=list(lineas),
        modelos=list(modelos),
        anios=list(anios),
        meses=list(meses),
    )


@st.cache_data(show_spinner=False, ttl=180, max_entries=30)
def _cargar_detalle_ventas_cached(
    sedes: tuple[str, ...],
    sistemas: tuple[str, ...],
    rotaciones: tuple[str, ...],
    unidades: tuple[str, ...],
    clientes: tuple[str, ...],
    vendedores: tuple[str, ...],
    lineas: tuple[str, ...],
    modelos: tuple[str, ...],
    anios: tuple[int, ...],
    meses: tuple[int, ...],
    limite: int | None = 200_000,
) -> pd.DataFrame:
    return obtener_detalle_ventas_filtrado(
        sedes=list(sedes),
        sistemas_precio=list(sistemas),
        rotaciones=list(rotaciones),
        unidades_negocio=list(unidades),
        clientes=list(clientes),
        vendedores=list(vendedores),
        lineas=list(lineas),
        modelos=list(modelos),
        anios=list(anios),
        meses=list(meses),
        limite=limite,
    )


def _fmt_num_local(value: float | int | None, decimals: int = 0, signed: bool = False) -> str:
    if value is None or pd.isna(value):
        return "—"
    try:
        n = float(value)
    except Exception:
        return "—"
    sign = "+" if signed and n > 0 else ""
    if decimals <= 0:
        txt = f"{abs(n):,.0f}"
    else:
        txt = f"{abs(n):,.{int(decimals)}f}"
    txt = txt.replace(",", "_").replace(".", ",").replace("_", ".")
    pref = "-" if n < 0 else sign
    return f"{pref}{txt}"


def _fmt_money_local(value: float | int | None, signed: bool = False) -> str:
    base = _fmt_num_local(value, decimals=0, signed=signed)
    if base == "—":
        return base
    return f"$ {base}"


def _fmt_pct_local(value: float | int | None, decimals: int = 2, signed: bool = False) -> str:
    if value is None or pd.isna(value):
        return "—"
    try:
        p = float(value) * 100.0
    except Exception:
        return "—"
    return f"{_fmt_num_local(p, decimals=decimals, signed=signed)}%"


def _ventas_formato_comparativo_dim(
    df: pd.DataFrame,
    nombre_col_dim: str,
    y_base: int,
    y_comp: int,
    *,
    top_n: int = 35,
) -> tuple[pd.DataFrame, dict[str, Any]] | tuple[None, None]:
    """
    Mismo formato que la tabla por líneas/mes: Venta/Margen año base, Venta/Margen año comp,
    AV, %V, %M. Filas = una por dimensión (sin desglose mensual). Opcional top por venta año comp.
    """
    if df is None or df.empty or "dim" not in df.columns:
        return None, None
    g = df.copy()
    for c in ("anio", "venta", "margen"):
        if c in g.columns:
            g[c] = pd.to_numeric(g[c], errors="coerce")
    g["dim"] = g["dim"].astype(str)
    g = g[g["anio"].isin([y_base, y_comp])]
    if g.empty:
        return None, None

    gb = (
        g.groupby(["dim", "anio"], dropna=False)
        .agg(venta=("venta", "sum"))
        .reset_index()
    )
    util = (
        g.assign(_u=lambda x: x["venta"] * x["margen"])
        .groupby(["dim", "anio"], dropna=False)["_u"]
        .sum()
        .reset_index(name="utilidad_sum")
    )
    gb = gb.merge(util, on=["dim", "anio"], how="left")
    gb["margen"] = gb.apply(
        lambda r: (r["utilidad_sum"] / r["venta"]) if r["venta"] and pd.notna(r["venta"]) and r["venta"] != 0 else None,
        axis=1,
    )

    base = gb[gb["anio"] == y_base].rename(
        columns={"venta": f"Venta {y_base}", "margen": f"Margen {y_base}"}
    )[["dim", f"Venta {y_base}", f"Margen {y_base}"]]
    comp = gb[gb["anio"] == y_comp].rename(
        columns={"venta": f"Venta {y_comp}", "margen": f"Margen {y_comp}"}
    )[["dim", f"Venta {y_comp}", f"Margen {y_comp}"]]

    merged = pd.merge(base, comp, on="dim", how="outer")
    merged["AV"] = merged[f"Venta {y_comp}"] - merged[f"Venta {y_base}"]
    vb = merged[f"Venta {y_base}"].replace({0: pd.NA})
    merged["%V"] = merged["AV"] / vb
    merged["%M"] = merged[f"Margen {y_comp}"] - merged[f"Margen {y_base}"]
    merged = merged.rename(columns={"dim": nombre_col_dim})

    # Top N por venta año comparación (como en Power BI)
    vc = f"Venta {y_comp}"
    if vc in merged.columns and top_n and top_n > 0:
        orden = merged.sort_values(vc, ascending=False, na_position="last").head(int(top_n))
    else:
        orden = merged.sort_values(nombre_col_dim, ascending=True)

    # Fila Total (ponderado)
    vb_col, mb_col = f"Venta {y_base}", f"Margen {y_base}"
    vc_col, mc_col = f"Venta {y_comp}", f"Margen {y_comp}"
    tot_vb = float(orden[vb_col].fillna(0).sum()) if vb_col in orden.columns else 0.0
    tot_vc = float(orden[vc_col].fillna(0).sum()) if vc_col in orden.columns else 0.0
    tot_mb = None
    tot_mc = None
    if vb_col in orden.columns and mb_col in orden.columns and tot_vb:
        tot_mb = float(
            (orden[vb_col].fillna(0) * orden[mb_col].fillna(0)).sum() / tot_vb
        )
    if vc_col in orden.columns and mc_col in orden.columns and tot_vc:
        tot_mc = float(
            (orden[vc_col].fillna(0) * orden[mc_col].fillna(0)).sum() / tot_vc
        )
    tot_av = tot_vc - tot_vb
    tot_pv = (tot_av / tot_vb) if tot_vb else None
    tot_pm = (tot_mc - tot_mb) if tot_mb is not None and tot_mc is not None else None

    fila_total = {nombre_col_dim: "Total", vb_col: tot_vb, mb_col: tot_mb, vc_col: tot_vc, "AV": tot_av, "%V": tot_pv, mc_col: tot_mc, "%M": tot_pm}
    show = pd.concat([orden, pd.DataFrame([fila_total])], ignore_index=True)

    fmt = {
        vb_col: _fmt_money_local,
        vc_col: _fmt_money_local,
        "AV": lambda x: _fmt_money_local(x, signed=True),
        "%V": lambda x: _fmt_pct_local(x, decimals=2, signed=True),
        mb_col: _fmt_pct_local,
        mc_col: _fmt_pct_local,
        "%M": lambda x: _fmt_pct_local(x, decimals=2, signed=True),
    }
    return show, fmt


def _ventas_modelo_opciones_filtradas(todas: list[str], texto: str) -> list[str]:
    """Reduce la lista de modelos a los que contienen `texto` (insensible a mayúsculas)."""
    if not todas:
        return []
    t = (texto or "").strip()
    if not t:
        return list(todas)
    tu = t.upper()
    return [m for m in todas if tu in str(m).upper()]


def _render_tab_resumen_ventas() -> None:
    st.markdown(
        """
<style>
.rv-hero{
  border:1px solid #2a3858;
  border-radius:14px;
  padding:14px 16px;
  background:linear-gradient(135deg,#0f1c37 0%,#101a31 55%,#0c162b 100%);
  margin-bottom:10px;
}
.rv-hero h3{
  margin:0;
  font-size:1.1rem;
  color:#e5e7eb;
}
.rv-hero p{
  margin:4px 0 0;
  color:#a3b0c6;
  font-size:.9rem;
}
.rv-section{
  margin:.45rem 0 .25rem;
  padding-left:.55rem;
  border-left:3px solid #38bdf8;
}
.rv-section b{color:#e5e7eb;}
.rv-section span{color:#9fb0cc;font-size:.86rem;}
.rv-chip-wrap{display:flex;flex-wrap:wrap;gap:6px;margin:.25rem 0 .55rem;}
.rv-chip{
  background:#101a31;border:1px solid #2a3858;color:#d1d5db;
  border-radius:999px;padding:3px 10px;font-size:.78rem;
}
div[data-testid="stMetric"]{
  border:1px solid #25314d;
  border-radius:10px;
  padding:.35rem .5rem;
  background:#0f1a30;
}
</style>
""",
        unsafe_allow_html=True,
    )
    st.markdown(
        """
<div class="rv-hero">
  <h3>Resumen de ventas</h3>
  <p>Análisis comercial sobre <code>ventas_raw</code> (pipeline 02), con enfoque ejecutivo y comparativo.</p>
</div>
""",
        unsafe_allow_html=True,
    )

    dims = _cargar_dimensiones_ventas_cached() or {}
    st.markdown('<div class="rv-section"><b>Filtros del tablero</b><br><span>Refina la vista por canal, producto, cliente, año y mes.</span></div>', unsafe_allow_html=True)
    with st.container(border=True):
        a1, a2 = st.columns([1, 1])
        with a1:
            if st.button("Limpiar filtros", key="ventas_clear_filters", use_container_width=True):
                for k in (
                    "ventas_sede_sel", "ventas_sys_sel", "ventas_rot_sel", "ventas_un_sel", "ventas_cli_sel",
                    "ventas_ven_sel", "ventas_linea_sel", "ventas_modelo_sel", "ventas_anio_sel", "ventas_mes_sel",
                ):
                    st.session_state[k] = []
                st.session_state["ventas_modelo_buscar_txt"] = ""
                st.rerun()
        with a2:
            st.button("Actualizar vista", key="ventas_refresh", use_container_width=True)

        c1, c2, c3, c4, c5, c6 = st.columns(6)
        with c1:
            sedes_sel = st.multiselect("Sede", options=dims.get("sede", []), default=[], key="ventas_sede_sel")
        with c2:
            sys_sel = st.multiselect("Sistema Precio", options=dims.get("sistema_precio", []), default=[], key="ventas_sys_sel")
        with c3:
            rot_sel = st.multiselect(
                "Rotación",
                options=dims.get("rotacion", []),
                default=[],
                key="ventas_rot_sel",
                help="Criterio plan 08: mismo formato que **margen SIESA** (`código - descripción`). Tras cambiar el pipeline, vuelve a cargar ventas.",
            )
        with c4:
            un_sel = st.multiselect("Descrip. UN", options=dims.get("descrip_un", []), default=[], key="ventas_un_sel")
        with c5:
            cli_sel = st.multiselect("Cliente", options=dims.get("cliente", []), default=[], key="ventas_cli_sel")
        with c6:
            ven_sel = st.multiselect("Vendedor", options=dims.get("vendedor", []), default=[], key="ventas_ven_sel")

        c7, c8, c9, c10 = st.columns(4)
        with c7:
            linea_sel = st.multiselect("Línea", options=dims.get("linea", []), default=[], key="ventas_linea_sel")
        with c8:
            modelo_buscar_txt = st.text_input(
                "Filtrar modelo (texto)",
                value="",
                key="ventas_modelo_buscar_txt",
                placeholder="Ej: 9900, MAGNUM...",
                help="Escribe parte del código o de la descripción; el desplegable **Modelo** solo mostrará coincidencias.",
            )
        with c9:
            modelo_todas = dims.get("modelo", []) or []
            modelo_opts = _ventas_modelo_opciones_filtradas(modelo_todas, modelo_buscar_txt)
            if (modelo_buscar_txt or "").strip() and not modelo_opts:
                st.caption("Ningún modelo coincide con el texto. Ajusta el filtro o bórralo para ver todos.")
            modelo_sel = st.multiselect(
                "Modelo",
                options=modelo_opts,
                default=[],
                key="ventas_modelo_sel",
                help="Criterio plan 03 (misma lógica que Modelo CNH). Usa **Filtrar modelo** para acotar por texto antes de elegir.",
            )
        with c10:
            anios_opts = [int(x) for x in (dims.get("anio", []) or []) if str(x).strip().isdigit()]
            anios_sel = st.multiselect("Año", options=anios_opts, default=anios_opts[-2:] if len(anios_opts) >= 2 else anios_opts, key="ventas_anio_sel")

        meses_opts = [int(x) for x in (dims.get("mes", []) or []) if str(x).strip().isdigit()]
        meses_sel = st.multiselect(
            "Mes",
            options=meses_opts,
            default=meses_opts,
            key="ventas_mes_sel",
            help="Selecciona uno o varios meses para comparar entre años.",
        )

    resumen_filtros = [
        f"Años: {len(anios_sel)}",
        f"Meses: {len(meses_sel)}",
        f"Sedes: {len(sedes_sel)}",
        f"Sistemas: {len(sys_sel)}",
        f"Clientes: {len(cli_sel)}",
        f"Modelos: {len(modelo_sel)}",
    ]
    chips_html = "".join([f'<span class="rv-chip">{x}</span>' for x in resumen_filtros])
    st.markdown(f'<div class="rv-chip-wrap">{chips_html}</div>', unsafe_allow_html=True)

    dash = _cargar_dashboard_ventas_cached(
        tuple(sedes_sel),
        tuple(sys_sel),
        tuple(rot_sel),
        tuple(un_sel),
        tuple(cli_sel),
        tuple(ven_sel),
        tuple(linea_sel),
        tuple(modelo_sel),
        tuple(int(x) for x in anios_sel),
        tuple(int(x) for x in meses_sel),
    )

    # Para el gráfico mensual, ignoramos filtro de mes:
    # si se eligen años 2025 y 2026, muestra todo 2025 + todo 2026.
    dash_graph = _cargar_dashboard_ventas_cached(
        tuple(sedes_sel),
        tuple(sys_sel),
        tuple(rot_sel),
        tuple(un_sel),
        tuple(cli_sel),
        tuple(ven_sel),
        tuple(linea_sel),
        tuple(modelo_sel),
        tuple(int(x) for x in anios_sel),
        tuple(),  # sin filtro de mes
    )

    # Gráfico: sin filtro de mes (todos los meses de los años elegidos).
    ts_graph = dash_graph.get("timeseries_mes") if isinstance(dash_graph, dict) else None
    if ts_graph is None or ts_graph.empty:
        st.info("No hay datos para los filtros seleccionados (o no existe `ventas_raw`).")
        return

    ts2 = ts_graph.copy()
    for c in ("venta", "utilidad", "margen"):
        if c in ts2.columns:
            ts2[c] = pd.to_numeric(ts2[c], errors="coerce")
    if "mes_inicio" in ts2.columns:
        try:
            ts2["mes_inicio"] = pd.to_datetime(ts2["mes_inicio"])
        except Exception:
            pass

    # KPIs: meses seleccionados solo del **último año** (max entre años elegidos; si no hay años, el año más reciente en datos).
    ts_kpi = dash.get("timeseries_mes") if isinstance(dash, dict) else None
    anios_int = [int(x) for x in anios_sel]
    ultimo_anio: int | None = max(anios_int) if anios_int else None
    if ts_kpi is not None and not ts_kpi.empty:
        ts_k = ts_kpi.copy()
        for c in ("venta", "utilidad", "margen"):
            if c in ts_k.columns:
                ts_k[c] = pd.to_numeric(ts_k[c], errors="coerce")
        if "anio" in ts_k.columns:
            ts_k["anio"] = pd.to_numeric(ts_k["anio"], errors="coerce")
            if ultimo_anio is None:
                try:
                    ultimo_anio = int(ts_k["anio"].dropna().max())
                except Exception:
                    ultimo_anio = None
            if ultimo_anio is not None:
                ts_k = ts_k[ts_k["anio"] == ultimo_anio]
        total_venta = float(ts_k["venta"].fillna(0).sum()) if "venta" in ts_k.columns else 0.0
        total_util = float(ts_k["utilidad"].fillna(0).sum()) if "utilidad" in ts_k.columns else 0.0
    else:
        total_venta = 0.0
        total_util = 0.0
    total_margen = (total_util / total_venta) if total_venta else None

    kpi_suffix = " (Último año - Meses seleccionados)"
    k1, k2, k3 = st.columns(3)
    with k1:
        st.metric(f"Venta{kpi_suffix}", _fmt_money_local(total_venta))
    with k2:
        st.metric(f"Utilidad{kpi_suffix}", _fmt_money_local(total_util))
    with k3:
        st.metric(f"Margen ponderado{kpi_suffix}", _fmt_pct_local(total_margen))

    st.markdown('<div class="rv-section"><b>Evolución mensual</b><br><span>Lectura de tendencia en venta, utilidad y margen.</span></div>', unsafe_allow_html=True)

    if _HAS_PLOTLY:
        import plotly.graph_objects as go  # type: ignore[import-not-found]

        fig = go.Figure()
        fig.add_trace(
            go.Bar(
                x=ts2.get("mes_inicio"),
                y=ts2.get("venta"),
                name="Venta",
                marker_color="#ef4444",
                opacity=0.9,
            )
        )
        fig.add_trace(
            go.Bar(
                x=ts2.get("mes_inicio"),
                y=ts2.get("utilidad"),
                name="Utilidad",
                marker_color="#9ca3af",
                opacity=0.9,
            )
        )
        if "margen" in ts2.columns:
            fig.add_trace(
                go.Scatter(
                    x=ts2.get("mes_inicio"),
                    y=(ts2["margen"] * 100.0),
                    name="Margen (%)",
                    mode="lines+markers",
                    yaxis="y2",
                    line=dict(color="#38bdf8", width=2),
                )
            )
        fig.update_layout(
            barmode="group",
            xaxis_title="Mes",
            yaxis_title="Valor",
            yaxis2=dict(title="Margen (%)", overlaying="y", side="right", rangemode="tozero"),
            legend=dict(orientation="h", y=-0.2),
            margin=dict(l=40, r=40, t=35, b=50),
            height=420,
        )
        st.plotly_chart(fig, use_container_width=True)

        st.markdown('<div class="rv-section"><b>Comparativo mensual por año</b><br><span>Series de años seleccionados sobre el mismo eje de meses.</span></div>', unsafe_allow_html=True)
        st.caption("Comparativo mensual según los **años** elegidos en el filtro (enero–diciembre).")

        from plotly.subplots import make_subplots  # type: ignore[import-not-found]

        meses_labels = ["ene", "feb", "mar", "abr", "may", "jun", "jul", "ago", "sep", "oct", "nov", "dic"]
        meses_ord = list(range(1, 13))
        _year_colors = ["#9ca3af", "#ef4444", "#38bdf8", "#a855f7", "#22c55e", "#eab308"]

        df_line = ts2.copy()
        if (
            "anio" in df_line.columns
            and "mes" in df_line.columns
            and "venta" in df_line.columns
            and "utilidad" in df_line.columns
        ):
            for c in ("venta", "utilidad", "anio", "mes"):
                df_line[c] = pd.to_numeric(df_line[c], errors="coerce")
            anios_plot = sorted([int(x) for x in anios_sel]) if anios_sel else sorted(
                df_line["anio"].dropna().unique().astype(int).tolist()
            )
            if anios_plot:
                df_line = df_line[df_line["anio"].isin(anios_plot)]
                g = df_line.groupby(["anio", "mes"], as_index=False)[["venta", "utilidad"]].sum()
                pv = g.pivot_table(index="mes", columns="anio", values="venta", aggfunc="sum").reindex(meses_ord).fillna(0)
                pu = (
                    g.pivot_table(index="mes", columns="anio", values="utilidad", aggfunc="sum")
                    .reindex(meses_ord)
                    .fillna(0)
                )
                fig2 = make_subplots(
                    rows=2,
                    cols=1,
                    shared_xaxes=True,
                    vertical_spacing=0.12,
                    subplot_titles=("Venta", "Utilidad"),
                )
                n_lineas = 0
                for i, y in enumerate(anios_plot):
                    if y not in pv.columns:
                        continue
                    n_lineas += 1
                    col = _year_colors[i % len(_year_colors)]
                    v_mill = (pv[y] / 1_000_000.0).tolist()
                    u_mill = (pu[y] / 1_000_000.0).tolist() if y in pu.columns else [0.0] * 12
                    fig2.add_trace(
                        go.Scatter(
                            x=meses_labels,
                            y=v_mill,
                            mode="lines+markers",
                            name=str(y),
                            line=dict(color=col, width=2.5),
                            marker=dict(size=7, color=col),
                            legendgroup=str(y),
                            hovertemplate="<b>%{fullData.name}</b><br>%{x}<br>$%{y:,.3f} mill.<extra></extra>",
                        ),
                        row=1,
                        col=1,
                    )
                    fig2.add_trace(
                        go.Scatter(
                            x=meses_labels,
                            y=u_mill,
                            mode="lines+markers",
                            name=str(y),
                            line=dict(color=col, width=2.5),
                            marker=dict(size=7, color=col),
                            legendgroup=str(y),
                            showlegend=False,
                            hovertemplate="<b>%{fullData.name}</b><br>%{x}<br>$%{y:,.3f} mill.<extra></extra>",
                        ),
                        row=2,
                        col=1,
                    )
                if n_lineas == 0:
                    st.info("No hay datos mensuales para los años seleccionados.")
                else:
                    fig2.update_layout(
                        height=640,
                        legend=dict(
                            orientation="h",
                            yanchor="bottom",
                            y=1.14,
                            xanchor="center",
                            x=0.5,
                            bgcolor="rgba(15, 26, 48, 0.7)",
                            bordercolor="rgba(56, 189, 248, 0.35)",
                            borderwidth=1,
                        ),
                        margin=dict(l=50, r=30, t=145, b=40),
                        hovermode="x unified",
                    )
                    for r in (1, 2):
                        fig2.update_yaxes(
                            tickprefix="$",
                            ticksuffix=" mill.",
                            tickformat=",.3f",
                            title_text="Millones COP",
                            gridcolor="rgba(148, 163, 184, 0.35)",
                            griddash="dash",
                            row=r,
                            col=1,
                        )
                    fig2.update_xaxes(title_text="Mes", row=2, col=1)
                    st.plotly_chart(fig2, use_container_width=True)
            else:
                st.caption("No hay años disponibles en los datos para este comparativo.")
        else:
            st.caption("Faltan columnas año/mes en la serie temporal; no se puede dibujar el comparativo.")

    else:
        if "mes_inicio" in ts2.columns:
            try:
                st.line_chart(ts2.set_index("mes_inicio")[["venta", "utilidad"]])
            except Exception:
                st.dataframe(ts2, width="stretch", hide_index=True)

    # Tabla estilo imagen: línea x mes, comparando años seleccionados
    st.markdown('<div class="rv-section"><b>Línea x Mes (comparativo)</b><br><span>Variación de venta y margen entre año base y año comparado.</span></div>', unsafe_allow_html=True)
    df_lm = dash.get("por_linea_mes", pd.DataFrame())
    if isinstance(df_lm, pd.DataFrame) and not df_lm.empty:
        df_lm2 = df_lm.copy()
        for c in ("anio", "mes", "venta", "margen"):
            if c in df_lm2.columns:
                df_lm2[c] = pd.to_numeric(df_lm2[c], errors="coerce")
        if "linea" in df_lm2.columns:
            df_lm2["linea"] = df_lm2["linea"].astype(str)

        # Solo meses/años seleccionados (ya vienen filtrados desde SQL, pero por seguridad).
        if anios_sel:
            df_lm2 = df_lm2[df_lm2["anio"].isin([int(x) for x in anios_sel])]
        if meses_sel:
            df_lm2 = df_lm2[df_lm2["mes"].isin([int(x) for x in meses_sel])]

        # Comparación: base = min(años), comp = max(años)
        yrs = sorted({int(x) for x in df_lm2["anio"].dropna().astype(int).tolist()})
        if len(yrs) >= 2:
            y_base, y_comp = yrs[0], yrs[-1]
            meses_ord = list(range(1, 13))
            meses_map = {1: "ene", 2: "feb", 3: "mar", 4: "abr", 5: "may", 6: "jun", 7: "jul", 8: "ago", 9: "sep", 10: "oct", 11: "nov", 12: "dic"}

            # Agregado por línea+mes
            g = (
                df_lm2.groupby(["linea", "mes", "anio"], dropna=False)
                .agg(venta=("venta", "sum"), utilidad=("utilidad", "sum"))
                .reset_index()
            )
            g["margen"] = g.apply(lambda r: (r["utilidad"] / r["venta"]) if r["venta"] else None, axis=1)

            base = g[g["anio"] == y_base].rename(columns={"venta": f"Venta {y_base}", "margen": f"Margen {y_base}"})
            comp = g[g["anio"] == y_comp].rename(columns={"venta": f"Venta {y_comp}", "margen": f"Margen {y_comp}"})
            merged = pd.merge(
                base[["linea", "mes", f"Venta {y_base}", f"Margen {y_base}"]],
                comp[["linea", "mes", f"Venta {y_comp}", f"Margen {y_comp}"]],
                on=["linea", "mes"],
                how="outer",
            )
            merged["AV"] = (merged.get(f"Venta {y_comp}") - merged.get(f"Venta {y_base}"))
            merged["%V"] = merged["AV"] / merged.get(f"Venta {y_base}").replace({0: pd.NA})
            merged["%M"] = (merged.get(f"Margen {y_comp}") - merged.get(f"Margen {y_base}"))  # puntos (proporción)
            merged["Mes"] = merged["mes"].astype("Int64").map(meses_map)

            # Orden de meses y líneas
            merged["_mes_ord"] = merged["mes"].astype("Int64")
            merged = merged.sort_values(["linea", "_mes_ord"], ascending=[True, True])

            show = merged.rename(columns={"linea": "Línea"}).drop(columns=["_mes_ord"], errors="ignore")
            cols_show = ["Línea", "Mes", f"Venta {y_base}", f"Margen {y_base}", f"Venta {y_comp}", "AV", "%V", f"Margen {y_comp}", "%M"]
            cols_show = [c for c in cols_show if c in show.columns]

            fmt = {
                f"Venta {y_base}": _fmt_money_local,
                f"Venta {y_comp}": _fmt_money_local,
                "AV": lambda x: _fmt_money_local(x, signed=True),
                "%V": lambda x: _fmt_pct_local(x, decimals=2, signed=True),
                f"Margen {y_base}": _fmt_pct_local,
                f"Margen {y_comp}": _fmt_pct_local,
                "%M": lambda x: _fmt_pct_local(x, decimals=2, signed=True),
            }
            st.dataframe(
                show[cols_show].style.format(fmt, na_rep="—"),
                width="stretch",
                hide_index=True,
            )
        else:
            st.caption("Selecciona al menos 2 años para ver comparación (ej. 2025 y 2026).")
    else:
        st.caption("Sin datos por línea/mes para los filtros actuales.")

    st.markdown('<div class="rv-section"><b>Comparativos por dimensión</b><br><span>Sede, sistema, rotación, modelo, vendedor y cliente.</span></div>', unsafe_allow_html=True)
    yrs_cmp = sorted(int(x) for x in anios_sel)
    yb_cmp = yrs_cmp[0] if len(yrs_cmp) >= 2 else None
    yc_cmp = yrs_cmp[-1] if len(yrs_cmp) >= 2 else None

    t1, t2 = st.columns(2)
    with t1:
        st.markdown("#### Ventas totales por sede (comparativo)")
        df_sd = dash.get("por_sede", pd.DataFrame())
        if yb_cmp is not None and yc_cmp is not None and isinstance(df_sd, pd.DataFrame) and not df_sd.empty:
            show_sd, fmt_sd = _ventas_formato_comparativo_dim(df_sd, "Sede", yb_cmp, yc_cmp, top_n=25)
            if show_sd is not None and fmt_sd is not None:
                st.dataframe(show_sd.style.format(fmt_sd, na_rep="—"), width="stretch", hide_index=True)
            else:
                st.caption("Sin datos para comparar.")
        elif len(yrs_cmp) < 2:
            st.caption("Selecciona al menos 2 años para ver el comparativo.")
        else:
            st.caption("Sin datos.")

    with t2:
        st.markdown("#### Ventas totales por sistema de precios (comparativo)")
        df_ss = dash.get("por_sistema", pd.DataFrame())
        if yb_cmp is not None and yc_cmp is not None and isinstance(df_ss, pd.DataFrame) and not df_ss.empty:
            show_ss, fmt_ss = _ventas_formato_comparativo_dim(df_ss, "Sistema Precio", yb_cmp, yc_cmp, top_n=25)
            if show_ss is not None and fmt_ss is not None:
                st.dataframe(show_ss.style.format(fmt_ss, na_rep="—"), width="stretch", hide_index=True)
            else:
                st.caption("Sin datos para comparar.")
        elif len(yrs_cmp) < 2:
            st.caption("Selecciona al menos 2 años para ver el comparativo.")
        else:
            st.caption("Sin datos.")

    st.markdown("#### Ventas totales por rotación (comparativo)")
    df_ro = dash.get("por_rotacion", pd.DataFrame())
    if yb_cmp is not None and yc_cmp is not None and isinstance(df_ro, pd.DataFrame) and not df_ro.empty:
        show_ro, fmt_ro = _ventas_formato_comparativo_dim(df_ro, "Rotación", yb_cmp, yc_cmp, top_n=25)
        if show_ro is not None and fmt_ro is not None:
            st.dataframe(show_ro.style.format(fmt_ro, na_rep="—"), width="stretch", hide_index=True)
        else:
            st.caption("Sin datos para comparar.")
    elif len(yrs_cmp) < 2:
        st.caption("Selecciona al menos 2 años para ver el comparativo.")
    else:
        st.caption(
            "Sin datos de rotación. Ejecuta de nuevo **02_ventas_precios_cnhV2.py** para alinear `Rotacion` con margen SIESA."
        )

    st.markdown("#### Ventas totales por modelo (comparativo)")
    df_mo = dash.get("por_modelo", pd.DataFrame())
    if yb_cmp is not None and yc_cmp is not None and isinstance(df_mo, pd.DataFrame) and not df_mo.empty:
        show_mo, fmt_mo = _ventas_formato_comparativo_dim(df_mo, "Modelo", yb_cmp, yc_cmp, top_n=30)
        if show_mo is not None and fmt_mo is not None:
            st.dataframe(show_mo.style.format(fmt_mo, na_rep="—"), width="stretch", hide_index=True)
        else:
            st.caption("Sin datos para comparar.")
    elif len(yrs_cmp) < 2:
        st.caption("Selecciona al menos 2 años para ver el comparativo.")
    else:
        st.caption(
            "Sin datos de modelo. Ejecuta de nuevo el pipeline **02_ventas_precios_cnhV2.py** para cargar la columna `Modelo` en `ventas_raw`."
        )

    b1, b2 = st.columns(2)
    with b1:
        st.markdown("#### Ventas totales por vendedor (comparativo, top)")
        df_vn = dash.get("por_vendedor", pd.DataFrame())
        if yb_cmp is not None and yc_cmp is not None and isinstance(df_vn, pd.DataFrame) and not df_vn.empty:
            show_vn, fmt_vn = _ventas_formato_comparativo_dim(df_vn, "Vendedor", yb_cmp, yc_cmp, top_n=30)
            if show_vn is not None and fmt_vn is not None:
                st.dataframe(show_vn.style.format(fmt_vn, na_rep="—"), width="stretch", hide_index=True)
            else:
                st.caption("Sin datos para comparar.")
        elif len(yrs_cmp) < 2:
            st.caption("Selecciona al menos 2 años para ver el comparativo.")
        else:
            st.caption("Sin datos.")

    with b2:
        st.markdown("#### Ventas totales por cliente (comparativo, top)")
        df_cl = dash.get("por_cliente", pd.DataFrame())
        if yb_cmp is not None and yc_cmp is not None and isinstance(df_cl, pd.DataFrame) and not df_cl.empty:
            show_cl, fmt_cl = _ventas_formato_comparativo_dim(df_cl, "Cliente", yb_cmp, yc_cmp, top_n=30)
            if show_cl is not None and fmt_cl is not None:
                st.dataframe(show_cl.style.format(fmt_cl, na_rep="—"), width="stretch", hide_index=True)
            else:
                st.caption("Sin datos para comparar.")
        elif len(yrs_cmp) < 2:
            st.caption("Selecciona al menos 2 años para ver el comparativo.")
        else:
            st.caption("Sin datos.")

    st.markdown('<div class="rv-section"><b>Detalle operativo</b><br><span>Consulta de detalle con descarga de vista y CSV completo.</span></div>', unsafe_allow_html=True)
    df_det = _cargar_detalle_ventas_cached(
        tuple(sedes_sel),
        tuple(sys_sel),
        tuple(rot_sel),
        tuple(un_sel),
        tuple(cli_sel),
        tuple(ven_sel),
        tuple(linea_sel),
        tuple(modelo_sel),
        tuple(int(x) for x in anios_sel),
        tuple(int(x) for x in meses_sel),
        limite=200_000,
    )
    if isinstance(df_det, pd.DataFrame) and not df_det.empty:
        show_det = _renombrar_negocio(df_det.copy())
        fmt_det: dict[str, Any] = {}
        for c in ("Valor Venta", "Precio Unit. Venta", "Utilidad", "Descuento", "Costo Unit.", "Costo Total", "Precio Esperado", "Venta Esperada", "Variacion Ventas"):
            if c in show_det.columns:
                show_det[c] = pd.to_numeric(show_det[c], errors="coerce")
                fmt_det[c] = _fmt_money_local
        for c in ("Margen", "Margen Sistema", "Margen Esperado"):
            if c in show_det.columns:
                show_det[c] = pd.to_numeric(show_det[c], errors="coerce")
                fmt_det[c] = _fmt_pct_local

        # Orden estratégico de columnas: contexto -> producto -> cliente/canal
        # -> organización -> precio/resultado -> clasificación.
        orden_preferido = [
            "Sede",
            "Descp. CO Mvto.",
            "Descrip. UN",
            "Bodega",
            "Descrip. Bodega",
            "Inst.",
            "Descripcion Instalacion",
            "Sistema Precio",
            "Cod. Sistema",
            "Cliente",
            "Nit Cliente",
            "Vendedor",
            "Dcto Factura",
            "Dcto. Remision",
            "Dcto. Pedido",
            "Fecha Factura",
            "Año Comparativo",
            "Referencia",
            "Ref Normalizada",
            "Descripcion",
            "Modelo",
            "Línea",
            "Rotación",
            "Rotacion",
            "Cant.",
            "U.M.",
            "Precio Unit. Venta",
            "Valor Venta",
            "Costo Unit.",
            "Costo Total",
            "Descuento",
            "Utilidad",
            "Margen",
            "Precio Esperado",
            "Venta Esperada",
            "Margen Esperado",
            "Variacion Ventas",
            "Precio CNH",
            "Margen Sistema",
            "Tipo Inv.",
            "Tipo item",
            "Uso Lista",
            "Tiene Precio",
            "Id Lista",
            "Descripcion Lista",
            "Motivo",
            "Descripcion Motivo",
            "CO Dcto",
            "CO",
            "Mvto.",
            "Rowid Item",
            "Rowid",
        ]
        cols_actuales = list(show_det.columns)
        cols_ordenadas = [c for c in orden_preferido if c in cols_actuales]
        cols_restantes = [c for c in cols_actuales if c not in cols_ordenadas]
        show_det = show_det[cols_ordenadas + cols_restantes]

        st.caption(f"Filas mostradas: {len(show_det):,} (tope visual: 200.000).")

        c_det_1, c_det_2 = st.columns([1, 1])
        with c_det_1:
            csv_vista = show_det.to_csv(index=False).encode("utf-8-sig")
            st.download_button(
                "Descargar CSV (vista actual)",
                data=csv_vista,
                file_name="detalle_ventas_filtrado_vista.csv",
                mime="text/csv",
                use_container_width=True,
            )
        with c_det_2:
            if st.button("Preparar CSV completo (sin tope)", key="btn_det_csv_full", use_container_width=True):
                st.session_state["_det_csv_full_ready"] = True

        if st.session_state.get("_det_csv_full_ready", False):
            with st.spinner("Generando CSV completo filtrado..."):
                df_det_full = _cargar_detalle_ventas_cached(
                    tuple(sedes_sel),
                    tuple(sys_sel),
                    tuple(rot_sel),
                    tuple(un_sel),
                    tuple(cli_sel),
                    tuple(ven_sel),
                    tuple(linea_sel),
                    tuple(modelo_sel),
                    tuple(int(x) for x in anios_sel),
                    tuple(int(x) for x in meses_sel),
                    limite=None,
                )
                csv_full = df_det_full.to_csv(index=False).encode("utf-8-sig")
            st.download_button(
                f"Descargar CSV completo ({len(df_det_full):,} filas)",
                data=csv_full,
                file_name="detalle_ventas_filtrado_completo.csv",
                mime="text/csv",
                use_container_width=True,
                key="dl_det_csv_full",
            )

        max_styler_cells = 262_144
        total_cells = int(show_det.shape[0] * show_det.shape[1])
        if total_cells <= max_styler_cells:
            st.dataframe(show_det.style.format(fmt_det, na_rep="—"), width="stretch", hide_index=True)
        else:
            st.info(
                "La tabla es muy grande para formato enriquecido; se muestra sin estilo para evitar error de render."
            )
            st.dataframe(show_det, width="stretch", hide_index=True)
    else:
        st.caption("Sin filas para el detalle completo con los filtros actuales.")


def _plotly_theme() -> dict:
    """Tema oscuro alineado al dashboard (legible en pantalla ancha)."""
    return {
        "template": "plotly_dark",
        "paper_bgcolor": "#111a2e",
        "plot_bgcolor": "#0f172a",
        "font": {"color": "#e5e7eb", "size": 12},
        # b alineado con t: más aire bajo el eje X antes del borde del chart (simetría con título arriba)
        "margin": {"l": 50, "r": 30, "t": 56, "b": 58},
    }


def _plotly_show(fig, caption: str | None = None, *, fig_scope: str = "default") -> None:
    """Muestra un gráfico Plotly y, opcionalmente, un pie de figura breve (Fig. n · interpretación)."""
    if not _HAS_PLOTLY or px is None:
        return
    fig.update_layout(**_plotly_theme())
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": True, "scrollZoom": True})
    if caption:
        _render_fig_caption(fig_scope, caption)


def _render_fig_caption(fig_scope: str, caption: str) -> None:
    """Pie de figura numerado (mismo contador que _plotly_show con caption)."""
    k = f"_fig_seq_{fig_scope}"
    st.session_state[k] = st.session_state.get(k, 0) + 1
    n = st.session_state[k]
    st.markdown(
        f'<p class="fig-caption"><span class="fig-caption__label">Fig. {n}</span> · {html.escape(caption)}</p>',
        unsafe_allow_html=True,
    )


def _margen_plotly_charts(df: pd.DataFrame, margen_col: str, precio_col: str) -> None:
    """Reporte gráfico storytelling: panorama → concentración → relaciones → segmentos → anomalías."""
    if not _HAS_PLOTLY or px is None:
        st.warning("Instala **Plotly** para ver gráficos: `pip install plotly` (mismo venv que Streamlit).")
        return
    st.session_state["_fig_seq_margen"] = 0
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    existencia_col = "Existencia" if "Existencia" in df.columns else ("Disponible" if "Disponible" in df.columns else None)

    n_max = 12_000
    if len(df) > n_max:
        df_sc = df.sample(n=n_max, random_state=42)
        st.caption(f"Muestra de **{n_max:,}** filas (de {len(df):,}) para gráficos de dispersión.")
    else:
        df_sc = df

    BANDS = [(-9999, 15, "Crítico < 15 %", "#ef4444"),
             (15, 30, "Sospechoso 15–30 %", "#f59e0b"),
             (30, 50, "Normal 30–50 %", "#22c55e"),
             (50, 60, "Alto sospechoso 50–60 %", "#f97316"),
             (60, 9999, "Muy alto > 60 %", "#dc2626")]
    BAND_LABELS = [b[2] for b in BANDS]
    BAND_COLORS = {b[2]: b[3] for b in BANDS}

    def _classify_band(m: float) -> str:
        for lo, hi, label, _ in BANDS:
            if lo <= m < hi:
                return label
        return BAND_LABELS[-1]

    df_b = df.copy()
    df_b["_banda"] = df_b[margen_col].apply(_classify_band)
    df_b["_banda"] = pd.Categorical(df_b["_banda"], categories=BAND_LABELS, ordered=True)
    if existencia_col and "Costo_Prom_Inst" in df_b.columns:
        df_b["_val_inv"] = df_b["Costo_Prom_Inst"].fillna(0) * df_b[existencia_col].fillna(0)
    else:
        df_b["_val_inv"] = 0.0

    # ════════════════════════════════════════════════════════════════════
    #  CAP 1 — Panorama: ¿cómo se distribuyen los márgenes?
    # ════════════════════════════════════════════════════════════════════
    st.markdown("### 1 · Panorama — ¿cómo se distribuyen los márgenes?")
    st.caption(
        "La **forma de la distribución** revela de inmediato si el portafolio está sano "
        "(concentrado en la banda normal 30-50 %) o si hay colas pesadas en zonas de riesgo. "
        "Los **indicadores por banda** cuantifican el volumen de cada zona."
    )

    band_counts = df_b["_banda"].value_counts().reindex(BAND_LABELS, fill_value=0)
    total = max(int(band_counts.sum()), 1)
    kpi_cols = st.columns(len(BANDS), gap="small")
    for i, (_, _, label, color) in enumerate(BANDS):
        n = int(band_counts.get(label, 0))
        pct = n / total * 100
        kpi_cols[i].markdown(
            f"<div style='text-align:center;border-left:3px solid {color};padding:0.35rem 0.45rem;border-radius:8px;"
            f"background:#101a31;border:1px solid #25314d'>"
            f"<span style='font-size:1.28em;font-weight:700;color:{color};line-height:1.15'>{n:,}</span><br>"
            f"<span style='font-size:0.78em;color:#cbd5e1'>{pct:.1f} % — {label}</span></div>",
            unsafe_allow_html=True,
        )

    r1c1, r1c2 = st.columns(2, gap="large")
    with r1c1:
        fig_hist = px.histogram(
            df, x=margen_col, nbins=80,
            title=f"Distribución de {margen_col} — zonas de decisión",
            labels={margen_col: "Margen (%)"},
        )
        fig_hist.update_traces(marker_line_width=0)
        fig_hist.add_vrect(x0=df[margen_col].min(), x1=15, fillcolor="red", opacity=0.07,
                           annotation_text="Crítico < 15 %", annotation_position="top left")
        fig_hist.add_vrect(x0=15, x1=30, fillcolor="orange", opacity=0.05,
                           annotation_text="Sospechoso 15–30 %", annotation_position="top left")
        fig_hist.add_vrect(x0=50, x1=60, fillcolor="orange", opacity=0.05,
                           annotation_text="Alto sosp. 50–60 %", annotation_position="top right")
        fig_hist.add_vrect(x0=60, x1=max(float(df[margen_col].max()), 61), fillcolor="red", opacity=0.07,
                           annotation_text="Muy alto > 60 %", annotation_position="top right")
        median_m = float(df[margen_col].median())
        fig_hist.add_vline(x=median_m, line_dash="dash", line_color="#facc15",
                           annotation_text=f"Mediana {median_m:.1f}%")
        _plotly_show(
            fig_hist,
            "El histograma cuenta referencias por tramo de margen (%); las bandas sombreadas y la mediana muestran si el portafolio se concentra en márgenes sanos o en colas de riesgo.",
            fig_scope="margen",
        )

    with r1c2:
        if existencia_col and "Costo_Prom_Inst" in df.columns:
            zona_agg = df_b.groupby("_banda", observed=True).agg(
                valor_inv=("_val_inv", "sum"), refs=("_ref_codigo", "nunique")
            ).reset_index()
            zona_agg["valor_mill"] = zona_agg["valor_inv"] / 1e6
            fig_zona = px.bar(
                zona_agg, x="_banda", y="valor_mill", text="refs",
                color="_banda", color_discrete_map=BAND_COLORS,
                title="Valor inventario (COP mill.) por banda de margen",
                labels={"_banda": "Banda", "valor_mill": "Valor inv. (COP mill.)", "refs": "Refs únicas"},
            )
            fig_zona.update_traces(textposition="outside")
            fig_zona.update_layout(showlegend=False)
            _plotly_show(
                fig_zona,
                "Barras: valor de inventario (COP mill.) por banda de margen; la altura indica cuánto capital está expuesto en cada zona de riesgo.",
                fig_scope="margen",
            )
        else:
            st.caption("Sin datos de existencia/costo para análisis de riesgo.")

    # ════════════════════════════════════════════════════════════════════
    #  CAP 2 — Concentración: ¿dónde está el dinero en riesgo?
    # ════════════════════════════════════════════════════════════════════
    st.markdown("---")
    st.markdown("### 2 · Concentración — ¿dónde está el dinero en riesgo?")
    st.caption(
        "El principio de **Pareto** suele cumplirse: pocas referencias concentran la mayor parte "
        "del inventario. Si esas pocas caen en bandas anormales, el impacto financiero es desproporcionado. "
        "Aquí identificamos **cuánto dinero** está expuesto y **en qué sistemas**."
    )

    if existencia_col and "Costo_Prom_Inst" in df.columns:
        ref_agg = (
            df_b.groupby("_ref_codigo", dropna=False)
            .agg(val_inv=("_val_inv", "sum"), margen_med=(margen_col, "median"))
            .reset_index()
            .sort_values("val_inv", ascending=False)
        )
        ref_agg = ref_agg[ref_agg["val_inv"] > 0]
        if not ref_agg.empty:
            ref_agg["cum_pct"] = ref_agg["val_inv"].cumsum() / ref_agg["val_inv"].sum() * 100
            ref_agg["rank"] = range(1, len(ref_agg) + 1)
            n80 = int((ref_agg["cum_pct"] <= 80).sum())
            top = ref_agg.head(100)
            ref_labels = top["_ref_codigo"].astype(str).tolist()
            fig_pareto = make_subplots(specs=[[{"secondary_y": True}]])
            fig_pareto.add_trace(
                go.Bar(
                    x=ref_labels, y=top["val_inv"] / 1e6,
                    name="Valor inv. (mill.)",
                    marker_color=["#ef4444" if m < 0 else "#3b82f6" for m in top["margen_med"]],
                    customdata=list(zip(top["rank"], top["margen_med"].round(1), top["cum_pct"].round(1))),
                    hovertemplate="<b>%{x}</b><br>Rank: %{customdata[0]}<br>Valor: %{y:,.1f} mill.<br>Margen med.: %{customdata[1]}%<br>Acum.: %{customdata[2]}%<extra></extra>",
                ),
                secondary_y=False,
            )
            fig_pareto.add_trace(
                go.Scatter(x=ref_labels, y=top["cum_pct"],
                           mode="lines", name="% acumulado", line=dict(color="#facc15", width=2)),
                secondary_y=True,
            )
            fig_pareto.add_hline(y=80, line_dash="dot", line_color="#a855f7", secondary_y=True,
                                 annotation_text=f"80 % del inventario ≈ {n80} refs")
            fig_pareto.update_layout(
                title=f"Pareto de inventario (top 100 refs) — rojo = margen negativo  ({n80} refs = 80 % del valor)",
                xaxis_title="Referencia", legend=dict(orientation="h", y=-0.18),
                xaxis=dict(type="category", tickangle=-60, tickfont=dict(size=9)),
            )
            fig_pareto.update_yaxes(title_text="Valor inv. (COP mill.)", secondary_y=False)
            fig_pareto.update_yaxes(title_text="% acumulado", secondary_y=True)
            _plotly_show(
                fig_pareto,
                "Pareto de referencias por valor de inventario; la curva amarilla muestra el % acumulado y la regla del 80/20 localiza pocas refs con mucho peso financiero.",
                fig_scope="margen",
            )
    else:
        st.caption("Sin datos de inventario para Pareto.")

    r2c1, r2c2 = st.columns(2, gap="large")
    with r2c1:
        if existencia_col and "Costo_Prom_Inst" in df.columns and "Sistema_Precio" in df.columns:
            df_sys_neg = df_b[df_b[margen_col] < 0]
            if not df_sys_neg.empty:
                neg_by_sys = (
                    df_sys_neg.groupby(df_sys_neg["Sistema_Precio"].astype(str), dropna=False)
                    .agg(valor_neg=("_val_inv", "sum"), refs=("_ref_codigo", "nunique"))
                    .reset_index()
                    .sort_values("valor_neg", ascending=False)
                    .head(15)
                )
                neg_by_sys["valor_mill"] = neg_by_sys["valor_neg"] / 1e6
                fig_neg = px.bar(
                    neg_by_sys, y="Sistema_Precio", x="valor_mill", orientation="h",
                    text="refs", color="valor_mill", color_continuous_scale="Reds",
                    title="Inventario con margen negativo — top 15 sistemas",
                    labels={"valor_mill": "Valor inv. (COP mill.)", "Sistema_Precio": "Sistema precio", "refs": "Refs"},
                )
                fig_neg.update_traces(textposition="outside")
                _plotly_show(
                    fig_neg,
                    "Inventario con margen negativo por sistema de precio; barras más largas = mayor exposición monetaria en ese sistema.",
                    fig_scope="margen",
                )
            else:
                st.caption("No hay referencias con margen negativo en el filtro actual.")
        else:
            st.caption("Sin datos para cruce sistema–inventario negativo.")

    with r2c2:
        anomaly_bands = ["Crítico < 15 %", "Sospechoso 15–30 %", "Alto sospechoso 50–60 %", "Muy alto > 60 %"]
        if existencia_col and "Costo_Prom_Inst" in df_b.columns:
            df_anom = df_b[df_b["_banda"].isin(anomaly_bands)].copy()
            if not df_anom.empty:
                top_anom = (
                    df_anom.groupby("_ref_codigo", dropna=False)
                    .agg(
                        val_inv=("_val_inv", "sum"),
                        margen_med=(margen_col, "median"),
                        n_bodegas=("Bodega", "nunique") if "Bodega" in df_anom.columns else (margen_col, "count"),
                        banda=("_banda", lambda s: s.mode().iloc[0] if not s.mode().empty else s.iloc[0]),
                    )
                    .reset_index()
                    .sort_values("val_inv", ascending=False)
                    .head(20)
                )
                top_anom["val_mill"] = top_anom["val_inv"] / 1e6
                fig_top = px.bar(
                    top_anom, y="_ref_codigo", x="val_mill", orientation="h",
                    color="banda", color_discrete_map=BAND_COLORS,
                    text=top_anom.apply(lambda r: f"{r['margen_med']:.0f}%  ({r['n_bodegas']} bod)", axis=1),
                    hover_data={"margen_med": ":.1f", "n_bodegas": True},
                    title="Top 20 refs anómalas por valor inventario",
                    labels={"val_mill": "Valor inv. (COP mill.)", "_ref_codigo": "Referencia", "banda": "Banda"},
                )
                fig_top.update_traces(textposition="outside")
                fig_top.update_layout(height=max(450, len(top_anom) * 22), yaxis=dict(autorange="reversed"))
                _plotly_show(
                    fig_top,
                    "Top 20 referencias anómalas por valor de inventario; el color indica la banda de margen y el texto resume margen típico y cobertura en bodegas.",
                    fig_scope="margen",
                )
            else:
                st.success("Todas las referencias están en la banda normal (30–50 %).")

    # ════════════════════════════════════════════════════════════════════
    #  CAP 3 — Relaciones: ¿qué determina el nivel de margen?
    # ════════════════════════════════════════════════════════════════════
    st.markdown("---")
    st.markdown("### 3 · Relaciones — ¿qué determina el nivel de margen?")
    st.caption(
        "Analizamos las dos variables que más influyen: el **precio lista** (¿a mayor precio, mejor margen?) "
        "y la **rotación** (¿los productos que más rotan tienen márgenes más comprimidos?). "
        "Si hay una **tendencia clara**, revela una regla de pricing a validar."
    )

    r3c1, r3c2 = st.columns(2, gap="large")
    with r3c1:
        if precio_col in df_sc.columns:
            color_col = "Rotacion" if "Rotacion" in df_sc.columns else None
            fig_sc = px.scatter(
                df_sc, x=precio_col, y=margen_col, opacity=0.30,
                color=color_col,
                title="Precio lista vs margen — por rotación",
                labels={precio_col: "Precio lista (COP)", margen_col: "Margen (%)"},
                marginal_x="box", marginal_y="histogram",
            )
            fig_sc.update_traces(marker=dict(size=5), selector=dict(type="scatter"))
            fig_sc.add_hline(y=0, line_dash="dot", line_color="#ef4444", annotation_text="Margen = 0")
            fig_sc.update_xaxes(tickformat="$,.0f")
            _plotly_show(
                fig_sc,
                "Cada punto es una referencia: precio lista frente a margen; las gráficas marginales muestran la forma de cada variable y la nube revela si hay relación entre precio y margen.",
                fig_scope="margen",
            )
        else:
            st.caption(f"Sin `{precio_col}` para dispersión.")

    with r3c2:
        if precio_col in df.columns:
            df_tmp = df[[precio_col, margen_col]].dropna()
            if not df_tmp.empty:
                df_tmp["_precio_bin"] = pd.qcut(df_tmp[precio_col], q=10, duplicates="drop")
                bin_agg = (
                    df_tmp.groupby("_precio_bin", observed=True)[margen_col]
                    .agg(media="mean", mediana="median", q25=lambda s: s.quantile(0.25))
                    .reset_index()
                    .sort_index()
                )
                bin_agg["label"] = bin_agg["_precio_bin"].astype(str)
                fig_trend = go.Figure()
                fig_trend.add_trace(go.Scatter(x=bin_agg["label"], y=bin_agg["media"],
                                               mode="lines+markers", name="Media", line=dict(color="#38bdf8", width=2)))
                fig_trend.add_trace(go.Scatter(x=bin_agg["label"], y=bin_agg["mediana"],
                                               mode="lines+markers", name="Mediana", line=dict(color="#22c55e", width=2, dash="dash")))
                fig_trend.add_trace(go.Scatter(x=bin_agg["label"], y=bin_agg["q25"],
                                               mode="lines", name="P25 (riesgo)", line=dict(color="#ef4444", width=1, dash="dot")))
                fig_trend.update_layout(title="Tendencia de margen por decil de precio lista",
                                        xaxis_title="Decil de precio", yaxis_title="Margen (%)",
                                        xaxis_tickangle=-40, legend=dict(orientation="h", y=-0.25))
                _plotly_show(
                    fig_trend,
                    "Media, mediana y P25 del margen por decil de precio lista; indica si el margen mejora o empeora al subir el precio de lista.",
                    fig_scope="margen",
                )

    if "Rotacion" in df.columns and existencia_col:
        rot_agg = (
            df.groupby(df["Rotacion"].astype(str), dropna=False)
            .agg(
                media_margen=(margen_col, "mean"),
                mediana_margen=(margen_col, "median"),
                refs=("_ref_codigo", "nunique"),
                exist_total=(existencia_col, "sum"),
            )
            .reset_index()
        )
        rot_agg = rot_agg[rot_agg["refs"] >= 3]
        if not rot_agg.empty:
            fig_bub = px.scatter(
                rot_agg, x="Rotacion", y="media_margen",
                size="exist_total", color="mediana_margen",
                color_continuous_scale="RdYlGn", color_continuous_midpoint=15,
                hover_data={"refs": True, "exist_total": ":,.0f", "mediana_margen": ":.1f"},
                title="Margen medio por rotación — tamaño = existencia total",
                labels={"media_margen": "Margen medio (%)", "mediana_margen": "Mediana", "exist_total": "Exist. total"},
            )
            fig_bub.add_hline(y=0, line_dash="dot", line_color="#ef4444")
            _plotly_show(
                fig_bub,
                "Burbuja por categoría de rotación: posición vertical = margen medio, tamaño = existencia total; el color refuerza la mediana de margen para detectar focos de riesgo.",
                fig_scope="margen",
            )

    # ════════════════════════════════════════════════════════════════════
    #  CAP 4 — Segmentos: ¿dónde se concentran los problemas?
    # ════════════════════════════════════════════════════════════════════
    st.markdown("---")
    st.markdown("### 4 · Segmentos — ¿dónde intervenir primero?")
    st.caption(
        "Cruzamos **bodega**, **sistema de precio** y **rotación** para descubrir "
        "los segmentos que necesitan intervención prioritaria. "
        "Las **bodegas de peor margen** y los **cruces sistema × rotación** revelan focos específicos."
    )

    r4c1, r4c2 = st.columns(2, gap="large")
    with r4c1:
        if "Bodega" in df.columns:
            bod_agg = (
                df.groupby(df["Bodega"].astype(str), dropna=False)[margen_col]
                .agg(mediana="median", q25=lambda s: s.quantile(0.25), pct_neg=lambda s: float((s < 0).mean() * 100), n="count")
                .reset_index()
                .sort_values("mediana", ascending=True)
                .head(20)
            )
            fig_bod = px.bar(
                bod_agg, y="Bodega", x="mediana", orientation="h",
                color="pct_neg", color_continuous_scale="RdYlGn_r",
                hover_data={"q25": ":.1f", "n": True, "pct_neg": ":.1f"},
                title="Mediana de margen por bodega — color = % refs negativas",
                labels={"mediana": "Mediana margen (%)", "pct_neg": "% negativas", "n": "Refs"},
            )
            _plotly_show(
                fig_bod,
                "Mediana de margen por bodega (orden ascendente); el color muestra el % de referencias con margen negativo en cada bodega.",
                fig_scope="margen",
            )
        else:
            st.caption("Sin columna `Bodega`.")

    with r4c2:
        if "Rotacion" in df.columns and "Sistema_Precio" in df.columns:
            top_rot = df["Rotacion"].astype(str).value_counts().head(8).index
            top_sys = df["Sistema_Precio"].astype(str).value_counts().head(10).index
            df_cross = df[df["Rotacion"].astype(str).isin(top_rot) & df["Sistema_Precio"].astype(str).isin(top_sys)]
            if not df_cross.empty:
                piv = df_cross.pivot_table(index="Sistema_Precio", columns="Rotacion", values=margen_col, aggfunc="median")
                fig_hm = px.imshow(
                    piv, text_auto=".1f", aspect="auto",
                    color_continuous_scale="RdYlGn", color_continuous_midpoint=15,
                    title="Mediana de margen: sistema × rotación (top combinaciones)",
                    labels={"color": "Mediana margen (%)"},
                )
                _plotly_show(
                    fig_hm,
                    "Mapa de calor: mediana de margen en el cruce sistema × rotación; celdas más cálidas o verdes señalan segmentos con peor o mejor margen.",
                    fig_scope="margen",
                )
            else:
                st.caption("Sin datos para heatmap sistema×rotación.")
        else:
            st.caption("Faltan `Rotacion` o `Sistema_Precio`.")

    if existencia_col and "Costo_Prom_Inst" in df_b.columns and "Bodega" in df_b.columns:
        st.markdown("#### Mapa de exposición: inventario por banda y bodega")
        tree_agg = (
            df_b.groupby(["_banda", df_b["Bodega"].astype(str)], observed=True)
            .agg(valor=("_val_inv", "sum"), refs=("_ref_codigo", "nunique"))
            .reset_index()
        )
        tree_agg = tree_agg[tree_agg["valor"] > 0]
        if not tree_agg.empty:
            tree_agg["valor_mill"] = tree_agg["valor"] / 1e6
            fig_tree = px.treemap(
                tree_agg, path=["_banda", "Bodega"], values="valor_mill",
                color="_banda", color_discrete_map=BAND_COLORS,
                hover_data={"refs": True, "valor_mill": ":.1f"},
                title="Valor de inventario (COP mill.) — banda de margen → bodega",
            )
            fig_tree.update_layout(margin=dict(t=50, l=10, r=10, b=10))
            _plotly_show(
                fig_tree,
                "Treemap jerárquico: banda de margen → bodega; el área es proporcional al valor de inventario para ver dónde se concentra el dinero en cada banda.",
                fig_scope="margen",
            )

    # ════════════════════════════════════════════════════════════════════
    #  CAP 5 — Anomalías: detección de inconsistencias
    # ════════════════════════════════════════════════════════════════════
    st.markdown("---")
    st.markdown("### 5 · Anomalías — detección de inconsistencias")
    st.caption(
        "Una misma referencia con **márgenes muy distintos entre bodegas** (>20 pp) sugiere "
        "errores de costo, transferencias sin ajuste o pricing desalineado. "
        "Estas inconsistencias son **ganancias rápidas**: corregirlas tiene impacto inmediato."
    )

    if "Bodega" in df_b.columns:
        ref_cross = (
            df_b.groupby("_ref_codigo", dropna=False)
            .agg(
                n_bodegas=("Bodega", "nunique"),
                margen_min=(margen_col, "min"),
                margen_max=(margen_col, "max"),
                margen_med=(margen_col, "median"),
            )
            .reset_index()
        )
        ref_cross["rango"] = ref_cross["margen_max"] - ref_cross["margen_min"]
        inconsist = ref_cross[(ref_cross["n_bodegas"] >= 2) & (ref_cross["rango"] > 20)].sort_values("rango", ascending=False).head(40)
        if not inconsist.empty:
            ic1, ic2 = st.columns(2, gap="large")
            with ic1:
                fig_inc = go.Figure()
                for _, row in inconsist.head(25).iterrows():
                    fig_inc.add_trace(go.Scatter(
                        x=[row["margen_min"], row["margen_med"], row["margen_max"]],
                        y=[row["_ref_codigo"]] * 3,
                        mode="lines+markers",
                        marker=dict(size=[8, 10, 8],
                                    color=[BAND_COLORS.get(_classify_band(row["margen_min"]), "#64748b"),
                                           "#3b82f6",
                                           BAND_COLORS.get(_classify_band(row["margen_max"]), "#64748b")]),
                        line=dict(color="#94a3b8", width=2),
                        name=str(row["_ref_codigo"]),
                        showlegend=False,
                        hovertemplate=f"Ref: {row['_ref_codigo']}<br>Min: {row['margen_min']:.1f}%<br>Med: {row['margen_med']:.1f}%<br>Max: {row['margen_max']:.1f}%<extra></extra>",
                    ))
                for lo, hi, label, color in BANDS:
                    if lo > -9999 and hi < 9999:
                        fig_inc.add_vline(x=lo, line_dash="dot", line_color=color, opacity=0.4)
                fig_inc.update_layout(
                    title=f"Top {min(25, len(inconsist))} refs con mayor dispersión entre bodegas",
                    xaxis_title="Margen (%)", yaxis_title="Referencia",
                    height=max(400, min(25, len(inconsist)) * 28),
                )
                _plotly_show(
                    fig_inc,
                    "Por referencia: trazo de margen mínimo, mediano y máximo entre bodegas; dispersión amplia sugiere inconsistencia de costos o precios entre ubicaciones.",
                    fig_scope="margen",
                )

            with ic2:
                fig_rng = px.scatter(
                    inconsist, x="margen_med", y="rango",
                    size="n_bodegas", color="rango",
                    color_continuous_scale="YlOrRd",
                    hover_data={"_ref_codigo": True, "margen_min": ":.1f", "margen_max": ":.1f", "n_bodegas": True},
                    title="Mediana de margen vs rango entre bodegas",
                    labels={"margen_med": "Mediana margen (%)", "rango": "Rango max−min (pp)", "n_bodegas": "N° bodegas"},
                )
                for lo, hi, label, color in BANDS:
                    if lo > -9999:
                        fig_rng.add_vline(x=lo, line_dash="dot", line_color=color, opacity=0.5,
                                          annotation_text=label.split(" ")[0] if lo > 0 else "")
                fig_rng.add_hline(y=20, line_dash="dot", line_color="#64748b", annotation_text="Umbral 20 pp")
                _plotly_show(
                    fig_rng,
                    "Cada punto es una referencia: mediana de margen en el eje X vs rango entre bodegas (max−min); hacia arriba y derecha hay más inconsistencia operativa.",
                    fig_scope="margen",
                )

            st.markdown(f"**{len(inconsist)} referencias** con dispersión > 20 pp entre bodegas.")
        else:
            st.success("No se detectan inconsistencias cross-bodega significativas (Δ > 20 pp).")

        anomalas = df_b[df_b["_banda"].isin(["Crítico < 15 %", "Muy alto > 60 %"])]
        if not anomalas.empty:
            st.markdown("#### Zonas extremas por bodega")
            an_agg = (
                anomalas.groupby([anomalas["Bodega"].astype(str), "_banda"], observed=True)
                .agg(refs=("_ref_codigo", "nunique"))
                .reset_index()
            )
            fig_an = px.bar(
                an_agg, x="Bodega", y="refs", color="_banda",
                color_discrete_map=BAND_COLORS, barmode="group",
                title="Refs con margen crítico (< 15 %) o demasiado alto (> 60 %) por bodega",
                labels={"refs": "Refs únicas", "Bodega": "Bodega", "_banda": "Banda"},
            )
            fig_an.update_xaxes(tickangle=-35)
            _plotly_show(
                fig_an,
                "Barras agrupadas: referencias con margen crítico o muy alto por bodega; prioriza auditorías donde hay más piezas en zonas extremas.",
                fig_scope="margen",
            )
        else:
            st.success("No hay referencias en zonas extremas con el filtro actual.")



def _auditoria_prepare_chart_df(df_fil: pd.DataFrame) -> pd.DataFrame:
    """Prepara columnas auxiliares para gráficos de auditoría (compartido entre ambas sub-pestañas)."""
    df = df_fil.copy()
    if "_abs_var_compra" in df.columns:
        df["_g_abs_var_compra"] = pd.to_numeric(df["_abs_var_compra"], errors="coerce")
    else:
        df["_g_abs_var_compra"] = pd.NA
    if "_abs_var_costo" in df.columns:
        df["_g_abs_var_costo"] = pd.to_numeric(df["_abs_var_costo"], errors="coerce")
    else:
        df["_g_abs_var_costo"] = pd.NA
    if df["_g_abs_var_compra"].notna().any() and df["_g_abs_var_costo"].notna().any():
        df["_g_score"] = 0.55 * df["_g_abs_var_compra"].fillna(0) + 0.45 * df["_g_abs_var_costo"].fillna(0)
    else:
        df["_g_score"] = pd.NA
    return df


def _auditoria_histogram_ratio_costo_lista_vlines(fig, df_clip: pd.DataFrame, col: str = "_ratio") -> None:
    """Líneas de referencia sobre el histograma de ratio costo/P.Lista.

    Líneas secundarias (P5, P95, Q1, Q3, Tukey) sin etiqueta — fondo, menos ruido visual.
    Etiquetas solo en μ, Md, Mo (si aplica) y umbrales 85 % / 100 %.
    """
    ser = pd.to_numeric(df_clip[col], errors="coerce").dropna()
    if ser.empty or len(ser) < 2:
        return
    mean_r = float(ser.mean())
    med_r = float(ser.median())
    q1 = float(ser.quantile(0.25))
    q3 = float(ser.quantile(0.75))
    p5 = float(ser.quantile(0.05))
    p95 = float(ser.quantile(0.95))
    iqr = q3 - q1
    fence_lo = q1 - 1.5 * iqr
    fence_hi = q3 + 1.5 * iqr
    x_lo = float(ser.min())
    x_hi = float(ser.max())
    try:
        nb = min(40, max(10, len(ser) // 8))
        mode_bin = pd.cut(ser, bins=nb, duplicates="drop").value_counts().idxmax()
        mode_r = float(mode_bin.mid)
    except Exception:
        rm = ser.round(1).mode()
        mode_r = float(rm.iloc[0]) if not rm.empty else med_r

    def _shape_v(x: float, color: str, dash: str = "dot", w: int = 1, opacity: float = 0.42) -> None:
        fig.add_shape(
            type="line",
            xref="x",
            yref="paper",
            x0=x,
            x1=x,
            y0=0,
            y1=1,
            line=dict(color=color, width=w, dash=dash),
            opacity=opacity,
            layer="below",
        )

    def _vl(
        x: float,
        txt: str,
        color: str,
        dash: str = "solid",
        w: float = 1.2,
        opacity: float = 1.0,
        pos: str = "top",
    ) -> None:
        fig.add_vline(
            x=x,
            line_dash=dash,
            line_color=color,
            line_width=w,
            opacity=opacity,
            annotation_text=txt,
            annotation_position=pos,
        )

    # Referencia estadística (sin texto — panel lateral explica el significado)
    if p95 - p5 > 2.5:
        _shape_v(p5, "#64748b", "dot", 1, 0.4)
        _shape_v(p95, "#64748b", "dot", 1, 0.4)
    _shape_v(q1, "#475569", "dot", 1, 0.45)
    _shape_v(q3, "#475569", "dot", 1, 0.45)
    if iqr > 0 and abs(fence_hi - p95) > 2 and x_lo - 5 <= fence_hi <= x_hi + 5:
        _shape_v(fence_hi, "#6366f1", "dash", 1, 0.38)
    if iqr > 0 and abs(fence_lo - p5) > 2 and x_lo - 5 <= fence_lo <= x_hi + 5:
        _shape_v(fence_lo, "#6366f1", "dash", 1, 0.38)

    # Lectura rápida: tendencia central y negocio (etiquetas cortas, posiciones alternas)
    _vl(mean_r, f"μ {mean_r:.0f}%", "#22d3ee", "solid", 1.5, 1.0, "top right")
    _vl(med_r, f"Md {med_r:.0f}%", "#facc15", "dash", 1.5, 1.0, "bottom left")
    if abs(mode_r - med_r) > 2.0:
        _vl(mode_r, f"Mo {mode_r:.0f}%", "#c084fc", "solid", 1.2, 1.0, "top left")
    fig.add_vline(
        x=85,
        line_dash="dot",
        line_color="#f59e0b",
        line_width=1.3,
        annotation_text="85%",
        annotation_position="bottom right",
    )
    fig.add_vline(
        x=100,
        line_dash="solid",
        line_color="#ef4444",
        line_width=1.5,
        annotation_text="100%",
        annotation_position="top right",
    )
    ann = getattr(fig.layout, "annotations", None)
    if ann:
        fig.update_annotations(font=dict(size=10, color="#e5e7eb"))


def _auditoria_charts_semaforo_st(df_fil: pd.DataFrame, ctx: dict) -> None:
    """Eje Semáforo — storytelling: resumen → panorama → anatomía → concentración → plan."""
    if not _HAS_PLOTLY or px is None:
        st.warning("Instala **Plotly** para ver gráficos: `pip install plotly`.")
        return
    st.session_state["_fig_seq_audit_sem"] = 0
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    _CL = dict(height=440)

    df = _auditoria_prepare_chart_df(df_fil)
    sem_col = ctx["sem_col"]
    ref_col = ctx["ref_col"]
    var_c = ctx["var_compra_col"]
    var_costo_pct = ctx["var_costo_base_col"]
    sistema_precio_col = ctx["sistema_precio_col"]
    costo_inv_col = ctx["costo_inv_col"]
    lower_map = ctx["lower_map"]
    ex_tot_col = ctx["ex_tot_col"]
    precio_ult_cop_col = ctx["precio_ult_cop_col"]
    precio_lista_col = ctx["precio_lista_col"]
    desc_col = ctx["desc_col"]
    rot_col = ctx["rot_col"]

    umbral_v = float(st.session_state.get("aud_umbral_var_costo", 15.0))

    # ── CAP 1 · Resumen ejecutivo ────────────────────────────────────────
    st.markdown("#### 1 · Resumen ejecutivo")
    st.caption("Vista rápida: severidad de la desalineación del precio de compra frente al costo de inventario.")

    n_total = len(df)
    n_critico = int(df[sem_col].map(_auditoria_es_semaforo_critico).sum()) if sem_col and sem_col in df.columns else 0
    n_mod_alto = int(df[sem_col].map(_auditoria_es_semaforo_mod_alto).sum()) if sem_col and sem_col in df.columns else 0
    pct_umbral_costo = float((df["_g_abs_var_costo"].fillna(-1) >= umbral_v).mean() * 100.0)
    valor_inv = _valor_inventario_cop_auditoria(df, lower_map)
    media_var_costo = float(df["_g_abs_var_costo"].dropna().mean()) if df["_g_abs_var_costo"].notna().any() else 0.0

    with st.container(border=True):
        st.markdown(
            '<span class="audit-exec-kpi-inner" aria-hidden="true"></span>',
            unsafe_allow_html=True,
        )
        s1, s2, s3 = st.columns(3, gap="medium")
        s1.metric("Filtradas", f"{n_total:,}")
        s2.metric("Crítico", f"{n_critico:,}", delta=f"{n_critico/max(n_total,1)*100:.1f}%", delta_color="inverse")
        s3.metric("Mod-alto", f"{n_mod_alto:,}", delta=f"{n_mod_alto/max(n_total,1)*100:.1f}%", delta_color="inverse")
        s4, s5, s6 = st.columns(3, gap="medium")
        s4.metric("Media |Δ costo|", f"{media_var_costo:.1f}%")
        s5.metric("≥ umbral costo", f"{pct_umbral_costo:.1f}%")
        s6.metric("Inv. expuesto", _fmt_cop_resumido(valor_inv) if valor_inv is not None else "N/D")

    # ── CAP 2 · Panorama ─────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("#### 2 · Panorama — distribución de alertas y score")
    st.caption("¿Qué proporción del portafolio es problemática según el semáforo y el score compuesto?")

    sub_s = df.dropna(subset=["_g_score"])
    _score_med = _score_p90 = _score_clip = _score_max = None
    if not sub_s.empty:
        _score_p90 = float(sub_s["_g_score"].quantile(0.90))
        _score_med = float(sub_s["_g_score"].median())
        _score_clip = max(float(sub_s["_g_score"].quantile(0.95)) * 1.15, 1.0)
        _score_max = float(sub_s["_g_score"].max())

    p1, p2 = st.columns(2, gap="medium")
    with p1:
        if sem_col and sem_col in df.columns:
            sem_norm = df[sem_col].fillna("SIN_DATO").astype(str).str.strip()
            vc = sem_norm.value_counts().reset_index()
            vc.columns = ["Semáforo", "n"]
            vc["Semáforo_ui"] = vc["Semáforo"].map(_auditoria_etiqueta_semaforo_ui)
            color_map = {ui: _auditoria_color_discreto_semaforo_ui(ui) for ui in vc["Semáforo_ui"].unique()}
            fig = px.bar(vc, x="Semáforo_ui", y="n", color="Semáforo_ui",
                         color_discrete_map=color_map, title="Distribución del semáforo",
                         labels={"Semáforo_ui": "Semáforo", "n": "N° refs"})
            fig.update_layout(showlegend=False, **_CL)
            _plotly_show(
                fig,
                "Barras: número de referencias por categoría de semáforo; la altura muestra el peso de cada severidad en el portafolio filtrado.",
                fig_scope="audit_sem",
            )
    with p2:
        if not sub_s.empty and _score_clip is not None and _score_med is not None and _score_p90 is not None:
            fig = px.histogram(sub_s[sub_s["_g_score"] <= _score_clip], x="_g_score", nbins=50,
                               title="Score de riesgo compuesto", labels={"_g_score": "Score"})
            fig.add_vline(x=_score_p90, line_dash="dash", line_color="#ef4444", annotation_text=f"P90={_score_p90:.1f}")
            fig.add_vline(x=_score_med, line_dash="dot", line_color="#facc15", annotation_text=f"Med={_score_med:.1f}")
            fig.update_layout(**_CL)
            _plotly_show(
                fig,
                "Histograma del score compuesto de riesgo; las líneas marcan mediana y P90 para situar la cola de mayor alerta.",
                fig_scope="audit_sem",
            )

    p3, p4 = st.columns(2, gap="medium")
    with p3:
        if sem_col and sem_col in df.columns and df["_g_abs_var_costo"].notna().any():
            df_box = df.dropna(subset=["_g_abs_var_costo"]).copy()
            df_box["_sem_str"] = df_box[sem_col].fillna("SIN_DATO").astype(str).str.strip()
            df_box["_sem_ui"] = df_box["_sem_str"].map(_auditoria_etiqueta_semaforo_ui)
            p95_bx = float(df_box["_g_abs_var_costo"].quantile(0.95))
            df_box = df_box[df_box["_g_abs_var_costo"] <= p95_bx * 1.1]
            cmap_bx = {ui: _auditoria_color_discreto_semaforo_ui(ui) for ui in df_box["_sem_ui"].unique()}
            fig = px.box(df_box, x="_sem_ui", y="_g_abs_var_costo", color="_sem_ui",
                         color_discrete_map=cmap_bx,
                         title="Severidad |Δ vs costo| por semáforo",
                         labels={"_sem_ui": "Semáforo", "_g_abs_var_costo": "|Δ vs costo| %"})
            fig.update_layout(showlegend=False, **_CL)
            _plotly_show(
                fig,
                "Diagrama de caja: distribución de |Δ vs costo| por semáforo; compara medianas, dispersión y valores atípicos entre severidades.",
                fig_scope="audit_sem",
            )
    with p4:
        if sem_col and sem_col in df.columns and rot_col and rot_col in df.columns:
            rot_sem = pd.crosstab(df[rot_col].astype(str), df[sem_col].fillna("SIN_DATO").astype(str))
            if not rot_sem.empty:
                rot_sem.columns = [_auditoria_etiqueta_semaforo_ui(c) for c in rot_sem.columns]
                rot_pct = rot_sem.div(rot_sem.sum(axis=1), axis=0) * 100
                fig = px.imshow(rot_pct, text_auto=".0f", aspect="auto",
                                color_continuous_scale="YlOrRd",
                                title="Rotación × semáforo (% fila)",
                                labels={"color": "%"})
                fig.update_layout(**_CL)
                _plotly_show(
                    fig,
                    "Mapa de calor: dentro de cada fila (rotación), qué % de referencias cae en cada semáforo; detecta rotaciones que concentran alertas.",
                    fig_scope="audit_sem",
                )

    if not sub_s.empty and _score_max is not None and _score_med is not None and _score_p90 is not None:
        st.caption("Resumen del score de riesgo (misma muestra que el histograma).")
        sm1, sm2, sm3 = st.columns(3, gap="small")
        sm1.metric("Mediana score", f"{_score_med:.1f}")
        sm2.metric("P90 score", f"{_score_p90:.1f}")
        sm3.metric("Máx score", f"{_score_max:.1f}")

    # ── CAP 3 · Anatomía ─────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("#### 3 · Anatomía — ¿de dónde vienen los desalineamientos?")
    st.caption(
        "Heatmap sistema × semáforo, scatter cobertura precio lista y bloque integrado de ratio costo/lista (histograma + KPIs)."
    )

    cap3_r1a, cap3_r1b = st.columns(2, gap="medium")
    with cap3_r1a:
        if sem_col and sistema_precio_col and sistema_precio_col in df.columns and sem_col in df.columns:
            top_sys = df[sistema_precio_col].astype(str).value_counts().head(10).index
            df_hm = df[df[sistema_precio_col].astype(str).isin(top_sys)]
            if not df_hm.empty:
                piv = pd.crosstab(df_hm[sistema_precio_col].astype(str), df_hm[sem_col].fillna("SIN_DATO").astype(str))
                piv.columns = [_auditoria_etiqueta_semaforo_ui(c) for c in piv.columns]
                fig = px.imshow(piv, text_auto=True, aspect="auto", color_continuous_scale="YlOrRd",
                                title="Sistema × semáforo (top 10)", labels={"color": "Refs"})
                fig.update_layout(**_CL)
                _plotly_show(
                    fig,
                    "Conteos por cruce sistema de precio × semáforo (top sistemas); el color indica volumen de referencias en cada celda.",
                    fig_scope="audit_sem",
                )

    with cap3_r1b:
        if precio_ult_cop_col and precio_lista_col and precio_ult_cop_col in df.columns and precio_lista_col in df.columns:
            df_sc_pl = df[[precio_ult_cop_col, precio_lista_col]].dropna()
            df_sc_pl = df_sc_pl[(df_sc_pl[precio_lista_col] > 0) & (df_sc_pl[precio_ult_cop_col] > 0)].copy()
            if not df_sc_pl.empty:
                df_sc_pl["_ratio"] = df_sc_pl[precio_ult_cop_col] / df_sc_pl[precio_lista_col] * 100
                p95_pl = float(df_sc_pl[precio_lista_col].quantile(0.95))
                p95_uc = float(df_sc_pl[precio_ult_cop_col].quantile(0.95))
                df_sc_v = df_sc_pl[(df_sc_pl[precio_lista_col] <= p95_pl * 1.1) & (df_sc_pl[precio_ult_cop_col] <= p95_uc * 1.1)]
                fig = px.scatter(df_sc_v, x=precio_lista_col, y=precio_ult_cop_col,
                                 opacity=0.35, color="_ratio", color_continuous_scale="RdYlGn_r",
                                 title="Precio lista vs costo compra — color=ratio%",
                                 labels={precio_lista_col: "P. Lista", precio_ult_cop_col: "Costo compra", "_ratio": "Ratio%"})
                mx_v = max(float(df_sc_v[precio_lista_col].max()), float(df_sc_v[precio_ult_cop_col].max()))
                fig.add_trace(go.Scatter(x=[0, mx_v], y=[0, mx_v], mode="lines",
                                         line=dict(color="#ef4444", dash="dash", width=1.5),
                                         name="100% = sin margen", showlegend=True))
                fig.update_traces(marker=dict(size=4), selector=dict(type="scatter"))
                fig.update_layout(**_CL)
                _plotly_show(
                    fig,
                    "Dispersión precio lista vs costo de compra; el color es el ratio costo/lista (%) y la diagonal roja es el punto sin margen (100 %).",
                    fig_scope="audit_sem",
                )

    # Fila 2: un solo bloque de análisis — ratio costo/lista (histograma + KPIs + lectura)
    _precio_ok = (
        precio_ult_cop_col
        and precio_lista_col
        and precio_ult_cop_col in df.columns
        and precio_lista_col in df.columns
    )
    if _precio_ok:
        df_pl = df[[precio_ult_cop_col, precio_lista_col]].dropna()
        df_pl = df_pl[(df_pl[precio_lista_col] > 0) & (df_pl[precio_ult_cop_col] > 0)].copy()
        if not df_pl.empty:
            df_pl["_ratio"] = df_pl[precio_ult_cop_col] / df_pl[precio_lista_col] * 100
            p95_r = float(df_pl["_ratio"].quantile(0.95))
            df_clip = df_pl[df_pl["_ratio"] <= max(p95_r * 1.1, 100)]
            n85 = int((df_pl["_ratio"] >= 85).sum())
            n100 = int((df_pl["_ratio"] >= 100).sum())
            n_compr = 0
            if var_c and var_c in df.columns:
                _vc = pd.to_numeric(df.loc[df_pl.index, var_c], errors="coerce")
                n_compr = int(((df_pl["_ratio"] >= 80) & (_vc.fillna(0) > 0)).sum())
            n_pop = max(len(df_pl), 1)
            with st.container(border=True):
                st.markdown(
                    '<div class="audit-ratio-unit-header">'
                    '<span class="audit-ratio-unit-header__kicker">Anatomía · Análisis integrado</span>'
                    '<p class="audit-ratio-unit-header__title">Ratio costo de compra ÷ precio de lista</p>'
                    '<p class="hint-text" style="margin:0.4rem 0 0 0;font-size:0.82rem;line-height:1.45;">'
                    "Una sola lectura: el histograma muestra la <strong>forma</strong> del ratio; "
                    "las tarjetas cuantifican <strong>riesgo</strong> sobre el mismo universo filtrado; "
                    "el texto resume cómo interpretar líneas y umbrales.</p>"
                    "</div>",
                    unsafe_allow_html=True,
                )
                cap3_r2a, cap3_r2b = st.columns([1.78, 0.9], gap="medium")
                with cap3_r2a:
                    fig = px.histogram(
                        df_clip,
                        x="_ratio",
                        nbins=50,
                        title="Distribución del ratio (%)",
                        labels={"_ratio": "Costo / P.Lista (%)"},
                        color_discrete_sequence=["#38bdf8"],
                    )
                    _auditoria_histogram_ratio_costo_lista_vlines(fig, df_clip)
                    fig.update_layout(**_CL)
                    fig.update_layout(title=dict(font=dict(size=14)))
                    _plotly_show(fig, caption=None, fig_scope="audit_sem")
                    rat = df_clip["_ratio"]
                    st.caption(
                        f"**Muestra del histograma:** μ {rat.mean():.1f}% · Md {rat.median():.1f}% · σ {rat.std():.1f}% · "
                        f"IQR {float(rat.quantile(0.75) - rat.quantile(0.25)):.1f} pp · n = **{len(rat):,}**"
                    )
                with cap3_r2b:
                    st.markdown(
                        '<p class="audit-ratio-side-title">Indicadores de riesgo (población filtrada)</p>',
                        unsafe_allow_html=True,
                    )
                    am1, am2, am3 = st.columns(3, gap="small")
                    with am1:
                        st.metric(
                            "≥ 85 % lista",
                            f"{n85:,}",
                            delta=f"{n85/n_pop*100:.1f}%",
                            delta_color="inverse",
                            help="Costo de compra ≥ 85 % del precio de lista: margen muy ajustado.",
                        )
                    with am2:
                        st.metric(
                            "≥ 100 %",
                            f"{n100:,}",
                            delta=f"{n100/n_pop*100:.1f}%",
                            delta_color="inverse",
                            help="Costo ≥ precio de lista: margen nulo o negativo.",
                        )
                    with am3:
                        st.metric(
                            "≥80 % + Δ > 0",
                            f"{n_compr:,}",
                            help="Ratio ≥ 80 % del precio lista y variación de compra positiva: revisar P.Lista o proveedor.",
                        )
                    st.markdown(
                        '<p class="hint-text" style="margin-top:0.55rem;line-height:1.5;">'
                        "<strong>Cómo leer el gráfico:</strong> el eje es <em>costo ÷ precio lista</em> (%): "
                        "a la izquierda hay más margen; a la derecha el costo absorbe más del precio público. "
                        "Las líneas tenues de fondo son P5, P95, cuartiles y Tukey; <strong>μ</strong>, <strong>Md</strong> y <strong>Mo</strong> (si sale) marcan la tendencia central. "
                        "Naranja ~85 % y roja 100 % son umbrales; las tarjetas cuentan refs en riesgo con la <strong>misma muestra</strong> que el histograma.</p>",
                        unsafe_allow_html=True,
                    )
                _render_fig_caption(
                    "audit_sem",
                    "Distribución del ratio costo ÷ precio lista (%): lectura integrada con las tarjetas de riesgo y el mismo filtro de referencias.",
                )

    # ── CAP 4 · Concentración ─────────────────────────────────────────────
    st.markdown("---")
    st.markdown("#### 4 · Concentración — ¿dónde se acumula el riesgo?")
    st.caption("Sistema, modelo, Pareto de inventario y rotación vs score: los 4 ejes de concentración.")

    co1, co2 = st.columns(2, gap="medium")
    with co1:
        if sistema_precio_col and sistema_precio_col in df.columns and df["_g_score"].notna().any():
            sys_agg = (
                df.groupby(df[sistema_precio_col].astype(str), dropna=False)
                .agg(score_max=("_g_score", "max"), score_media=("_g_score", "mean"),
                     refs=(ref_col, "nunique") if ref_col else ("_g_score", "count"))
                .reset_index().sort_values("score_media", ascending=False).head(12)
            )
            fig = px.scatter(sys_agg, x="score_media", y="score_max", size="refs",
                             text=sistema_precio_col, color="score_media",
                             color_continuous_scale="YlOrRd",
                             title="Sistemas: score medio vs máx",
                             labels={"score_media": "Score medio", "score_max": "Score máx", "refs": "Refs"})
            fig.update_traces(textposition="top center", textfont_size=8)
            fig.update_layout(**_CL)
            _plotly_show(
                fig,
                "Burbujas por sistema: score medio frente a score máximo; el tamaño es el número de referencias y el color refuerza el score medio para priorizar sistemas.",
                fig_scope="audit_sem",
            )

    with co2:
        modelo_col = ctx.get("modelo_col")
        if modelo_col and modelo_col in df.columns and df["_g_abs_var_costo"].notna().any():
            mod_sem = (
                df.groupby(df[modelo_col].astype(str), dropna=False)
                .agg(media_costo=("_g_abs_var_costo", "mean"),
                     refs=(ref_col, "nunique") if ref_col else ("_g_abs_var_costo", "count"),
                     pct_crit=("_g_abs_var_costo", lambda s: float((s >= umbral_v).sum() / max(len(s), 1) * 100)))
                .reset_index().sort_values("media_costo", ascending=False).head(12)
            )
            fig = px.bar(mod_sem, y=modelo_col, x="media_costo", orientation="h",
                         color="pct_crit", color_continuous_scale="Reds", text="refs",
                         title="Modelos con mayor |Δ vs costo|",
                         labels={"media_costo": "Media |Δ costo|%", modelo_col: "Modelo", "pct_crit": "%>=umbral"})
            fig.update_traces(textposition="outside")
            fig.update_layout(**_CL)
            _plotly_show(
                fig,
                "Barras horizontales: modelos con mayor |Δ vs costo| medio; el color muestra el % de referencias que superan el umbral de costo.",
                fig_scope="audit_sem",
            )

    co3, co4 = st.columns(2, gap="medium")
    with co3:
        ex_int_col = lower_map.get("existencia_intermedio")
        costo_int_col = lower_map.get("costo_intermedio")
        can_pareto = (ex_int_col and costo_int_col and ex_int_col in df.columns and costo_int_col in df.columns) or (
            ex_tot_col and costo_inv_col and ex_tot_col in df.columns and costo_inv_col in df.columns)
        if can_pareto and df["_g_score"].notna().any():
            df_p = df.copy()
            if ex_int_col and costo_int_col and ex_int_col in df_p.columns:
                df_p["_val_inv"] = pd.to_numeric(df_p[ex_int_col], errors="coerce").fillna(0) * pd.to_numeric(df_p[costo_int_col], errors="coerce").fillna(0)
            else:
                df_p["_val_inv"] = pd.to_numeric(df_p[ex_tot_col], errors="coerce").fillna(0) * pd.to_numeric(df_p[costo_inv_col], errors="coerce").fillna(0)
            ref_p = (
                df_p.groupby(ref_col if ref_col else "_g_score", dropna=False)
                .agg(val_inv=("_val_inv", "sum"), score=("_g_score", "max"))
                .reset_index().sort_values("val_inv", ascending=False)
            )
            ref_p = ref_p[ref_p["val_inv"] > 0]
            if not ref_p.empty:
                ref_p["cum_pct"] = ref_p["val_inv"].cumsum() / ref_p["val_inv"].sum() * 100
                ref_p["rank"] = range(1, len(ref_p) + 1)
                n80 = int((ref_p["cum_pct"] <= 80).sum())
                _ref_key = ref_col if ref_col else "_g_score"
                top60 = ref_p.head(60).copy()
                q75_sc = float(ref_p["score"].quantile(0.75))
                fig = make_subplots(specs=[[{"secondary_y": True}]])
                fig.add_trace(
                    go.Bar(
                        x=top60["rank"],
                        y=top60["val_inv"] / 1e6,
                        name="Inv. (M)",
                        marker_color=["#ef4444" if s > q75_sc else "#3b82f6" for s in top60["score"]],
                        customdata=list(
                            zip(
                                top60[_ref_key].astype(str),
                                top60["score"].round(2),
                                top60["cum_pct"].round(2),
                            )
                        ),
                        hovertemplate=(
                            "<b>Referencia</b>: %{customdata[0]}<br>"
                            "Rank: %{x}<br>"
                            "Valor inv. (M COP mill.): %{y:,.2f}<br>"
                            "Score: %{customdata[1]:.1f}<br>"
                            "% acumulado: %{customdata[2]:.1f}%<br>"
                            "<extra></extra>"
                        ),
                    ),
                    secondary_y=False,
                )
                fig.add_trace(
                    go.Scatter(x=top60["rank"], y=top60["cum_pct"],
                               mode="lines", name="% acum.", line=dict(color="#facc15", width=2)),
                    secondary_y=True)
                fig.add_hline(y=80, line_dash="dot", line_color="#a855f7", secondary_y=True,
                              annotation_text=f"80%={n80} refs")
                fig.update_layout(title="Pareto inv. expuesto (rojo=score alto)",
                                  xaxis_title="Rank", legend=dict(orientation="h", y=-0.2), **_CL)
                fig.update_yaxes(title_text="COP mill.", secondary_y=False)
                fig.update_yaxes(title_text="% acum.", secondary_y=True)
                _plotly_show(
                    fig,
                    "Pareto de inventario expuesto por referencia: barras = valor (COP mill.), curva = % acumulado; rojo = score de riesgo alto en esa ref.",
                    fig_scope="audit_sem",
                )
        else:
            st.caption("Sin datos de inventario para Pareto.")

    with co4:
        if rot_col and rot_col in df.columns and df["_g_abs_var_costo"].notna().any():
            rot_agg_s = (
                df.groupby(df[rot_col].astype(str), dropna=False)
                .agg(media_costo=("_g_abs_var_costo", "mean"),
                     score_medio=("_g_score", "mean"),
                     refs=(ref_col, "nunique") if ref_col else ("_g_abs_var_costo", "count"),
                     pct_crit=("_g_abs_var_costo", lambda s: float((s >= umbral_v).sum() / max(len(s), 1) * 100)))
                .reset_index()
            )
            rot_agg_s = rot_agg_s[rot_agg_s["refs"] >= 2]
            if not rot_agg_s.empty:
                fig = px.scatter(rot_agg_s, x=rot_col, y="media_costo", size="refs",
                                 color="pct_crit", color_continuous_scale="YlOrRd",
                                 title="Rotación vs |Δ costo| medio",
                                 labels={rot_col: "Rotación", "media_costo": "Media |Δ costo|%", "pct_crit": "%>=umbral"})
                fig.update_layout(**_CL)
                _plotly_show(
                    fig,
                    "Dispersión rotación vs |Δ costo| medio; el tamaño es el nº de refs y el color el % de referencias críticas respecto al umbral.",
                    fig_scope="audit_sem",
                )

    # ── CAP 5 · Plan de acción ────────────────────────────────────────────
    st.markdown("---")
    st.markdown("#### 5 · Plan de acción — top 20 por desalineación vs costo")
    st.caption("Prioridad = 0,60·|Δ vs costo| + 0,25·|Δ compra| + 0,15·score, ×1,5 si semáforo crítico.")

    df_cand = df.copy()
    df_cand["_pri_sem"] = (
        df_cand["_g_abs_var_costo"].fillna(0) * 0.60
        + df_cand["_g_abs_var_compra"].fillna(0) * 0.25
        + df_cand["_g_score"].fillna(0) * 0.15
    )
    if sem_col and sem_col in df_cand.columns:
        df_cand.loc[df_cand[sem_col].map(_auditoria_es_semaforo_critico), "_pri_sem"] *= 1.5
        mask_mod = df_cand[sem_col].map(_auditoria_es_semaforo_mod_alto) & ~df_cand[sem_col].map(_auditoria_es_semaforo_critico)
        df_cand.loc[mask_mod, "_pri_sem"] *= 1.2

    if ref_col and ref_col in df_cand.columns:
        agg_c: dict = {"_pri_sem": ("_pri_sem", "max")}
        if var_costo_pct and var_costo_pct in df_cand.columns:
            agg_c["var_costo"] = (var_costo_pct, "first")
        if var_c and var_c in df_cand.columns:
            agg_c["var_compra"] = (var_c, "first")
        if sem_col and sem_col in df_cand.columns:
            agg_c["semaforo"] = (sem_col, "first")
        if desc_col and desc_col in df_cand.columns:
            agg_c["descripcion"] = (desc_col, "first")
        if sistema_precio_col and sistema_precio_col in df_cand.columns:
            agg_c["sistema"] = (sistema_precio_col, "first")

        top = (
            df_cand.groupby(ref_col, dropna=False).agg(**agg_c)
            .reset_index().sort_values("_pri_sem", ascending=False).head(20)
        )
        if not top.empty:
            bar_c = []
            for _, r in top.iterrows():
                s = _auditoria_normalizar_etiqueta_semaforo(r.get("semaforo", ""))
                bar_c.append("#ef4444" if s == "CRITICO" else "#f59e0b" if s == "MODERADO ALTO" else "#3b82f6")
            vc_col = "var_costo" if "var_costo" in top.columns else None
            vp_col = "var_compra" if "var_compra" in top.columns else None
            text_vals = []
            for _, r in top.iterrows():
                vc_v = f"{float(r.get('var_costo', 0)):+.1f}%" if vc_col else ""
                vp_v = f"{float(r.get('var_compra', 0)):+.1f}%" if vp_col else ""
                text_vals.append(f"Δcosto {vc_v}  Δcompra {vp_v}".strip())
            fig = go.Figure(go.Bar(
                y=top[ref_col].astype(str), x=top["_pri_sem"], orientation="h",
                marker_color=bar_c, text=text_vals, textposition="outside"))
            fig.update_layout(
                title="Top 20 — prioridad por desalineación vs costo inv.",
                xaxis_title="Índice de prioridad", yaxis_title="Referencia",
                height=max(400, len(top) * 22), yaxis=dict(autorange="reversed"))
            _plotly_show(
                fig,
                "Barras horizontales: top 20 referencias por índice de prioridad de desalineación frente al costo de inventario; el texto resume Δ costo y Δ compra.",
                fig_scope="audit_sem",
            )


def _auditoria_charts_variacion_st(df_fil: pd.DataFrame, ctx: dict) -> None:
    """Eje Variación compra — storytelling: resumen → panorama → anatomía → concentración → plan."""
    if not _HAS_PLOTLY or px is None:
        st.warning("Instala **Plotly** para ver gráficos: `pip install plotly`.")
        return
    st.session_state["_fig_seq_audit_var"] = 0
    import plotly.graph_objects as go

    _CL = dict(height=440)

    df = _auditoria_prepare_chart_df(df_fil)
    sem_col = ctx["sem_col"]
    ref_col = ctx["ref_col"]
    var_c = ctx["var_compra_col"]
    var_costo_pct = ctx["var_costo_base_col"]
    dias_col = ctx["dias_col"]
    sistema_precio_col = ctx["sistema_precio_col"]
    modelo_col = ctx["modelo_col"]
    precio_ult_cop_col = ctx["precio_ult_cop_col"]
    precio_pen_cop_col = ctx["precio_pen_cop_col"]
    rot_col = ctx["rot_col"]
    desc_col = ctx["desc_col"]
    pais_ult_col = ctx["pais_ult_col"]

    umbral_c = float(st.session_state.get("aud_umbral_var_compra", 20.0))
    umbral_v = float(st.session_state.get("aud_umbral_var_costo", 15.0))

    n_max = 10_000
    if len(df) > n_max:
        df_sc = df.sample(n=n_max, random_state=7)
    else:
        df_sc = df

    # ── CAP 1 · Resumen ejecutivo ────────────────────────────────────────
    st.markdown("#### 1 · Resumen ejecutivo")
    st.caption("Vista rápida: magnitud y dirección del cambio de precio entre la última y penúltima compra.")

    n_total = len(df)
    var_series = pd.to_numeric(df[var_c], errors="coerce") if var_c and var_c in df.columns else pd.Series(dtype=float)
    n_sube = int((var_series.fillna(0) > umbral_c).sum())
    n_baja = int((var_series.fillna(0) < -umbral_c).sum())
    n_doble = int(((df["_g_abs_var_compra"].fillna(0) >= umbral_c) & (df["_g_abs_var_costo"].fillna(0) >= umbral_v)).sum())
    media_abs = float(df["_g_abs_var_compra"].dropna().mean()) if df["_g_abs_var_compra"].notna().any() else 0.0
    med_abs = float(df["_g_abs_var_compra"].dropna().median()) if df["_g_abs_var_compra"].notna().any() else 0.0

    with st.container(border=True):
        st.markdown(
            '<span class="audit-exec-kpi-inner" aria-hidden="true"></span>',
            unsafe_allow_html=True,
        )
        v1, v2, v3 = st.columns(3, gap="medium")
        v1.metric("Filtradas", f"{n_total:,}")
        v2.metric("Subió > umbral", f"{n_sube:,}", delta=f"{n_sube/max(n_total,1)*100:.1f}%", delta_color="inverse")
        v3.metric("Bajó > umbral", f"{n_baja:,}", delta=f"{n_baja/max(n_total,1)*100:.1f}%", delta_color="off")
        v4, v5, v6 = st.columns(3, gap="medium")
        v4.metric("Media |Δ compra|", f"{media_abs:.1f}%")
        v5.metric("Mediana |Δ compra|", f"{med_abs:.1f}%")
        v6.metric("Doble problema", f"{n_doble:,}", delta=f"{n_doble/max(n_total,1)*100:.1f}%", delta_color="inverse")

    # ── CAP 2 · Panorama ─────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("#### 2 · Panorama — magnitud del cambio entre compras")
    st.caption("Distribución del Δ% y contraste penúltima vs última compra (puntos lejos de la diagonal = saltos).")

    p1, p2 = st.columns(2, gap="medium")
    with p1:
        if precio_ult_cop_col and precio_pen_cop_col and precio_ult_cop_col in df.columns and precio_pen_cop_col in df.columns:
            df_pc = df[[precio_ult_cop_col, precio_pen_cop_col]].dropna().copy()
            if not df_pc.empty:
                df_pc["_delta_pct"] = (df_pc[precio_ult_cop_col] - df_pc[precio_pen_cop_col]) / df_pc[precio_pen_cop_col].replace(0, float("nan")) * 100
                p95 = float(df_pc["_delta_pct"].abs().quantile(0.95))
                df_clip = df_pc[df_pc["_delta_pct"].abs() <= p95 * 1.1]
                fig = px.histogram(df_clip, x="_delta_pct", nbins=60,
                                   color_discrete_sequence=["#38bdf8"],
                                   title="Cambio % última vs penúltima",
                                   labels={"_delta_pct": "Cambio (%)"})
                fig.add_vline(x=0, line_dash="solid", line_color="#64748b")
                fig.add_vline(x=umbral_c, line_dash="dash", line_color="#ef4444", annotation_text=f"+{umbral_c}%")
                fig.add_vline(x=-umbral_c, line_dash="dash", line_color="#22c55e", annotation_text=f"-{umbral_c}%")
                fig.update_layout(**_CL)
                _plotly_show(
                    fig,
                    "Histograma del cambio % entre última y penúltima compra; el centro es sin cambio y las líneas marcan ± el umbral configurado.",
                    fig_scope="audit_var",
                )

    with p2:
        if precio_ult_cop_col and precio_pen_cop_col and precio_ult_cop_col in df.columns and precio_pen_cop_col in df.columns:
            df_pc2 = df[[precio_ult_cop_col, precio_pen_cop_col]].dropna().copy()
            if not df_pc2.empty:
                df_pc2["_abs_delta"] = ((df_pc2[precio_ult_cop_col] - df_pc2[precio_pen_cop_col]) / df_pc2[precio_pen_cop_col].replace(0, float("nan")) * 100).abs()
                p95a = float(df_pc2["_abs_delta"].quantile(0.95))
                df_pc2 = df_pc2[df_pc2["_abs_delta"] <= p95a * 1.1]
                fig = px.scatter(df_pc2, x=precio_pen_cop_col, y=precio_ult_cop_col,
                                 opacity=0.35, color="_abs_delta", color_continuous_scale="YlOrRd",
                                 title="Penúltima vs última — color=|Δ%|",
                                 labels={precio_pen_cop_col: "Penúltima", precio_ult_cop_col: "Última", "_abs_delta": "|Δ%|"})
                mx = max(float(df_pc2[precio_pen_cop_col].max()), float(df_pc2[precio_ult_cop_col].max()))
                fig.add_trace(go.Scatter(x=[0, mx], y=[0, mx], mode="lines",
                                         line=dict(color="#64748b", dash="dot", width=1),
                                         showlegend=False))
                fig.update_traces(marker=dict(size=4), selector=dict(type="scatter"))
                fig.update_layout(**_CL)
                _plotly_show(
                    fig,
                    "Comparación directa de precios penúltima vs última compra; sobre la diagonal subió el precio, bajo la diagonal bajó; el color intensifica el |Δ %|.",
                    fig_scope="audit_var",
                )

    # ── CAP 3 · Anatomía ─────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("#### 3 · Anatomía — ¿de dónde vienen las variaciones?")
    st.caption("Cuadrante doble problema, temporalidad y dirección del cambio por sistema de precio.")

    a1, a2 = st.columns(2, gap="medium")
    with a1:
        has_both = df_sc["_g_abs_var_compra"].notna() & df_sc["_g_abs_var_costo"].notna()
        d2 = df_sc[has_both].copy()
        if not d2.empty:
            p97c = float(d2["_g_abs_var_compra"].quantile(0.97))
            p97v = float(d2["_g_abs_var_costo"].quantile(0.97))
            xm = max(p97c * 1.1, umbral_c * 2)
            ym = max(p97v * 1.1, umbral_v * 2)
            d2v = d2[(d2["_g_abs_var_compra"] <= xm) & (d2["_g_abs_var_costo"] <= ym)]
            if sem_col and sem_col in d2v.columns:
                d2v = d2v.copy()
                d2v["_sem_ui"] = d2v[sem_col].map(_auditoria_etiqueta_semaforo_ui)
                _cmap_q = {ui: _auditoria_color_discreto_semaforo_ui(ui) for ui in d2v["_sem_ui"].dropna().unique()}
                fig = px.scatter(
                    d2v, x="_g_abs_var_compra", y="_g_abs_var_costo", color="_sem_ui",
                    color_discrete_map=_cmap_q,
                    opacity=0.45, hover_data=[ref_col] if ref_col and ref_col in d2v.columns else None,
                    title="Cuadrante doble variación (P97)",
                    labels={"_g_abs_var_compra": "|Δ compra|%", "_g_abs_var_costo": "|Δ costo|%", "_sem_ui": "Semáforo"},
                )
            else:
                fig = px.scatter(d2v, x="_g_abs_var_compra", y="_g_abs_var_costo", color=None,
                                 opacity=0.45, hover_data=[ref_col] if ref_col and ref_col in d2v.columns else None,
                                 title="Cuadrante doble variación (P97)",
                                 labels={"_g_abs_var_compra": "|Δ compra|%", "_g_abs_var_costo": "|Δ costo|%"})
            fig.update_traces(marker=dict(size=5))
            fig.add_vline(x=umbral_c, line_dash="dot", line_color="#f59e0b")
            fig.add_hline(y=umbral_v, line_dash="dot", line_color="#f59e0b")
            fig.add_vrect(x0=umbral_c, x1=xm, y0=umbral_v, y1=ym, fillcolor="red", opacity=0.06,
                          annotation_text="Doble problema")
            fig.update_layout(**_CL)
            _plotly_show(
                fig,
                "Cada punto es una referencia: |Δ compra| vs |Δ costo|; la zona sombreada destaca el doble problema (ambas magnitudes por encima del umbral).",
                fig_scope="audit_var",
            )

    with a2:
        has_both_f = df["_g_abs_var_compra"].notna() & df["_g_abs_var_costo"].notna()
        d2f = df[has_both_f].copy()
        if not d2f.empty:
            d2f["_cuad"] = "Bajo ambos"
            d2f.loc[(d2f["_g_abs_var_compra"] >= umbral_c) & (d2f["_g_abs_var_costo"] < umbral_v), "_cuad"] = "Solo Δ compra"
            d2f.loc[(d2f["_g_abs_var_compra"] < umbral_c) & (d2f["_g_abs_var_costo"] >= umbral_v), "_cuad"] = "Solo Δ costo"
            d2f.loc[(d2f["_g_abs_var_compra"] >= umbral_c) & (d2f["_g_abs_var_costo"] >= umbral_v), "_cuad"] = "Doble problema"
            qc = d2f["_cuad"].value_counts().reset_index()
            qc.columns = ["Cuadrante", "n"]
            cmap_q = {"Doble problema": "#ef4444", "Solo Δ compra": "#f59e0b", "Solo Δ costo": "#3b82f6", "Bajo ambos": "#22c55e"}
            fig = px.bar(qc, x="Cuadrante", y="n", color="Cuadrante",
                         color_discrete_map=cmap_q, title="Distribución por cuadrante",
                         labels={"n": "Refs"})
            fig.update_layout(showlegend=False, **_CL)
            _plotly_show(
                fig,
                "Barras: cuántas referencias caen en cada cuadrante (solo compra, solo costo, doble problema o bajo ambos).",
                fig_scope="audit_var",
            )

    a3, a4 = st.columns(2, gap="medium")
    with a3:
        if dias_col and dias_col in df_sc.columns and df_sc["_g_abs_var_compra"].notna().any():
            d3 = df_sc.dropna(subset=[dias_col, "_g_abs_var_compra"])
            if not d3.empty:
                p95d = float(d3[dias_col].quantile(0.95))
                p95v = float(d3["_g_abs_var_compra"].quantile(0.95))
                d3c = d3[(d3[dias_col] <= p95d * 1.1) & (d3["_g_abs_var_compra"] <= p95v * 1.1)]
                use_sc = "_g_score" in d3c.columns and d3c["_g_score"].notna().any()
                fig = px.scatter(d3c, x=dias_col, y="_g_abs_var_compra", opacity=0.4,
                                 color="_g_score" if use_sc else None,
                                 color_continuous_scale="YlOrRd" if use_sc else None,
                                 title="Días entre compras vs |Δ compra|",
                                 labels={dias_col: "Días", "_g_abs_var_compra": "|Δ compra|%"})
                fig.update_traces(marker=dict(size=4), selector=dict(type="scatter"))
                fig.update_layout(**_CL)
                _plotly_show(
                    fig,
                    "Relación entre días entre compras y magnitud de |Δ compra|; el color puede ser el score de riesgo cuando hay dato.",
                    fig_scope="audit_var",
                )

    with a4:
        if (precio_ult_cop_col and precio_pen_cop_col and sistema_precio_col
                and all(c in df.columns for c in [precio_ult_cop_col, precio_pen_cop_col, sistema_precio_col])):
            sys_dir = (
                df.groupby(df[sistema_precio_col].astype(str), dropna=False)
                .agg(
                    subio=(var_c, lambda s: int((pd.to_numeric(s, errors="coerce") > umbral_c).sum())) if var_c and var_c in df.columns else (precio_ult_cop_col, "count"),
                    bajo=(var_c, lambda s: int((pd.to_numeric(s, errors="coerce") < -umbral_c).sum())) if var_c and var_c in df.columns else (precio_ult_cop_col, "count"),
                    estable=(var_c, lambda s: int((pd.to_numeric(s, errors="coerce").abs() <= umbral_c).sum())) if var_c and var_c in df.columns else (precio_ult_cop_col, "count"),
                    refs=(ref_col, "nunique") if ref_col else (precio_ult_cop_col, "count"),
                )
                .reset_index().sort_values("subio", ascending=False).head(12)
            )
            if not sys_dir.empty and var_c:
                sys_m = sys_dir.melt(id_vars=[sistema_precio_col, "refs"],
                                     value_vars=["subio", "bajo", "estable"],
                                     var_name="Dir", value_name="n")
                dir_lab = {"subio": f"Subió>{umbral_c}%", "bajo": f"Bajó>{umbral_c}%", "estable": "Estable"}
                dir_col = {"subio": "#ef4444", "bajo": "#22c55e", "estable": "#64748b"}
                sys_m["Dir"] = sys_m["Dir"].map(dir_lab)
                cmap_d = {v: dir_col[k] for k, v in dir_lab.items()}
                fig = px.bar(sys_m, y=sistema_precio_col, x="n", color="Dir",
                             orientation="h", barmode="stack", color_discrete_map=cmap_d,
                             title=f"Dirección por sistema (±{umbral_c}%)",
                             labels={"n": "Refs", sistema_precio_col: "Sistema"})
                fig.update_layout(**_CL)
                _plotly_show(
                    fig,
                    "Barras apiladas por sistema: cuántas referencias subieron, bajaron o quedaron estables frente al umbral de variación de compra.",
                    fig_scope="audit_var",
                )

    # ── CAP 4 · Concentración ─────────────────────────────────────────────
    st.markdown("---")
    st.markdown("#### 4 · Concentración — ¿en qué segmentos?")
    st.caption("Modelo, país de origen y rotación: los tres ejes de la cadena de abastecimiento.")

    co1, co2 = st.columns(2, gap="medium")
    with co1:
        if modelo_col and modelo_col in df.columns and df["_g_abs_var_compra"].notna().any():
            mod_agg = (
                df.groupby(df[modelo_col].astype(str), dropna=False)
                .agg(media_var=("_g_abs_var_compra", "mean"),
                     refs=(ref_col, "nunique") if ref_col else ("_g_abs_var_compra", "count"),
                     pct_sobre=("_g_abs_var_compra", lambda s: float((s >= umbral_c).sum() / max(len(s), 1) * 100)))
                .reset_index().sort_values("media_var", ascending=False).head(12)
            )
            fig = px.bar(mod_agg, y=modelo_col, x="media_var", orientation="h",
                         color="pct_sobre", color_continuous_scale="Reds", text="refs",
                         title="Modelos con mayor |Δ compra|",
                         labels={"media_var": "Media |Δ|%", modelo_col: "Modelo", "pct_sobre": "%≥umbral"})
            fig.update_traces(textposition="outside")
            fig.update_layout(**_CL)
            _plotly_show(
                fig,
                "Modelos con mayor |Δ compra| medio; el color indica el % de referencias que superan el umbral de variación de compra.",
                fig_scope="audit_var",
            )

    with co2:
        if pais_ult_col and pais_ult_col in df.columns and df["_g_abs_var_compra"].notna().any():
            pais_agg = (
                df.groupby(df[pais_ult_col].fillna("SIN DATO").astype(str), dropna=False)
                .agg(media_var=("_g_abs_var_compra", "mean"),
                     refs=(ref_col, "nunique") if ref_col else ("_g_abs_var_compra", "count"),
                     pct_sobre=("_g_abs_var_compra", lambda s: float((s >= umbral_c).sum() / max(len(s), 1) * 100)))
                .reset_index()
            )
            pais_agg = pais_agg[pais_agg["refs"] >= 3].sort_values("media_var", ascending=False).head(12)
            if not pais_agg.empty:
                fig = px.bar(pais_agg, y=pais_ult_col, x="media_var", orientation="h",
                             color="pct_sobre", color_continuous_scale="YlOrRd", text="refs",
                             title="Variación por país de origen",
                             labels={"media_var": "Media |Δ|%", pais_ult_col: "País", "pct_sobre": "%≥umbral"})
                fig.update_traces(textposition="outside")
                fig.update_layout(**_CL)
                _plotly_show(
                    fig,
                    "País de origen con mayor variación media de compra; prioriza orígenes con más inestabilidad de precio.",
                    fig_scope="audit_var",
                )

    if rot_col and rot_col in df.columns and df["_g_abs_var_compra"].notna().any():
        rot_agg = (
            df.groupby(df[rot_col].astype(str), dropna=False)
            .agg(media_var=("_g_abs_var_compra", "mean"),
                 refs=(ref_col, "nunique") if ref_col else ("_g_abs_var_compra", "count"),
                 pct_crit=("_g_abs_var_compra", lambda s: float((s >= umbral_c).sum() / max(len(s), 1) * 100)))
            .reset_index()
        )
        rot_agg = rot_agg[rot_agg["refs"] >= 2]
        if not rot_agg.empty:
            ro1, ro2 = st.columns(2)
            with ro1:
                fig = px.scatter(rot_agg, x=rot_col, y="media_var", size="refs",
                                 color="pct_crit", color_continuous_scale="YlOrRd",
                                 title="Rotación vs variación",
                                 labels={rot_col: "Rotación", "media_var": "Media |Δ|%", "pct_crit": "%≥umbral"})
                fig.add_hline(y=umbral_c, line_dash="dot", line_color="#ef4444")
                fig.update_layout(**_CL)
                _plotly_show(
                    fig,
                    "Rotación vs |Δ compra| medio; el tamaño es el nº de referencias y la línea horizontal marca el umbral de compra.",
                    fig_scope="audit_var",
                )
            with ro2:
                if dias_col and dias_col in df.columns and df["_g_abs_var_compra"].notna().any():
                    d4 = df.dropna(subset=[dias_col, "_g_abs_var_compra"])
                    if not d4.empty:
                        d4["_dias_bin"] = pd.qcut(d4[dias_col], q=8, duplicates="drop")
                        dias_a = (
                            d4.groupby("_dias_bin", observed=True)["_g_abs_var_compra"]
                            .agg(media="mean", p75=lambda s: s.quantile(0.75))
                            .reset_index()
                        )
                        dias_a["label"] = dias_a["_dias_bin"].astype(str)
                        fig = go.Figure()
                        fig.add_trace(go.Scatter(x=dias_a["label"], y=dias_a["media"],
                                                 mode="lines+markers", name="Media",
                                                 line=dict(color="#38bdf8", width=2)))
                        fig.add_trace(go.Scatter(x=dias_a["label"], y=dias_a["p75"],
                                                 mode="lines", name="P75",
                                                 line=dict(color="#ef4444", width=1.5, dash="dash")))
                        fig.update_layout(title="Tendencia |Δ| por octil de días",
                                          xaxis_title="Días (octil)", yaxis_title="|Δ compra|%",
                                          xaxis_tickangle=-35, legend=dict(orientation="h", y=-0.25),
                                          **_CL)
                        _plotly_show(
                            fig,
                            "Tendencia de |Δ compra| según tramos de días entre compras (octiles); media y P75 muestran si el cambio crece con el tiempo sin comprar.",
                            fig_scope="audit_var",
                        )

    # ── CAP 5 · Plan de acción ────────────────────────────────────────────
    st.markdown("---")
    st.markdown("#### 5 · Plan de acción — top 20 por variación de compra")
    st.caption("Prioridad = 0,60·|Δ compra| + 0,25·|Δ costo| + 0,15·score, ×1,5 si semáforo crítico.")

    df_cand = df.copy()
    df_cand["_pri_var"] = (
        df_cand["_g_abs_var_compra"].fillna(0) * 0.60
        + df_cand["_g_abs_var_costo"].fillna(0) * 0.25
        + df_cand["_g_score"].fillna(0) * 0.15
    )
    if sem_col and sem_col in df_cand.columns:
        df_cand.loc[df_cand[sem_col].map(_auditoria_es_semaforo_critico), "_pri_var"] *= 1.5
        mask_mod = df_cand[sem_col].map(_auditoria_es_semaforo_mod_alto) & ~df_cand[sem_col].map(_auditoria_es_semaforo_critico)
        df_cand.loc[mask_mod, "_pri_var"] *= 1.2

    if ref_col and ref_col in df_cand.columns:
        agg_c: dict = {"_pri_var": ("_pri_var", "max")}
        if var_c and var_c in df_cand.columns:
            agg_c["var_compra"] = (var_c, "first")
        if var_costo_pct and var_costo_pct in df_cand.columns:
            agg_c["var_costo"] = (var_costo_pct, "first")
        if sem_col and sem_col in df_cand.columns:
            agg_c["semaforo"] = (sem_col, "first")
        if desc_col and desc_col in df_cand.columns:
            agg_c["descripcion"] = (desc_col, "first")
        if sistema_precio_col and sistema_precio_col in df_cand.columns:
            agg_c["sistema"] = (sistema_precio_col, "first")

        top = (
            df_cand.groupby(ref_col, dropna=False).agg(**agg_c)
            .reset_index().sort_values("_pri_var", ascending=False).head(20)
        )
        if not top.empty:
            bar_c = []
            for _, r in top.iterrows():
                s = _auditoria_normalizar_etiqueta_semaforo(r.get("semaforo", ""))
                bar_c.append("#ef4444" if s == "CRITICO" else "#f59e0b" if s == "MODERADO ALTO" else "#3b82f6")
            text_vals = []
            for _, r in top.iterrows():
                vp = f"{float(r.get('var_compra', 0)):+.1f}%" if "var_compra" in top.columns else ""
                vc = f"{float(r.get('var_costo', 0)):+.1f}%" if "var_costo" in top.columns else ""
                text_vals.append(f"Δcompra {vp}  Δcosto {vc}".strip())
            fig = go.Figure(go.Bar(
                y=top[ref_col].astype(str), x=top["_pri_var"], orientation="h",
                marker_color=bar_c, text=text_vals, textposition="outside"))
            fig.update_layout(
                title="Top 20 — prioridad por variación de compra",
                xaxis_title="Índice de prioridad", yaxis_title="Referencia",
                height=max(400, len(top) * 22), yaxis=dict(autorange="reversed"))
            _plotly_show(
                fig,
                "Top 20 referencias por índice de prioridad de variación de compra; el texto muestra Δ compra y Δ costo para acción inmediata.",
                fig_scope="audit_var",
            )




_render_header_y_actualizacion()

tabs = st.tabs(
    [
        "Consulta referencias",
        "Resumen de ventas",
        "Reporte margen SIESA",
        "Auditoría referencias",
    ]
)
with tabs[0]:
    _render_tab_consulta()
with tabs[1]:
    _render_tab_resumen_ventas()
with tabs[2]:
    _render_tab_margen()
with tabs[3]:
    _render_tab_auditoria_referencias()
