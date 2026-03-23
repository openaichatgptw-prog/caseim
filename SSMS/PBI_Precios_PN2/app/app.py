from __future__ import annotations

import html
import io
import math
import re
from typing import Final

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
    refrescar_catalogo_bodegas_auditoria,
    obtener_dataset_margenes,
    obtener_resumen_referencia,
    obtener_ultimas_ventas,
    sync_read_db,
)
from services.filter_prefs import load_filter_prefs_into_session, render_reset_filters_button, save_tab_filter_prefs
from services.pipeline_runner import ejecutar_pipelines
from services.sql_reports_loader import SQL_001_KEY, SQL_002_KEY, SQL_003_KEY

PIPELINE_OPCIONES: Final[list[str]] = [
    "01_Mejora_pipeline_precios_chnV21.py",
    "02_ventas_precios_cnhV2.py",
    "03_Maestro_historico.py",
]
SQL_001_OPCION: Final[str] = "SQL 001 — Margen SIESA (margen_siesa_raw)"
SQL_002_OPCION: Final[str] = "SQL 002 — Atributos refs (atributos_referencias_raw)"
SQL_003_OPCION: Final[str] = "SQL 003 — Auditoría refs (auditoria_raw)"
SQL_OPCIONES: Final[list[str]] = [SQL_001_OPCION, SQL_002_OPCION, SQL_003_OPCION]


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
        margin-top: 1rem;
        margin-bottom: 0.5rem;
        font-weight: 650;
        font-size: 1rem;
    }
    div[data-testid="stMetric"] {
        border: 1px solid var(--ui-border, #25314d);
        border-radius: var(--ui-radius, 10px);
        padding: 0.38rem 0.5rem;
        background: var(--ui-surface, #101a31);
        box-shadow: var(--ui-shadow, 0 3px 10px rgba(0, 0, 0, 0.22));
        min-height: auto;
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
    }
    div[data-testid="stMetric"] [data-testid="stMetricDelta"] {
        font-size: 0.72rem !important;
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
        padding: 0.48rem 0.55rem;
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
        height: 0.45rem;
    }
    /* Hasta 10 alternas: rejilla 5×2 en desktop, se adapta en móvil */
    .alt-chip-grid {
        display: grid;
        grid-template-columns: repeat(5, minmax(0, 1fr));
        gap: 0.35rem;
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
        gap: 0.5rem;
        margin-top: 0.35rem;
        align-items: stretch;
    }
    .origin-grid-unified > .ui-card--origin {
        min-width: 0;
    }
    .origin-grid-unified .ui-origin-col--px {
        font-size: 1.02rem;
    }
    @media (max-width: 768px) {
        .origin-grid-unified {
            grid-template-columns: 1fr;
        }
        .origin-grid-unified .ui-origin-col--px {
            font-size: 1.1rem;
        }
    }
    /* Una fila densa: origen | precio | disponibilidad (sin hueco a la derecha) */
    .ui-card--origin-row {
        display: grid;
        grid-template-columns: minmax(0, 1.1fr) minmax(3.5rem, 1fr) minmax(4.2rem, 0.9fr);
        align-items: center;
        column-gap: 0.45rem;
        padding: 0.38rem 0.55rem !important;
        min-width: 0;
    }
    .ui-origin-name-line {
        display: flex;
        flex-wrap: wrap;
        align-items: center;
        gap: 0.35rem;
    }
    .ui-origin-name {
        font-size: 0.8rem;
        font-weight: 650;
        color: var(--ui-muted);
    }
    .ui-origin-col--px {
        font-size: 1.1rem;
        font-weight: 700;
        color: #f8fafc;
        text-align: center;
        line-height: 1.15;
    }
    .ui-origin-col--dp {
        text-align: right;
        display: flex;
        flex-direction: column;
        align-items: flex-end;
        gap: 0.08rem;
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
    .ui-best-pill {
        display: inline-block;
        font-size: 0.6rem;
        font-weight: 700;
        letter-spacing: 0.05em;
        text-transform: uppercase;
        color: #0d9488;
        background: rgba(20, 184, 166, 0.14);
        border-radius: 999px;
        padding: 0.1rem 0.4rem;
        white-space: nowrap;
    }
    @media (max-width: 640px) {
        .ui-card--origin-row {
            grid-template-columns: 1fr;
            row-gap: 0.45rem;
        }
        .ui-origin-col--px {
            text-align: left;
        }
        .ui-origin-col--dp {
            align-items: flex-start;
            text-align: left;
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
    if _fmt_consulta_display(val) == "—":
        return "—"
    try:
        return f"{float(val):,.2f}"
    except (TypeError, ValueError):
        return "—"


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
        return f"${v/1_000_000_000:,.2f} mil M"
    if av >= 1_000_000:
        return f"${v/1_000_000:,.1f} M"
    if av >= 1_000:
        return f"${v/1_000:,.1f} mil"
    return f"${v:,.0f}"


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
        # Precios COP ajustados (formato moneda estándar en grid)
        if "precio" in lc or "log" in lc:
            return st.column_config.NumberColumn(label, format="dollar")
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
            "semaforo",
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
        return st.column_config.NumberColumn(label, format="$%,.0f")
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
        return st.column_config.NumberColumn(label, format="dollar")
    if any(x in lc for x in ("valor_inventario", "absvar_costo")) and "pct" not in lc:
        return st.column_config.NumberColumn(label, format="$%,.0f")
    if any(x in lc for x in ("costo_min", "costo_max", "costo_intermedio")) and "var_" not in lc:
        return st.column_config.NumberColumn(label, format="$%,.0f")
    if lc.startswith("var_"):
        return st.column_config.NumberColumn(label, format="%.2f%%")
    if any(x in lc for x in ("existencia", "disponible", "dias", "nrobod", "numcostos")):
        return st.column_config.NumberColumn(label, format="%,.0f")
    if lc == "escostounico":
        return st.column_config.NumberColumn(label, format="%,.0f")
    # Rescate: cualquier otro campo de precio (no variación %)
    if "precio" in lc and not lc.startswith("var_"):
        return st.column_config.NumberColumn(label, format="dollar")
    if "costo" in lc and not lc.startswith("var_") and "bodega" not in lc:
        if "pct" in lc:
            return st.column_config.NumberColumn(label, format="%.2f%%")
        return st.column_config.NumberColumn(label, format="$%,.0f")
    return st.column_config.TextColumn(label, width="medium")


def _auditoria_build_column_config(orig_cols: list[str], alias_cols: dict[str, str]) -> dict[str, st.column_config.Column]:
    out: dict[str, st.column_config.Column] = {}
    for orig in orig_cols:
        lbl = alias_cols.get(orig, orig)
        out[lbl] = _auditoria_one_column_config(orig, lbl)
    return out


def _auditoria_vista_bloques(
    lower_map: dict[str, str],
) -> list[tuple[str, list[str]]]:
    """Bloques alineados al objetivo: priorizar refs con Δ última vs penúltima y Δ última vs costo prom. inv."""
    return [
        (
            "Identificación y política",
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
            [
                "semaforo_variacion",
                "_score_alerta",
            ],
        ),
        (
            "Problema 1 — variación última vs penúltima compra",
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
    "Margen_Objetivo_Sistema": "Margen sistema",
    "Valor": "Valor",
    "Pct_sobre_margen": "% sobre margen",
    # Auditoría referencias (SQL 00)
    "U.M.": "U.M.",
    "Costo_Min": "Costo mín.",
    "Bodega_CostoMin": "Bod. costo mín.",
    "Costo_Intermedio": "Costo interm.",
    "Costo_Max": "Costo máx.",
    "Bodega_CostoMax": "Bod. costo máx.",
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
def _cargar_margenes_para_dashboard() -> tuple:
    df = obtener_dataset_margenes(limite=0, margen_min=-10_000.0, margen_max=100.0)
    df = _coerce_margen_numeric_columns(df)
    margen_col = None
    for cand in ("Margen09", "Margen_Pct", "Margen04"):
        if cand in df.columns:
            margen_col = cand
            break
    return df, margen_col


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
                preset = st.radio(
                    "Modo",
                    ["Rápida", "Completa", "Personalizada"],
                    horizontal=True,
                    index=1,
                )
                todas_opciones = PIPELINE_OPCIONES + SQL_OPCIONES
                if preset == "Rápida":
                    seleccion = PIPELINE_OPCIONES.copy()
                    st.caption("Rápida: actualiza 01, 02 y 03 (sin SQL 00).")
                elif preset == "Completa":
                    seleccion = todas_opciones.copy()
                    st.caption("Completa: actualiza 01, 02, 03 y las 3 consultas SQL 00.")
                else:
                    selec_todas = st.checkbox("Seleccionar todas", value=True)
                    if selec_todas:
                        seleccion = todas_opciones.copy()
                    else:
                        seleccion = st.multiselect(
                            "Consultas/procesos",
                            options=todas_opciones,
                            default=PIPELINE_OPCIONES,
                        )
                forzar_actualizar = st.checkbox(
                    "Forzar actualización (cerrar lecturas)",
                    value=False,
                )
                incluye_sql_003 = SQL_003_OPCION in seleccion
                incluye_algun_sql = any(op in seleccion for op in SQL_OPCIONES)
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

    if not seleccion:
        st.warning("Selecciona al menos un proceso para actualizar.")
        return

    if forzar_actualizar:
        st.info("Modo forzado activo: se ejecutarán también las consultas SQL 00 seleccionadas.")

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
        if SQL_003_OPCION in seleccion or forzar_actualizar:
            bodegas_aud = list(st.session_state.get("auditoria_bodegas_sel") or [])
        with st.spinner("Ejecutando pipelines..."):
            ok, _ = ejecutar_pipelines(
                log_callback=_on_log_update,
                ejecutar_reportes_sql=(ejecutar_sql or forzar_actualizar),
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

    st.markdown('<div class="section-title">Búsqueda</div>', unsafe_allow_html=True)

    texto_busqueda = st.text_input(
        "Referencia principal, alterna, normalizada o texto en descripción",
        placeholder="Ej: código de pieza, ref. alterna o palabra del nombre",
        key="consulta_txt_busqueda",
    )

    try:
        df_refs = buscar_referencias(texto_busqueda) if texto_busqueda else None
    except Exception as exc:
        st.error(f"No fue posible leer datos de DuckDB: {exc}")
        return

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
        if "consulta_coincidencias" in st.session_state:
            _cur = st.session_state["consulta_coincidencias"]
            if _cur not in opciones:
                del st.session_state["consulta_coincidencias"]
        seleccionado = st.selectbox("Coincidencias", opts_keys, key="consulta_coincidencias")
        ref_norm = opciones[seleccionado]
    elif texto_busqueda:
        st.warning("No se encontraron coincidencias por referencia ni por descripción.")
        ref_norm = None
    else:
        ref_norm = None

    save_tab_filter_prefs("consulta")

    if not ref_norm:
        return

    try:
        resumen = obtener_resumen_referencia(ref_norm)
    except Exception as exc:
        st.error(f"No fue posible consultar la referencia: {exc}")
        return

    if not resumen:
        st.info("La referencia no tiene datos en `resultado_precios_lista`.")
        return

    refs_alternas = str(resumen.get("RefsAlternas", "") or "").strip()
    ref_original = str(resumen.get("Referencia_Original", "") or "").strip().upper()

    c1, c2, c3, c4 = st.columns(4, gap="small")
    c1.metric("Referencia", str(resumen.get("Referencia_Original", "-")))
    c2.metric(
        "Precio prorrateo",
        f"{resumen.get('Precio Prorrateo', 0):,.2f}"
        if resumen.get("Precio Prorrateo") is not None
        else "-",
    )
    c3.metric("Disponibilidad total", f"{resumen.get('_disp_total', 0):,.2f}")
    c4.metric("Disponible", resumen.get("_disponible", "NO"))

    st.markdown('<div class="consulta-strip-gap"></div>', unsafe_allow_html=True)

    if refs_alternas:
        refs = _parse_refs_alternas(refs_alternas)
        if refs:
            total_alt = len(refs)
            refs_show = refs[:_CONSULTA_MAX_ALTERNAS]
            chips_html: list[str] = []
            for ref in refs_show:
                is_selected = ref.upper() == ref_original
                cls = "alt-chip selected" if is_selected else "alt-chip"
                role = "Principal" if is_selected else "Alterna"
                title_attr = html.escape(ref, quote=True)
                chips_html.append(
                    f'<div class="{cls}" title="{title_attr}">'
                    f'<span class="alt-chip-role">{html.escape(role)}</span>'
                    f'<span class="alt-chip-ref">{html.escape(ref)}</span>'
                    f"</div>"
                )
            cap_extra = ""
            if total_alt > _CONSULTA_MAX_ALTERNAS:
                cap_extra = (
                    f'<p class="alt-chip-note">Mostrando {_CONSULTA_MAX_ALTERNAS} de {total_alt} '
                    "alternas registradas.</p>"
                )
            st.markdown(
                '<div class="ui-card ui-card--tight">'
                '<div class="ui-card-heading">Referencias alternas</div>'
                '<div class="alt-chip-grid">'
                + "".join(chips_html)
                + "</div>"
                + cap_extra
                + "</div>",
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                '<div class="ui-card ui-card--tight">'
                '<div class="ui-card-heading">Referencias alternas</div>'
                '<p style="margin:0;color:#9ca3af;font-size:0.88rem;">Sin alternas registradas.</p>'
                "</div>",
                unsafe_allow_html=True,
            )
    else:
        st.markdown(
            '<div class="ui-card ui-card--tight">'
            '<div class="ui-card-heading">Referencias alternas</div>'
            '<p style="margin:0;color:#9ca3af;font-size:0.88rem;">Sin alternas registradas.</p>'
            "</div>",
            unsafe_allow_html=True,
        )

    st.markdown('<div class="section-title">Mejor precio por origen</div>', unsafe_allow_html=True)
    candidatos = [
        ("Brasil", _to_float(resumen.get("Precio Brasil")), _to_float(resumen.get("disp_br"))),
        ("USA", _to_float(resumen.get("Precio Usa")), _to_float(resumen.get("disp_usa"))),
        ("Europa", _to_float(resumen.get("Precio Europa")), _to_float(resumen.get("disp_eur"))),
    ]
    flags = {"Brasil": "🇧🇷", "USA": "🇺🇸", "Europa": "🇪🇺"}
    disponibles = [
        (origen, precio, disp)
        for origen, precio, disp in candidatos
        if precio is not None and disp is not None and disp > 0
    ]
    mejores_origenes = set()
    if disponibles:
        best_price = min(precio for _, precio, _ in disponibles)
        mejores_origenes = {origen for origen, precio, _ in disponibles if precio == best_price}

    origin_cells: list[str] = []
    for origen, precio, disp in candidatos:
        is_best_origin = origen in mejores_origenes
        flag = flags.get(origen, "")
        precio_s = f"${precio:,.2f}" if precio is not None else "—"
        disp_s = f"{disp:,.2f}" if disp is not None else "—"
        cls = "ui-card ui-card--tight ui-card--origin" + (" is-best" if is_best_origin else "")
        pill_html = '<span class="ui-best-pill">Mejor precio</span>' if is_best_origin else ""
        origin_cells.append(
            f'<div class="{cls} ui-card--origin-row">'
            f'<div><div class="ui-origin-name-line">'
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
    st.markdown(
        f'<div class="origin-grid-unified">{"".join(origin_cells)}</div>',
        unsafe_allow_html=True,
    )

    if not mejores_origenes:
        st.info("Ningún origen tiene disponibilidad para recomendar precio.")

    st.markdown('<div class="section-title">Última compra registrada</div>', unsafe_allow_html=True)
    compra_cols = st.columns(4, gap="small")
    compra_cols[0].metric("Fecha", _fmt_consulta_fecha(resumen.get("Ult. Fecha Compra")))
    compra_cols[1].metric("Proveedor", _fmt_consulta_display(resumen.get("Proveedor")))
    compra_cols[2].metric("Último valor (USD)", _fmt_consulta_money(resumen.get("Último Valor (USD)")))
    compra_cols[3].metric("Valor liq. (COP)", _fmt_consulta_money(resumen.get("Valor Liq. (COP)")))

    try:
        ventas = obtener_ultimas_ventas(ref_norm, limite=20)
    except Exception as exc:
        st.error(f"No fue posible consultar ventas: {exc}")
        ventas = None
    st.markdown('<div class="section-title">Últimas ventas</div>', unsafe_allow_html=True)
    if ventas is None:
        st.info("Consulta de ventas no disponible en este momento.")
    elif ventas.empty:
        st.info("No hay ventas en `ventas_raw` para esta referencia.")
    else:
        st.dataframe(_renombrar_negocio(ventas), width="stretch", hide_index=True)


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
    filtros1, filtros2, filtros3 = st.columns([2.0, 1.0, 1.0], gap="large")
    with filtros1:
        sub_m1, sub_m2 = st.columns([1.35, 1.0], gap="medium")
        with sub_m1:
            txt_busqueda = st.text_input(
                "Buscar referencia / alterna / descripción / rotación / equipo",
                placeholder="Ej: 12345 o texto",
                key="margen_filtro_busqueda",
            ).strip()
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

    filtros5, filtros6, filtros7, filtros8 = st.columns([1, 1, 1, 1], gap="large")
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
            f"{precio_col} (rango)",
            min_value=0.0,
            max_value=max_precio_slider,
            step=1.0,
            key="margen_precio_range",
            on_change=_sync_precio_from_slider,
        )
        p1, p2 = st.columns(2, gap="small")
        with p1:
            st.number_input(
                f"Desde {precio_col}",
                min_value=0.0,
                max_value=max_precio_slider,
                step=1.0,
                key="margen_precio_desde",
                on_change=_sync_precio_from_inputs,
            )
        with p2:
            st.number_input(
                f"Hasta {precio_col}",
                min_value=0.0,
                max_value=max_precio_slider,
                step=1.0,
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
    with filtros8:
        sistemas = (
            sorted([str(x) for x in df_margen["Sistema_Precio"].dropna().unique()])
            if "Sistema_Precio" in df_margen.columns
            else []
        )
        _init_multiselect_list("margen_filtro_sistema", sistemas)
        sistema_sel = st.multiselect("Sistema precio", options=sistemas, key="margen_filtro_sistema")

        def _sync_margen_from_slider() -> None:
            lo, hi = st.session_state["margen_pct_range"]
            st.session_state["margen_pct_desde"] = float(lo)
            st.session_state["margen_pct_hasta"] = float(hi)

        def _sync_margen_from_inputs() -> None:
            lo = float(st.session_state.get("margen_pct_desde", -100.0))
            hi = float(st.session_state.get("margen_pct_hasta", 100.0))
            lo = max(-300.0, min(lo, 300.0))
            hi = max(-300.0, min(hi, 300.0))
            if lo > hi:
                lo, hi = hi, lo
            st.session_state["margen_pct_desde"] = lo
            st.session_state["margen_pct_hasta"] = hi
            st.session_state["margen_pct_range"] = (lo, hi)

        st.session_state.setdefault("margen_pct_range", (-100.0, 100.0))
        st.session_state.setdefault("margen_pct_desde", float(st.session_state["margen_pct_range"][0]))
        st.session_state.setdefault("margen_pct_hasta", float(st.session_state["margen_pct_range"][1]))
        _sync_margen_from_inputs()

        st.slider(
            "Rango de margen (%)",
            min_value=-300.0,
            max_value=300.0,
            step=1.0,
            key="margen_pct_range",
            on_change=_sync_margen_from_slider,
        )
        m1, m2 = st.columns(2, gap="small")
        with m1:
            st.number_input(
                "Desde Margen %",
                min_value=-300.0,
                max_value=300.0,
                step=1.0,
                key="margen_pct_desde",
                on_change=_sync_margen_from_inputs,
            )
        with m2:
            st.number_input(
                "Hasta Margen %",
                min_value=-300.0,
                max_value=300.0,
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
    df_filtrado = df_filtrado[
        (df_filtrado[margen_col] >= float(margen_desde))
        & (df_filtrado[margen_col] <= float(margen_hasta))
    ]
    if precio_09_desde > precio_09_hasta:
        precio_09_desde, precio_09_hasta = precio_09_hasta, precio_09_desde
    if existencia_desde > existencia_hasta:
        existencia_desde, existencia_hasta = existencia_hasta, existencia_desde

    if precio_col in df_filtrado.columns:
        df_filtrado = df_filtrado[
            (df_filtrado[precio_col].fillna(0) >= float(precio_09_desde))
            & (df_filtrado[precio_col].fillna(0) <= float(precio_09_hasta))
        ]
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
            if v is None:
                return "-"
            av = abs(float(v))
            if av >= 1_000_000:
                return f"${v/1_000_000:,.1f} mill."
            if av >= 1_000:
                return f"${v/1_000:,.1f} mil"
            return f"${v:,.0f}"

        # KPIs en 2×3: menos apretados en pantallas medianas y alturas más uniformes.
        r1a, r1b, r1c = st.columns(3, gap="small")
        r1a.metric("Refs", f"{total_refs:,.0f}")
        r1b.metric("Inv", _fmt_short_money(valor_inventario))
        r1c.metric("Exist", f"{existencia_total:,.0f}")
        r2a, r2b, r2c = st.columns(3, gap="small")
        r2a.metric("Margen %", _to_percent_text(margen_total))
        r2b.metric("Margen $", _fmt_short_money(margen_nominal))
        r2c.metric("Costo Bodega", _fmt_short_money(costo_prom_bod))

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
                    column_config[label] = st.column_config.NumberColumn(label, format="$%,.0f")
            for col in ["Precio_Lista_09", "Precio_Lista_04"]:
                label = _label_negocio(col)
                if label in df_show.columns:
                    column_config[label] = st.column_config.NumberColumn(label, format="dollar")
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
                st.dataframe(agg_show, width="stretch", hide_index=True)
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

    filtros1, filtros2, filtros3 = st.columns([2.0, 1.0, 1.0], gap="large")
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

    precio_lo = precio_hi = None
    ex_tot_lo = ex_tot_hi = None

    filtros5, filtros6, filtros7, filtros8 = st.columns([1, 1, 1, 1], gap="large")
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
                    key="aud_precio_range",
                    on_change=_sync_aud_precio_from_slider,
                )
                p1, p2 = st.columns(2, gap="small")
                with p1:
                    st.number_input(
                        f"Desde {precio_lista_col}",
                        min_value=0.0,
                        max_value=max_precio_slider,
                        step=1.0,
                        key="aud_precio_desde",
                        on_change=_sync_aud_precio_from_inputs,
                    )
                with p2:
                    st.number_input(
                        f"Hasta {precio_lista_col}",
                        min_value=0.0,
                        max_value=max_precio_slider,
                        step=1.0,
                        key="aud_precio_hasta",
                        on_change=_sync_aud_precio_from_inputs,
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

    with filtros7:
        sistemas = (
            sorted([str(x) for x in df[sistema_precio_col].dropna().unique()])
            if sistema_precio_col and sistema_precio_col in df.columns
            else []
        )
        _init_multiselect_list("aud_refs_sistema_sel", sistemas)
        sistema_sel = st.multiselect("Sistema precio", options=sistemas, key="aud_refs_sistema_sel")
    with filtros8:
        st.empty()

    dias_lo = dias_hi = None
    filtros9, filtros10 = st.columns([1.2, 2.8], gap="large")
    with filtros9:
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

    with filtros10:
        solo_significativas = st.checkbox(
            "Solo variación fuerte (Eje 2)",
            value=False,
            key="aud_solo_significativas",
            help="Filtra refs para tablas y Eje 2. El Eje 1 (Semáforo) siempre ve todas las refs.",
        )
        sliders_activos = solo_significativas
        u1, u2, u3 = st.columns(3, gap="medium")
        with u1:
            umbral_var_compra = st.slider(
                "Umbral |Δ compra| (%)",
                min_value=0.0,
                max_value=300.0,
                value=20.0,
                step=1.0,
                key="aud_umbral_var_compra",
                disabled=not sliders_activos,
                help="Valor absoluto de variación % entre última y penúltima compra. "
                + ("Activo." if sliders_activos else "Desactivado — marca «Solo variación fuerte» para filtrar."),
            )
        with u2:
            umbral_var_costo = st.slider(
                "Umbral |Δ vs costo inv.| (%)",
                min_value=0.0,
                max_value=300.0,
                value=15.0,
                step=1.0,
                key="aud_umbral_var_costo",
                disabled=not sliders_activos,
                help="Variación % de última compra frente a costo prom. inventario. "
                + ("Activo." if sliders_activos else "Desactivado — marca «Solo variación fuerte» para filtrar."),
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
        # no el costo promedio de inventario.
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
        search_cols = [c for c in [ref_col, desc_col, sistema_precio_col, equipo_col] if c]
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

    st.markdown("#### Auditoría referencias")

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

    st.caption(
        "**Cuadro de mando** del conjunto filtrado: cuántas filas superan cada umbral en los **dos problemas** "
        "(variación **última vs penúltima compra** y **última vs costo prom. inv.**). "
        "**Valor inv.** ≈ impacto económico del slice en inventario."
    )
    st.caption(
        "**Semáforo** (`Semaforo_Variacion`): mide sobre todo la **alineación precio última compra vs costos extremos** "
        "(mín./máx. por referencia), con reglas sobre `ABSVar_Costo` / `ABSVar_Costo_Pct` y cuartiles globales en SQL. "
        "Es **distinto** de «subió mucho entre penúltima y última compra» (usa `Var_PrecioCOP` y los umbrales |Δ compra| arriba)."
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
        help="Filas con categoría **Crítico** en datos (SQL: CRÍTICO). No confunde con **Alineado** (SQL: NO CRÍTICO). "
        "El semáforo refleja alineación última compra vs costos extremos; ver texto aclaratorio arriba.",
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
            ex_int_c = lower_map.get("existencia_intermedio")
            ex_tot_resumen = (
                lower_map.get("existencia_total")
                if lower_map.get("existencia_total") in df_fil.columns
                else (
                    "_Existencia_suma_niveles"
                    if "_Existencia_suma_niveles" in df_fil.columns
                    else None
                )
            )
            costo_int_c = lower_map.get("costo_intermedio")
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
                    lower_map.get("absvar_costo_pct"),
                    "_abs_var_costo",
                    lower_map.get("var_costomin_preciocop"),
                    precio_lista_col,
                    ex_tot_resumen,
                    costo_min_c,
                    costo_int_c,
                    costo_max_c,
                    ex_int_c,
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
                "**problema 2** (costo prom. inv., var. vs costo SQL, |Δ vs costo|) → magnitud (lista, stock, costos) → contexto."
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
                "**Una fila = una referencia**, todas las columnas. **Orden de lectura:** identificación → **semáforo** → "
                "**Δ última vs penúltima** (días, var. USD/COP/TRM) → **costo prom. inv.** → **Δ vs costo** (SQL) → "
                "contexto (costos bodega, stock, lista) → **evidencia** (fechas y precios última/penúltima compra) → "
                "**ajustes app** (factor logístico, precios COP ajustados, |Δ| y score)."
            )
            orden_cols = _auditoria_column_order_full(df_vista, lower_map, costo_inv_col)
            df_full = df_vista[orden_cols].copy()
            df_full = _auditoria_coerce_display_dtypes(df_full)
            df_full = df_full.rename(columns=alias_cols)
            df_full = _hacer_columnas_unicas(df_full)
            _auditoria_df_map_semaforo_ui(df_full, sem_col, alias_cols)
            cfg_full = _auditoria_build_column_config(orden_cols, alias_cols)
            cfg_full = {k: v for k, v in cfg_full.items() if k in df_full.columns}
            st.dataframe(
                df_full,
                width="stretch",
                height=680,
                hide_index=True,
                column_config=cfg_full,
            )
    
        with subtabs[2]:
            st.markdown("##### Vista operativa — revisar y exportar")
            st.caption(
                "**Misma priorización** que arriba, en **bloques** (menos scroll horizontal): primero **semáforo y score**, "
                "luego **problema 1** y **problema 2**, después contexto y evidencia de compras, y factores logísticos. "
                "Al final, **CSV** del slice para Excel u operaciones."
            )
            st.markdown("**Lectura por bloques**")
            st.caption(
                "En **cada bloque**, las primeras columnas son siempre **Ref.**, **Refs alternas** y **Descripción** (cuando existan en datos). "
                "Luego el resto del bloque: prioridad → variación entre compras → vs inventario → negocio → comprobantes → ajuste COP."
            )
            bloques = _auditoria_vista_bloques(lower_map)
            ref_alt_col = lower_map.get("referencias_alternas")
            for bi, (titulo, keys) in enumerate(bloques):
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
                df_b = df_vista[cols_b].copy()
                df_b = _auditoria_coerce_display_dtypes(df_b)
                df_b = df_b.rename(columns=alias_cols)
                df_b = _hacer_columnas_unicas(df_b)
                _auditoria_df_map_semaforo_ui(df_b, sem_col, alias_cols)
                cfg_b = _auditoria_build_column_config(cols_b, alias_cols)
                cfg_b = {k: v for k, v in cfg_b.items() if k in df_b.columns}
                st.markdown(f"**{titulo}**")
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


def _plotly_theme() -> dict:
    """Tema oscuro alineado al dashboard (legible en pantalla ancha)."""
    return {
        "template": "plotly_dark",
        "paper_bgcolor": "#111a2e",
        "plot_bgcolor": "#0f172a",
        "font": {"color": "#e5e7eb", "size": 12},
        "margin": {"l": 50, "r": 30, "t": 56, "b": 46},
    }


def _plotly_show(fig) -> None:
    if not _HAS_PLOTLY or px is None:
        return
    fig.update_layout(**_plotly_theme())
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": True, "scrollZoom": True})


def _margen_plotly_charts(df: pd.DataFrame, margen_col: str, precio_col: str) -> None:
    """Reporte gráfico storytelling: panorama → concentración → relaciones → segmentos → anomalías."""
    if not _HAS_PLOTLY or px is None:
        st.warning("Instala **Plotly** para ver gráficos: `pip install plotly` (mismo venv que Streamlit).")
        return
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
        _plotly_show(fig_hist)

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
            _plotly_show(fig_zona)
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
            _plotly_show(fig_pareto)
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
                _plotly_show(fig_neg)
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
                _plotly_show(fig_top)
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
            _plotly_show(fig_sc)
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
                _plotly_show(fig_trend)

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
            _plotly_show(fig_bub)

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
            _plotly_show(fig_bod)
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
                _plotly_show(fig_hm)
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
            _plotly_show(fig_tree)

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
                _plotly_show(fig_inc)

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
                _plotly_show(fig_rng)

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
            _plotly_show(fig_an)
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


def _auditoria_charts_semaforo_st(df_fil: pd.DataFrame, ctx: dict) -> None:
    """Eje Semáforo — storytelling: resumen → panorama → anatomía → concentración → plan."""
    if not _HAS_PLOTLY or px is None:
        st.warning("Instala **Plotly** para ver gráficos: `pip install plotly`.")
        return
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

    s1, s2, s3 = st.columns(3, gap="small")
    s1.metric("Filtradas", f"{n_total:,}")
    s2.metric("Crítico", f"{n_critico:,}", delta=f"{n_critico/max(n_total,1)*100:.1f}%", delta_color="inverse")
    s3.metric("Mod-alto", f"{n_mod_alto:,}", delta=f"{n_mod_alto/max(n_total,1)*100:.1f}%", delta_color="inverse")
    s4, s5, s6 = st.columns(3, gap="small")
    s4.metric("≥ umbral costo", f"{pct_umbral_costo:.1f}%")
    s5.metric("Media |Δ costo|", f"{media_var_costo:.1f}%")
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
            _plotly_show(fig)
    with p2:
        if not sub_s.empty and _score_clip is not None and _score_med is not None and _score_p90 is not None:
            fig = px.histogram(sub_s[sub_s["_g_score"] <= _score_clip], x="_g_score", nbins=50,
                               title="Score de riesgo compuesto", labels={"_g_score": "Score"})
            fig.add_vline(x=_score_p90, line_dash="dash", line_color="#ef4444", annotation_text=f"P90={_score_p90:.1f}")
            fig.add_vline(x=_score_med, line_dash="dot", line_color="#facc15", annotation_text=f"Med={_score_med:.1f}")
            fig.update_layout(**_CL)
            _plotly_show(fig)

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
            _plotly_show(fig)
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
                _plotly_show(fig)

    if not sub_s.empty and _score_max is not None and _score_med is not None and _score_p90 is not None:
        st.caption("Resumen del score de riesgo (misma muestra que el histograma).")
        sm1, sm2, sm3 = st.columns(3, gap="small")
        sm1.metric("Mediana score", f"{_score_med:.1f}")
        sm2.metric("P90 score", f"{_score_p90:.1f}")
        sm3.metric("Máx score", f"{_score_max:.1f}")

    # ── CAP 3 · Anatomía ─────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("#### 3 · Anatomía — ¿de dónde vienen los desalineamientos?")
    st.caption("Heatmap sistema × semáforo, scatter de cobertura precio lista y distribución del ratio costo/lista.")

    a1, a2 = st.columns(2, gap="medium")
    with a1:
        if sem_col and sistema_precio_col and sistema_precio_col in df.columns and sem_col in df.columns:
            top_sys = df[sistema_precio_col].astype(str).value_counts().head(10).index
            df_hm = df[df[sistema_precio_col].astype(str).isin(top_sys)]
            if not df_hm.empty:
                piv = pd.crosstab(df_hm[sistema_precio_col].astype(str), df_hm[sem_col].fillna("SIN_DATO").astype(str))
                piv.columns = [_auditoria_etiqueta_semaforo_ui(c) for c in piv.columns]
                fig = px.imshow(piv, text_auto=True, aspect="auto", color_continuous_scale="YlOrRd",
                                title="Sistema × semáforo (top 10)", labels={"color": "Refs"})
                fig.update_layout(**_CL)
                _plotly_show(fig)

    with a2:
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
                _plotly_show(fig)

    a3, a4 = st.columns(2, gap="medium")
    with a3:
        if precio_ult_cop_col and precio_lista_col and precio_ult_cop_col in df.columns and precio_lista_col in df.columns:
            df_pl = df[[precio_ult_cop_col, precio_lista_col]].dropna()
            df_pl = df_pl[(df_pl[precio_lista_col] > 0) & (df_pl[precio_ult_cop_col] > 0)].copy()
            if not df_pl.empty:
                df_pl["_ratio"] = df_pl[precio_ult_cop_col] / df_pl[precio_lista_col] * 100
                p95_r = float(df_pl["_ratio"].quantile(0.95))
                df_clip = df_pl[df_pl["_ratio"] <= max(p95_r * 1.1, 100)]
                fig = px.histogram(df_clip, x="_ratio", nbins=50,
                                   title="Distribución costo/precio lista (%)",
                                   labels={"_ratio": "Costo / P.Lista (%)"},
                                   color_discrete_sequence=["#38bdf8"])
                fig.add_vline(x=100, line_dash="solid", line_color="#ef4444", annotation_text="100%")
                fig.add_vline(x=85, line_dash="dot", line_color="#f59e0b", annotation_text="85%")
                med_r = float(df_clip["_ratio"].median())
                fig.add_vline(x=med_r, line_dash="dash", line_color="#facc15", annotation_text=f"Med {med_r:.0f}%")
                fig.update_layout(**_CL)
                _plotly_show(fig)
    with a4:
        if precio_ult_cop_col and precio_lista_col and precio_ult_cop_col in df.columns and precio_lista_col in df.columns:
            df_pl2 = df[[precio_ult_cop_col, precio_lista_col]].dropna()
            df_pl2 = df_pl2[(df_pl2[precio_lista_col] > 0) & (df_pl2[precio_ult_cop_col] > 0)].copy()
            if not df_pl2.empty:
                df_pl2["_ratio"] = df_pl2[precio_ult_cop_col] / df_pl2[precio_lista_col] * 100
                n85 = int((df_pl2["_ratio"] >= 85).sum())
                n100 = int((df_pl2["_ratio"] >= 100).sum())
                n_compr = 0
                if var_c and var_c in df.columns:
                    df_pl2["_var_c"] = pd.to_numeric(df.loc[df_pl2.index, var_c], errors="coerce")
                    n_compr = len(df_pl2[(df_pl2["_ratio"] >= 80) & (df_pl2["_var_c"] > 0)])
                am1, am2, am3 = st.columns(3, gap="small")
                am1.metric(
                    "Costo ≥ 85% P.Lista",
                    f"{n85:,}",
                    delta=f"{n85/max(len(df_pl2),1)*100:.1f}%",
                    delta_color="inverse",
                )
                am2.metric(
                    "Costo ≥ 100% (margen neg.)",
                    f"{n100:,}",
                    delta=f"{n100/max(len(df_pl2),1)*100:.1f}%",
                    delta_color="inverse",
                )
                am3.metric(
                    "Comprimidas (≥80% + subió)",
                    f"{n_compr:,}",
                    help="Costo alto + precio subió = cambio urgente de P.Lista",
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
            _plotly_show(fig)

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
            _plotly_show(fig)

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
                _plotly_show(fig)
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
                _plotly_show(fig)

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
            _plotly_show(fig)


def _auditoria_charts_variacion_st(df_fil: pd.DataFrame, ctx: dict) -> None:
    """Eje Variación compra — storytelling: resumen → panorama → anatomía → concentración → plan."""
    if not _HAS_PLOTLY or px is None:
        st.warning("Instala **Plotly** para ver gráficos: `pip install plotly`.")
        return
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

    v1, v2, v3 = st.columns(3, gap="small")
    v1.metric("Filtradas", f"{n_total:,}")
    v2.metric("Subió > umbral", f"{n_sube:,}", delta=f"{n_sube/max(n_total,1)*100:.1f}%", delta_color="inverse")
    v3.metric("Bajó > umbral", f"{n_baja:,}", delta=f"{n_baja/max(n_total,1)*100:.1f}%", delta_color="off")
    v4, v5, v6 = st.columns(3, gap="small")
    v4.metric("Doble problema", f"{n_doble:,}", delta=f"{n_doble/max(n_total,1)*100:.1f}%", delta_color="inverse")
    v5.metric("Media |Δ compra|", f"{media_abs:.1f}%")
    v6.metric("Mediana |Δ compra|", f"{med_abs:.1f}%")

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
                _plotly_show(fig)

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
                _plotly_show(fig)

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
            _plotly_show(fig)

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
            _plotly_show(fig)

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
                _plotly_show(fig)

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
                _plotly_show(fig)

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
            _plotly_show(fig)

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
                _plotly_show(fig)

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
                _plotly_show(fig)
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
                        _plotly_show(fig)

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
            _plotly_show(fig)




_render_header_y_actualizacion()

tabs = st.tabs(
    [
        "Consulta referencias",
        "Reporte margen SIESA",
        "Auditoría referencias",
    ]
)
with tabs[0]:
    _render_tab_consulta()
with tabs[1]:
    _render_tab_margen()
with tabs[2]:
    _render_tab_auditoria_referencias()
