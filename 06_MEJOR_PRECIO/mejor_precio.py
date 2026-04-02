# -*- coding: utf-8 -*-
"""
Monitor estrategico de costo vs inventario y reposicion (CSV -> CSV).
Carpeta: 06_MEJOR_PRECIO

Salida base (7 columnas) + CONFIANZA (ALTA|MEDIA|BAJA). Con --auditoria se añade AUDITORIA (traza corta).

OK_MARGEN_OBJETIVO usa MARGEN_OBJETIVO_PCT y MARGEN_TOLERANCIA_PCT: SI si el margen alcanza al menos
  objetivo - tolerancia (puntos porcentuales); NO si queda por debajo de esa banda inferior.

Notas: [REVISAR MANUAL] en NO_CALCULABLE (el CODIGO indica la causa). [ATENCION] en calculados con
  riesgo operativo (repos inestables, avisos de revision rapida, o solo inventario sin repos).

Codigos NO_CALCULABLE (ejemplos):
  NC_INV_RANGO_AMPLIO       costo min/max bodegas demasiado lejanos (un solo costo no defendible)
  NC_CONFLICTO_ABASTECIMIENTO  origen barato con poco stock y origen caro con mucho (ratio de precios alto)
  NC_REPOS_DISP_INSUFICIENTE   repos muy distintos y ninguno con disp. suficiente
  NC_SIN_DATOS              sin inventario ni repos en datos

Que se elimino respecto a versiones anteriores:
  columnas de estabilidad detallada, diff USA-BR, alertas de volatilidad multiples,
  revision manual en columnas separadas, observaciones largas, confianza/alertas duplicadas.
  Todo lo necesario va en NOTA_DECISION o en CODIGO.

Uso:
  python mejor_precio.py [entrada.csv] [salida.csv]
  python mejor_precio.py --auditoria
  python mejor_precio.py --excel libro.xlsx

Columna opcional de entrada: Categoria (nombre alineado con SPREAD_MAX_POR_CATEGORIA) para el umbral
  de dispersion de inventario; si falta o el nombre no coincide, se usa PCT_SPREAD_MAX_COSTO_INV.

Hiperparametros: solo en HIPERPARAMETROS (arriba).
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd


# ═══════════════════════════════════════════════════════════════════════════════
# HIPERPARAMETROS — Ajuste únicamente aquí. El resto del script los lee por nombre.
# (Explicación en lenguaje directo; valores son números que puedes subir o bajar.)
# ═══════════════════════════════════════════════════════════════════════════════

# -- Margen sobre lista (decisión comercial simple) --
# MARGEN_OBJETIVO_PCT: Margen bruto de referencia sobre lista (PRECIO - costo) / PRECIO * 100.
# MARGEN_TOLERANCIA_PCT: Puntos porcentuales por debajo del objetivo que aún cuentan como “en margen”
#   (ej. objetivo 40 y tolerancia 15 → cumple desde 25% hacia arriba). Por encima del objetivo sigue SI.
MARGEN_OBJETIVO_PCT = 40.0
MARGEN_TOLERANCIA_PCT = 15.0

# -- Cuándo dos repos (USA/BR) se consideran “parecidos” (se elige el más barato sin drama) --
# PCT_ESTABILIDAD_REPOS: Si la diferencia entre costo USA y BR, dividida por el mayor,
#   es menor que este valor (ej. 0.20 = 20 %), los tratamos como estables.
PCT_ESTABILIDAD_REPOS = 0.20

# FACTOR_NOMINAL_CUARTILES: Cuatro factores (uno por tramo de cuartil del costo de repo).
#   Sirve para el tope en dólares de la diferencia USA–BR cuando el % ya pasó pero en
#   dinero la brecha sigue siendo pequeña. Artículos más caros permiten más diferencia en $.
FACTOR_NOMINAL_CUARTILES = (0.30, 0.25, 0.20, 0.15)

# -- Disponibilidad mínima exigida según cuánto cuesta el artículo (cuartiles de PRECIO en el lote) --
# DISP_MIN_POR_TRANO_PRECIO: Cuatro pisos (artículo muy barato … muy caro). Si la disp.
#   del origen no llega al piso de su tramo, no confías en ese origen hasta por disponibilidad.
DISP_MIN_POR_TRANO_PRECIO = (5.0, 8.0, 10.0, 15.0)

# -- Cuándo NO calcular costo automático (evitar decisiones falsas) --
# PCT_SPREAD_MAX_COSTO_INV: Umbral por defecto de dispersion (max-min)/max en inventario.
#   Filas con columna opcional Categoria usan SPREAD_MAX_POR_CATEGORIA si el nombre coincide.
PCT_SPREAD_MAX_COSTO_INV = 0.45

# SPREAD_MAX_POR_CATEGORIA: nombre de categoria (cualquier mayusculas) -> fraccion 0..1
#   (ej. 0.30 = 30%). Valores > 1 se interpretan como porcentaje / 100.
SPREAD_MAX_POR_CATEGORIA: dict[str, float] = {
    "General": 0.40,
    "Filtros": 0.30,
    "Lubricantes": 0.15,
}

# RATIO_REPOS_CONFLICTO: Si el costo del origen caro dividido entre el del barato supera
#   este número (ej. 150/40 = 3.75) y además el barato tiene pocas unidades, hay conflicto
#   de abastecimiento: no forzamos un costo único automático.
RATIO_REPOS_CONFLICTO = 2.0

# DISP_MIN_ORIGEN_BARATO: Por debajo de estas unidades en el origen más barato, junto con
#   RATIO_REPOS_CONFLICTO, declaramos conflicto (ej. USA 40 USD con 2 unidades vs BR caro con stock).
DISP_MIN_ORIGEN_BARATO = 5.0

# PCT_DIF_REPOS_EXTREMA_NC: Si USA y BR difieren más que este % (relativo al mayor costo)
#   y además ninguno cumple el piso de disponibilidad, marcamos no calculable por divergencia extrema.
PCT_DIF_REPOS_EXTREMA_NC = 0.55

# -- Revisión manual (solo enciende un aviso en NOTA; no añade columnas de análisis) --
# UMBRAL_DISPINV_TRAMO_MIN: Unidades en DISPINVmin a partir de las cuales consideras
#   “mucho stock” en el tramo de costo mínimo (política interna).
UMBRAL_DISPINV_TRAMO_MIN = 50.0

# UMBRAL_INV_UNIDAD_EXT_DISP: Unidades máximas de inventario (DISPINV) para cruzar con
#   disponibilidad externa baja (revisión de abastecimiento).
UMBRAL_INV_UNIDAD_EXT_DISP = 30.0

# PCT_DIF_REPOS_REVISION: Si la divergencia USA/BR supera este %, sugerimos revisar aunque haya costo.
PCT_DIF_REPOS_REVISION = 0.35

# -- Salida y lectura de archivos --
IMPRIMIR_CUARTILES_EN_CONSOLA = True
CSV_SEP = ";"
CSV_ENCODING = "utf-8-sig"
CSV_HEADER_ROW = 0
BASE_DIR = Path(__file__).resolve().parent
INPUT_CSV = BASE_DIR / "entrada.csv"
OUTPUT_CSV = BASE_DIR / "salida_precio.csv"

EPS = 1e-12

# ═══════════════════════════════════════════════════════════════════════════════
# Columnas de entrada / salida (nombres fijos)
# ═══════════════════════════════════════════════════════════════════════════════

COL = {
    "inv_min": "CostoINVmin",
    "inv_max": "costoINVmax",
    "repo_usa": "CostoREPOU",
    "repo_br": "COSTOREPOBR",
    "disp_usa": "DISPUSA",
    "disp_br": "DISPBR",
    "disp_inv_min": "DISPINVmin",
    "disp_inv_max": "DISPINVmax",
    "precio": "PRECIO",
    "categoria": "Categoria",
}

_COL_REQUERIDAS = (
    "inv_min", "inv_max", "repo_usa", "repo_br",
    "disp_usa", "disp_br", "precio",
)

# Salida ejecutiva: 7 base + CONFIANZA; + AUDITORIA con --auditoria
OUT_ESTADO = "ESTADO"
OUT_CODIGO = "CODIGO"
OUT_MEJOR_COSTO = "MEJOR_COSTO"
OUT_ORIGEN = "ORIGEN"
OUT_MARGEN = "MARGEN_PCT_LISTA"
OUT_OK_MARGEN = "OK_MARGEN_OBJETIVO"
OUT_NOTA = "NOTA_DECISION"
OUT_CONFIANZA = "CONFIANZA"
OUT_AUDITORIA = "AUDITORIA"

ORDEN_ENTRADA_KEYS = (
    "inv_min", "disp_inv_min", "inv_max", "disp_inv_max",
    "repo_usa", "disp_usa", "repo_br", "disp_br", "precio", "categoria",
)

ALIASES_COLUMNA_ENTRADA = {
    "COSTOREPOl": "COSTOREPOBR",
    "costorepol": "COSTOREPOBR",
    "CATEGORIA": "Categoria",
    "categoria": "Categoria",
}


def _aplicar_alias_columnas_entrada(df: pd.DataFrame) -> pd.DataFrame:
    ren = {a: b for a, b in ALIASES_COLUMNA_ENTRADA.items() if a in df.columns and b not in df.columns}
    return df.rename(columns=ren) if ren else df


def _enterizar_disponibilidades(df: pd.DataFrame, c: dict[str, str]) -> None:
    for key in ("disp_usa", "disp_br", "disp_inv_min", "disp_inv_max"):
        name = c.get(key)
        if not name or name not in df.columns:
            continue
        s = pd.to_numeric(df[name], errors="coerce")
        df[name] = np.round(s.fillna(0)).astype(np.int64)


def _safe(val, default=0.0):
    if val is None:
        return default
    try:
        if np.isnan(val) or np.isinf(val):
            return default
    except (TypeError, ValueError):
        return default
    return float(val)


def _valor_fila_opcional(row, cols_present, c, key, default=0.0):
    name = c.get(key)
    if not name or name not in cols_present:
        return default
    return _safe(row[name], default)


def _valor_categoria(row, cols_present, c) -> str | None:
    name = c.get("categoria")
    if not name or name not in cols_present:
        return None
    v = row[name]
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return None
    s = str(v).strip()
    return s if s else None


def _spread_max_para_categoria(val: str | None) -> float:
    """Fraccion maxima de dispersion de inventario para la fila; desconocido -> PCT_SPREAD_MAX_COSTO_INV."""
    default = PCT_SPREAD_MAX_COSTO_INV
    if not val:
        return default
    cf = val.strip().casefold()
    for nombre, raw in SPREAD_MAX_POR_CATEGORIA.items():
        if nombre.strip().casefold() == cf:
            v = float(raw)
            if v > 1.0:
                v = v / 100.0
            return max(0.0, min(1.0, v))
    return default


def _confianza(nota: str, estado: str, codigo: str, ok_margen: str) -> str:
    if estado == "NO_CALCULABLE":
        return "BAJA"
    if ok_margen == "NO":
        return "BAJA"
    if codigo == "OK_SOLO_INV" or "[ATENCION]" in (nota or ""):
        return "MEDIA"
    return "ALTA"


def _validar_entrada(df: pd.DataFrame, c: dict) -> int:
    """Avisos rapidos de calidad de datos (consola). No altera el calculo."""
    avisos = 0
    nombres = {k: c[k] for k in ("precio", "inv_min", "inv_max", "repo_usa", "repo_br") if c.get(k) in df.columns}
    for pos, (_, row) in enumerate(df.iterrows(), start=2):
        if "precio" in nombres:
            pr = pd.to_numeric(row[nombres["precio"]], errors="coerce")
            if pd.notna(pr) and pr <= 0:
                print(f"  [QA entrada] fila {pos}: PRECIO <= 0")
                avisos += 1
        if "inv_min" in nombres and "inv_max" in nombres:
            imin = pd.to_numeric(row[nombres["inv_min"]], errors="coerce")
            imax = pd.to_numeric(row[nombres["inv_max"]], errors="coerce")
            if pd.notna(imin) and pd.notna(imax) and imin > imax:
                print(f"  [QA entrada] fila {pos}: CostoINVmin > costoINVmax")
                avisos += 1
        for rk in ("repo_usa", "repo_br"):
            if rk not in nombres:
                continue
            v = pd.to_numeric(row[nombres[rk]], errors="coerce")
            if pd.notna(v) and v < 0:
                print(f"  [QA entrada] fila {pos}: {nombres[rk]} negativo")
                avisos += 1
    if avisos:
        print(f"  Total avisos QA entrada: {avisos}")
    else:
        print("  (sin avisos)")
    return avisos


def _calcular_cuartiles(serie: pd.Series) -> tuple[float, float, float]:
    s = pd.to_numeric(serie, errors="coerce").dropna()
    if s.empty:
        return 0.0, 0.0, 0.0
    return tuple(s.quantile([0.25, 0.50, 0.75]).values)  # type: ignore[return-value]


def _umbral_nominal(max_repo: float, rq1: float, rq2: float, rq3: float) -> float:
    f1, f2, f3, f4 = FACTOR_NOMINAL_CUARTILES
    if max_repo <= rq1:
        return rq1 * f1 + EPS
    if max_repo <= rq2:
        return rq2 * f2 + EPS
    if max_repo <= rq3:
        return rq3 * f3 + EPS
    return rq3 * f4 + EPS


def _umbral_disp_por_precio(
    precio: float, pq1: float, pq2: float, pq3: float,
    dq1: float, dq2: float, dq3: float,
) -> float:
    a, b, c, d = DISP_MIN_POR_TRANO_PRECIO
    if precio <= pq1:
        return max(a, dq1)
    if precio <= pq2:
        return max(b, dq1)
    if precio <= pq3:
        return max(c, dq2)
    return max(d, dq2)


def _referencia_no_calculable(
    *,
    tiene_usa: bool,
    tiene_br: bool,
    inv_lo: float,
    inv_cost: float,
    repo_usa_raw: float,
    repo_br_raw: float,
    disp_usa: float,
    disp_br: float,
    umbral_disp: float,
    pct_spread_max: float,
) -> tuple[bool, str, str, str]:
    """
    Devuelve (es_nc, codigo, mensaje_para_NOTA, traza_auditoria si nc).
    """
    if inv_cost > 0 and inv_lo >= 0:
        spread = (inv_cost - inv_lo) / max(inv_cost, EPS)
        if spread > pct_spread_max:
            return (
                True,
                "NC_INV_RANGO_AMPLIO",
                "Alta dispersion de costo entre tramos de inventario (min vs max en bodegas).",
                "NC:INV_SPREAD",
            )

    if not tiene_usa and not tiene_br and inv_cost <= 0:
        return True, "NC_SIN_DATOS", "Sin costo de inventario ni de reposicion en el archivo.", "NC:SIN_DATOS"

    if tiene_usa and tiene_br and repo_usa_raw > 0 and repo_br_raw > 0:
        r_hi = max(repo_usa_raw, repo_br_raw)
        r_lo = min(repo_usa_raw, repo_br_raw)
        ratio = r_hi / max(r_lo, EPS)
        if repo_usa_raw <= repo_br_raw:
            disp_barato, lab_b, lab_c = disp_usa, "USA", "BR"
        else:
            disp_barato, lab_b, lab_c = disp_br, "BR", "USA"
        if ratio >= RATIO_REPOS_CONFLICTO and disp_barato < DISP_MIN_ORIGEN_BARATO:
            return (
                True,
                "NC_CONFLICTO_ABASTECIMIENTO",
                f"Origen barato ({lab_b}) con stock bajo y origen caro ({lab_c}) con mejor disponibilidad; ratio de precios alto.",
                "NC:CONFLICTO_AB",
            )
        pct_dif = abs(repo_usa_raw - repo_br_raw) / max(r_hi, EPS)
        usa_ok = disp_usa >= umbral_disp
        br_ok = disp_br >= umbral_disp
        if pct_dif >= PCT_DIF_REPOS_EXTREMA_NC and not usa_ok and not br_ok:
            return (
                True,
                "NC_REPOS_DISP_INSUFICIENTE",
                "Repos USA/BR muy distintos y ambos con disponibilidad por debajo del piso del tramo.",
                "NC:REPOS_DISP",
            )

    return False, "OK", "", ""


def _nota_manual_nc(codigo: str, mensaje: str) -> str:
    """Prefijo unico para NO_CALCULABLE: el codigo resume la causa para lectura rapida."""
    return f"[REVISAR MANUAL] {mensaje} [Codigo: {codigo}]"


def _margen_lista(precio_lista: float, costo: float) -> tuple[float, str]:
    pl = _safe(precio_lista, 0.0)
    if pl <= 0 or costo is None or (isinstance(costo, float) and np.isnan(costo)):
        return np.nan, "N/A"
    m = (pl - float(costo)) / pl * 100.0
    m = round(m, 2)
    piso = MARGEN_OBJETIVO_PCT - MARGEN_TOLERANCIA_PCT
    ok = "SI" if m >= piso else "NO"
    return m, ok


def _nota_revision_rapida(
    *,
    disp_inv_min: float,
    inv_lo: float,
    inv_cost: float,
    m_ext_disp: float,
    m_inv_disp: float,
    umbral_disp: float,
    pct_diff: float,
    etiqueta_estab: str,
) -> str:
    """Una sola frase opcional para NOTA si hace falta aviso sin llenar de columnas."""
    partes: list[str] = []
    if (
        disp_inv_min >= UMBRAL_DISPINV_TRAMO_MIN
        and inv_lo > 0
        and inv_lo < inv_cost
    ):
        partes.append("Mucho stock en tramo de costo minimo.")
    if m_ext_disp < umbral_disp and m_inv_disp >= UMBRAL_INV_UNIDAD_EXT_DISP:
        partes.append("Repos externos con poca disp. e inventario alto.")
    if pd.notna(pct_diff) and pct_diff > PCT_DIF_REPOS_REVISION and etiqueta_estab == "INESTABLE":
        partes.append("Repos USA/BR muy divergentes.")
    return " ".join(partes) if partes else ""


def _evaluar_fila(
    repo_usa: float,
    repo_br: float,
    disp_usa: float,
    disp_br: float,
    disp_inv_min: float,
    disp_inv_max: float,
    inv_min: float,
    inv_max: float,
    precio: float,
    rq1: float,
    rq2: float,
    rq3: float,
    dq1: float,
    dq2: float,
    dq3: float,
    pq1: float,
    pq2: float,
    pq3: float,
    *,
    pct_spread_max: float,
    modo_auditoria: bool,
) -> dict:
    repo_usa = _safe(repo_usa, np.nan)
    repo_br = _safe(repo_br, np.nan)
    disp_usa = _safe(disp_usa, 0)
    disp_br = _safe(disp_br, 0)
    disp_inv_min = _safe(disp_inv_min, 0)
    disp_inv_max = _safe(disp_inv_max, 0)
    inv_min = _safe(inv_min, 0)
    inv_max = _safe(inv_max, 0)
    precio = _safe(precio, 0)

    tiene_usa = not np.isnan(repo_usa)
    tiene_br = not np.isnan(repo_br)
    repo_usa_raw = repo_usa
    repo_br_raw = repo_br
    inv_lo = min(inv_min, inv_max)
    inv_hi = max(inv_min, inv_max)
    inv_cost = inv_hi

    umbral_disp_pre = _umbral_disp_por_precio(precio, pq1, pq2, pq3, dq1, dq2, dq3)

    nc, codigo, msg_nc, aud_nc = _referencia_no_calculable(
        tiene_usa=tiene_usa,
        tiene_br=tiene_br,
        inv_lo=inv_lo,
        inv_cost=inv_cost,
        repo_usa_raw=repo_usa_raw,
        repo_br_raw=repo_br_raw,
        disp_usa=float(disp_usa),
        disp_br=float(disp_br),
        umbral_disp=umbral_disp_pre,
        pct_spread_max=pct_spread_max,
    )
    if nc:
        nota_nc = _nota_manual_nc(codigo, msg_nc)
        out: dict = {
            OUT_ESTADO: "NO_CALCULABLE",
            OUT_CODIGO: codigo,
            OUT_MEJOR_COSTO: np.nan,
            OUT_ORIGEN: "N/A",
            OUT_MARGEN: np.nan,
            OUT_OK_MARGEN: "N/A",
            OUT_NOTA: nota_nc,
            OUT_CONFIANZA: _confianza(nota_nc, "NO_CALCULABLE", codigo, "N/A"),
        }
        if modo_auditoria:
            out[OUT_AUDITORIA] = aud_nc
        return out

    if not tiene_usa and not tiene_br:
        mc = round(inv_cost, 4) if inv_cost > 0 else np.nan
        m_pct, ok_m = _margen_lista(precio, mc)
        piso_m = MARGEN_OBJETIVO_PCT - MARGEN_TOLERANCIA_PCT
        nota = (
            f"[ATENCION] Costo desde INVENTARIO {mc}. Margen lista {m_pct}% "
            f"(objetivo {MARGEN_OBJETIVO_PCT}% ±{MARGEN_TOLERANCIA_PCT} pp, piso {piso_m}%: {ok_m}). "
            "Sin reposicion en datos; validar abastecimiento."
        )
        out_si = {
            OUT_ESTADO: "CALCULADO",
            OUT_CODIGO: "OK_SOLO_INV",
            OUT_MEJOR_COSTO: mc,
            OUT_ORIGEN: "INVENTARIO" if inv_cost > 0 else "N/A",
            OUT_MARGEN: m_pct,
            OUT_OK_MARGEN: ok_m,
            OUT_NOTA: nota,
            OUT_CONFIANZA: _confianza(nota, "CALCULADO", "OK_SOLO_INV", ok_m),
        }
        if modo_auditoria:
            out_si[OUT_AUDITORIA] = "OK:SOLO_INV"
        return out_si

    if not tiene_usa:
        repo_usa = np.inf
    if not tiene_br:
        repo_br = np.inf

    max_repo = max(repo_usa, repo_br) if min(repo_usa, repo_br) != np.inf else 0
    min_repo = min(repo_usa, repo_br)

    if tiene_usa and tiene_br and max_repo > 0:
        pct_diff = abs(repo_usa - repo_br) / (max_repo + EPS)
        abs_diff = abs(repo_usa - repo_br)
        nom_thresh = _umbral_nominal(max_repo, rq1, rq2, rq3)
        es_estable = (pct_diff < PCT_ESTABILIDAD_REPOS) and (abs_diff <= nom_thresh)
    else:
        pct_diff = np.nan
        abs_diff = 0.0
        es_estable = False

    if es_estable:
        mejor_repo = min_repo
        origen = "USA" if repo_usa <= repo_br else "BR"
        etiqueta_estab = "ESTABLE"
    else:
        umbral_disp = _umbral_disp_por_precio(precio, pq1, pq2, pq3, dq1, dq2, dq3)
        usa_ok = disp_usa >= umbral_disp and tiene_usa
        br_ok = disp_br >= umbral_disp and tiene_br
        if usa_ok and br_ok:
            mejor_repo = min(repo_usa, repo_br)
            origen = "USA" if repo_usa <= repo_br else "BR"
        elif usa_ok:
            mejor_repo = repo_usa
            origen = "USA"
        elif br_ok:
            mejor_repo = repo_br
            origen = "BR"
        else:
            if disp_usa >= disp_br and tiene_usa:
                mejor_repo = repo_usa
                origen = "USA"
            elif tiene_br:
                mejor_repo = repo_br
                origen = "BR"
            else:
                mejor_repo = repo_usa
                origen = "USA"
        if pd.notna(pct_diff) and pct_diff < PCT_ESTABILIDAD_REPOS:
            etiqueta_estab = "PCT_OK"
        elif tiene_usa and tiene_br and abs_diff <= _umbral_nominal(max_repo, rq1, rq2, rq3):
            etiqueta_estab = "NOM_OK"
        elif tiene_usa and tiene_br:
            etiqueta_estab = "INESTABLE"
        else:
            etiqueta_estab = "ORIGEN_UNICO"

    if mejor_repo == np.inf:
        mejor_repo = np.nan

    umbral_disp = _umbral_disp_por_precio(precio, pq1, pq2, pq3, dq1, dq2, dq3)
    m_ext_disp = max(disp_usa, disp_br)
    m_inv_disp = max(disp_inv_min, disp_inv_max)

    if inv_cost > 0 and not np.isnan(mejor_repo) and inv_cost > mejor_repo:
        mejor_costo = inv_cost
        origen_final = "INVENTARIO"
    elif np.isnan(mejor_repo):
        mejor_costo = inv_cost if inv_cost > 0 else np.nan
        origen_final = "INVENTARIO" if inv_cost > 0 else "N/A"
    else:
        mejor_costo = mejor_repo
        origen_final = origen

    mc_round = round(mejor_costo, 4) if pd.notna(mejor_costo) else np.nan
    m_pct, ok_m = _margen_lista(precio, mc_round)
    piso_m = MARGEN_OBJETIVO_PCT - MARGEN_TOLERANCIA_PCT

    extra = _nota_revision_rapida(
        disp_inv_min=disp_inv_min,
        inv_lo=inv_lo,
        inv_cost=inv_cost,
        m_ext_disp=m_ext_disp,
        m_inv_disp=m_inv_disp,
        umbral_disp=umbral_disp,
        pct_diff=pct_diff,
        etiqueta_estab=etiqueta_estab,
    )
    base = (
        f"Costo sugerido {mc_round} desde {origen_final}. Margen sobre lista {m_pct}% "
        f"(objetivo {MARGEN_OBJETIVO_PCT}% ±{MARGEN_TOLERANCIA_PCT} pp, piso {piso_m}%: {ok_m})."
    )
    cuerpo = f"{base} {extra}".strip() if extra else base
    necesita_atencion = bool(extra) or etiqueta_estab == "INESTABLE"
    nota = f"[ATENCION] {cuerpo}" if necesita_atencion else cuerpo

    costo_tipo = "INV" if origen_final == "INVENTARIO" else "REPO"
    aud = f"OK:{etiqueta_estab}|{origen_final}|{costo_tipo}"
    if len(aud) > 96:
        aud = aud[:93] + "..."

    out_ok = {
        OUT_ESTADO: "CALCULADO",
        OUT_CODIGO: "OK",
        OUT_MEJOR_COSTO: mc_round,
        OUT_ORIGEN: origen_final,
        OUT_MARGEN: m_pct,
        OUT_OK_MARGEN: ok_m,
        OUT_NOTA: nota,
        OUT_CONFIANZA: _confianza(nota, "CALCULADO", "OK", ok_m),
    }
    if modo_auditoria:
        out_ok[OUT_AUDITORIA] = aud
    return out_ok


def _orden_entrada(df: pd.DataFrame, c: dict) -> list[str]:
    orden: list[str] = []
    for key in ORDEN_ENTRADA_KEYS:
        nm = c[key]
        if nm in df.columns:
            orden.append(nm)
    for col in df.columns:
        if col not in orden:
            orden.append(col)
    return orden


def _calcular_resultados(
    df: pd.DataFrame,
    col_map: dict | None = None,
    *,
    modo_auditoria: bool = False,
) -> pd.DataFrame:
    c = {**COL, **(col_map or {})}
    for key in _COL_REQUERIDAS:
        if c[key] not in df.columns:
            raise KeyError(f"Falta columna {c[key]}")

    cols_present = set(df.columns)
    if any(c[k] not in cols_present for k in ("disp_inv_min", "disp_inv_max")):
        print("  Aviso: DISPINV opcional ausente; se usa 0.")
    if c.get("categoria") and c["categoria"] in cols_present:
        print(f"  Categoria: columna '{c['categoria']}' detectada (SPREAD_MAX_POR_CATEGORIA).")

    rq1, rq2, rq3 = _calcular_cuartiles(
        pd.concat([pd.to_numeric(df[c["repo_usa"]], errors="coerce"),
                   pd.to_numeric(df[c["repo_br"]], errors="coerce")])
    )
    dq1, dq2, dq3 = _calcular_cuartiles(
        pd.concat([
            pd.to_numeric(df[c["disp_usa"]], errors="coerce"),
            pd.to_numeric(df[c["disp_br"]], errors="coerce"),
            *[pd.to_numeric(df[c[k]], errors="coerce") for k in ("disp_inv_min", "disp_inv_max")
              if c[k] in df.columns],
        ])
    )
    pq1, pq2, pq3 = _calcular_cuartiles(pd.to_numeric(df[c["precio"]], errors="coerce"))

    if IMPRIMIR_CUARTILES_EN_CONSOLA:
        print(f"  Cuartiles repo: Q1={rq1:.4f} Q2={rq2:.4f} Q3={rq3:.4f}")
        print(f"  Cuartiles disp: Q1={dq1:.1f} Q2={dq2:.1f} Q3={dq3:.1f}")
        print(f"  Cuartiles PRECIO lista: Q1={pq1:.2f} Q2={pq2:.2f} Q3={pq3:.2f}")
        print()

    filas = []
    for _, row in df.iterrows():
        pct_sm = _spread_max_para_categoria(_valor_categoria(row, cols_present, c))
        filas.append(
            _evaluar_fila(
                repo_usa=row[c["repo_usa"]],
                repo_br=row[c["repo_br"]],
                disp_usa=row[c["disp_usa"]],
                disp_br=row[c["disp_br"]],
                disp_inv_min=_valor_fila_opcional(row, cols_present, c, "disp_inv_min", 0.0),
                disp_inv_max=_valor_fila_opcional(row, cols_present, c, "disp_inv_max", 0.0),
                inv_min=row[c["inv_min"]],
                inv_max=row[c["inv_max"]],
                precio=row[c["precio"]],
                rq1=rq1,
                rq2=rq2,
                rq3=rq3,
                dq1=dq1,
                dq2=dq2,
                dq3=dq3,
                pq1=pq1,
                pq2=pq2,
                pq3=pq3,
                pct_spread_max=pct_sm,
                modo_auditoria=modo_auditoria,
            )
        )
    return pd.DataFrame(filas, index=df.index)


def imprimir_resumen(df_in: pd.DataFrame, df_out: pd.DataFrame) -> None:
    n = len(df_out)
    print("=" * 58)
    print(" RESUMEN EJECUTIVO")
    print("=" * 58)
    print(f"  Referencias: {n}")
    if OUT_ESTADO in df_out.columns:
        for v, cnt in df_out[OUT_ESTADO].value_counts().items():
            print(f"  {v}: {cnt} ({cnt/n*100:.1f}%)")
    if OUT_CODIGO in df_out.columns:
        print("  -- Codigos --")
        for v, cnt in df_out[OUT_CODIGO].value_counts().head(12).items():
            print(f"    {v}: {cnt}")
    calc = df_out[df_out[OUT_ESTADO] == "CALCULADO"]
    if len(calc) and OUT_OK_MARGEN in calc.columns:
        okn = (calc[OUT_OK_MARGEN] == "SI").sum()
        piso = MARGEN_OBJETIVO_PCT - MARGEN_TOLERANCIA_PCT
        print(
            f"  En margen (>= piso {piso}% = objetivo {MARGEN_OBJETIVO_PCT}% "
            f"- {MARGEN_TOLERANCIA_PCT} pp): {okn} / {len(calc)}"
        )
    nc_blk = df_out[df_out[OUT_ESTADO] == "NO_CALCULABLE"]
    if len(nc_blk) and OUT_CODIGO in nc_blk.columns:
        print("  -- Top 3 motivos NO_CALCULABLE (CODIGO) --")
        for v, cnt in nc_blk[OUT_CODIGO].value_counts().head(3).items():
            print(f"    {v}: {cnt}")
    if len(calc) and OUT_NOTA in calc.columns:
        atn = calc[OUT_NOTA].astype(str).str.contains("[ATENCION]", regex=False).sum()
        print(f"  Calculados con [ATENCION] en nota: {atn} / {len(calc)} ({atn / len(calc) * 100:.1f}%)")
    if OUT_CONFIANZA in df_out.columns:
        print("  -- CONFIANZA --")
        for v, cnt in df_out[OUT_CONFIANZA].value_counts().items():
            print(f"    {v}: {cnt} ({cnt / n * 100:.1f}%)")
    print("=" * 58)


def _columnas_salida_resultado(df_res: pd.DataFrame) -> list[str]:
    base = [
        OUT_ESTADO,
        OUT_CODIGO,
        OUT_MEJOR_COSTO,
        OUT_ORIGEN,
        OUT_MARGEN,
        OUT_OK_MARGEN,
        OUT_NOTA,
        OUT_CONFIANZA,
    ]
    if OUT_AUDITORIA in df_res.columns:
        base.append(OUT_AUDITORIA)
    return [x for x in base if x in df_res.columns]


def _concat_salida(df_in: pd.DataFrame, df_res: pd.DataFrame, c: dict) -> pd.DataFrame:
    orden_in = _orden_entrada(df_in, c)
    cols_res = _columnas_salida_resultado(df_res)
    return pd.concat([df_in[orden_in], df_res[cols_res]], axis=1)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Monitor de costo vs inventario y reposicion (salida corta para decisiones).",
    )
    parser.add_argument("entrada", nargs="?", type=Path, default=None)
    parser.add_argument("salida", nargs="?", type=Path, default=None)
    parser.add_argument("--sep", default=None)
    parser.add_argument("--encoding", default=None)
    parser.add_argument("-o", "--output", type=Path, default=None)
    parser.add_argument("--excel", type=Path, default=None)
    parser.add_argument("--sheet", default=0)
    parser.add_argument("--excel-header", type=int, default=0)
    parser.add_argument(
        "--auditoria",
        action="store_true",
        help="Incluye columna AUDITORIA (traza corta) en la salida.",
    )
    args = parser.parse_args()

    sep = CSV_SEP if args.sep is None else args.sep
    encoding = CSV_ENCODING if args.encoding is None else args.encoding

    if args.excel is not None:
        p = args.excel
        if not p.exists():
            sys.exit(f"No encontrado: {p}")
        try:
            sh = int(args.sheet)
        except ValueError:
            sh = args.sheet
        df = pd.read_excel(p, sheet_name=sh, header=args.excel_header, engine="openpyxl")
        df = _aplicar_alias_columnas_entrada(df)
        _enterizar_disponibilidades(df, COL)
        print("  QA entrada (pre-calculo):")
        _validar_entrada(df, COL)
        out = _calcular_resultados(df, modo_auditoria=args.auditoria)
        imprimir_resumen(df, out)
        dest = args.salida or args.output or p.with_name(p.stem + "_monitor" + p.suffix)
        try:
            _concat_salida(df, out, COL).to_excel(dest, index=False, engine="openpyxl")
        except PermissionError:
            sys.exit(f"No se pudo escribir (archivo abierto?): {dest}")
        print(f"\nGuardado: {dest}")
        return

    pin = Path(args.entrada or INPUT_CSV)
    pout = Path(args.salida or args.output or OUTPUT_CSV)
    if not pin.exists():
        sys.exit(f"No existe: {pin}")

    print(f"\nLeyendo: {pin}")
    df = pd.read_csv(pin, sep=sep, encoding=encoding, header=CSV_HEADER_ROW)
    print(f"  {len(df)} filas\n")
    df = _aplicar_alias_columnas_entrada(df)
    _enterizar_disponibilidades(df, COL)
    print("  QA entrada (pre-calculo):")
    _validar_entrada(df, COL)
    out = _calcular_resultados(df, modo_auditoria=args.auditoria)
    imprimir_resumen(df, out)
    final = _concat_salida(df, out, COL)
    pout.parent.mkdir(parents=True, exist_ok=True)
    try:
        final.to_csv(pout, sep=sep, encoding=encoding, index=False)
    except PermissionError:
        sys.exit(
            f"No se pudo escribir: {pout} (cierre el archivo o use otra ruta con -o)."
        )
    print(f"\nGuardado: {pout}")


if __name__ == "__main__":
    main()
