# -*- coding: utf-8 -*-
"""
Estimación del Mejor Precio de Reposición – MEWP MarketShare.

Algoritmo:
  1. Estabilidad de costos USA vs BR (porcentual <20 % + nominal por cuartil).
  2. Disponibilidad (umbral fijo ≥10 ó adaptativo por cuartiles de precio).
  3. Comparación contra costo de inventario.
  4. Alerta de alta volatilidad (precio + disponibilidad USA/BR + inventario).
  5. DISPINVmin / DISPINVmax: unidades de stock en inventario (min/max).

Uso directo (CSV por defecto en esta carpeta):
    python mewp_mejor_precio.py
    python mewp_mejor_precio.py -i entrada.csv -o salida_precio.csv
Modo Excel:
    python mewp_mejor_precio.py --excel datos.xlsx [--sheet HOJA] [-o salida.xlsx]

Parametros por defecto: ver BASE_DIR, INPUT_CSV, OUTPUT_CSV al inicio del codigo.

Uso como módulo:
    from mewp_mejor_precio import estimar_mejor_precio
    df_resultado = estimar_mejor_precio(df)
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# ─────────────────────────────────────────────────────────────────────
# Nombres de columna esperados (ajustar si el Excel usa otros nombres)
# ─────────────────────────────────────────────────────────────────────
COL = {
    "inv_min":      "CostoINVmin",
    "inv_max":      "costoINVmax",
    "repo_usa":     "CostoREPOU",
    "repo_br":      "COSTOREPOBR",
    "disp_usa":     "DISPUSA",
    "disp_br":      "DISPBR",
    "disp_inv_min": "DISPINVmin",
    "disp_inv_max": "DISPINVmax",
    "precio":       "PRECIO",
}

# Columnas obligatorias (las de inventario DISP pueden omitirse: se asume 0)
_COL_REQUERIDAS = (
    "inv_min",
    "inv_max",
    "repo_usa",
    "repo_br",
    "disp_usa",
    "disp_br",
    "precio",
)

# Columnas de salida
OUT_MEJOR_PRECIO      = "MEJOR_PRECIO"
OUT_ORIGEN            = "ORIGEN_PRECIO"
OUT_RAZON             = "RAZON_SELECCION"
OUT_ESTABILIDAD       = "ESTABILIDAD_REPO"
OUT_PCT_DIFF          = "PCT_DIFF_REPO"
OUT_ALERTA            = "ALERTA_VOLATILIDAD"

_EPS = 1e-12

# ---------- Parametros por defecto (misma carpeta que este script) ----------
BASE_DIR = Path(__file__).resolve().parent
INPUT_CSV = BASE_DIR / "entrada.csv"
OUTPUT_CSV = BASE_DIR / "salida_precio.csv"
CSV_SEP = ";"
CSV_ENCODING = "utf-8-sig"
CSV_HEADER_ROW = 0


# ─────────────────────────────────────────────────────────────────────
# Funciones auxiliares
# ─────────────────────────────────────────────────────────────────────

def _safe(val, default=0.0):
    """Devuelve *default* si el valor es NaN / None / inf."""
    if val is None:
        return default
    try:
        if np.isnan(val) or np.isinf(val):
            return default
    except (TypeError, ValueError):
        return default
    return float(val)


def _valor_fila_opcional(
    row: pd.Series,
    cols_present: set,
    c: dict[str, str],
    key: str,
    default: float = 0.0,
) -> float:
    """Lee columna opcional (ej. DISPINVmin); si no existe, *default*."""
    name = c.get(key)
    if not name or name not in cols_present:
        return default
    return _safe(row[name], default)


def _calcular_cuartiles_repo(
    df: pd.DataFrame,
    c: dict[str, str] | None = None,
) -> tuple[float, float, float]:
    """Q1, Q2, Q3 de todos los costos de reposicion (USA + BR combinados)."""
    c = c or COL
    todos = pd.concat([
        pd.to_numeric(df[c["repo_usa"]], errors="coerce"),
        pd.to_numeric(df[c["repo_br"]], errors="coerce"),
    ]).dropna()
    if todos.empty:
        return 0.0, 0.0, 0.0
    return tuple(todos.quantile([0.25, 0.50, 0.75]).values)  # type: ignore[return-value]


def _calcular_cuartiles_disp(
    df: pd.DataFrame,
    c: dict[str, str] | None = None,
) -> tuple[float, float, float]:
    """Q1, Q2, Q3 de disponibilidades: USA, BR y (si existen) DISPINVmin/max."""
    c = c or COL
    series_list = [
        pd.to_numeric(df[c["disp_usa"]], errors="coerce"),
        pd.to_numeric(df[c["disp_br"]], errors="coerce"),
    ]
    for k in ("disp_inv_min", "disp_inv_max"):
        nom = c.get(k)
        if nom and nom in df.columns:
            series_list.append(pd.to_numeric(df[nom], errors="coerce"))
    todos = pd.concat(series_list).dropna()
    if todos.empty:
        return 0.0, 0.0, 0.0
    return tuple(todos.quantile([0.25, 0.50, 0.75]).values)  # type: ignore[return-value]


def _calcular_cuartiles_precio(
    df: pd.DataFrame,
    c: dict[str, str] | None = None,
) -> tuple[float, float, float]:
    """Q1, Q2, Q3 del precio actual."""
    c = c or COL
    serie = pd.to_numeric(df[c["precio"]], errors="coerce").dropna()
    if serie.empty:
        return 0.0, 0.0, 0.0
    return tuple(serie.quantile([0.25, 0.50, 0.75]).values)  # type: ignore[return-value]


def _umbral_nominal(max_repo: float,
                    rq1: float, rq2: float, rq3: float) -> float:
    """
    Umbral máximo de diferencia absoluta entre CostoREPOUSA y COSTOREPOBR
    para considerar el par «estable».

    Cuanto mayor sea el costo de reposición, más estricto (en proporción)
    es el techo nominal, porque un 20 % de $100 M es mucho más que un
    20 % de $1.

    Cuartil del costo        Factor del cuartil
    ≤ Q1 (barato)            30 % de Q1
    Q1 – Q2 (medio-bajo)    25 % de Q2
    Q2 – Q3 (medio-alto)    20 % de Q3
    > Q3  (caro)             15 % de Q3
    """
    if max_repo <= rq1:
        return rq1 * 0.30 + _EPS
    if max_repo <= rq2:
        return rq2 * 0.25 + _EPS
    if max_repo <= rq3:
        return rq3 * 0.20 + _EPS
    return rq3 * 0.15 + _EPS


def _umbral_disp_por_precio(precio: float,
                            pq1: float, pq2: float, pq3: float,
                            dq1: float, dq2: float, dq3: float) -> float:
    """
    Umbral mínimo de disponibilidad adaptado al cuartil de precio.

    Artículos caros → exigir más disponibilidad para confiar en el origen.
    Artículos baratos → aceptar menor disponibilidad.

    Si el cuartil de disponibilidad Q1 ya es > 10, se usa ese piso;
    en caso contrario el mínimo absoluto es 10.

    Cuartil de precio      Umbral de disponibilidad
    ≤ Q1                   max(5,  dQ1)
    Q1 – Q2                max(8,  dQ1)
    Q2 – Q3                max(10, dQ2)
    > Q3                   max(15, dQ2)
    """
    if precio <= pq1:
        return max(5.0, dq1)
    if precio <= pq2:
        return max(8.0, dq1)
    if precio <= pq3:
        return max(10.0, dq2)
    return max(15.0, dq2)


# ─────────────────────────────────────────────────────────────────────
# Lógica principal de pricing por fila
# ─────────────────────────────────────────────────────────────────────

def _evaluar_fila(
    repo_usa: float, repo_br: float,
    disp_usa: float, disp_br: float,
    disp_inv_min: float, disp_inv_max: float,
    inv_min: float, inv_max: float,
    precio: float,
    rq1: float, rq2: float, rq3: float,
    dq1: float, dq2: float, dq3: float,
    pq1: float, pq2: float, pq3: float,
) -> dict:
    repo_usa = _safe(repo_usa, np.nan)
    repo_br  = _safe(repo_br, np.nan)
    disp_usa = _safe(disp_usa, 0)
    disp_br  = _safe(disp_br, 0)
    disp_inv_min = _safe(disp_inv_min, 0)
    disp_inv_max = _safe(disp_inv_max, 0)
    inv_min  = _safe(inv_min, 0)
    inv_max  = _safe(inv_max, 0)
    precio   = _safe(precio, 0)

    tiene_usa = not np.isnan(repo_usa)
    tiene_br  = not np.isnan(repo_br)
    inv_cost  = max(inv_min, inv_max)

    # --- Sin datos de reposición ---
    if not tiene_usa and not tiene_br:
        return {
            OUT_MEJOR_PRECIO: inv_cost if inv_cost > 0 else np.nan,
            OUT_ORIGEN:       "INVENTARIO" if inv_cost > 0 else "SIN_DATOS",
            OUT_RAZON:        "Sin costo de reposición disponible",
            OUT_ESTABILIDAD:  "N/A",
            OUT_PCT_DIFF:     np.nan,
            OUT_ALERTA:       "SIN_REPO" if inv_cost == 0 else "",
        }

    # Si solo uno de los dos orígenes tiene dato, usar ese
    if not tiene_usa:
        repo_usa = np.inf
    if not tiene_br:
        repo_br = np.inf

    max_repo = max(repo_usa, repo_br) if min(repo_usa, repo_br) != np.inf else 0
    min_repo = min(repo_usa, repo_br)

    # ── 1. Estabilidad ──────────────────────────────────────────────
    if tiene_usa and tiene_br and max_repo > 0:
        pct_diff = abs(repo_usa - repo_br) / (max_repo + _EPS)
        abs_diff = abs(repo_usa - repo_br)
        nom_thresh = _umbral_nominal(max_repo, rq1, rq2, rq3)
        es_estable = (pct_diff < 0.20) and (abs_diff <= nom_thresh)
    else:
        pct_diff = np.nan
        abs_diff = 0.0
        es_estable = False  # con un solo origen no se puede evaluar

    # ── 2. Selección por estabilidad o disponibilidad ────────────────
    if es_estable:
        mejor_repo = min_repo
        origen = "USA" if repo_usa <= repo_br else "BR"
        razon = "Repos estable -> menor costo"
        etiqueta_estab = "ESTABLE"
    else:
        umbral_disp = _umbral_disp_por_precio(precio, pq1, pq2, pq3,
                                               dq1, dq2, dq3)
        usa_ok = disp_usa >= umbral_disp and tiene_usa
        br_ok  = disp_br  >= umbral_disp and tiene_br

        if usa_ok and br_ok:
            mejor_repo = min(repo_usa, repo_br)
            origen = "USA" if repo_usa <= repo_br else "BR"
            razon = f"Ambos con disp>={umbral_disp:.0f} -> menor costo"
        elif usa_ok:
            mejor_repo = repo_usa
            origen = "USA"
            razon = f"Solo USA con disp>={umbral_disp:.0f}"
        elif br_ok:
            mejor_repo = repo_br
            origen = "BR"
            razon = f"Solo BR con disp>={umbral_disp:.0f}"
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
            razon = f"Ninguno alcanza disp>={umbral_disp:.0f} -> mejor disp relativa"

        if pd.notna(pct_diff) and pct_diff < 0.20:
            etiqueta_estab = "PCT_OK_NOM_ALTO"
        elif tiene_usa and tiene_br and abs_diff <= _umbral_nominal(max_repo, rq1, rq2, rq3):
            etiqueta_estab = "NOM_OK_PCT_ALTO"
        elif tiene_usa and tiene_br:
            etiqueta_estab = "INESTABLE"
        else:
            etiqueta_estab = "ORIGEN_UNICO"

    # Protección contra inf
    if mejor_repo == np.inf:
        mejor_repo = np.nan

    # ── 3. Comparación con inventario ────────────────────────────────
    if inv_cost > 0 and not np.isnan(mejor_repo) and inv_cost > mejor_repo:
        mejor_precio = inv_cost
        origen_final = "INVENTARIO"
        razon = f"CostoINV ({inv_cost:.2f}) > Repo {origen} ({mejor_repo:.2f})"
    elif np.isnan(mejor_repo):
        mejor_precio = inv_cost if inv_cost > 0 else np.nan
        origen_final = "INVENTARIO" if inv_cost > 0 else "SIN_DATOS"
    else:
        mejor_precio = mejor_repo
        origen_final = origen

    # ── 4. Alerta de volatilidad ─────────────────────────────────────
    alerta = _evaluar_volatilidad(
        disp_usa,
        disp_br,
        disp_inv_min,
        disp_inv_max,
        pct_diff,
    )

    return {
        OUT_MEJOR_PRECIO: round(mejor_precio, 4) if pd.notna(mejor_precio) else np.nan,
        OUT_ORIGEN:       origen_final,
        OUT_RAZON:        razon,
        OUT_ESTABILIDAD:  etiqueta_estab,
        OUT_PCT_DIFF:     round(pct_diff * 100, 1) if pd.notna(pct_diff) else np.nan,
        OUT_ALERTA:       alerta,
    }


def _evaluar_volatilidad(
    disp_usa: float,
    disp_br: float,
    disp_inv_min: float,
    disp_inv_max: float,
    pct_diff_repo: float,
) -> str:
    """
    Volatilidad de repos (USA vs BR), de inventario (min vs max unidades)
    y cruce inventario vs externos.
    """
    max_disp = max(disp_usa, disp_br)
    if max_disp > 0:
        pct_diff_disp = abs(disp_usa - disp_br) / max_disp
    else:
        pct_diff_disp = 0.0

    disp_vol_repo = pct_diff_disp > 0.50 or abs(disp_usa - disp_br) > 20

    inv_hi = max(disp_inv_min, disp_inv_max)
    inv_spread_abs = abs(disp_inv_max - disp_inv_min)
    if inv_hi > 0:
        pct_inv_int = inv_spread_abs / inv_hi
    else:
        pct_inv_int = 0.0
    disp_vol_inv = inv_hi > 0 and (pct_inv_int > 0.50 or inv_spread_abs > 20)

    m_ext = max(disp_usa, disp_br)
    m_inv = inv_hi
    if m_ext > 0 and m_inv > 0:
        cross = abs(m_ext - m_inv) / max(m_ext, m_inv)
        disp_vol_cross = cross > 0.50 or abs(m_ext - m_inv) > 20
    else:
        disp_vol_cross = False

    disp_vol = disp_vol_repo or disp_vol_inv or disp_vol_cross

    precio_vol = pd.notna(pct_diff_repo) and pct_diff_repo > 0.40

    if precio_vol and disp_vol:
        return "ALTA VOLATILIDAD (precio + disp)"
    if precio_vol:
        return "PRECIO VARIABLE"
    if disp_vol_inv and not disp_vol_repo:
        return "INV DISP VARIABLE"
    if disp_vol_cross and not disp_vol_repo and not disp_vol_inv:
        return "INV vs REPO DISP VARIABLE"
    if disp_vol:
        return "DISP VARIABLE"
    return ""


# ─────────────────────────────────────────────────────────────────────
# API pública
# ─────────────────────────────────────────────────────────────────────

def estimar_mejor_precio(df: pd.DataFrame,
                         col_map: dict | None = None) -> pd.DataFrame:
    """
    Recibe un DataFrame con las columnas de la imagen (o mapeadas) y
    devuelve un DataFrame con las 6 columnas de resultado alineadas al
    indice original.

    Parámetros
    ----------
    df : DataFrame con al menos: CostoINVmin, costoINVmax, CostoREPOU,
         COSTOREPOBR, DISPUSA, DISPBR, PRECIO.
         Opcionales: DISPINVmin, DISPINVmax (unidades de stock inventario);
         si faltan, se asume 0.
    col_map : dict opcional para sobrescribir los nombres de columna
              (mismas claves que el dict COL global).

    Retorna
    -------
    DataFrame con columnas:
      MEJOR_PRECIO, ORIGEN_PRECIO, RAZON_SELECCION,
      ESTABILIDAD_REPO, PCT_DIFF_REPO, ALERTA_VOLATILIDAD
    """
    c = {**COL, **(col_map or {})}

    for key in _COL_REQUERIDAS:
        nombre = c[key]
        if nombre not in df.columns:
            raise KeyError(
                f"Columna '{nombre}' (clave '{key}') no encontrada. "
                f"Columnas disponibles: {list(df.columns)}"
            )

    cols_present = set(df.columns)
    missing_inv = [c[k] for k in ("disp_inv_min", "disp_inv_max") if c[k] not in cols_present]
    if missing_inv:
        print(f"  Aviso: columnas opcionales ausentes {missing_inv}; se usa 0.")

    rq1, rq2, rq3 = _calcular_cuartiles_repo(df, c)
    dq1, dq2, dq3 = _calcular_cuartiles_disp(df, c)
    pq1, pq2, pq3 = _calcular_cuartiles_precio(df, c)

    print(f"  Cuartiles Repo  (Q1={rq1:.4f}, Q2={rq2:.4f}, Q3={rq3:.4f})")
    print(f"  Cuartiles Disp  (Q1={dq1:.1f}, Q2={dq2:.1f}, Q3={dq3:.1f})")
    print(f"  Cuartiles Precio(Q1={pq1:.2f}, Q2={pq2:.2f}, Q3={pq3:.2f})")
    print(f"  Umbral nominal (ejemplo Q2): {_umbral_nominal(rq2, rq1, rq2, rq3):.4f}")
    print()

    resultados = []
    for _, row in df.iterrows():
        r = _evaluar_fila(
            repo_usa=row[c["repo_usa"]],
            repo_br=row[c["repo_br"]],
            disp_usa=row[c["disp_usa"]],
            disp_br=row[c["disp_br"]],
            disp_inv_min=_valor_fila_opcional(row, cols_present, c, "disp_inv_min", 0.0),
            disp_inv_max=_valor_fila_opcional(row, cols_present, c, "disp_inv_max", 0.0),
            inv_min=row[c["inv_min"]],
            inv_max=row[c["inv_max"]],
            precio=row[c["precio"]],
            rq1=rq1, rq2=rq2, rq3=rq3,
            dq1=dq1, dq2=dq2, dq3=dq3,
            pq1=pq1, pq2=pq2, pq3=pq3,
        )
        resultados.append(r)

    return pd.DataFrame(resultados, index=df.index)


# ─────────────────────────────────────────────────────────────────────
# Resumen de diagnóstico
# ─────────────────────────────────────────────────────────────────────

def imprimir_resumen(df_in: pd.DataFrame, df_out: pd.DataFrame) -> None:
    n = len(df_out)
    print("=" * 60)
    print("RESUMEN DE ESTIMACION DE MEJOR PRECIO")
    print("=" * 60)
    print(f"  Total referencias evaluadas: {n}")
    print()

    print("  -- Origen seleccionado --")
    for val, cnt in df_out[OUT_ORIGEN].value_counts().items():
        print(f"      {val:20s}  {cnt:>6d}  ({cnt/n*100:5.1f} %)")
    print()

    print("  -- Estabilidad --")
    for val, cnt in df_out[OUT_ESTABILIDAD].value_counts().items():
        print(f"      {val:25s}  {cnt:>6d}  ({cnt/n*100:5.1f} %)")
    print()

    print("  -- Alertas --")
    alertas = df_out[df_out[OUT_ALERTA].astype(str).str.len() > 0]
    if alertas.empty:
        print("      Ninguna alerta generada.")
    else:
        for val, cnt in alertas[OUT_ALERTA].value_counts().items():
            print(f"      {val:40s}  {cnt:>6d}")
    print()

    col_precio_orig = COL["precio"]
    if col_precio_orig in df_in.columns:
        precio_orig = pd.to_numeric(df_in[col_precio_orig], errors="coerce")
        mejor = pd.to_numeric(df_out[OUT_MEJOR_PRECIO], errors="coerce")
        validos = precio_orig.notna() & mejor.notna() & (precio_orig > 0)
        if validos.any():
            diff = mejor[validos] - precio_orig[validos]
            print("  -- Comparacion vs PRECIO actual --")
            print(f"      Diff media:   {diff.mean():+.4f}")
            print(f"      Diff mediana: {diff.median():+.4f}")
            print(f"      Mejor > Precio actual: {(diff > 0).sum()}")
            print(f"      Mejor < Precio actual: {(diff < 0).sum()}")
            print(f"      Mejor = Precio actual: {(diff == 0).sum()}")
    print("=" * 60)


# ─────────────────────────────────────────────────────────────────────
# CSV / CLI
# ─────────────────────────────────────────────────────────────────────


def procesar_csv(
    path_in: Path | str | None = None,
    path_out: Path | str | None = None,
    *,
    sep: str | None = None,
    encoding: str | None = None,
    header: int | None = None,
) -> pd.DataFrame:
    """
    Lee *path_in* (default INPUT_CSV), calcula columnas de salida y escribe *path_out*
    (default OUTPUT_CSV). Devuelve el DataFrame concatenado (entrada + resultado).
    """
    pin = Path(path_in or INPUT_CSV)
    pout = Path(path_out or OUTPUT_CSV)
    sep = CSV_SEP if sep is None else sep
    encoding = CSV_ENCODING if encoding is None else encoding
    header = CSV_HEADER_ROW if header is None else header

    if not pin.exists():
        raise FileNotFoundError(
            f"No existe el CSV de entrada: {pin}\n"
            f"Coloque '{INPUT_CSV.name}' en {BASE_DIR} o use -i / --input."
        )

    print(f"\nLeyendo CSV: {pin} (sep={repr(sep)}, encoding={encoding}) ...")
    df = pd.read_csv(pin, sep=sep, encoding=encoding, header=header)
    print(f"  {len(df)} filas x {len(df.columns)} columnas\n")

    df_precio = estimar_mejor_precio(df)
    imprimir_resumen(df, df_precio)
    df_final = pd.concat([df, df_precio], axis=1)

    pout.parent.mkdir(parents=True, exist_ok=True)
    df_final.to_csv(pout, sep=sep, encoding=encoding, index=False)
    print(f"\nGuardado: {pout}")
    return df_final


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Mejor precio MEWP: CSV por defecto (entrada.csv -> salida_precio.csv)."
    )
    parser.add_argument(
        "-i",
        "--input",
        type=Path,
        default=None,
        help=f"CSV de entrada (default: {INPUT_CSV})",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=None,
        help=f"CSV de salida (default: {OUTPUT_CSV})",
    )
    parser.add_argument(
        "--sep",
        default=None,
        help=f"Separador CSV (default: {repr(CSV_SEP)})",
    )
    parser.add_argument(
        "--encoding",
        default=None,
        help=f"Encoding (default: {CSV_ENCODING})",
    )
    parser.add_argument(
        "--csv-header",
        type=int,
        default=None,
        metavar="N",
        help="Fila de encabezados del CSV, 0-based (default: 0).",
    )
    parser.add_argument(
        "--excel",
        type=Path,
        default=None,
        metavar="ARCHIVO.xlsx",
        help="Si se indica, lee Excel en lugar de CSV.",
    )
    parser.add_argument("--sheet", default=0, help="Hoja Excel (nombre o indice).")
    parser.add_argument(
        "--excel-header",
        type=int,
        default=0,
        help="Fila encabezado Excel, 0-based (default: 0).",
    )
    args = parser.parse_args()

    if args.excel is not None:
        path_in = args.excel
        if not path_in.exists():
            sys.exit(f"No se encontro: {path_in}")
        try:
            sheet = int(args.sheet)
        except ValueError:
            sheet = args.sheet
        print(f"\nLeyendo Excel: {path_in} (hoja={sheet}, header={args.excel_header}) ...")
        df = pd.read_excel(
            path_in, sheet_name=sheet, header=args.excel_header, engine="openpyxl"
        )
        print(f"  {len(df)} filas x {len(df.columns)} columnas\n")
        df_precio = estimar_mejor_precio(df)
        imprimir_resumen(df, df_precio)
        df_final = pd.concat([df, df_precio], axis=1)
        if args.output is not None:
            path_out = Path(args.output)
        else:
            path_out = path_in.with_name(path_in.stem + "_precio" + path_in.suffix)
        df_final.to_excel(path_out, index=False, engine="openpyxl")
        print(f"\nGuardado: {path_out}")
        return

    try:
        procesar_csv(
            args.input,
            args.output,
            sep=args.sep,
            encoding=args.encoding,
            header=args.csv_header,
        )
    except FileNotFoundError as e:
        sys.exit(str(e))


if __name__ == "__main__":
    main()
