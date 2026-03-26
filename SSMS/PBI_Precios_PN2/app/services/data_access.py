from __future__ import annotations

from pathlib import Path
import shutil
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any
import json

import duckdb
import pandas as pd


ROOT_DIR = Path(__file__).resolve().parents[2]
MASTER_DB_PATH = ROOT_DIR / "pipeline.duckdb"
READ_DB_PATH = ROOT_DIR / "pipeline_read.duckdb"
_DEBUG_PATH_CANDIDATES = [
    Path(__file__).resolve().parents[5] / "debug-06d74e.log",  # workspace root
    ROOT_DIR / "debug-06d74e.log",  # project root fallback
]
DEBUG_LOG_PATH = _DEBUG_PATH_CANDIDATES[0]
DEBUG_SESSION_ID = "06d74e"


# region agent log
def _debug_log(run_id: str, hypothesis_id: str, location: str, message: str, data: dict[str, Any]) -> None:
    try:
        payload = {
            "sessionId": DEBUG_SESSION_ID,
            "runId": run_id,
            "hypothesisId": hypothesis_id,
            "location": location,
            "message": message,
            "data": data,
            "timestamp": int(time.time() * 1000),
        }
        line = json.dumps(payload, ensure_ascii=False) + "\n"
        for p in _DEBUG_PATH_CANDIDATES:
            try:
                with p.open("a", encoding="utf-8") as fh:
                    fh.write(line)
                break
            except Exception:
                continue
    except Exception:
        pass
# endregion


def sync_read_db(force: bool = False) -> None:
    if not MASTER_DB_PATH.exists():
        raise FileNotFoundError(f"No existe la base de datos: {MASTER_DB_PATH}")

    needs_copy = force or (not READ_DB_PATH.exists())
    if not needs_copy and READ_DB_PATH.exists():
        needs_copy = MASTER_DB_PATH.stat().st_mtime > READ_DB_PATH.stat().st_mtime

    if not needs_copy:
        return

    last_error: Exception | None = None
    for _ in range(8):
        try:
            shutil.copy2(MASTER_DB_PATH, READ_DB_PATH)
            return
        except Exception as exc:
            last_error = exc
            time.sleep(0.3)
    raise RuntimeError(
        "No fue posible sincronizar la copia de lectura de DuckDB."
    ) from last_error


def _connect() -> duckdb.DuckDBPyConnection:
    # Sincroniza en cada lectura (usa mtime y copia solo si hay cambios).
    # Asi la UI ve datos nuevos sin reiniciar Streamlit.
    sync_read_db()
    last_error: Exception | None = None
    for _ in range(8):
        try:
            return duckdb.connect(str(READ_DB_PATH), read_only=True)
        except duckdb.IOException as exc:
            last_error = exc
            msg = str(exc).lower()
            # En Windows DuckDB puede bloquear temporalmente cuando otro proceso escribe.
            if "being utilized by another process" in msg or "already open in" in msg:
                time.sleep(0.4)
                continue
            raise

    raise RuntimeError(
        "La base de datos está en uso por otro proceso. "
        "Espera unos segundos y vuelve a intentar."
    ) from last_error


def _table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    sql = """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE lower(table_name) = lower(?)
    """
    return con.execute(sql, [table_name]).fetchone()[0] > 0


def _table_has_column(
    con: duckdb.DuckDBPyConnection, table_name: str, column_name: str
) -> bool:
    sql = """
        SELECT COUNT(*)
        FROM information_schema.columns
        WHERE lower(table_name) = lower(?)
          AND lower(column_name) = lower(?)
    """
    return con.execute(sql, [table_name, column_name]).fetchone()[0] > 0


def refrescar_catalogo_bodegas_auditoria() -> tuple[bool, str]:
    """
    Recrea solo la tabla `bodegas_dim` en la base maestra a partir de `margen_siesa_raw`
    y sincroniza la copia de lectura. No ejecuta pipelines ni SQL Server.
    """
    from services.sql_reports_loader import refrescar_bodegas_dim_desde_margen

    ok, msg = refrescar_bodegas_dim_desde_margen(MASTER_DB_PATH)
    if ok:
        try:
            sync_read_db(force=True)
        except Exception as exc:
            return False, f"{msg} (aviso: no se pudo sincronizar lectura: {exc})"
    return ok, msg


def obtener_catalogo_bodegas_auditoria() -> pd.DataFrame:
    """
    Catálogo de bodegas desde `bodegas_dim` (se crea al ejecutar SQL 00 tras cargar `margen_siesa_raw`).
    Usado en el selector de bodegas para la auditoría de referencias.
    """
    try:
        with _connect() as con:
            if not _table_exists(con, "bodegas_dim"):
                return pd.DataFrame(columns=["Bodega", "Nom_Bodega", "Nom_Instalacion"])
            return con.execute(
                """
                SELECT Bodega, Nom_Bodega, Nom_Instalacion
                FROM bodegas_dim
                ORDER BY COALESCE(Nom_Bodega, ''), Bodega
                """
            ).df()
    except Exception:
        return pd.DataFrame(columns=["Bodega", "Nom_Bodega", "Nom_Instalacion"])


def _safe_number(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    try:
        return float(value)
    except Exception:
        return None


def _norm_sql_expr(col_expr: str) -> str:
    """Replica la normalizacion de referencia usada en pipelines 01/02/03."""
    c = f"CAST({col_expr} AS VARCHAR)"
    return (
        "UPPER(trim("
        "regexp_replace("
        "regexp_replace("
        "regexp_replace("
        "regexp_replace("
        "regexp_replace("
        "regexp_replace("
        "replace("
        "trim(replace(replace(replace({c},chr(9),''),chr(10),''),chr(13),'')),"
        "'_','-'"
        "),"
        "'[^A-Za-z0-9.\\-\"/ ]','','g'),"
        "'^\\.+|\\.+$','','g'),"
        "'\\.{{2,}}','.','g'),"
        "'-{{2,}}','-','g'),"
        "'\\s*-\\s*','-','g'),"
        "'[ ]{{2,}}',' ','g')"
        "))"
    ).format(c=c)


def buscar_referencias(texto: str, limite: int = 30) -> pd.DataFrame:
    filtro = (texto or "").strip()
    if not filtro:
        return pd.DataFrame(columns=["Referencia_Original", "Referencia_Normalizada", "Descripción"])

    pat = f"%{filtro}%"
    pref = f"{filtro}%"

    with _connect() as con:
        if not _table_exists(con, "resultado_precios_lista"):
            return pd.DataFrame(columns=["Referencia_Original", "Referencia_Normalizada", "Descripción"])

        tiene_refs_alt = _table_has_column(con, "resultado_precios_lista", "RefsAlternas")

        if tiene_refs_alt:
            sql = """
                SELECT DISTINCT
                    Referencia_Original,
                    Referencia_Normalizada,
                    "Descripción"
                FROM resultado_precios_lista
                WHERE upper(Referencia_Original) LIKE upper(?)
                   OR upper(Referencia_Normalizada) LIKE upper(?)
                   OR upper(COALESCE("Descripción", '')) LIKE upper(?)
                   OR upper(COALESCE(CAST(RefsAlternas AS VARCHAR), '')) LIKE upper(?)
                ORDER BY
                    CASE
                        WHEN upper(Referencia_Original) LIKE upper(?) THEN 1
                        WHEN upper(COALESCE("Descripción", '')) LIKE upper(?) THEN 2
                        WHEN upper(COALESCE(CAST(RefsAlternas AS VARCHAR), '')) LIKE upper(?) THEN 3
                        ELSE 4
                    END,
                    Referencia_Original
                LIMIT ?
            """
            params = [pat, pat, pat, pat, pref, pref, pat, limite]
        else:
            sql = """
                SELECT DISTINCT
                    Referencia_Original,
                    Referencia_Normalizada,
                    "Descripción"
                FROM resultado_precios_lista
                WHERE upper(Referencia_Original) LIKE upper(?)
                   OR upper(Referencia_Normalizada) LIKE upper(?)
                   OR upper(COALESCE("Descripción", '')) LIKE upper(?)
                ORDER BY
                    CASE
                        WHEN upper(Referencia_Original) LIKE upper(?) THEN 1
                        WHEN upper(COALESCE("Descripción", '')) LIKE upper(?) THEN 2
                        ELSE 3
                    END,
                    Referencia_Original
                LIMIT ?
            """
            params = [pat, pat, pat, pref, pref, limite]

        return con.execute(sql, params).df()


def obtener_resumen_referencia(ref_norm: str) -> dict[str, Any] | None:
    sql = """
        SELECT *
        FROM resultado_precios_lista
        WHERE Referencia_Normalizada = ?
        LIMIT 1
    """
    with _connect() as con:
        if not _table_exists(con, "resultado_precios_lista"):
            return None
        df = con.execute(sql, [ref_norm]).df()
        if df.empty:
            return None
        row = df.iloc[0].to_dict()

    disp_br = _safe_number(row.get("disp_br"))
    disp_usa = _safe_number(row.get("disp_usa"))
    disp_eur = _safe_number(row.get("disp_eur"))
    disp_total = sum(v for v in [disp_br, disp_usa, disp_eur] if v is not None)

    row["_disp_total"] = disp_total
    row["_disponible"] = "SI" if disp_total > 0 else "NO"
    return row


def _align_df_columns_to_expected(df: pd.DataFrame, expected: list[str]) -> pd.DataFrame:
    """
    DuckDB puede devolver alias en minúsculas (p. ej. referencia_cruce en vez de Referencia_Cruce).
    Sin esto, base_cols no reconoce la columna y se rellena toda con None.
    """
    if df.empty:
        return df
    lower_map: dict[str, str] = {}
    for c in df.columns:
        lower_map[str(c).lower()] = str(c)
    ren: dict[str, str] = {}
    for want in expected:
        if want in df.columns:
            continue
        lw = want.lower()
        if lw in lower_map:
            actual = lower_map[lw]
            if actual != want:
                ren[actual] = want
    return df.rename(columns=ren) if ren else df


def _blank_ref_val(x: Any) -> bool:
    if x is None:
        return True
    if isinstance(x, float) and pd.isna(x):
        return True
    s = str(x).strip()
    return s == "" or s.lower() in ("none", "nan")


def _refill_referencia_cruce(df: pd.DataFrame) -> pd.DataFrame:
    """
    Garantiza Referencia_Cruce cuando el SELECT devolvió NULL (p. ej. alias DuckDB) o vacío.

    - Principal: prioriza Referencia_Original.
    - Normalizada: prioriza Referencia_Normalizada.
    - Alterna: ref. maestra del renglón (normalizada u original).
    """
    if df.empty or "Referencia_Cruce" not in df.columns:
        return df
    out = df.copy()
    has_tipo = "Tipo_Coincidencia" in out.columns
    has_ro = "Referencia_Original" in out.columns
    has_rn = "Referencia_Normalizada" in out.columns

    for idx in out.index:
        tc = str(out.loc[idx, "Tipo_Coincidencia"]).strip() if has_tipo else ""
        ro = out.loc[idx, "Referencia_Original"] if has_ro else None
        rn = out.loc[idx, "Referencia_Normalizada"] if has_rn else None
        # Alterna: ref. maestra del renglón (sin sufijo de entrada)
        if tc == "Alterna":
            master = rn if not _blank_ref_val(rn) else ro
            if _blank_ref_val(master):
                continue
            out.loc[idx, "Referencia_Cruce"] = str(master).strip()
            continue

        cur = out.loc[idx, "Referencia_Cruce"]
        if not _blank_ref_val(cur):
            continue

        if tc == "Principal":
            master = ro if not _blank_ref_val(ro) else rn
        elif tc == "Normalizada":
            master = rn if not _blank_ref_val(rn) else ro
        else:
            master = rn if not _blank_ref_val(rn) else ro

        if _blank_ref_val(master):
            continue

        out.loc[idx, "Referencia_Cruce"] = str(master).strip()

    return out


def obtener_resumen_referencias_masivo(referencias: list[str]) -> pd.DataFrame:
    """
    Devuelve un resumen por cada referencia de entrada (principal, normalizada o alterna).
    Mantiene el orden de entrada y marca filas sin coincidencia.
    """
    base_cols = [
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
        "DNET BRA USD",
        "DNET USA USD",
        "DNET EUR EURO",
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
    if not referencias:
        return pd.DataFrame(columns=base_cols)

    cleaned: list[str] = []
    for x in referencias:
        s = str(x or "").strip()
        if s:
            cleaned.append(s)
    if not cleaned:
        return pd.DataFrame(columns=base_cols)

    df_input = pd.DataFrame(
        {
            "orden": list(range(len(cleaned))),
            "referencia_entrada": cleaned,
        }
    )

    with _connect() as con:
        if not _table_exists(con, "resultado_precios_lista"):
            out = df_input.rename(columns={"referencia_entrada": "Referencia_Entrada"})[
                ["Referencia_Entrada"]
            ].copy()
            out["Estado"] = "Sin datos en resultado_precios_lista"
            out["Tipo_Coincidencia"] = None
            for c in base_cols:
                if c not in out.columns:
                    out[c] = None
            return out[base_cols]

        has_refs_alt = _table_has_column(con, "resultado_precios_lista", "RefsAlternas")
        rpl_cols = [r[0] for r in con.execute("DESCRIBE resultado_precios_lista").fetchall()]
        rpl_col_set = {str(c).strip().lower(): c for c in rpl_cols}

        has_aud = _table_exists(con, "auditoria_raw")
        aud_col_set: dict[str, str] = {}
        if has_aud:
            aud_cols = [r[0] for r in con.execute("DESCRIBE auditoria_raw").fetchall()]
            aud_col_set = {str(c).strip().lower(): c for c in aud_cols}

        # Maestro histórico (script 03): Tipo_Origen por ref. normalizada en columna REF.
        has_o3_tab = _table_exists(con, "origen_precios_tablero")
        has_o3_ref = _table_has_column(con, "origen_precios_tablero", "REF") if has_o3_tab else False
        has_o3_tipo = _table_has_column(con, "origen_precios_tablero", "Tipo_Origen") if has_o3_tab else False
        # Inventario SIESA consolidado (reporte 00) se toma desde auditoria_raw
        # para evitar recalcular/sumar en la app.

        def _pick_rpl_col(*cands: str) -> str | None:
            for cand in cands:
                key = str(cand).strip().lower()
                if key in rpl_col_set:
                    return str(rpl_col_set[key])
            return None

        def _pick_aud_col(*cands: str) -> str | None:
            for cand in cands:
                key = str(cand).strip().lower()
                if key in aud_col_set:
                    return str(aud_col_set[key])
            return None

        def _sel_or_null(alias: str, *cands: str) -> str:
            col = _pick_rpl_col(*cands)
            if not col:
                return f"CAST(NULL AS VARCHAR) AS \"{alias}\""
            return f"p.{_duck_quote_ident(col)} AS \"{alias}\""

        sel_desc = _sel_or_null("Descripción", "Descripción", "Descripci�n", "Descripcion")
        sel_ult_usd = _sel_or_null(
            "Ultimo Valor USD",
            "Ultimo Valor USD",
            "Último Valor (USD)",
            "�ltimo Valor (USD)",
            "Ultimo Valor (USD)",
        )
        sel_vlr_liq = _sel_or_null(
            "Valor Liquido COP",
            "Valor Liquido COP",
            "Valor Líquido COP",
            "Valor Liq. (COP)",
            "Valor L�q. (COP)",
        )
        sel_costo_min = _sel_or_null("Costo_Min", "Costo_Min")
        sel_existencia_total = _sel_or_null("Existencia_Total", "Existencia_Total")
        sel_tipo_origen = _sel_or_null("Tipo_Origen", "Tipo_Origen", "Tipo Origen")
        sel_precio_lista_09 = _sel_or_null("Precio_Lista_09", "Precio_Lista_09")
        sel_ult_precio_venta = _sel_or_null(
            "Ult. Precio Venta",
            "Ult. Precio Venta",
            "Ultimo Valor Venta",
            "Valor Ult. Venta",
        )
        sel_fecha_ult_venta = _sel_or_null(
            "Fecha Ult. Venta",
            "Fecha Ult. Venta",
            "Fecha_Ultima_Venta",
            "Fecha Última Venta",
        )
        p_costo_min_expr = (
            f'try_cast(p.{_duck_quote_ident(_pick_rpl_col("Costo_Min"))} AS DOUBLE)'
            if _pick_rpl_col("Costo_Min")
            else "NULL"
        )
        p_costo_prom_expr = (
            f'try_cast(p.{_duck_quote_ident(_pick_rpl_col("Costo_Prom_Inst"))} AS DOUBLE)'
            if _pick_rpl_col("Costo_Prom_Inst")
            else "NULL"
        )
        p_existencia_total_expr = (
            f'try_cast(p.{_duck_quote_ident(_pick_rpl_col("Existencia_Total"))} AS DOUBLE)'
            if _pick_rpl_col("Existencia_Total")
            else "NULL"
        )
        _tipo_col_rpl = _pick_rpl_col("Tipo_Origen", "Tipo Origen")
        p_tipo_origen_expr = (
            f"NULLIF(trim(CAST(p.{_duck_quote_ident(_tipo_col_rpl)} AS VARCHAR)), '')"
            if _tipo_col_rpl
            else "NULL"
        )
        p_precio_lista_09_expr = (
            f'try_cast(p.{_duck_quote_ident(_pick_rpl_col("Precio_Lista_09"))} AS DOUBLE)'
            if _pick_rpl_col("Precio_Lista_09")
            else "NULL"
        )
        p_costo_max_expr = (
            f'try_cast(p.{_duck_quote_ident(_pick_rpl_col("Costo_Max"))} AS DOUBLE)'
            if _pick_rpl_col("Costo_Max")
            else "NULL"
        )
        _bod_cmin_rpl = _pick_rpl_col("Bodega_CostoMin", "Bodega Costo Min")
        p_bod_cmin_expr = (
            f"NULLIF(trim(CAST(p.{_duck_quote_ident(_bod_cmin_rpl)} AS VARCHAR)), '')"
            if _bod_cmin_rpl
            else "NULL"
        )
        _bod_cmax_rpl = _pick_rpl_col("Bodega_CostoMax", "Bodega Costo Max")
        p_bod_cmax_expr = (
            f"NULLIF(trim(CAST(p.{_duck_quote_ident(_bod_cmax_rpl)} AS VARCHAR)), '')"
            if _bod_cmax_rpl
            else "NULL"
        )

        aud_ref_col = _pick_aud_col("Referencia_Normalizada", "Referencia", "Referencia_Original")
        aud_costo_min_col = _pick_aud_col("Costo_Min")
        aud_costo_max_col = _pick_aud_col("Costo_Max")
        aud_bod_cmin_col = _pick_aud_col("Bodega_CostoMin", "Bodega Costo Min")
        aud_bod_cmax_col = _pick_aud_col("Bodega_CostoMax", "Bodega Costo Max")
        aud_existencia_total_col = _pick_aud_col("Existencia_Total")
        aud_tipo_origen_col = _pick_aud_col("Tipo_Origen", "Tipo Origen")
        aud_precio_lista_col = _pick_aud_col("Precio_Lista_09")
        aud_fecha_ult_compra_col = _pick_aud_col("Fecha_Ultima_Compra")
        aud_pais_ult_col = _pick_aud_col("Pais_Ultima")
        aud_prov_ult_col = _pick_aud_col("Proveedor_Ultima")
        aud_comp_ult_col = _pick_aud_col("Comprador_Ultima")
        aud_usd_ult_col = _pick_aud_col("Precio_USD_Ultima")
        aud_cop_ult_col = _pick_aud_col("Precio_COP_Ultima")
        aud_trm_ult_col = _pick_aud_col("TRM_Ultima")

        def _aud_any_value(col_name: str | None, cast_sql: str = "") -> str:
            if not col_name:
                return "CAST(NULL AS VARCHAR)"
            q = _duck_quote_ident(col_name)
            if cast_sql:
                return f"ANY_VALUE(CAST({q} AS {cast_sql}))"
            return f"ANY_VALUE({q})"

        def _aud_any_ts(col_name: str | None) -> str:
            if not col_name:
                return "ANY_VALUE(CAST(NULL AS TIMESTAMP))"
            q = _duck_quote_ident(col_name)
            return f"ANY_VALUE(try_cast({q} AS TIMESTAMP))"

        def _aud_any_double(col_name: str | None) -> str:
            if not col_name:
                return "ANY_VALUE(CAST(NULL AS DOUBLE))"
            q = _duck_quote_ident(col_name)
            return f"ANY_VALUE(try_cast({q} AS DOUBLE))"

        def _aud_any_varchar(col_name: str | None) -> str:
            if not col_name:
                return "ANY_VALUE(CAST(NULL AS VARCHAR))"
            q = _duck_quote_ident(col_name)
            return f"ANY_VALUE(NULLIF(trim(CAST({q} AS VARCHAR)), ''))"

        aud_cte = ""
        aud_join = ""
        aud_costo_expr = "NULL"
        aud_costo_max_expr = "CAST(NULL AS DOUBLE)"
        aud_bod_cmin_expr = "CAST(NULL AS VARCHAR)"
        aud_bod_cmax_expr = "CAST(NULL AS VARCHAR)"
        aud_exist_expr = "NULL"
        aud_tipo_expr = "NULL"
        aud_pl_expr = "NULL"
        if has_aud and aud_ref_col:
            aud_ref_q = _duck_quote_ident(aud_ref_col)
            aud_cte = f"""
                ,
                aud AS (
                    SELECT
                        upper(trim(CAST({aud_ref_q} AS VARCHAR))) AS ref_norm_key,
                        {_aud_any_value(aud_costo_min_col)} AS "Costo_Min_AUD",
                        {_aud_any_double(aud_costo_max_col)} AS "Costo_Max_AUD",
                        {_aud_any_varchar(aud_bod_cmin_col)} AS "Bodega_CostoMin_AUD",
                        {_aud_any_varchar(aud_bod_cmax_col)} AS "Bodega_CostoMax_AUD",
                        {_aud_any_value(aud_existencia_total_col)} AS "Existencia_Total_AUD",
                        {_aud_any_value(aud_tipo_origen_col, "VARCHAR")} AS "Tipo_Origen_AUD",
                        {_aud_any_value(aud_precio_lista_col)} AS "Precio_Lista_09_AUD",
                        {_aud_any_ts(aud_fecha_ult_compra_col)} AS "Fecha_Ultima_Compra_AUD",
                        {_aud_any_varchar(aud_pais_ult_col)} AS "Pais_Ultima_AUD",
                        {_aud_any_varchar(aud_prov_ult_col)} AS "Proveedor_Ultima_AUD",
                        {_aud_any_varchar(aud_comp_ult_col)} AS "Comprador_Ultima_AUD",
                        {_aud_any_double(aud_usd_ult_col)} AS "Precio_USD_Ultima_AUD",
                        {_aud_any_double(aud_cop_ult_col)} AS "Precio_COP_Ultima_AUD",
                        {_aud_any_double(aud_trm_ult_col)} AS "TRM_Ultima_AUD"
                    FROM auditoria_raw
                    WHERE {aud_ref_q} IS NOT NULL
                    GROUP BY 1
                )
            """
            aud_join = """
                LEFT JOIN aud
                  ON upper(trim(CAST(p.Referencia_Normalizada AS VARCHAR))) = aud.ref_norm_key
            """
            aud_costo_expr = 'try_cast(aud."Costo_Min_AUD" AS DOUBLE)'
            aud_costo_max_expr = 'try_cast(aud."Costo_Max_AUD" AS DOUBLE)'
            aud_bod_cmin_expr = 'NULLIF(trim(CAST(aud."Bodega_CostoMin_AUD" AS VARCHAR)), \'\')'
            aud_bod_cmax_expr = 'NULLIF(trim(CAST(aud."Bodega_CostoMax_AUD" AS VARCHAR)), \'\')'
            aud_exist_expr = 'try_cast(aud."Existencia_Total_AUD" AS DOUBLE)'
            aud_tipo_expr = 'NULLIF(trim(CAST(aud."Tipo_Origen_AUD" AS VARCHAR)), \'\')'
            aud_pl_expr = 'try_cast(aud."Precio_Lista_09_AUD" AS DOUBLE)'
            aud_fecha_ult_expr = 'aud."Fecha_Ultima_Compra_AUD"'
            aud_pais_ult_expr = 'aud."Pais_Ultima_AUD"'
            aud_prov_ult_expr = 'aud."Proveedor_Ultima_AUD"'
            aud_comp_ult_expr = 'aud."Comprador_Ultima_AUD"'
            aud_usd_ult_expr = 'aud."Precio_USD_Ultima_AUD"'
            aud_cop_ult_expr = 'aud."Precio_COP_Ultima_AUD"'
            aud_trm_ult_expr = 'aud."TRM_Ultima_AUD"'
        else:
            aud_fecha_ult_expr = "CAST(NULL AS TIMESTAMP)"
            aud_pais_ult_expr = "CAST(NULL AS VARCHAR)"
            aud_prov_ult_expr = "CAST(NULL AS VARCHAR)"
            aud_comp_ult_expr = "CAST(NULL AS VARCHAR)"
            aud_usd_ult_expr = "CAST(NULL AS DOUBLE)"
            aud_cop_ult_expr = "CAST(NULL AS DOUBLE)"
            aud_trm_ult_expr = "CAST(NULL AS DOUBLE)"

        join_o3 = ""
        o3_tipo_expr = "NULL"
        if has_o3_ref and has_o3_tipo:
            join_o3 = """
                LEFT JOIN origen_precios_tablero o3
                  ON upper(trim(CAST(o3."REF" AS VARCHAR))) = upper(trim(CAST(p.Referencia_Normalizada AS VARCHAR)))
            """
            o3_tipo_expr = 'NULLIF(trim(CAST(o3."Tipo_Origen" AS VARCHAR)), \'\')'

        con.register("tmp_refs_input", df_input)
        try:
            join_alt = ""
            rank_alt = ""
            tipo_alt = ""
            if has_refs_alt:
                join_alt = (
                    "OR (',' || regexp_replace(upper(COALESCE(CAST(r.RefsAlternas AS VARCHAR), '')), "
                    "'[\\s;()]+', ',', 'g') || ',') "
                    "LIKE ('%,' || upper(trim(i.referencia_entrada)) || ',%')"
                )
                rank_alt = (
                    "WHEN (',' || regexp_replace(upper(COALESCE(CAST(r.RefsAlternas AS VARCHAR), '')), "
                    "'[\\s;()]+', ',', 'g') || ',') "
                    "LIKE ('%,' || upper(trim(i.referencia_entrada)) || ',%') THEN 3"
                )
                tipo_alt = "WHEN 3 THEN 'Alterna'"

            sql = f"""
                WITH inp AS (
                    SELECT
                        orden,
                        trim(CAST(referencia_entrada AS VARCHAR)) AS referencia_entrada
                    FROM tmp_refs_input
                    WHERE trim(CAST(referencia_entrada AS VARCHAR)) <> ''
                ),
                cand AS (
                    SELECT
                        i.orden,
                        i.referencia_entrada,
                        r.*,
                        CASE
                            WHEN upper(trim(CAST(r.Referencia_Original AS VARCHAR))) = upper(trim(i.referencia_entrada)) THEN 1
                            WHEN upper(trim(CAST(r.Referencia_Normalizada AS VARCHAR))) = upper(trim(i.referencia_entrada)) THEN 2
                            {rank_alt}
                            ELSE 99
                        END AS match_rank
                    FROM inp i
                    JOIN resultado_precios_lista r
                      ON upper(trim(CAST(r.Referencia_Original AS VARCHAR))) = upper(trim(i.referencia_entrada))
                      OR upper(trim(CAST(r.Referencia_Normalizada AS VARCHAR))) = upper(trim(i.referencia_entrada))
                      {join_alt}
                ),
                pick AS (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (
                            PARTITION BY referencia_entrada
                            ORDER BY
                                match_rank,
                                (
                                    COALESCE(try_cast(disp_br AS DOUBLE), 0)
                                    + COALESCE(try_cast(disp_usa AS DOUBLE), 0)
                                    + COALESCE(try_cast(disp_eur AS DOUBLE), 0)
                                ) DESC,
                                Referencia_Original
                        ) AS rn
                    FROM cand
                )
                {aud_cte}
                SELECT
                    i.orden,
                    i.referencia_entrada AS Referencia_Entrada,
                    CASE
                        WHEN p.match_rank IS NULL THEN NULL
                        WHEN p.match_rank = 1 THEN COALESCE(
                            NULLIF(trim(CAST(p.Referencia_Original AS VARCHAR)), ''),
                            NULLIF(trim(CAST(p.Referencia_Normalizada AS VARCHAR)), '')
                        )
                        WHEN p.match_rank IN (2, 3) THEN COALESCE(
                            NULLIF(trim(CAST(p.Referencia_Normalizada AS VARCHAR)), ''),
                            NULLIF(trim(CAST(p.Referencia_Original AS VARCHAR)), '')
                        )
                        ELSE NULL
                    END AS Referencia_Cruce,
                    CASE WHEN p.referencia_entrada IS NULL THEN 'Sin coincidencia' ELSE 'OK' END AS Estado,
                    CASE p.match_rank
                        WHEN 1 THEN 'Principal'
                        WHEN 2 THEN 'Normalizada'
                        {tipo_alt}
                        ELSE NULL
                    END AS Tipo_Coincidencia,
                    p.Referencia_Original,
                    p.Referencia_Normalizada,
                    {sel_desc},
                    p.RefsAlternas,
                    p."Precio Prorrateo" AS "Precio Prorrateo",
                    p."Precio Brasil" AS "Precio Brasil",
                    p."Precio Usa" AS "Precio Usa",
                    p."Precio Europa" AS "Precio Europa",
                    p.disp_br,
                    p.disp_usa,
                    p.disp_eur,
                    p."DNET BRA USD" AS "DNET BRA USD",
                    p."DNET USA USD" AS "DNET USA USD",
                    p."DNET EUR EURO" AS "DNET EUR EURO",
                    p."Ult. Fecha Compra" AS "Ult. Fecha Compra",
                    p.Proveedor,
                    {sel_ult_usd},
                    {sel_vlr_liq},
                    COALESCE({p_costo_min_expr}, {aud_costo_expr}) AS "Costo_Min",
                    COALESCE({p_costo_max_expr}, {aud_costo_max_expr}) AS "Costo_Max",
                    COALESCE({p_existencia_total_expr}, {aud_exist_expr}) AS "Existencia_Total",
                    COALESCE({p_tipo_origen_expr}, {aud_tipo_expr}, {o3_tipo_expr}) AS "Tipo_Origen",
                    COALESCE({p_precio_lista_09_expr}, {aud_pl_expr}) AS "Precio_Lista_09",
                    {sel_ult_precio_venta},
                    {sel_fecha_ult_venta},
                    {aud_fecha_ult_expr} AS "Fecha_Ultima_Compra",
                    {aud_pais_ult_expr} AS "Pais_Ultima",
                    {aud_prov_ult_expr} AS "Proveedor_Ultima",
                    {aud_comp_ult_expr} AS "Comprador_Ultima",
                    {aud_usd_ult_expr} AS "Precio_USD_Ultima",
                    {aud_cop_ult_expr} AS "Precio_COP_Ultima",
                    {aud_trm_ult_expr} AS "TRM_Ultima"
                FROM inp i
                LEFT JOIN pick p
                  ON p.referencia_entrada = i.referencia_entrada
                 AND p.rn = 1
                {aud_join}
                {join_o3}
                ORDER BY i.orden
            """
            out = con.execute(sql).df()
            out = _align_df_columns_to_expected(out, base_cols)
            out = _refill_referencia_cruce(out)
        finally:
            try:
                con.unregister("tmp_refs_input")
            except Exception:
                pass

    for c in ("disp_br", "disp_usa", "disp_eur"):
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")
    out["_disp_total"] = (
        out.get("disp_br", 0).fillna(0)
        + out.get("disp_usa", 0).fillna(0)
        + out.get("disp_eur", 0).fillna(0)
    )
    out["_disponible"] = out["_disp_total"].apply(lambda v: "SI" if pd.notna(v) and float(v) > 0 else "NO")

    if "orden" in out.columns:
        out = out.drop(columns=["orden"])
    for c in base_cols:
        if c not in out.columns:
            out[c] = None
    return out[base_cols]


def obtener_ultimas_ventas(ref_norm: str, limite: int = 20) -> pd.DataFrame:
    sql = """
        SELECT *
        FROM ventas_raw
        WHERE Ref_Normalizada = ?
        ORDER BY "Fecha Factura" DESC
        LIMIT ?
    """
    with _connect() as con:
        if not _table_exists(con, "ventas_raw"):
            return pd.DataFrame()
        df = con.execute(sql, [ref_norm, limite]).df()

    if df.empty:
        return df

    columnas_preferidas = [
        "Fecha Factura",
        "Cliente",
        "Vendedor",
        "Cant.",
        "Precio Unit. Venta",
        "Valor Venta",
        "Margen",
        "Bodega",
        "Descripcion Lista",
    ]
    columnas = [c for c in columnas_preferidas if c in df.columns]
    return df[columnas] if columnas else df


_MARGEN_COL_CANDIDATES = ("Margen09", "Margen_Pct", "Margen04")


def _pick_margen_column(cols: list[str]) -> str:
    """Elige la columna de margen presente en margen_siesa_raw (mismo orden que la app)."""
    lower_map = {str(c).lower(): c for c in cols}
    for cand in _MARGEN_COL_CANDIDATES:
        if cand.lower() in lower_map:
            return lower_map[cand.lower()]
    raise ValueError(
        "En `margen_siesa_raw` no hay ninguna de las columnas "
        f"{', '.join(_MARGEN_COL_CANDIDATES)}."
    )


def _duck_quote_ident(name: str) -> str:
    return '"' + str(name).replace('"', "") + '"'


def _margen_sql_expr(table_alias: str, quoted_col: str) -> str:
    """
    Expresión numérica alineada al valor guardado en `margen_siesa_raw` (origen SSMS).
    Acepta VARCHAR con coma decimal o espacios; si no parsea, queda NULL (igual que
    filas sin margen válido en el reporte).
    """
    col = f"{table_alias}.{quoted_col}"
    return (
        f"try_cast(replace(trim(cast({col} AS VARCHAR)), ',', '.') AS DOUBLE)"
    )


@dataclass(frozen=True)
class MargenJoinParts:
    """Fragmentos SQL compartidos entre reporte de margen y auditoría de cruces."""

    norm_m_ref: str
    norm_a_ref: str
    norm_r_orig: str
    norm_r_norm: str
    norm_o_ref: str
    alt_sql: str
    coalesce_join_key: str
    rmap_alt_block: str
    join_attr_sql: str
    join_o3_sql: str
    has_attr: bool
    origen_full: bool
    needs_r_bridge: bool
    has_alt: bool


def _build_margen_join_fragments(con: duckdb.DuckDBPyConnection) -> MargenJoinParts:
    """Misma lógica que `obtener_dataset_margenes` (RPL + alternas + atributos + origen)."""
    has_origen_tablero = _table_exists(con, "origen_precios_tablero")
    has_origen_ref = _table_has_column(con, "origen_precios_tablero", "REF") if has_origen_tablero else False
    has_origen_tipo = _table_has_column(con, "origen_precios_tablero", "Tipo_Origen") if has_origen_tablero else False
    has_rpl = _table_exists(con, "resultado_precios_lista")
    has_rpl_orig = _table_has_column(con, "resultado_precios_lista", "Referencia_Original") if has_rpl else False
    has_rpl_norm = _table_has_column(con, "resultado_precios_lista", "Referencia_Normalizada") if has_rpl else False
    has_alt = (
        _table_exists(con, "referencias_alternas")
        and _table_has_column(con, "referencias_alternas", "Ref_Alt_Norm")
        and _table_has_column(con, "referencias_alternas", "Ref_Norm")
    )
    has_attr = _table_exists(con, "atributos_referencias_raw")

    norm_m_ref = _norm_sql_expr("m.Referencia")
    norm_a_ref = _norm_sql_expr("a.Referencia")
    norm_r_orig = _norm_sql_expr("Referencia_Original")
    norm_r_norm = _norm_sql_expr("Referencia_Normalizada")
    norm_o_ref = _norm_sql_expr("o3.REF")
    norm_ra_alt = _norm_sql_expr("ra.Ref_Alt_Norm")
    norm_ra_principal = _norm_sql_expr("ra.Ref_Norm")

    origen_full = (
        has_origen_tablero
        and has_origen_ref
        and has_origen_tipo
        and has_rpl
        and has_rpl_orig
        and has_rpl_norm
    )
    needs_r_bridge = bool(
        has_rpl
        and has_rpl_orig
        and has_rpl_norm
        and (has_attr or (has_origen_tablero and has_origen_ref and has_origen_tipo))
    )
    alt_sql = ""
    coalesce_join_key = norm_m_ref
    rmap_alt_block = ""
    if needs_r_bridge:
        coalesce_join_key = f"COALESCE(rmap.ref_norm, {norm_m_ref})"
        if has_alt:
            alt_sql = f"""
            LEFT JOIN (
                SELECT DISTINCT
                    {norm_ra_alt} AS ref_alt_k,
                    {norm_ra_principal} AS ref_principal_k
                FROM referencias_alternas AS ra
                WHERE ra.Ref_Alt_Norm IS NOT NULL
                  AND ra.Ref_Norm IS NOT NULL
            ) alt
                ON {norm_m_ref} = alt.ref_alt_k
"""
            coalesce_join_key = f"COALESCE(rmap.ref_norm, alt.ref_principal_k, {norm_m_ref})"
        rmap_alt_block = f"""
            LEFT JOIN (
                SELECT ref_orig, ANY_VALUE(ref_norm) AS ref_norm
                FROM (
                    SELECT DISTINCT
                        {norm_r_orig} AS ref_orig,
                        {norm_r_norm} AS ref_norm
                    FROM resultado_precios_lista
                    WHERE Referencia_Original IS NOT NULL
                      AND Referencia_Normalizada IS NOT NULL
                ) s
                GROUP BY ref_orig
            ) rmap
                ON {norm_m_ref} = rmap.ref_orig
            {alt_sql}
"""
    join_attr_sql = ""
    if has_attr:
        if needs_r_bridge:
            join_attr_sql = f"""
            LEFT JOIN atributos_referencias_raw a
                ON {coalesce_join_key} = {norm_a_ref}
"""
        else:
            join_attr_sql = f"""
            LEFT JOIN atributos_referencias_raw a
                ON {norm_m_ref} = {norm_a_ref}
"""
    join_o3_sql = ""
    if origen_full:
        join_o3_sql = f"""
            LEFT JOIN origen_precios_tablero o3
                ON {coalesce_join_key} = {norm_o_ref}
"""
    return MargenJoinParts(
        norm_m_ref=norm_m_ref,
        norm_a_ref=norm_a_ref,
        norm_r_orig=norm_r_orig,
        norm_r_norm=norm_r_norm,
        norm_o_ref=norm_o_ref,
        alt_sql=alt_sql,
        coalesce_join_key=coalesce_join_key,
        rmap_alt_block=rmap_alt_block,
        join_attr_sql=join_attr_sql,
        join_o3_sql=join_o3_sql,
        has_attr=has_attr,
        origen_full=origen_full,
        needs_r_bridge=needs_r_bridge,
        has_alt=has_alt,
    )


def obtener_dataset_margenes(
    limite: int | None = 10_000,
    margen_min: float = -10_000.0,
    margen_max: float = 100.0,
) -> pd.DataFrame:
    """
    Filas alineadas al reporte `00_precio_margen_SIESA.sql` (tabla `margen_siesa_raw`).

    El filtro por rango de margen se aplica en SQL sobre **toda** la tabla, no sobre
    un LIMIT arbitrario (evita que los márgenes negativos “desaparezcan”).

    Nota: Margen09 puede ser < -100% cuando Costo_Prom_Inst >> Precio_Lista_09; el
    rango del slider en la app debe incluir esos valores para coincidir con SSMS.

    - limite: máximo de filas después del filtro; 0 o None = sin límite.
    - Orden: Referencia, Bodega (como el ORDER BY del .sql).
    """
    with _connect() as con:
        if not _table_exists(con, "margen_siesa_raw"):
            raise ValueError("No existe `margen_siesa_raw`. Ejecuta 'Actualizar datos'.")
        # region agent log
        _debug_log(
            run_id="run1",
            hypothesis_id="H1",
            location="data_access.py:obtener_dataset_margenes:start",
            message="Inicio carga dataset margenes",
            data={"limite": limite, "margen_min": margen_min, "margen_max": margen_max},
        )
        # endregion

        meta = con.execute("SELECT * FROM margen_siesa_raw LIMIT 0").df()
        margen_src = _pick_margen_column(list(meta.columns))
        mq = _duck_quote_ident(margen_src)
        margen_expr = _margen_sql_expr("m", mq)
        has_margen09 = _table_has_column(con, "margen_siesa_raw", "Margen09")
        has_margen04 = _table_has_column(con, "margen_siesa_raw", "Margen04")
        has_margen_pct = _table_has_column(con, "margen_siesa_raw", "Margen_Pct")
        has_tipo_origen = _table_has_column(con, "margen_siesa_raw", "Tipo_Origen")
        has_tipo_origen_sp = _table_has_column(con, "margen_siesa_raw", "Tipo Origen")
        has_dias_max = _table_has_column(con, "margen_siesa_raw", "Dias_Desde_Fecha_Max")
        has_dias_max_sp = _table_has_column(con, "margen_siesa_raw", "Dias Desde Fecha Max")
        has_origen_tablero = _table_exists(con, "origen_precios_tablero")
        has_origen_ref = _table_has_column(con, "origen_precios_tablero", "REF") if has_origen_tablero else False
        has_origen_tipo = _table_has_column(con, "origen_precios_tablero", "Tipo_Origen") if has_origen_tablero else False
        has_rpl = _table_exists(con, "resultado_precios_lista")
        has_rpl_orig = _table_has_column(con, "resultado_precios_lista", "Referencia_Original") if has_rpl else False
        has_rpl_norm = _table_has_column(con, "resultado_precios_lista", "Referencia_Normalizada") if has_rpl else False
        has_alt = (
            _table_exists(con, "referencias_alternas")
            and _table_has_column(con, "referencias_alternas", "Ref_Alt_Norm")
            and _table_has_column(con, "referencias_alternas", "Ref_Norm")
        )
        has_attr = _table_exists(con, "atributos_referencias_raw")
        # region agent log
        total_margen_rows = int(con.execute("SELECT COUNT(*) FROM margen_siesa_raw").fetchone()[0])
        _debug_log(
            run_id="run1",
            hypothesis_id="H2",
            location="data_access.py:obtener_dataset_margenes:meta",
            message="Estado tablas y columnas para cruce",
            data={
                "total_margen_rows": total_margen_rows,
                "has_origen_tablero": has_origen_tablero,
                "has_origen_ref": has_origen_ref,
                "has_origen_tipo": has_origen_tipo,
                "has_rpl_map": bool(has_rpl and has_rpl_orig and has_rpl_norm),
                "has_referencias_alternas": has_alt,
                "has_attr": has_attr,
            },
        )
        # endregion

        parts = _build_margen_join_fragments(con)
        norm_m_ref = parts.norm_m_ref
        norm_a_ref = parts.norm_a_ref
        norm_r_orig = parts.norm_r_orig
        norm_r_norm = parts.norm_r_norm
        norm_o_ref = parts.norm_o_ref
        alt_sql = parts.alt_sql
        coalesce_join_key = parts.coalesce_join_key
        rmap_alt_block = parts.rmap_alt_block
        join_attr_sql = parts.join_attr_sql
        join_o3_sql = parts.join_o3_sql
        has_attr = parts.has_attr
        origen_full = parts.origen_full
        has_alt = parts.has_alt

        attr_select = ""
        if has_attr:
            # Cruce obligatorio: `LEFT JOIN atributos_referencias_raw a` (ver `join_attr_sql`).
            # `Margen_Objetivo_Sistema` = margen objetivo por sistema precio (misma consulta 00 que el resto de atributos).
            attr_select = """
                a.Sistema_Precio_Item AS Sistema_Precio,
                a.Equipo_CNH,
                a.Modelo_CNH,
                a.Margen_Objetivo_Sistema
            """
        if origen_full:
            # region agent log
            total_for_join = int(con.execute("SELECT COUNT(*) FROM margen_siesa_raw").fetchone()[0])
            with_tipo = int(
                con.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM margen_siesa_raw m
                    LEFT JOIN (
                        SELECT ref_orig, ANY_VALUE(ref_norm) AS ref_norm
                        FROM (
                            SELECT DISTINCT
                                {norm_r_orig} AS ref_orig,
                                {norm_r_norm} AS ref_norm
                            FROM resultado_precios_lista
                            WHERE Referencia_Original IS NOT NULL
                              AND Referencia_Normalizada IS NOT NULL
                        ) s
                        GROUP BY ref_orig
                    ) rmap ON {norm_m_ref} = rmap.ref_orig
                    {alt_sql}
                    LEFT JOIN origen_precios_tablero o3
                        ON {coalesce_join_key} = {norm_o_ref}
                    WHERE o3.Tipo_Origen IS NOT NULL
                      AND TRIM(CAST(o3.Tipo_Origen AS VARCHAR)) <> ''
                    """
                ).fetchone()[0]
            )
            _debug_log(
                run_id="run1",
                hypothesis_id="H3",
                location="data_access.py:obtener_dataset_margenes:join_diag",
                message="Cobertura Tipo_Origen (rmap + alternas + directo)",
                data={
                    "filas_margen": total_for_join,
                    "filas_con_tipo_origen": with_tipo,
                    "pct": round(100.0 * with_tipo / total_for_join, 4) if total_for_join else 0.0,
                    "usa_alternas": has_alt,
                },
            )
            # endregion

        margen09_sql = (
            f"({_margen_sql_expr('m', _duck_quote_ident('Margen09'))}) AS \"Margen09\","
            if has_margen09
            else "CAST(NULL AS DOUBLE) AS \"Margen09\","
        )
        margen04_sql = (
            f"({_margen_sql_expr('m', _duck_quote_ident('Margen04'))}) AS \"Margen04\","
            if has_margen04
            else "CAST(NULL AS DOUBLE) AS \"Margen04\","
        )
        margenpct_sql = (
            f"({_margen_sql_expr('m', _duck_quote_ident('Margen_Pct'))}) AS \"Margen_Pct\","
            if has_margen_pct
            else "CAST(NULL AS DOUBLE) AS \"Margen_Pct\","
        )
        if has_origen_tablero and has_origen_ref and has_origen_tipo and has_rpl and has_rpl_orig and has_rpl_norm:
            tipo_origen_sql = 'COALESCE(CAST(o3."Tipo_Origen" AS VARCHAR), \'\') AS "Tipo_Origen",'
        else:
            tipo_origen_sql = (
                f"COALESCE(CAST(m.{_duck_quote_ident('Tipo_Origen')} AS VARCHAR), '') AS \"Tipo_Origen\","
                if has_tipo_origen
                else (
                    f"COALESCE(CAST(m.{_duck_quote_ident('Tipo Origen')} AS VARCHAR), '') AS \"Tipo_Origen\","
                    if has_tipo_origen_sp
                    else "CAST(NULL AS VARCHAR) AS \"Tipo_Origen\","
                )
            )
        dias_max_sql = (
            f"CAST(m.{_duck_quote_ident('Dias_Desde_Fecha_Max')} AS BIGINT) AS \"Dias_Desde_Fecha_Max\","
            if has_dias_max
            else (
                f"CAST(m.{_duck_quote_ident('Dias Desde Fecha Max')} AS BIGINT) AS \"Dias_Desde_Fecha_Max\","
                if has_dias_max_sp
                else "CAST(NULL AS BIGINT) AS \"Dias_Desde_Fecha_Max\","
            )
        )

        sql = f"""
            SELECT
                m.Descripcion,
                {norm_m_ref} AS Referencia_Original,
                m.Referencia_Alternas,
                CAST(m.Existencia AS BIGINT) AS Existencia,
                CAST(m.Disponible AS BIGINT) AS Disponible,
                CAST(m.Precio_Lista_09 AS DOUBLE) AS Precio_Lista_09,
                CAST(m.Precio_Lista_04 AS DOUBLE) AS Precio_Lista_04,
                CAST(m.Costo_Prom_Inst AS DOUBLE) AS Costo_Prom_Inst,
                {margen09_sql}
                {margen04_sql}
                {margenpct_sql}
                {tipo_origen_sql}
                {dias_max_sql}
                (CAST(m.Costo_Prom_Inst AS DOUBLE) * CAST(m.Existencia AS DOUBLE)) AS Valor_Inventario,
                ({margen_expr}) AS {_duck_quote_ident(margen_src)},
                m.Bodega,
                COALESCE(m.Nom_Bodega, '') AS Nom_Bodega,
                COALESCE(m.Nom_Instalacion, '') AS Nom_Instalacion,
                m.Rotacion
                {"," if has_attr else ""}
                {attr_select if has_attr else ""}
            FROM margen_siesa_raw m
            {rmap_alt_block}
            {join_attr_sql}
            {join_o3_sql}
            WHERE ({margen_expr}) BETWEEN ? AND ?
            ORDER BY m.Referencia, m.Bodega
        """

        params: list[Any] = [float(margen_min), float(margen_max)]

        if limite is not None and int(limite) > 0:
            sql += " LIMIT ?"
            params.append(int(limite))

        out_df = con.execute(sql, params).df()
        # region agent log
        tipo_non_empty = int(out_df["Tipo_Origen"].fillna("").astype(str).str.strip().ne("").sum()) if "Tipo_Origen" in out_df.columns else 0
        _debug_log(
            run_id="run1",
            hypothesis_id="H4",
            location="data_access.py:obtener_dataset_margenes:result",
            message="Resultado final de dataset margenes",
            data={"rows_out": int(len(out_df)), "tipo_origen_no_vacio": tipo_non_empty},
        )
        # endregion
        return out_df


def _fmt_mtime(path: Path) -> str:
    if not path.exists():
        return ""
    try:
        return datetime.fromtimestamp(path.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return ""


def obtener_auditoria_dashboard() -> dict[str, Any]:
    """
    Resumen para la pestaña Auditoría: archivos DuckDB, filas por tabla y
    cobertura de cruces (misma lógica de joins que el reporte de margen).
    """
    out: dict[str, Any] = {
        "archivos": pd.DataFrame(),
        "tablas": pd.DataFrame(),
        "cruces": pd.DataFrame(),
        "flags": {},
        "error": None,
    }
    rows_archivo: list[dict[str, Any]] = []
    for label, path in (
        ("Maestro (escritura)", MASTER_DB_PATH),
        ("Copia lectura (app)", READ_DB_PATH),
    ):
        if path.exists():
            st = path.stat()
            rows_archivo.append(
                {
                    "Rol": label,
                    "Archivo": path.name,
                    "Existe": "Sí",
                    "Tam_MB": round(st.st_size / (1024 * 1024), 2),
                    "Modificado": _fmt_mtime(path),
                }
            )
        else:
            rows_archivo.append(
                {
                    "Rol": label,
                    "Archivo": path.name,
                    "Existe": "No",
                    "Tam_MB": None,
                    "Modificado": "",
                }
            )
    out["archivos"] = pd.DataFrame(rows_archivo)

    try:
        with _connect() as con:
            if not _table_exists(con, "margen_siesa_raw"):
                out["error"] = "No existe `margen_siesa_raw`. Ejecuta 'Actualizar datos'."
                return out

            parts = _build_margen_join_fragments(con)
            out["flags"] = {
                "origen_tablero_completo": parts.origen_full,
                "puente_rpl_alternas": parts.needs_r_bridge,
                "tabla_atributos": parts.has_attr,
                "referencias_alternas": parts.has_alt,
                "auditoria_referencias": _table_exists(con, "auditoria_raw"),
            }

            tablas_audit = [
                "margen_siesa_raw",
                "atributos_referencias_raw",
                "resultado_precios_lista",
                "origen_precios_tablero",
                "referencias_alternas",
                "ventas_raw",
                "auditoria_raw",
            ]
            rows_tab: list[dict[str, Any]] = []
            for t in tablas_audit:
                if _table_exists(con, t):
                    n = int(con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0])
                    rows_tab.append({"Tabla": t, "Filas": n, "Estado": "OK"})
                else:
                    rows_tab.append({"Tabla": t, "Filas": None, "Estado": "No cargada"})
            out["tablas"] = pd.DataFrame(rows_tab)

            selects: list[str] = ["COUNT(*) AS filas_margen"]
            if parts.needs_r_bridge:
                selects.append(
                    "SUM(CASE WHEN rmap.ref_norm IS NOT NULL THEN 1 ELSE 0 END) AS filas_en_mapa_rpl"
                )
            if parts.needs_r_bridge and parts.has_alt:
                selects.append(
                    "SUM(CASE WHEN alt.ref_principal_k IS NOT NULL THEN 1 ELSE 0 END) AS filas_match_alternas"
                )
            if parts.origen_full:
                selects.append(
                    "SUM(CASE WHEN o3.\"Tipo_Origen\" IS NOT NULL "
                    "AND TRIM(CAST(o3.\"Tipo_Origen\" AS VARCHAR)) <> '' "
                    "THEN 1 ELSE 0 END) AS filas_con_tipo_origen_o3"
                )
            if parts.has_attr:
                selects.append(
                    "SUM(CASE WHEN a.Sistema_Precio_Item IS NOT NULL "
                    "AND TRIM(CAST(a.Sistema_Precio_Item AS VARCHAR)) <> '' "
                    "THEN 1 ELSE 0 END) AS filas_con_sistema_precio"
                )

            sql_cruce = f"""
                SELECT {", ".join(selects)}
                FROM margen_siesa_raw m
                {parts.rmap_alt_block}
                {parts.join_attr_sql}
                {parts.join_o3_sql}
            """
            cruces_df = con.execute(sql_cruce).df()
            if not cruces_df.empty:
                row = cruces_df.iloc[0].to_dict()

                def _ival(key: str) -> int:
                    v = row.get(key)
                    try:
                        if v is None or pd.isna(v):
                            return 0
                        return int(v)
                    except Exception:
                        return 0

                total = float(_ival("filas_margen"))
                orden_metricas: list[tuple[str, str]] = [
                    ("filas_margen", "Filas inventario margen (total)"),
                    ("filas_en_mapa_rpl", "Match en mapa RPL (Referencia_Original → normalizada)"),
                    ("filas_match_alternas", "Match vía referencias alternas"),
                    ("filas_con_tipo_origen_o3", "Con Tipo_Origen (origen_precios_tablero vía puente)"),
                    ("filas_con_sistema_precio", "Con Sistema precio (atributos vía mismo puente que margen)"),
                ]
                rows_metric: list[dict[str, Any]] = []
                for key, label in orden_metricas:
                    if key not in row:
                        continue
                    val = _ival(key)
                    if key == "filas_margen":
                        rows_metric.append(
                            {
                                "Metrica": label,
                                "Valor": val,
                                "Pct_sobre_margen": 100.0,
                            }
                        )
                    else:
                        pct = round(100.0 * val / total, 2) if total > 0 else 0.0
                        rows_metric.append(
                            {"Metrica": label, "Valor": val, "Pct_sobre_margen": pct}
                        )
                out["cruces"] = pd.DataFrame(rows_metric)
            else:
                out["cruces"] = pd.DataFrame()

    except Exception as exc:
        out["error"] = str(exc)

    return out


def obtener_resumen_ventas_x_bodega() -> pd.DataFrame:
    """
    Equivalente al resumen DAX por Bodega/Referencia/Descripción/Sistema Precio.
    Si no existe LLAVE en ventas_raw, se construye como Bodega|Referencia.
    """
    with _connect() as con:
        if not _table_exists(con, "ventas_raw"):
            return pd.DataFrame()

        has_ano = _table_has_column(con, "ventas_raw", "Ano Comparativo")
        has_anio = _table_has_column(con, "ventas_raw", "Año Comparativo")
        if not (has_ano or has_anio):
            return pd.DataFrame()

        ano_col = '"Ano Comparativo"' if has_ano else '"Año Comparativo"'
        has_llave = _table_has_column(con, "ventas_raw", "LLAVE")
        llave_expr = 'CAST("LLAVE" AS VARCHAR)' if has_llave else 'COALESCE(CAST(Bodega AS VARCHAR), \'\') || \'|\' || COALESCE(CAST(Referencia AS VARCHAR), \'\')'

        sql = f"""
            SELECT
                Bodega,
                Referencia,
                COALESCE(Descripcion, '') AS Descripcion,
                COALESCE("Sistema Precio", '') AS "Sistema Precio",
                ({llave_expr}) AS LLAVE,
                SUM(CASE WHEN {ano_col} = 'Ult. 12 Meses' THEN COALESCE("Cant.", 0) ELSE 0 END) AS "Cant 12M",
                SUM(CASE WHEN {ano_col} = '13 a 24 meses' THEN COALESCE("Cant.", 0) ELSE 0 END) AS "Cant 13_24",
                SUM(CASE WHEN {ano_col} = 'Ult. 12 Meses' THEN COALESCE("Valor Venta", 0) ELSE 0 END) AS "Valor 12M",
                SUM(CASE WHEN {ano_col} = '13 a 24 meses' THEN COALESCE("Valor Venta", 0) ELSE 0 END) AS "Valor 13_24",
                CASE
                    WHEN SUM(CASE WHEN {ano_col} = 'Ult. 12 Meses' THEN COALESCE("Cant.", 0) ELSE 0 END) = 0 THEN NULL
                    ELSE
                        SUM(CASE WHEN {ano_col} = 'Ult. 12 Meses' THEN COALESCE("Valor Venta", 0) ELSE 0 END)
                        / NULLIF(SUM(CASE WHEN {ano_col} = 'Ult. 12 Meses' THEN COALESCE("Cant.", 0) ELSE 0 END), 0)
                END AS "Precio Prom 12M",
                CASE
                    WHEN SUM(CASE WHEN {ano_col} = '13 a 24 meses' THEN COALESCE("Cant.", 0) ELSE 0 END) = 0 THEN NULL
                    ELSE
                        SUM(CASE WHEN {ano_col} = '13 a 24 meses' THEN COALESCE("Valor Venta", 0) ELSE 0 END)
                        / NULLIF(SUM(CASE WHEN {ano_col} = '13 a 24 meses' THEN COALESCE("Cant.", 0) ELSE 0 END), 0)
                END AS "Precio Prom 13_24"
            FROM ventas_raw
            GROUP BY
                Bodega, Referencia, Descripcion, "Sistema Precio", ({llave_expr})
        """
        return con.execute(sql).df()


def obtener_auditoria_referencias(
    limite: int | None = 20_000,
    semaforo: str | None = None,
    texto_busqueda: str = "",
) -> pd.DataFrame:
    """
    Dataset de auditoría (script 3 de `00_Reportes_SQL.py` => tabla `auditoria_raw`).
    Permite filtrar por semáforo y texto para la pestaña de auditoría de referencias.
    """
    with _connect() as con:
        if not _table_exists(con, "auditoria_raw"):
            return pd.DataFrame()

        meta = con.execute("SELECT * FROM auditoria_raw LIMIT 0").df()
        cols = list(meta.columns)
        lower_map = {str(c).lower(): str(c) for c in cols}

        sem_col = lower_map.get("semaforo_variacion")
        ref_col = lower_map.get("referencia")
        desc_col = lower_map.get("descripcion") or lower_map.get("descripción")

        where_parts: list[str] = []
        params: list[Any] = []

        sem_val = (semaforo or "").strip()
        if sem_val and sem_col:
            where_parts.append(f"TRIM(CAST({_duck_quote_ident(sem_col)} AS VARCHAR)) = ?")
            params.append(sem_val)

        q = (texto_busqueda or "").strip()
        if q:
            search_cols: list[str] = []
            for cand in (
                ref_col,
                desc_col,
                sem_col,
                lower_map.get("sistema_precio_item"),
                lower_map.get("sistema_precio"),
                lower_map.get("modelo_cnh"),
            ):
                if cand and cand not in search_cols:
                    search_cols.append(cand)
            if search_cols:
                like_parts: list[str] = []
                for c in search_cols:
                    like_parts.append(f"UPPER(CAST({_duck_quote_ident(c)} AS VARCHAR)) LIKE UPPER(?)")
                    params.append(f"%{q}%")
                where_parts.append("(" + " OR ".join(like_parts) + ")")

        sql = "SELECT * FROM auditoria_raw"
        if where_parts:
            sql += " WHERE " + " AND ".join(where_parts)
        if sem_col:
            sql += f" ORDER BY {_duck_quote_ident(sem_col)}, {_duck_quote_ident(ref_col) if ref_col else _duck_quote_ident(sem_col)}"
        elif ref_col:
            sql += f" ORDER BY {_duck_quote_ident(ref_col)}"
        if limite is not None and int(limite) > 0:
            sql += " LIMIT ?"
            params.append(int(limite))

        return con.execute(sql, params).df()


def ejecutar_sql_laboratorio(sql_text: str, limite: int = 5_000) -> pd.DataFrame:
    """
    Ejecuta consultas SQL ad-hoc en DuckDB para auditoría.
    Solo permite sentencias de lectura (SELECT/WITH/SHOW/DESCRIBE/EXPLAIN).
    """
    sql = (sql_text or "").strip()
    if not sql:
        return pd.DataFrame()

    sql_upper = sql.upper()
    head = sql_upper.lstrip()
    allowed = ("SELECT", "WITH", "SHOW", "DESCRIBE", "EXPLAIN")
    if not head.startswith(allowed):
        raise ValueError("Solo se permiten consultas de lectura: SELECT, WITH, SHOW, DESCRIBE o EXPLAIN.")

    blocked_tokens = [
        "INSERT ",
        "UPDATE ",
        "DELETE ",
        "MERGE ",
        "DROP ",
        "ALTER ",
        "CREATE ",
        "REPLACE ",
        "TRUNCATE ",
        "ATTACH ",
        "DETACH ",
        "COPY ",
        "CALL ",
    ]
    for token in blocked_tokens:
        if token in sql_upper:
            raise ValueError(f"La consulta contiene una operación no permitida ({token.strip()}).")

    if ";" in sql:
        raise ValueError("Usa una sola sentencia SQL (sin ';').")

    with _connect() as con:
        df = con.execute(sql).df()

    if limite is not None and int(limite) > 0 and len(df) > int(limite):
        return df.head(int(limite))
    return df
