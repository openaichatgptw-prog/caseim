from __future__ import annotations

import configparser
import importlib.util
import os
import re
from pathlib import Path
from typing import Callable

import duckdb
import pandas as pd
import pyodbc


ROOT_DIR = Path(__file__).resolve().parents[2]
CHUNK_SIZE = 100_000
SQL_TIMEOUT_SEG = 600  # 10 minutos por consulta SQL

SQL_REPORTS = [
    ("00_precio_margen_SIESA.sql", "margen_siesa_raw"),
    ("00_atributos_referencias.sql", "atributos_referencias_raw"),
    # Nota: el SQL de auditoría suele estar solo en `00_Reportes_SQL.py` (SQL_AUDITORIA).
]


def _sql_server_batch_sin_go(sql_text: str) -> str:
    """Igual que `00_Reportes_SQL.py`: GO no es válido en pyodbc."""
    return re.sub(
        r"(?im)^[ \t]*GO[ \t]*(?:--.*)?$",
        "",
        sql_text,
    )


def _config_path() -> Path:
    candidates = [ROOT_DIR / "Config.ini", ROOT_DIR / "config.ini"]
    for path in candidates:
        if path.exists():
            return path
    raise FileNotFoundError("No se encontró Config.ini/config.ini")


def _build_conn_str() -> str:
    cfg = configparser.ConfigParser()
    cfg.read(_config_path(), encoding="utf-8")
    server = cfg["SQLSERVER"]["server"]
    database = cfg["SQLSERVER"]["database"]
    user = cfg["SQLSERVER"]["db_user"]
    password = cfg["SQLSERVER"]["db_pass"].strip('"')
    return (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={server};DATABASE={database};UID={user};PWD={password};TrustServerCertificate=yes;Connection Timeout=30;"
    )


def _load_sql_text(sql_path: Path) -> str:
    if not sql_path.exists():
        raise FileNotFoundError(f"No se encontró SQL: {sql_path}")
    return sql_path.read_text(encoding="utf-8")


def _columna_referencia_texto_fijo(nombre: str) -> bool:
    c = str(nombre).strip().lower().replace(" ", "_")
    if c in ("referencia", "referencia_alternas"):
        return True
    if "referencia_altern" in c or "referencias_altern" in c:
        return True
    return False


def _dataframe_para_duckdb(df: pd.DataFrame) -> pd.DataFrame:
    """
    float64 en numéricos + object mayormente numérico → evita DECIMAL(10,2) en DuckDB
    que revienta con valores grandes en chunks posteriores.
    """
    work_df = df.copy()
    for col in work_df.columns:
        serie = work_df[col]
        if pd.api.types.is_datetime64_any_dtype(serie):
            continue
        if _columna_referencia_texto_fijo(col):
            continue
        if pd.api.types.is_numeric_dtype(serie):
            work_df[col] = serie.astype("float64")
        elif serie.dtype == object:
            converted = pd.to_numeric(serie, errors="coerce")
            nn = int(serie.notna().sum())
            if nn > 0 and converted.notna().sum() / nn > 0.9:
                work_df[col] = converted.astype("float64")
    return work_df


def _auditoria_bodegas_desde_env() -> list[str]:
    raw = (os.environ.get("AUDITORIA_BODEGAS") or "").strip()
    if not raw:
        return []
    return [x.strip() for x in raw.split(",") if x.strip()]


def _aplicar_filtro_bodegas_sql_auditoria(sql_text: str, codigos: list[str]) -> str:
    """
    Sustituye el marcador en SQL_AUDITORIA por AND b.f150_id IN (...).
    Lista vacía = sin filtro adicional (solo WHERE 1=1).
    """
    marker = "/*__AUDITORIA_FILTER_BODEGAS__*/"
    if marker not in sql_text:
        return sql_text
    if not codigos:
        return sql_text.replace(marker, "")
    esc: list[str] = []
    for c in codigos:
        s = str(c).strip().replace("'", "''")
        if s:
            esc.append(f"'{s}'")
    if not esc:
        return sql_text.replace(marker, "")
    return sql_text.replace(marker, f"AND b.f150_id IN ({','.join(esc)})")


def _quote_ident_duck(name: str) -> str:
    if not name:
        return name
    if any(ch in name for ch in (' ', '.', '-')):
        return '"' + name.replace('"', '""') + '"'
    return name


def _crear_tabla_bodegas_dim(duckdb_path: Path, log_callback: Callable[[str], None] | None = None) -> None:
    """Catálogo de bodegas desde margen_siesa_raw para la UI (multiselect)."""
    with duckdb.connect(str(duckdb_path)) as duck:
        try:
            duck.execute("SELECT 1 FROM margen_siesa_raw LIMIT 1")
        except Exception:
            return
        cols = [r[0] for r in duck.execute("DESCRIBE margen_siesa_raw").fetchall()]
        lower = {c.lower(): c for c in cols}
        b = lower.get("bodega")
        if not b:
            if log_callback:
                log_callback("  bodegas_dim: no hay columna Bodega en margen_siesa_raw")
            return
        nb = lower.get("nom_bodega")
        ni = lower.get("nom_instalacion")
        qb = _quote_ident_duck(b)
        sel_b = f"TRIM(CAST({qb} AS VARCHAR)) AS Bodega"
        sel_nb = (
            f"TRIM(CAST({_quote_ident_duck(nb)} AS VARCHAR)) AS Nom_Bodega"
            if nb
            else "CAST(NULL AS VARCHAR) AS Nom_Bodega"
        )
        sel_ni = (
            f"TRIM(CAST({_quote_ident_duck(ni)} AS VARCHAR)) AS Nom_Instalacion"
            if ni
            else "CAST(NULL AS VARCHAR) AS Nom_Instalacion"
        )
        q = f"""
        CREATE OR REPLACE TABLE bodegas_dim AS
        SELECT DISTINCT {sel_b}, {sel_nb}, {sel_ni}
        FROM margen_siesa_raw
        WHERE {qb} IS NOT NULL AND TRIM(CAST({qb} AS VARCHAR)) <> ''
        """
        duck.execute("PRAGMA threads=4")
        duck.execute(q)
        n = int(duck.execute("SELECT COUNT(*) FROM bodegas_dim").fetchone()[0])
        if log_callback:
            log_callback(f"  Tabla bodegas_dim actualizada: {n:,} bodegas distintas")


def refrescar_bodegas_dim_desde_margen(duckdb_path: Path) -> tuple[bool, str]:
    """
    Solo recrea `bodegas_dim` desde `margen_siesa_raw` (sin SQL Server ni pipelines).
    Útil para actualizar el listado de bodegas en la UI sin ejecutar el paso 00 completo.
    """
    if not duckdb_path.exists():
        return False, f"No existe la base: {duckdb_path.name}"
    try:
        with duckdb.connect(str(duckdb_path)) as duck:
            duck.execute("SELECT 1 FROM margen_siesa_raw LIMIT 1")
    except Exception:
        return (
            False,
            "No existe `margen_siesa_raw` o está vacía. Ejecuta antes la carga SQL 00 (margen).",
        )
    try:
        _crear_tabla_bodegas_dim(duckdb_path, log_callback=None)
        with duckdb.connect(str(duckdb_path)) as duck:
            n = int(duck.execute("SELECT COUNT(*) FROM bodegas_dim").fetchone()[0])
        return True, f"Catálogo actualizado: {n:,} bodegas."
    except Exception as exc:
        return False, str(exc)


_MAX_PERSIST_RETRIES = 3
_RETRY_WAIT_SECS = [5, 15, 30]


def _persist_query_to_duckdb(
    sql_text: str,
    table_name: str,
    duckdb_path: Path,
    log_callback: Callable[[str], None] | None = None,
) -> int:
    """
    Carga el resultset completo del SQL en DuckDB (misma lógica que 00_Reportes_SQL.py):
    lectura por chunks desde SQL Server sin tope de filas en el total.

    Incluye reintentos con reconexión ante cortes de red (error 08S01 / 10054).
    Si falla, la tabla previa en DuckDB se conserva intacta.
    """
    import time

    conn_str = _build_conn_str()
    sql_clean = _sql_server_batch_sin_go(sql_text)
    staging = f"_staging_{table_name}"
    last_exc: Exception | None = None

    for attempt in range(1, _MAX_PERSIST_RETRIES + 1):
        total = 0
        conn_sql: pyodbc.Connection | None = None
        try:
            conn_sql = pyodbc.connect(conn_str)
            with duckdb.connect(str(duckdb_path)) as duck:
                cur = conn_sql.cursor()
                if hasattr(cur, "timeout"):
                    cur.timeout = SQL_TIMEOUT_SEG
                cur.arraysize = CHUNK_SIZE
                cur.execute(sql_clean)
                duck.execute("PRAGMA threads=4")

                while cur.description is None:
                    if not cur.nextset():
                        raise ValueError(
                            f"El SQL {table_name} no devolvió un resultset final."
                        )

                cols = [
                    c[0].strip() if c[0] else f"col_{i}"
                    for i, c in enumerate(cur.description)
                ]
                primera = True
                chunks = 0
                while True:
                    rows = cur.fetchmany(CHUNK_SIZE)
                    if not rows:
                        break

                    df = _dataframe_para_duckdb(
                        pd.DataFrame.from_records(rows, columns=cols)
                    )
                    duck.register("tmp_chunk", df)

                    if primera:
                        duck.execute(
                            f"CREATE OR REPLACE TABLE {staging} AS "
                            "SELECT * FROM tmp_chunk"
                        )
                        primera = False
                    else:
                        duck.execute(
                            f"INSERT INTO {staging} SELECT * FROM tmp_chunk"
                        )

                    duck.unregister("tmp_chunk")
                    total += len(df)
                    chunks += 1
                    if log_callback:
                        log_callback(
                            f"  {table_name} - chunk {chunks}: {total:,} filas acumuladas"
                        )

                if primera:
                    col_defs = ", ".join([f'"{c}" VARCHAR' for c in cols])
                    duck.execute(f"CREATE OR REPLACE TABLE {staging} ({col_defs})")

                duck.execute(f"DROP TABLE IF EXISTS {table_name}")
                duck.execute(f"ALTER TABLE {staging} RENAME TO {table_name}")

            return total

        except Exception as exc:
            last_exc = exc
            try:
                with duckdb.connect(str(duckdb_path)) as duck_clean:
                    duck_clean.execute(f"DROP TABLE IF EXISTS {staging}")
            except Exception:
                pass

            is_network = "08S01" in str(exc) or "10054" in str(exc) or "Communication link" in str(exc)
            if is_network and attempt < _MAX_PERSIST_RETRIES:
                wait = _RETRY_WAIT_SECS[min(attempt - 1, len(_RETRY_WAIT_SECS) - 1)]
                if log_callback:
                    log_callback(
                        f"  ⚠ Conexión perdida en {table_name} (intento {attempt}/{_MAX_PERSIST_RETRIES}). "
                        f"Reintentando en {wait}s..."
                    )
                time.sleep(wait)
                continue
            raise
        finally:
            if conn_sql is not None:
                try:
                    conn_sql.close()
                except Exception:
                    pass

    raise last_exc  # type: ignore[misc]


SQL_001_KEY = "sql_001_margen_siesa"
SQL_002_KEY = "sql_002_atributos_refs"
SQL_003_KEY = "sql_003_auditoria"

ALL_SQL_KEYS = [SQL_001_KEY, SQL_002_KEY, SQL_003_KEY]


def cargar_reportes_sql_en_duckdb(
    log_callback: Callable[[str], None] | None = None,
    duckdb_path_override: Path | None = None,
    auditoria_bodegas: list[str] | None = None,
    sql_queries: list[str] | None = None,
) -> tuple[bool, str]:
    """
    Ejecuta las consultas SQL embebidas seleccionadas.

    ``sql_queries`` filtra cuáles de las 3 consultas se ejecutan
    (claves: ``SQL_001_KEY``, ``SQL_002_KEY``, ``SQL_003_KEY``).
    ``None`` o lista vacía = ejecutar todas.
    """
    pipeline_00 = ROOT_DIR / "00_Reportes_SQL.py"
    if pipeline_00.exists():
        try:
            spec = importlib.util.spec_from_file_location("pipeline00_module", str(pipeline_00))
            if spec is None or spec.loader is None:
                raise RuntimeError("No fue posible cargar `00_Reportes_SQL.py`.")
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
        except Exception as exc:
            return False, f"Error cargando `00_Reportes_SQL.py`: {exc}"

        cfg = configparser.ConfigParser()
        cfg.read(_config_path(), encoding="utf-8")
        duckdb_name = cfg["SALIDA"]["duckdb"]
        duckdb_path = duckdb_path_override or (ROOT_DIR / duckdb_name)

        sql_embebido = [
            (SQL_001_KEY, "consulta_interna_precio_margen_siesa", "margen_siesa_raw", getattr(module, "SQL_PRECIO_MARGEN_SIESA", None)),
            (SQL_002_KEY, "consulta_interna_atributos_referencias", "atributos_referencias_raw", getattr(module, "SQL_ATRIBUTOS_REFERENCIAS", None)),
            (SQL_003_KEY, "consulta_interna_auditoria", "auditoria_raw", getattr(module, "SQL_AUDITORIA", None)),
        ]

        keys_a_ejecutar = set(sql_queries) if sql_queries else set(ALL_SQL_KEYS)

        logs: list[str] = []
        try:
            if auditoria_bodegas is None:
                codigos_bodegas = _auditoria_bodegas_desde_env()
            else:
                codigos_bodegas = list(auditoria_bodegas)
            logs.append("\n### Paso 00 (SQL embebido desde 00_Reportes_SQL.py)")
            if log_callback:
                log_callback("### Paso 00 (SQL embebido desde 00_Reportes_SQL.py)")
            for key, sql_name, table_name, sql_text in sql_embebido:
                if key not in keys_a_ejecutar:
                    logs.append(f"\n### Omitido: {sql_name} (no seleccionado)")
                    if log_callback:
                        log_callback(f"### Omitido: {sql_name} -> {table_name} (no seleccionado)")
                    continue
                if not sql_text:
                    raise ValueError(f"No se encontró SQL embebido para `{sql_name}` en `00_Reportes_SQL.py`.")
                logs.append(f"\n### Ejecutando SQL embebido: {sql_name} -> {table_name}")
                if log_callback:
                    log_callback(f"### Ejecutando SQL embebido: {sql_name} -> {table_name}")
                txt_sql = str(sql_text)
                if key == SQL_003_KEY:
                    bods = codigos_bodegas
                    txt_sql = _aplicar_filtro_bodegas_sql_auditoria(txt_sql, bods)
                    if bods and log_callback:
                        log_callback(
                            f"  Filtro auditoría por bodegas ({len(bods)}): {', '.join(bods[:12])}"
                            + ("..." if len(bods) > 12 else "")
                        )
                total = _persist_query_to_duckdb(
                    txt_sql,
                    table_name,
                    duckdb_path,
                    log_callback=log_callback,
                )
                if table_name == "margen_siesa_raw":
                    _crear_tabla_bodegas_dim(duckdb_path, log_callback=log_callback)
                logs.append(f"Filas cargadas: {total:,}")
                if log_callback:
                    log_callback(f"Filas cargadas en {table_name}: {total:,}")
            return True, "\n".join(logs).strip()
        except Exception as exc:
            logs.append(f"\nError SQL reportes: {exc}")
            return False, "\n".join(logs).strip()

    # Fallback legado: usa archivos .sql si existe esa estructura.
    cfg = configparser.ConfigParser()
    cfg.read(_config_path(), encoding="utf-8")
    duckdb_name = cfg["SALIDA"]["duckdb"]
    duckdb_path = duckdb_path_override or (ROOT_DIR / duckdb_name)

    logs: list[str] = []
    try:
        for sql_file, table_name in SQL_REPORTS:
            sql_path = ROOT_DIR / sql_file
            logs.append(f"\n### Ejecutando SQL: {sql_file} -> {table_name}")
            sql_text = _load_sql_text(sql_path)
            total = _persist_query_to_duckdb(sql_text, table_name, duckdb_path)
            logs.append(f"Filas cargadas: {total:,}")
        return True, "\n".join(logs).strip()
    except Exception as exc:
        logs.append(f"\nError SQL reportes: {exc}")
        return False, "\n".join(logs).strip()
