from __future__ import annotations

import subprocess
import sys
from pathlib import Path
import os
import time
import configparser
import shutil
from typing import Callable

import duckdb

from services.sql_reports_loader import cargar_reportes_sql_en_duckdb


ROOT_DIR = Path(__file__).resolve().parents[2]
TIMEOUT_PIPELINE_SEG = 1800  # 30 minutos por script (02 ventas puede ser lento)
EJECUTAR_REPORTES_SQL = False

PIPELINES = [
    "01_Mejora_pipeline_precios_chnV21.py",
    "02_ventas_precios_cnhV2.py",
    "03_Maestro_historico.py",
]

def _resolve_master_db_path() -> Path:
    cfg = configparser.ConfigParser()
    candidates = [ROOT_DIR / "Config.ini", ROOT_DIR / "config.ini"]
    loaded = cfg.read([str(p) for p in candidates], encoding="utf-8")
    if not loaded or "SALIDA" not in cfg or "duckdb" not in cfg["SALIDA"]:
        return ROOT_DIR / "pipeline.duckdb"
    return ROOT_DIR / cfg["SALIDA"]["duckdb"]


def _publish_work_db(work_db: Path, master_db: Path) -> None:
    """Publica solo las tablas que existen en la BD de trabajo hacia la maestra (merge, no reemplazo)."""
    if not work_db.exists():
        raise FileNotFoundError(f"No existe la base temporal de trabajo: {work_db}")
    master_db.parent.mkdir(parents=True, exist_ok=True)

    last_error: Exception | None = None
    max_attempts = 20
    for attempt in range(1, max_attempts + 1):
        try:
            with duckdb.connect(str(master_db)) as con:
                work_db_sql = str(work_db).replace("'", "''")
                con.execute(f"ATTACH '{work_db_sql}' AS work (READ_ONLY)")
                tables = [
                    row[0]
                    for row in con.execute(
                        "SELECT table_name "
                        "FROM information_schema.tables "
                        "WHERE table_catalog = 'work' "
                        "  AND table_schema = 'main'"
                    ).fetchall()
                ]
                for t in tables:
                    con.execute(f'CREATE OR REPLACE TABLE main."{t}" AS SELECT * FROM work."{t}"')
                con.execute("DETACH work")
            return
        except Exception as exc:
            last_error = exc
            # Windows/OneDrive puede mantener locks breves; reintentamos con backoff.
            wait_secs = min(3.0, 0.4 * attempt)
            time.sleep(wait_secs)
    raise RuntimeError(
        f"No fue posible publicar tablas de trabajo en {master_db}. "
        f"Último error: {last_error}"
    ) from last_error


def _is_duckdb_lock_error(text: str) -> bool:
    txt = (text or "").lower()
    if not txt:
        return False
    lock_signals = [
        "cannot open file",
        "already open in",
        "being utilized by another process",
        "utilizado por otro proceso",
        "the process cannot access the file",
        "permission denied",
        "io error",
    ]
    mentions_duckdb = "duckdb" in txt or ".duckdb" in txt
    return mentions_duckdb and any(sig in txt for sig in lock_signals)


def _cleanup_stale_work_dbs(master_db_path: Path, keep_paths: set[Path] | None = None) -> None:
    """
    Limpia bases temporales huérfanas `pipeline.work*.duckdb` (y su WAL) para
    evitar acumulación en OneDrive/Windows.
    """
    keep = {p.resolve() for p in (keep_paths or set())}
    parent = master_db_path.parent
    stem = master_db_path.stem
    for p in parent.glob(f"{stem}.work*.duckdb"):
        try:
            rp = p.resolve()
            if rp in keep:
                continue
            wal = p.with_suffix(".duckdb.wal")
            if wal.exists():
                wal.unlink()
            if p.exists():
                p.unlink()
        except Exception:
            # Si está bloqueado en ese momento, se reintentará en la siguiente corrida.
            continue


def ejecutar_pipelines(
    log_callback: Callable[[str], None] | None = None,
    ejecutar_reportes_sql: bool | None = None,
    pipelines_a_ejecutar: list[str] | None = None,
    auditoria_bodegas: list[str] | None = None,
    sql_queries: list[str] | None = None,
) -> tuple[bool, str]:
    salida: list[str] = []
    ok = True
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUTF8"] = "1"
    master_db_path = _resolve_master_db_path()
    work_db_path = master_db_path.with_name(f"{master_db_path.stem}.work.duckdb")
    _cleanup_stale_work_dbs(master_db_path, keep_paths={work_db_path})
    env["PIPELINE_DUCKDB_PATH"] = str(work_db_path)
    for suffix in [".duckdb", ".duckdb.wal"]:
        p = work_db_path.with_suffix(suffix) if suffix != ".duckdb" else work_db_path
        try:
            if p.exists():
                p.unlink()
        except Exception:
            pass

    def _append_log(msg: str) -> None:
        salida.append(msg)
        if log_callback:
            log_callback("\n".join(salida).strip())

    scripts = PIPELINES if pipelines_a_ejecutar is None else [s for s in PIPELINES if s in pipelines_a_ejecutar]

    if not scripts and not (EJECUTAR_REPORTES_SQL if ejecutar_reportes_sql is None else bool(ejecutar_reportes_sql)):
        return True, "No se seleccionaron procesos para ejecutar."

    _append_log(f"Usando base temporal de trabajo: {work_db_path.name}")

    for script in scripts:
        script_path = ROOT_DIR / script
        if not script_path.exists():
            return False, f"No se encontró el script: {script_path}"

        _append_log(f"\n### Ejecutando: {script}\n")
        max_reintentos = 6
        for intento in range(1, max_reintentos + 1):
            try:
                proceso = subprocess.run(
                    [sys.executable, "-X", "utf8", str(script_path)],
                    cwd=str(ROOT_DIR),
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    env=env,
                    timeout=TIMEOUT_PIPELINE_SEG,
                )
            except subprocess.TimeoutExpired:
                ok = False
                _append_log(
                    f"\nError en {script}: excedió {TIMEOUT_PIPELINE_SEG // 60} minutos."
                )
                break

            if proceso.returncode == 0:
                if proceso.stdout:
                    _append_log(proceso.stdout.strip())
                if proceso.stderr:
                    _append_log("\n[stderr]\n" + proceso.stderr.strip())
                break

            err_full = f"{proceso.stdout or ''}\n{proceso.stderr or ''}"
            lock_error = _is_duckdb_lock_error(err_full)
            if lock_error and intento < max_reintentos:
                espera = round(min(8.0, 1.2 * (1.6 ** (intento - 1))), 1)
                _append_log(
                    f"Reintento {intento}/{max_reintentos-1} por bloqueo de DuckDB "
                    f"(esperando {espera}s)..."
                )
                time.sleep(espera)
                continue

            if proceso.stdout:
                _append_log(proceso.stdout.strip())
            if proceso.stderr:
                _append_log("\n[stderr]\n" + proceso.stderr.strip())
            ok = False
            _append_log(f"\nError en {script} (código {proceso.returncode})")
            break

        if not ok:
            break

    correr_reportes_sql = EJECUTAR_REPORTES_SQL if ejecutar_reportes_sql is None else bool(ejecutar_reportes_sql)

    if correr_reportes_sql:
        ok_sql, log_sql = cargar_reportes_sql_en_duckdb(
            log_callback=_append_log,
            duckdb_path_override=work_db_path,
            auditoria_bodegas=auditoria_bodegas,
            sql_queries=sql_queries,
        )
        _append_log("\n" + log_sql)
        if not ok_sql:
            ok = False
    else:
        _append_log(
            "\n### SQL 00_* omitido en actualización desde UI "
            "(EJECUTAR_REPORTES_SQL=False)."
        )

    if ok:
        try:
            if work_db_path.exists():
                _publish_work_db(work_db_path, master_db_path)
                _append_log(
                    f"\nTablas publicadas correctamente en: {master_db_path.name}"
                )
            else:
                ok = False
                _append_log(
                    "\nError: no se generó la base temporal de trabajo."
                )
        except Exception as exc:
            ok = False
            _append_log(f"\nError publicando tablas: {exc}")

    try:
        for suffix in [".duckdb", ".duckdb.wal"]:
            p = work_db_path.with_suffix(suffix) if suffix != ".duckdb" else work_db_path
            if p.exists():
                p.unlink()
    except Exception:
        pass
    _cleanup_stale_work_dbs(master_db_path, keep_paths={work_db_path})

    return ok, "\n".join(salida).strip()
