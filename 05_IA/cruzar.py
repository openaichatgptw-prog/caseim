# -*- coding: utf-8 -*-
"""
Cruza el Excel de origen (MARKETSHARE, hoja MEWPS_FORKLIFT, etc.) con el resultado
de la clasificación (checkpoint con idx, o CSV de salida por orden de filas).

Genera un nuevo .xlsx con todas las columnas del libro más clasificacion, marca, Estado.

Uso:
  python cruzar.py
  python cruzar.py --salida MiSalida.xlsx
  python cruzar.py --checkpoint otro_checkpoint.csv

Prioridad de resultados: si existe CHECKPOINT_CSV con columna idx, se cruza por idx
(posición 0-based = misma fila que en el clasificador). Si no, usa OUTPUT_CSV alineado
por orden (mismo número de filas que el Excel leído).

Requiere: pip install pandas openpyxl
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

_CONFIG = "ConfigClasificadorMewp.json"
_COLS_RES = ["clasificacion", "marca", "Estado"]


def _load_cfg(base: Path) -> dict:
    p = base / _CONFIG
    if not p.exists():
        raise SystemExit(f"No se encontró {p}")
    with open(p, encoding="utf-8") as f:
        return json.load(f)


def _aplicar_por_idx(df: pd.DataFrame, chk: pd.DataFrame) -> pd.DataFrame:
    """chk tiene columnas idx, clasificacion, marca, Estado."""
    if "idx" not in chk.columns:
        raise ValueError("checkpoint sin columna idx")
    m = chk.drop_duplicates(subset=["idx"], keep="last").set_index("idx")
    out = df.copy().reset_index(drop=True)
    for c in _COLS_RES:
        if c in m.columns:
            out[c] = out.index.map(lambda i: m.at[i, c] if i in m.index else pd.NA)
        else:
            out[c] = pd.NA
    return out


def _aplicar_por_orden(df: pd.DataFrame, res: pd.DataFrame) -> pd.DataFrame:
    """Misma cantidad de filas; copia columnas de resultado por posición."""
    if len(df) != len(res):
        raise SystemExit(
            f"Filas Excel ({len(df)}) ≠ filas resultado ({len(res)}). "
            "Use el checkpoint con idx o un CSV generado para el mismo libro."
        )
    out = df.copy().reset_index(drop=True)
    for c in _COLS_RES:
        if c in res.columns:
            out[c] = res[c].values
        else:
            out[c] = pd.NA
    return out


def main() -> None:
    base = Path(__file__).resolve().parent
    cfg = _load_cfg(base)

    ap = argparse.ArgumentParser(description="Cruzar Excel de origen con clasificación MEWP/Forklift.")
    ap.add_argument(
        "--excel",
        default=cfg.get("INPUT_XLSX", "MARKETSHARE.xlsx"),
        help="Archivo Excel de entrada (relativo a 05_IA si no es ruta absoluta)",
    )
    ap.add_argument("--sheet", default=cfg.get("SHEET_NAME", "MEWPS_FORKLIFT"))
    ap.add_argument("--header-row", type=int, default=int(cfg.get("HEADER_ROW", 2)))
    ap.add_argument(
        "--checkpoint",
        default=cfg.get("CHECKPOINT_CSV", "Mepwpsforklif_checkpoint.csv"),
    )
    ap.add_argument(
        "--csv",
        default=cfg.get("OUTPUT_CSV", "Mepwpsforklif.csv"),
        help="CSV de resultados si no hay checkpoint con idx",
    )
    ap.add_argument(
        "--salida",
        default=cfg.get("OUTPUT_XLSX_CRUZ", "MARKETSHARE_MEWP_clasificado.xlsx"),
        help="Nombre del Excel de salida",
    )
    args = ap.parse_args()

    path_xlsx = Path(args.excel)
    if not path_xlsx.is_absolute():
        path_xlsx = (base / path_xlsx).resolve()
    if not path_xlsx.exists():
        raise SystemExit(f"No se encontró el Excel: {path_xlsx}")

    df = pd.read_excel(
        path_xlsx,
        sheet_name=args.sheet,
        header=args.header_row,
        engine="openpyxl",
    )

    cp = Path(args.checkpoint)
    if not cp.is_absolute():
        cp = base / cp
    csv_alt = Path(args.csv)
    if not csv_alt.is_absolute():
        csv_alt = base / csv_alt

    for c in _COLS_RES:
        if c in df.columns:
            df = df.drop(columns=[c])

    if cp.exists():
        chk = pd.read_csv(cp, encoding="utf-8-sig")
        if "idx" in chk.columns:
            out = _aplicar_por_idx(df, chk)
            fuente = f"checkpoint por idx ({cp.name})"
        else:
            out = _aplicar_por_orden(df, chk)
            fuente = f"archivo sin idx, por orden ({cp.name})"
    elif csv_alt.exists():
        res = pd.read_csv(csv_alt, encoding="utf-8-sig")
        out = _aplicar_por_orden(df, res)
        fuente = f"CSV por orden ({csv_alt.name})"
    else:
        raise SystemExit(
            f"No hay checkpoint ({cp}) ni CSV ({csv_alt}). Ejecute antes el clasificador."
        )

    salida = Path(args.salida)
    if not salida.is_absolute():
        salida = base / salida

    # Evitar sobrescribir el origen por error
    if salida.resolve() == path_xlsx.resolve():
        raise SystemExit("El archivo de salida no puede ser el mismo que el Excel de entrada.")

    out.to_excel(salida, sheet_name=args.sheet[:31], index=False, engine="openpyxl")

    n_ok = out[_COLS_RES[0]].notna().sum() if _COLS_RES[0] in out.columns else 0
    print(f"Cruce listo: {fuente}")
    print(f"Filas: {len(out)} | Con clasificacion: {n_ok}")
    print(f"Guardado: {salida}")


if __name__ == "__main__":
    main()
