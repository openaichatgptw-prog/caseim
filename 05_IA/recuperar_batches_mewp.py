# -*- coding: utf-8 -*-
"""
Descarga resultados de jobs de OpenAI Batch API ya en estado "completed" y los
fusiona con el checkpoint CSV (mismos custom_id row-0, row-1, ...).

Uso:
  1) Cree batch_ids.txt en esta carpeta, un ID por línea (ej. batch_69ccb5388d6c8190...).
  2) python recuperar_batches_mewp.py

Opciones:
  python recuperar_batches_mewp.py --ids-file mis_batches.txt
  python recuperar_batches_mewp.py --solo-listar   # solo muestra estado de cada batch, no escribe

Requiere el mismo ConfigClasificadorMewp.json y Excel que el clasificador (para Id fila y N filas).
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd
from openai import OpenAI

from clasificar_marketshare_mewp_llm import (
    _api_key,
    _completed_to_lists,
    _guardar_csv_resultados,
    _id_fila_por_fila,
    _load_checkpoint,
    _load_config,
    _parse_batch_output_lines,
    _save_checkpoint,
)


def _leer_ids(path: Path) -> list[str]:
    if not path.exists():
        raise SystemExit(
            f"No existe {path}. Cree el archivo con un batch_id por línea "
            f"(ej. batch_69ccb5388d6c8190963b4430caeec148)."
        )
    ids: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if not line.startswith("batch_"):
            line = line.split("/")[-1].strip()
        ids.append(line)
    if not ids:
        raise SystemExit(f"{path} no contiene IDs válidos.")
    return ids


def _descargar_batch(client: OpenAI, bid: str) -> dict[int, tuple[str, str, str]]:
    job = client.batches.retrieve(bid)
    st = job.status
    if st != "completed":
        print(f"  Omitido (estado={st}): {bid}")
        return {}
    out_id = job.output_file_id
    if not out_id:
        print(f"  Sin output_file_id: {bid}")
        return {}
    raw = client.files.content(out_id)
    text = raw.text if hasattr(raw, "text") else raw.read().decode("utf-8")
    parsed = _parse_batch_output_lines(text)
    print(f"  OK: {bid} -> {len(parsed)} filas en salida")
    return parsed


def main() -> None:
    ap = argparse.ArgumentParser(description="Recuperar salidas de Batch API ya completados.")
    ap.add_argument(
        "--ids-file",
        default="batch_ids.txt",
        help="Archivo con un batch_id por línea (default: batch_ids.txt)",
    )
    ap.add_argument(
        "--solo-listar",
        action="store_true",
        help="Solo muestra estado de cada batch; no fusiona ni escribe archivos.",
    )
    args = ap.parse_args()

    base = Path(__file__).resolve().parent
    cfg = _load_config(base)
    ids_path = base / args.ids_file

    excel_rel = cfg["INPUT_XLSX"]
    path_xlsx = (base / excel_rel).resolve()
    if not path_xlsx.exists():
        raise SystemExit(f"No se encontró el Excel: {path_xlsx}")

    sheet = cfg["SHEET_NAME"]
    header_row = int(cfg.get("HEADER_ROW", 2))
    out_csv = str(cfg.get("OUTPUT_CSV", "Mepwpsforklif.csv"))
    checkpoint_name = str(cfg.get("CHECKPOINT_CSV", "Mepwpsforklif_checkpoint.csv"))
    checkpoint_path = base / checkpoint_name

    client = OpenAI(api_key=_api_key(cfg))
    batch_ids = _leer_ids(ids_path)

    if args.solo_listar:
        for bid in batch_ids:
            j = client.batches.retrieve(bid)
            rc = j.request_counts
            extra = ""
            if rc is not None:
                extra = f" completed={getattr(rc, 'completed', '?')}/{getattr(rc, 'total', '?')}"
            print(f"{bid}  status={j.status}{extra}")
        return

    merged: dict[int, tuple[str, str, str]] = {}
    for bid in batch_ids:
        print(f"Procesando {bid}...")
        part = _descargar_batch(client, bid)
        merged.update(part)

    if not merged:
        raise SystemExit("No se obtuvo ninguna fila de los batches (¿todos incompletos?).")

    print(f"Total índices únicos recuperados del/los batch: {len(merged)}")

    existente = _load_checkpoint(checkpoint_path)
    if existente:
        print(f"Checkpoint previo: {len(existente)} filas; se fusionan (los batch recuperados pisan mismo idx).")
    combined = {**existente, **merged}

    df = pd.read_excel(path_xlsx, sheet_name=sheet, header=header_row, engine="openpyxl")
    n = len(df)
    ids_fila = _id_fila_por_fila(df)

    _save_checkpoint(checkpoint_path, ids_fila, combined)
    clasificaciones, marcas, estados = _completed_to_lists(n, ids_fila, combined)
    csv_path = _guardar_csv_resultados(
        base, out_csv, ids_fila, clasificaciones, marcas, estados
    )

    print(f"Checkpoint actualizado: {checkpoint_path} ({len(combined)} filas con resultado)")
    print(f"CSV: {csv_path}")
    print("Resumen clasificacion:", pd.Series(clasificaciones).value_counts().head(15).to_string())


if __name__ == "__main__":
    main()
