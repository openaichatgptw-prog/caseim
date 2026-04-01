# -*- coding: utf-8 -*-
"""
Clasifica con OpenAI la hoja de MARKETSHARE (p. ej. MEWPS_FORKLIFT):
- clasificacion: MEWPS | FORKLIFT | NA
- marca, Estado: NUEVO | USADO | NA

Los resultados se guardan en un CSV (no se modifica el Excel). Columnas: Id fila,
clasificacion, marca, Estado.

Checkpoint: tras cada batch (modo lotes) o cada N filas (modo uno a uno) se guarda
CHECKPOINT_CSV para poder reanudar si se interrumpe el proceso.

Al ejecutar: menú 1=Batch API / 2=Uno a uno (si la terminal es interactiva).

Requiere: pip install openai pandas openpyxl
"""
from __future__ import annotations

import json
import os
import re
import sys
import tempfile
import time
from pathlib import Path

import pandas as pd
from openai import OpenAI

_CONFIG_NAME = "ConfigClasificadorMewp.json"

_SYSTEM = (
    "Clasificador de importaciones de maquinaria. Solo JSON, sin markdown:\n"
    '{"clasificacion":"...","marca":"...","Estado":"..."}\n\n'

    "clasificacion ∈ {MEWPS,FORKLIFT,NA}:\n"
    "MEWPS: plataforma elevadora/tijera/scissor, boom lift, man lift, AWP, MEWP.\n"
    "FORKLIFT: montacargas, carretilla elevadora, reach truck, apilador con mástil, transpaleta motorizada.\n"
    "NA: repuesto suelto, parte, accesorio sin equipo completo, u otro producto.\n\n"

    "marca (MAYÚSCULAS): fabricante del equipo principal.\n"
    "Prioridad:\n"
    "1) MARCA explícita en la descripción (no motor/batería/neumático si hay otra marca del bien).\n"
    "2) MODELO/LÍNEA/REFERENCIA → inferir fabricante. Tabla orientativa:\n"
    "MEWP: GS-1930,GS-3246,Z-45/25J,S-65,S-85,AWP-30→GENIE; "
    "1930ES,2646ES,450AJ,600S,800AJ,Toucan→JLG; Compact 10,HA16 RTJ→HAULOTTE; "
    "M30-4→MANITOU; SKYTRAK→JLG; Optimum 8→HAULOTTE; Star 10→HAULOTTE.\n"
    "FORKLIFT: 8FGCU25,8FD30→TOYOTA; H2.5XT,H3.0XT→HYSTER; DP25→CAT; "
    "GP25N→MITSUBISHI; E20,E25,H30D→LINDE; EFG 216,ETV 216,DFG25→JUNGHEINRICH; "
    "FC 5200,ESR 5200→CROWN; CPCD30→HANGCHA; FD30→HELI; RX60-25→STILL; "
    "D30S→DOOSAN; C25L→CLARK; FD30T→KOMATSU; CPD→HELI; CHL→HELI; "
    "FB→NOBLELIFT; LEV→NOBLELIFT.\n"
    "Aplica criterio similar a otros códigos reconocibles del sector.\n"
    "3) PROVEEDOR: si incluye nombre de fabricante (JLG, GENIE, TOYOTA MATERIAL HANDLING, "
    "HAULOTTE, CROWN, LINDE, HYSTER-YALE, HANGCHA, NOBLELIFT, MANITOU, TEREX, "
    "SNORKEL, SKYJACK, EP EQUIPMENT, MAXIMAL, BAOLI), extraer la marca comercial.\n"
    "4) NA si nada permite identificar fabricante. No inventes marcas.\n\n"

    "Estado ∈ {NUEVO,USADO,NA}:\n"
    "NUEVO: mercancía nueva, primera, nuevo.\n"
    "USADO: mercancía usada, segunda, seminuevo, remanufacturado, refurbished.\n"
    "NA: contradictorio o indeterminado.\n\n"

    "Si es repuesto o parte suelta sin equipo completo → clasificacion=NA, marca=NA."
)

def _user_payload(descripcion: str, proveedor: str) -> str:
    return (
        f"Descripción:\n{descripcion}\n\n"
        f"Proveedor:\n{proveedor}"
    )


def _completion_body(
    descripcion: str,
    proveedor: str,
    model: str,
    temperature: float,
    max_chars: int,
    max_prov_chars: int,
) -> dict:
    desc = (descripcion or "")[:max_chars]
    prov = (proveedor or "")[:max_prov_chars]
    return {
        "model": model,
        "temperature": temperature,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": _SYSTEM},
            {"role": "user", "content": _user_payload(desc, prov)},
        ],
    }


def _load_config(base: Path) -> dict:
    path = base / _CONFIG_NAME
    if not path.exists():
        raise SystemExit(f"No se encontró {path}")
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _api_key(cfg: dict) -> str:
    k = (cfg.get("OPENAI_API_KEY") or "").strip()
    if not k:
        k = (os.environ.get("OPENAI_API_KEY") or "").strip()
    if not k:
        raise SystemExit(
            "Defina OPENAI_API_KEY en el entorno o en ConfigClasificadorMewp.json"
        )
    return k


def _strip_json_fence(text: str) -> str:
    t = text.strip()
    if t.startswith("```"):
        t = re.sub(r"^```(?:json)?\s*", "", t, flags=re.I)
        t = re.sub(r"\s*```\s*$", "", t)
    return t.strip()


def _normalize_result(d: dict) -> tuple[str, str, str]:
    c = str(d.get("clasificacion", "NA")).upper().strip()
    if c not in ("MEWPS", "FORKLIFT", "NA"):
        c = "NA"
    m = str(d.get("marca", "NA")).strip()
    if not m:
        m = "NA"
    else:
        m = m.upper()
    e = str(d.get("Estado", d.get("estado", "NA"))).upper().strip()
    if e not in ("NUEVO", "USADO", "NA"):
        e = "NA"
    return c, m, e


def _id_fila_por_fila(df: pd.DataFrame) -> list:
    """Un id por fila del DataFrame: columna 'fila'/'id' si existe; si no, 1..n."""
    mapping = {str(c).strip().lower(): c for c in df.columns}
    for key in ("fila", "id fila", "id_fila", "nro fila", "nro_fila", "id"):
        if key in mapping:
            return df[mapping[key]].tolist()
    return list(range(1, len(df) + 1))


def _guardar_csv_resultados(
    base: Path,
    nombre_csv: str,
    ids_fila: list,
    clasificaciones: list[str],
    marcas: list[str],
    estados: list[str],
) -> Path:
    out = base / nombre_csv
    pd.DataFrame(
        {
            "Id fila": ids_fila,
            "clasificacion": clasificaciones,
            "marca": marcas,
            "Estado": estados,
        }
    ).to_csv(out, index=False, encoding="utf-8-sig")
    return out


def _load_checkpoint(path: Path) -> dict[int, tuple[str, str, str]]:
    """idx (0-based) -> (clasificacion, marca, Estado)."""
    if not path.exists():
        return {}
    try:
        df = pd.read_csv(path, encoding="utf-8-sig")
    except Exception:
        return {}
    if "idx" not in df.columns:
        return {}
    out: dict[int, tuple[str, str, str]] = {}
    for _, row in df.iterrows():
        try:
            ix = int(row["idx"])
        except (TypeError, ValueError):
            continue
        out[ix] = (
            str(row.get("clasificacion", "NA")),
            str(row.get("marca", "NA")),
            str(row.get("Estado", "NA")),
        )
    return out


def _save_checkpoint(
    path: Path,
    ids_fila: list,
    completed: dict[int, tuple[str, str, str]],
) -> None:
    """Solo filas completadas; columna idx para reanudar."""
    rows = []
    for idx in sorted(completed.keys()):
        c, m, e = completed[idx]
        idf = ids_fila[idx] if idx < len(ids_fila) else idx + 1
        rows.append({"idx": idx, "Id fila": idf, "clasificacion": c, "marca": m, "Estado": e})
    pd.DataFrame(rows).to_csv(path, index=False, encoding="utf-8-sig")


def _parse_message_to_tuple(content: str) -> tuple[str, str, str]:
    raw = _strip_json_fence(content.strip())
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return "NA", "NA", "NA"
    return _normalize_result(data)


def clasificar_fila_sync(
    client: OpenAI,
    model: str,
    descripcion: str,
    proveedor: str,
    max_chars: int,
    max_prov_chars: int,
    temperature: float,
) -> tuple[str, str, str]:
    body = _completion_body(
        descripcion, proveedor, model, temperature, max_chars, max_prov_chars
    )
    try:
        resp = client.chat.completions.create(**body)
        text = (resp.choices[0].message.content or "").strip()
    except Exception as ex:
        print("Error API:", ex)
        return "NA", "NA", "NA"
    return _parse_message_to_tuple(text)


def _parse_batch_output_lines(out_text: str) -> dict[int, tuple[str, str, str]]:
    by_idx: dict[int, tuple[str, str, str]] = {}
    for line in out_text.splitlines():
        line = line.strip()
        if not line:
            continue
        obj = json.loads(line)
        m = re.match(r"^row-(\d+)$", str(obj.get("custom_id", "")))
        if not m:
            continue
        idx = int(m.group(1))
        if obj.get("error"):
            by_idx[idx] = ("NA", "NA", "NA")
            continue
        resp = obj.get("response") or {}
        if resp.get("status_code") != 200:
            by_idx[idx] = ("NA", "NA", "NA")
            continue
        b = resp.get("body") or {}
        ch = (b.get("choices") or [{}])[0]
        content = ((ch.get("message") or {}).get("content")) or ""
        by_idx[idx] = _parse_message_to_tuple(content)
    return by_idx


def _wait_batch_job(client: OpenAI, bid: str, poll_s: float):
    """Espera a que el batch termine. La API puede tardar mucho en subir completed (0/N al inicio es normal)."""
    terminal = {"completed", "failed", "expired", "cancelled"}
    t0 = time.monotonic()
    first = True
    while True:
        job = client.batches.retrieve(bid)
        st = job.status
        elapsed = int(time.monotonic() - t0)
        mm, ss = elapsed // 60, elapsed % 60
        if first:
            print(
                "  (Batch asíncrono: puede permanecer 0/N completados varios minutos u horas; "
                "no es un error.)"
            )
            first = False
        rc = job.request_counts
        if rc is not None:
            done = getattr(rc, "completed", None)
            tot_req = getattr(rc, "total", None)
            failed = getattr(rc, "failed", None)
            if done is not None and tot_req is not None:
                extra = f" fallidos={failed}" if failed else ""
                print(
                    f"  [{mm:02d}:{ss:02d}] Estado: {st}  "
                    f"({done}/{tot_req} completados){extra}"
                )
            else:
                print(f"  [{mm:02d}:{ss:02d}] Estado: {st}")
        else:
            print(f"  [{mm:02d}:{ss:02d}] Estado: {st}")
        if st in terminal:
            break
        time.sleep(poll_s)
    return client.batches.retrieve(bid)


def _run_one_batch_chunk(
    client: OpenAI,
    model: str,
    desc_series: pd.Series,
    prov_series: pd.Series,
    indices: list[int],
    max_chars: int,
    max_prov: int,
    temperature: float,
    poll_s: float,
    chunk_label: str,
) -> dict[int, tuple[str, str, str]]:
    if not indices:
        return {}
    tmp_in = tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", suffix=".jsonl", delete=False
    )
    tmp_path = Path(tmp_in.name)
    try:
        for i in indices:
            desc = desc_series.iloc[i]
            prov = prov_series.iloc[i]
            body = _completion_body(desc, prov, model, temperature, max_chars, max_prov)
            req = {
                "custom_id": f"row-{i}",
                "method": "POST",
                "url": "/v1/chat/completions",
                "body": body,
            }
            tmp_in.write(json.dumps(req, ensure_ascii=False) + "\n")
        tmp_in.close()

        with open(tmp_path, "rb") as f:
            batch_file = client.files.create(file=f, purpose="batch")

        batch_job = client.batches.create(
            input_file_id=batch_file.id,
            endpoint="/v1/chat/completions",
            completion_window="24h",
            metadata={"description": f"MEWP clasificacion {chunk_label}"},
        )
        bid = batch_job.id
        print(f"Batch {chunk_label}: {bid} ({len(indices)} filas; esperando…)")

        job = _wait_batch_job(client, bid, poll_s)
        if job.status != "completed":
            raise SystemExit(
                f"Batch {chunk_label} terminó con estado {job.status}. "
                f"Detalle: {getattr(job, 'errors', None)}"
            )

        out_id = job.output_file_id
        if not out_id:
            raise SystemExit(f"Batch {chunk_label} sin output_file_id")

        out_bytes = client.files.content(out_id)
        out_text = (
            out_bytes.text
            if hasattr(out_bytes, "text")
            else out_bytes.read().decode("utf-8")
        )

        err_id = job.error_file_id
        if err_id:
            err_bytes = client.files.content(err_id)
            err_txt = (
                err_bytes.text
                if hasattr(err_bytes, "text")
                else err_bytes.read().decode("utf-8")
            )
            nerr = len([x for x in err_txt.splitlines() if x.strip()])
            if nerr:
                print(f"Aviso ({chunk_label}): {nerr} líneas en archivo de errores")

        return _parse_batch_output_lines(out_text)
    finally:
        try:
            tmp_path.unlink(missing_ok=True)
        except OSError:
            pass


def _run_batch(
    client: OpenAI,
    model: str,
    desc_series: pd.Series,
    prov_series: pd.Series,
    max_chars: int,
    max_prov: int,
    temperature: float,
    poll_s: float,
    chunk_rows: int,
    n: int,
    ids_fila: list,
    checkpoint_path: Path,
    completed: dict[int, tuple[str, str, str]],
) -> dict[int, tuple[str, str, str]]:
    if chunk_rows < 1:
        chunk_rows = 300
    total_chunks = (n + chunk_rows - 1) // chunk_rows
    for c in range(total_chunks):
        start = c * chunk_rows
        end = min(start + chunk_rows, n)
        pending = [i for i in range(start, end) if i not in completed]
        label = f"{c + 1}/{total_chunks}"
        if not pending:
            print(f"Batch {label}: ya estaba hecho ({start}-{end - 1}), omitiendo.")
            continue
        part = _run_one_batch_chunk(
            client,
            model,
            desc_series,
            prov_series,
            pending,
            max_chars,
            max_prov,
            temperature,
            poll_s,
            label,
        )
        completed.update(part)
        _save_checkpoint(checkpoint_path, ids_fila, completed)
        print(
            f"  Checkpoint guardado: {checkpoint_path} "
            f"({len(completed)}/{n} filas)"
        )
    return completed


def _run_sync(
    client: OpenAI,
    model: str,
    desc_series: pd.Series,
    prov_series: pd.Series,
    max_chars: int,
    max_prov: int,
    temperature: float,
    sleep_s: float,
    n: int,
    ids_fila: list,
    checkpoint_path: Path,
    completed: dict[int, tuple[str, str, str]],
    save_every: int,
) -> dict[int, tuple[str, str, str]]:
    for i, (desc, prov) in enumerate(zip(desc_series, prov_series)):
        if i in completed:
            continue
        c, m, e = clasificar_fila_sync(
            client, model, desc, prov, max_chars, max_prov, temperature
        )
        completed[i] = (c, m, e)
        if (i + 1) % 25 == 0 or i == 0:
            print(f"Procesadas {i + 1}/{n} (completadas en total: {len(completed)})...")
        if sleep_s > 0:
            time.sleep(sleep_s)
        if save_every > 0 and (
            len(completed) % save_every == 0 or i == n - 1
        ):
            _save_checkpoint(checkpoint_path, ids_fila, completed)
            print(f"  Checkpoint: {checkpoint_path} ({len(completed)}/{n})")

    return completed


def _elegir_modo_interactivo(cfg: dict) -> bool:
    """True = batch, False = sync. Si no hay TTY, usa config."""
    if not sys.stdin.isatty():
        return bool(cfg.get("USE_BATCH_API", True))
    print()
    print("  Modo de ejecución:")
    print("    1 = Batch API (más barato, asíncrono, guarda tras cada lote)")
    print("    2 = Uno a uno / síncrono (inmediato, guarda cada SYNC_SAVE_EVERY filas)")
    default = "1" if cfg.get("USE_BATCH_API", True) else "2"
    try:
        r = input(f"  Elija 1 o 2 [{default}]: ").strip() or default
    except EOFError:
        r = default
    return r != "2"


def _preguntar_reanudar(checkpoint_path: Path) -> bool:
    if not checkpoint_path.exists():
        return False
    try:
        prev = _load_checkpoint(checkpoint_path)
    except Exception:
        return False
    if not prev:
        return False
    if not sys.stdin.isatty():
        return os.environ.get("MEWP_RESUME", "").strip().lower() in (
            "1",
            "true",
            "yes",
            "s",
            "si",
            "y",
        )
    print()
    print(f"  Existe checkpoint ({len(prev)} filas): {checkpoint_path}")
    try:
        r = input("  ¿Reanudar desde el checkpoint? [S/n]: ").strip().lower()
    except EOFError:
        return True
    return r in ("", "s", "si", "sí", "y", "yes")


def _completed_to_lists(
    n: int,
    ids_fila: list,
    completed: dict[int, tuple[str, str, str]],
) -> tuple[list[str], list[str], list[str]]:
    clasificaciones = []
    marcas = []
    estados = []
    for i in range(n):
        t = completed.get(i, ("NA", "NA", "NA"))
        clasificaciones.append(t[0])
        marcas.append(t[1])
        estados.append(t[2])
    return clasificaciones, marcas, estados


def main() -> None:
    base = Path(__file__).resolve().parent
    cfg = _load_config(base)
    excel_rel = cfg["INPUT_XLSX"]
    path = (base / excel_rel).resolve()
    if not path.exists():
        raise SystemExit(f"No se encontró el Excel: {path}")

    sheet = cfg["SHEET_NAME"]
    header_row = int(cfg.get("HEADER_ROW", 2))
    model = cfg.get("MODEL", "gpt-4o-mini")
    sleep_s = float(cfg.get("SLEEP_SECONDS", 0.0))
    max_chars = int(cfg.get("MAX_DESC_CHARS", 8000))
    max_prov = int(cfg.get("MAX_PROVEEDOR_CHARS", 400))
    temperature = float(cfg.get("TEMPERATURE", 0.0))
    batch_poll = float(cfg.get("BATCH_POLL_SECONDS", 30.0))
    batch_chunk_rows = int(cfg.get("BATCH_CHUNK_ROWS", 300))
    out_csv = str(cfg.get("OUTPUT_CSV", "Mepwpsforklif.csv"))
    checkpoint_name = str(cfg.get("CHECKPOINT_CSV", "Mepwpsforklif_checkpoint.csv"))
    checkpoint_path = base / checkpoint_name
    sync_save_every = int(cfg.get("SYNC_SAVE_EVERY", 25))

    use_batch = _elegir_modo_interactivo(cfg)

    client = OpenAI(api_key=_api_key(cfg))

    try:
        df = pd.read_excel(path, sheet_name=sheet, header=header_row, engine="openpyxl")
    except ValueError as e:
        raise SystemExit(
            f"No se pudo leer la hoja '{sheet}'. Revise SHEET_NAME en el JSON. Error: {e}"
        ) from e

    desc_col = next(
        (c for c in df.columns if "mercanc" in str(c).lower()),
        None,
    )
    if desc_col is None:
        raise SystemExit("No se encontró columna de descripción de mercancía")

    prov_col = "Proveedor" if "Proveedor" in df.columns else None
    n = len(df)

    desc_series = df[desc_col].fillna("").astype(str)
    prov_series = (
        df[prov_col].fillna("").astype(str) if prov_col else pd.Series([""] * n)
    )
    ids_fila = _id_fila_por_fila(df)

    resume = _preguntar_reanudar(checkpoint_path)
    if resume:
        completed = _load_checkpoint(checkpoint_path)
        print(f"Reanudando: {len(completed)} filas ya en checkpoint.")
    else:
        completed = {}
        if checkpoint_path.exists():
            try:
                checkpoint_path.unlink()
            except OSError:
                pass

    if use_batch:
        completed = _run_batch(
            client,
            model,
            desc_series,
            prov_series,
            max_chars,
            max_prov,
            temperature,
            batch_poll,
            batch_chunk_rows,
            n,
            ids_fila,
            checkpoint_path,
            completed,
        )
    else:
        completed = _run_sync(
            client,
            model,
            desc_series,
            prov_series,
            max_chars,
            max_prov,
            temperature,
            sleep_s,
            n,
            ids_fila,
            checkpoint_path,
            completed,
            sync_save_every,
        )

    clasificaciones, marcas, estados = _completed_to_lists(n, ids_fila, completed)

    csv_path = _guardar_csv_resultados(
        base, out_csv, ids_fila, clasificaciones, marcas, estados
    )
    _save_checkpoint(checkpoint_path, ids_fila, completed)

    print(f"Resultados CSV: {csv_path}")
    print(f"Checkpoint: {checkpoint_path}")
    print(f"(Excel origen no modificado: {path}, hoja {sheet})")
    print("Resumen clasificacion:", pd.Series(clasificaciones).value_counts().to_string())
    print("Resumen Estado:", pd.Series(estados).value_counts().to_string())


if __name__ == "__main__":
    main()
