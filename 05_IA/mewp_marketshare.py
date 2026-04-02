# -*- coding: utf-8 -*-
"""
Clasificación MEWP / Forklift sobre MARKETSHARE (OpenAI + Excel).

Ejecución: python mewp_marketshare.py

Flujo interactivo paso a paso (sin subcomandos): comprueba archivos, opción de
recuperar batches desde OpenAI (batch_ids.txt), clasificación por Batch API o
uno a uno, checkpoint cada N filas, y genera un .xlsx nuevo con las columnas
clasificacion (MEWPS|FORKLIFT|TELEHANDLER|NA), marca, Estado.

Clave de fila: **idx** (entero 0-based) = posición de la fila en el DataFrame
leído del Excel (misma fila que en la hoja, en orden, bajo HEADER_ROW). Los
batches usan custom_id row-{idx}. No usar columnas del libro tipo "Id fila" /
"id" para unir resultados: en MARKETSHARE pueden repetirse; solo **idx** es
estable para join con el Excel de salida.

Config: ConfigClasificadorMewp.json en esta carpeta (mantener las claves existentes;
opcional: BATCH_IDS_FILE, por defecto "batch_ids.txt"). Si la entrada no es una consola
interactiva, las preguntas usan el valor por defecto indicado y USE_BATCH_API para el modo.

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
_COLS_CRUZ = ["clasificacion", "marca", "Estado"]
_DEFAULT_BATCH_IDS = "batch_ids.txt"

_SYSTEM = (
    "Clasificador MEWP/forklift: Descripcion mercancia + Proveedor (MARKETSHARE). MERCANCIA NUEVA/USADA, MARCA/MODELO; a veces NO TIENE.\n"
    "Salida: solo JSON valido sin markdown. Claves: clasificacion, marca, Estado.\n\n"

    "clasificacion ∈ {MEWPS,FORKLIFT,TELEHANDLER,NA}:\n"
    "MEWPS — plataforma: tijera, boom/articulada, telescopica, man lift, AWP, spider, mastil vertical; brazo articulado electrico como plataforma de trabajo.\n"
    "FORKLIFT — montacargas, reach truck, order picker con mastil, transpaleta/apilador motorizado; carretilla telescopica si no cumple el criterio estricto de TELEHANDLER abajo.\n"
    "TELEHANDLER — SOLO si el texto nombra explicitamente telehandler, telescopic handler, manipulador telescopico tipo telehandler (o sinonimo inequivoco), "
    "o describe el equipo sin ambiguedad como tal; ante la menor duda frente a montacargas o plataforma, NO uses TELEHANDLER (elige MEWPS, FORKLIFT o NA).\n"
    "NA — repuesto/parte sin equipo; ascensor/escalera pasajeros; otro bien; duda entre clases.\n\n"

    "Marca: SOLO fabricante del equipo principal en MAYUSCULAS; si no, NA. Ignorar motor/bateria/neumatico salvo unico bien.\n"
    "Desempate MARCA vs Proveedor: si el texto declara MARCA y el Proveedor indica otro fabricante distinto, prima la MARCA del texto; "
    "si modelo/serie apunta a un fabricante y el texto declara otra MARCA distinta, prima la MARCA del texto salvo error de digitacion evidente.\n"
    "Orden de comprobacion (primera regla con confianza): "
    "(1) MARCA explicita en texto; "
    "(2) modelo/serie inequivoca — ej. GS-1930,Z-45→GENIE; 1930ES,450AJ→JLG; Compact10,HA16→HAULOTTE; 8FGCU→TOYOTA; EFG216→JUNGHEINRICH; FC5200→CROWN; DP25→CAT (orientativo); "
    "(3) Proveedor=fabricante sin contradiccion con (1)-(2); "
    "(4) NO TIENE + proveedor generico + modelo ambiguo → NA.\n"
    "Precision>cobertura: ante duda, marca NA.\n\n"

    "Estado ∈ {NUEVO,USADO,NA}: NUEVO nuevo/MERCANCIA NUEVA; USADO usado/refurbished/reman; NA si no claro.\n\n"

    "Repuesto sin equipo completo: clasificacion NA, marca NA."
)


# --- Entrada interactiva / entorno ---


def _is_tty() -> bool:
    return sys.stdin.isatty() and sys.stdout.isatty()


def _ask_yes_no(prompt: str, default_yes: bool = True) -> bool:
    if not _is_tty():
        return default_yes
    suf = "[S/n]" if default_yes else "[s/N]"
    try:
        r = input(f"{prompt} {suf}: ").strip().lower()
    except EOFError:
        return default_yes
    if not r:
        return default_yes
    return r in ("s", "si", "sí", "y", "yes", "1", "true")


def _ask_line(prompt: str, default: str = "") -> str:
    if not _is_tty():
        return default
    try:
        r = input(f"{prompt} [{default}]: ").strip()
    except EOFError:
        return default
    return r if r else default


def _ask_choice_batch_or_sync(cfg: dict) -> bool:
    """True = Batch API, False = síncrono."""
    if not _is_tty():
        return bool(cfg.get("USE_BATCH_API", True))
    default = "1" if cfg.get("USE_BATCH_API", True) else "2"
    print()
    print("  Modo de ejecución:")
    print("    1 = Batch API (asíncrono, guarda tras cada lote de BATCH_CHUNK_ROWS)")
    print("    2 = Uno a uno (inmediato, guarda cada SYNC_SAVE_EVERY filas)")
    r = _ask_line("  Elija 1 o 2", default)
    return r.strip() != "2"


# --- Config y API ---


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
    if c not in ("MEWPS", "FORKLIFT", "TELEHANDLER", "NA"):
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


def _checkpoint_cell_str(val) -> str:
    try:
        if val is None or pd.isna(val):
            return "NA"
    except (TypeError, ValueError):
        pass
    s = str(val).strip()
    if not s or s.lower() in ("nan", "none", "<na>"):
        return "NA"
    return s


def _load_checkpoint(path: Path) -> dict[int, tuple[str, str, str]]:
    if not path.exists():
        return {}
    try:
        df = pd.read_csv(path, encoding="utf-8-sig", keep_default_na=False, na_values=[])
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
            _checkpoint_cell_str(row.get("clasificacion", "NA")),
            _checkpoint_cell_str(row.get("marca", "NA")),
            _checkpoint_cell_str(row.get("Estado", "NA")),
        )
    return out


def _save_checkpoint(
    path: Path,
    completed: dict[int, tuple[str, str, str]],
) -> None:
    """Solo columnas idx + resultados. idx = índice 0-based estable (no Id fila del Excel)."""
    rows = []
    for idx in sorted(completed.keys()):
        c, m, e = completed[idx]
        c, m, e = _checkpoint_cell_str(c), _checkpoint_cell_str(m), _checkpoint_cell_str(e)
        rows.append({"idx": idx, "clasificacion": c, "marca": m, "Estado": e})
    pd.DataFrame(rows).to_csv(path, index=False, encoding="utf-8-sig")


def _guardar_csv_resultados(
    base: Path,
    nombre_csv: str,
    n: int,
    clasificaciones: list[str],
    marcas: list[str],
    estados: list[str],
) -> Path:
    """Una fila por posición 0..n-1; columna idx alinea con el DataFrame del Excel."""
    out = base / nombre_csv
    pd.DataFrame(
        {
            "idx": list(range(n)),
            "clasificacion": clasificaciones,
            "marca": marcas,
            "Estado": estados,
        }
    ).to_csv(out, index=False, encoding="utf-8-sig")
    return out


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
        _save_checkpoint(checkpoint_path, completed)
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
    checkpoint_path: Path,
    completed: dict[int, tuple[str, str, str]],
    save_every: int,
) -> dict[int, tuple[str, str, str]]:
    log_every = max(1, save_every)
    for i, (desc, prov) in enumerate(zip(desc_series, prov_series)):
        if i in completed:
            continue
        c, m, e = clasificar_fila_sync(
            client, model, desc, prov, max_chars, max_prov, temperature
        )
        completed[i] = (c, m, e)
        if (i + 1) % log_every == 0 or i == 0:
            print(f"Procesadas {i + 1}/{n} (completadas en total: {len(completed)})...")
        if sleep_s > 0:
            time.sleep(sleep_s)
        se = save_every if save_every > 0 else n
        if se > 0 and (len(completed) % se == 0 or i == n - 1):
            _save_checkpoint(checkpoint_path, completed)
            print(f"  Checkpoint: {checkpoint_path} ({len(completed)}/{n})")

    return completed


def _completed_to_lists(
    n: int,
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


def _leer_batch_ids(path: Path) -> list[str]:
    if not path.exists():
        return []
    ids: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if not line.startswith("batch_"):
            line = line.split("/")[-1].strip()
        ids.append(line)
    return ids


def _descargar_un_batch(client: OpenAI, bid: str) -> dict[int, tuple[str, str, str]]:
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


def _fusionar_recuperar(
    base: Path,
    cfg: dict,
    merged: dict[int, tuple[str, str, str]],
) -> None:
    excel_rel = cfg["INPUT_XLSX"]
    path_xlsx = (base / excel_rel).resolve()
    if not path_xlsx.exists():
        raise SystemExit(f"No se encontró el Excel: {path_xlsx}")

    sheet = cfg["SHEET_NAME"]
    header_row = int(cfg.get("HEADER_ROW", 2))
    out_csv = str(cfg.get("OUTPUT_CSV", "Mepwpsforklif.csv"))
    checkpoint_name = str(cfg.get("CHECKPOINT_CSV", "Mepwpsforklif_checkpoint.csv"))
    checkpoint_path = base / checkpoint_name

    if not merged:
        print("No se obtuvo ninguna fila nueva de los batches (¿todos incompletos?).")
        return

    print(f"Total índices únicos recuperados del/los batch: {len(merged)}")

    existente = _load_checkpoint(checkpoint_path)
    if existente:
        print(
            f"Checkpoint previo: {len(existente)} filas; se fusionan "
            f"(los batch recuperados pisan mismo idx)."
        )
    combined = {**existente, **merged}

    df = pd.read_excel(path_xlsx, sheet_name=sheet, header=header_row, engine="openpyxl")
    n = len(df)

    _save_checkpoint(checkpoint_path, combined)
    clasificaciones, marcas, estados = _completed_to_lists(n, combined)
    _guardar_csv_resultados(
        base, out_csv, n, clasificaciones, marcas, estados
    )

    print(f"Checkpoint actualizado: {checkpoint_path} ({len(combined)} filas con resultado)")
    print("Resumen clasificacion:", pd.Series(clasificaciones).value_counts().head(15).to_string())


def _aplicar_por_idx(df: pd.DataFrame, chk: pd.DataFrame) -> pd.DataFrame:
    """
    Join: resultado[idx] -> fila Excel en posición idx (índice 0-based del DataFrame).
    No usa columnas del libro tipo Id fila (pueden repetirse).
    """
    if "idx" not in chk.columns:
        raise ValueError("checkpoint sin columna idx")
    m = chk.drop_duplicates(subset=["idx"], keep="last").set_index("idx")
    out = df.copy().reset_index(drop=True)
    for c in _COLS_CRUZ:
        if c in m.columns:
            out[c] = out.index.map(lambda i: m.at[i, c] if i in m.index else pd.NA)
        else:
            out[c] = pd.NA
    return out


def _aplicar_por_orden(df: pd.DataFrame, res: pd.DataFrame) -> pd.DataFrame:
    if len(df) != len(res):
        raise SystemExit(
            f"Filas Excel ({len(df)}) ≠ filas resultado ({len(res)}). "
            "Use el checkpoint con idx o un CSV generado para el mismo libro."
        )
    out = df.copy().reset_index(drop=True)
    for c in _COLS_CRUZ:
        if c in res.columns:
            out[c] = res[c].values
        else:
            out[c] = pd.NA
    return out


def escribir_xlsx_clasificado(base: Path, cfg: dict) -> Path:
    """
    Une INPUT_XLSX con checkpoint (preferido) o OUTPUT_CSV y escribe OUTPUT_XLSX_CRUZ.
    Elimina columnas clasificacion/marca/Estado previas del Excel y las vuelve a añadir.
    """
    excel_rel = cfg.get("INPUT_XLSX", "MARKETSHARE.xlsx")
    path_xlsx = (base / excel_rel).resolve()
    if not path_xlsx.exists():
        raise SystemExit(f"No se encontró el Excel: {path_xlsx}")

    sheet = cfg.get("SHEET_NAME", "MEWPS_FORKLIFT")
    header_row = int(cfg.get("HEADER_ROW", 2))
    salida_nom = str(cfg.get("OUTPUT_XLSX_CRUZ", "MARKETSHARE_MEWP_clasificado.xlsx"))
    salida = base / salida_nom
    if not salida.is_absolute():
        salida = salida.resolve()

    cp = base / str(cfg.get("CHECKPOINT_CSV", "Mepwpsforklif_checkpoint.csv"))
    csv_alt = base / str(cfg.get("OUTPUT_CSV", "Mepwpsforklif.csv"))

    df = pd.read_excel(
        path_xlsx,
        sheet_name=sheet,
        header=header_row,
        engine="openpyxl",
    )

    for c in _COLS_CRUZ:
        if c in df.columns:
            df = df.drop(columns=[c])

    read_kw = {"encoding": "utf-8-sig", "keep_default_na": False, "na_values": []}

    if cp.exists():
        chk = pd.read_csv(cp, **read_kw)
        if "idx" in chk.columns:
            out = _aplicar_por_idx(df, chk)
            fuente = f"checkpoint por idx ({cp.name})"
        else:
            out = _aplicar_por_orden(df, chk)
            fuente = (
                f"checkpoint sin idx, por orden de filas (legado; preferir regenerar con idx) "
                f"({cp.name})"
            )
    elif csv_alt.exists():
        res = pd.read_csv(csv_alt, **read_kw)
        if "idx" in res.columns:
            out = _aplicar_por_idx(df, res)
            fuente = f"CSV por idx ({csv_alt.name})"
        else:
            out = _aplicar_por_orden(df, res)
            fuente = (
                f"CSV sin columna idx, por orden (legado; riesgo si Id fila repetía) "
                f"({csv_alt.name})"
            )
    else:
        raise SystemExit(
            f"No hay checkpoint ({cp.name}) ni CSV ({csv_alt.name}). "
            "Ejecute clasificación o recuperación de batches primero."
        )

    if salida.resolve() == path_xlsx.resolve():
        raise SystemExit("El archivo de salida no puede ser el mismo que el Excel de entrada.")

    out.to_excel(salida, sheet_name=str(sheet)[:31], index=False, engine="openpyxl")

    col0 = _COLS_CRUZ[0]
    n_ok = out[col0].notna().sum() if col0 in out.columns else 0
    print(f"Excel clasificado: {fuente}")
    print(f"Filas: {len(out)} | Con clasificacion: {n_ok}")
    print(f"Guardado: {salida}")
    return salida


def _listar_estado_batches(client: OpenAI, batch_ids: list[str]) -> None:
    print()
    print("  Estado en OpenAI de los IDs en batch_ids.txt:")
    for bid in batch_ids:
        j = client.batches.retrieve(bid)
        rc = j.request_counts
        extra = ""
        if rc is not None:
            extra = f" completed={getattr(rc, 'completed', '?')}/{getattr(rc, 'total', '?')}"
        print(f"    {bid}  status={j.status}{extra}")


def _paso_recuperar_batches(base: Path, cfg: dict, client: OpenAI) -> None:
    ids_nom = str(cfg.get("BATCH_IDS_FILE", _DEFAULT_BATCH_IDS))
    ids_path = base / ids_nom
    batch_ids = _leer_batch_ids(ids_path)
    if not batch_ids:
        print()
        print(f"  No hay IDs de batch en {ids_path} (archivo ausente o vacío). Se omite recuperación.")
        return

    print()
    print(f"  Descargando y fusionando {len(batch_ids)} batch(es) desde OpenAI…")
    if _is_tty():
        ver = _ask_yes_no("  ¿Listar antes el estado de cada batch en el panel?", True)
        if ver:
            _listar_estado_batches(client, batch_ids)

    merged: dict[int, tuple[str, str, str]] = {}
    for bid in batch_ids:
        print(f"Procesando {bid}...")
        merged.update(_descargar_un_batch(client, bid))

    if merged:
        _fusionar_recuperar(base, cfg, merged)
    else:
        print("No se fusionó nada nuevo desde la API.")


def main() -> None:
    base = Path(__file__).resolve().parent
    cfg = _load_config(base)

    excel_rel = cfg.get("INPUT_XLSX", "MARKETSHARE.xlsx")
    path_excel = (base / excel_rel).resolve()
    out_xlsx_nom = str(cfg.get("OUTPUT_XLSX_CRUZ", "MARKETSHARE_MEWP_clasificado.xlsx"))
    path_out_xlsx = (base / out_xlsx_nom).resolve()
    checkpoint_name = str(cfg.get("CHECKPOINT_CSV", "Mepwpsforklif_checkpoint.csv"))
    checkpoint_path = base / checkpoint_name
    out_csv = str(cfg.get("OUTPUT_CSV", "Mepwpsforklif.csv"))

    print()
    print("=== Clasificación MEWP / Forklift (MARKETSHARE) ===")
    print()

    # 1) Excel de entrada
    if not path_excel.exists():
        raise SystemExit(f"No se encontró el Excel de entrada: {path_excel}")
    print(f"Excel de entrada OK: {path_excel}")

    # 2) Archivo de salida .xlsx ya existe
    if path_out_xlsx.exists():
        print()
        print(f"  Ya existe el archivo de salida: {path_out_xlsx}")
        if not _ask_yes_no("  ¿Sobrescribirlo al final del proceso?", True):
            raise SystemExit("Cancelado por el usuario (no sobrescribir salida).")

    # 3) Checkpoint / datos previos
    chk_prev = _load_checkpoint(checkpoint_path)
    completed: dict[int, tuple[str, str, str]] = {}
    if checkpoint_path.exists() and not chk_prev:
        print(f"  Aviso: {checkpoint_path} existe pero no tiene columna idx válida; se ignora.")
    elif chk_prev:
        print()
        print(f"  Hay checkpoint con {len(chk_prev)} fila(s) clasificada(s): {checkpoint_path}")
        if _ask_yes_no("  ¿Reanudar desde el último guardado (solo faltantes)?", True):
            completed = dict(chk_prev)
        else:
            if _ask_yes_no("  ¿Borrar el checkpoint y empezar de cero?", True):
                try:
                    checkpoint_path.unlink(missing_ok=True)
                except OSError as e:
                    raise SystemExit(f"No se pudo borrar el checkpoint: {e}") from e
                print("  Checkpoint eliminado.")
                completed = {}
            else:
                raise SystemExit(
                    "Para empezar de cero debe permitir borrar el checkpoint, "
                    "o elija reanudar para no perder datos."
                )

    client = OpenAI(api_key=_api_key(cfg))

    # 4) Recuperar desde OpenAI (batch_ids.txt)
    ids_nom = str(cfg.get("BATCH_IDS_FILE", _DEFAULT_BATCH_IDS))
    ids_path = base / ids_nom
    raw_ids = _leer_batch_ids(ids_path)
    if raw_ids:
        print()
        print(f"  {ids_path} contiene {len(raw_ids)} batch_id(s).")
        if _ask_yes_no("  ¿Recuperar resultados desde la sesión de batch en OpenAI?", False):
            _paso_recuperar_batches(base, cfg, client)
            chk_after = _load_checkpoint(checkpoint_path)
            if chk_after:
                completed = dict(chk_after)

    sheet = cfg["SHEET_NAME"]
    header_row = int(cfg.get("HEADER_ROW", 2))
    model = cfg.get("MODEL", "gpt-4o-mini")
    sleep_s = float(cfg.get("SLEEP_SECONDS", 0.0))
    max_chars = int(cfg.get("MAX_DESC_CHARS", 8000))
    max_prov = int(cfg.get("MAX_PROVEEDOR_CHARS", 400))
    temperature = float(cfg.get("TEMPERATURE", 0.0))
    batch_poll = float(cfg.get("BATCH_POLL_SECONDS", 30.0))
    batch_chunk_rows = int(cfg.get("BATCH_CHUNK_ROWS", 300))
    sync_save_every = int(cfg.get("SYNC_SAVE_EVERY", 25))

    try:
        df = pd.read_excel(path_excel, sheet_name=sheet, header=header_row, engine="openpyxl")
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

    pendientes = sum(1 for i in range(n) if i not in completed)
    print()
    print(f"  Filas en hoja: {n} | Ya con resultado en memoria/checkpoint: {len(completed)} | Pendientes: {pendientes}")

    if pendientes == 0:
        print()
        print("  No hay filas pendientes; solo se generará el Excel de salida.")
    else:
        use_batch = _ask_choice_batch_or_sync(cfg)
        print()
        if not _ask_yes_no("  ¿Continuar con la clasificación de las filas pendientes?", True):
            print("  Clasificación omitida; se intentará solo exportar Excel con datos actuales.")

        else:
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
                    checkpoint_path,
                    completed,
                    sync_save_every,
                )

            clasificaciones, marcas, estados = _completed_to_lists(n, completed)
            csv_path = _guardar_csv_resultados(
                base, out_csv, n, clasificaciones, marcas, estados
            )
            _save_checkpoint(checkpoint_path, completed)
            print()
            print(f"Resultados CSV: {csv_path}")
            print(f"Checkpoint: {checkpoint_path}")
            print("Resumen clasificacion:", pd.Series(clasificaciones).value_counts().to_string())
            print("Resumen Estado:", pd.Series(estados).value_counts().to_string())

    print()
    print("--- Generando Excel con columnas clasificacion, marca, Estado ---")
    if not _ask_yes_no("  ¿Exportar ahora el .xlsx clasificado?", True):
        print("  Exportación cancelada.")
        return

    escribir_xlsx_clasificado(base, cfg)
    print()
    print("Listo.")


if __name__ == "__main__":
    main()
