"""
Persistencia de filtros por pestaña principal en JSON (sobrevive al cerrar la app).
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Final

import streamlit as st

_PREFS_PATH: Final[Path] = Path(__file__).resolve().parent.parent / "user_filter_prefs.json"

# Claves de widgets / estado a guardar por pestaña (Streamlit session_state).
TAB_FILTER_KEYS: Final[dict[str, list[str]]] = {
    "consulta": [
        "consulta_txt_busqueda",
        "consulta_coincidencias",
        "consulta_origen_disp_umbral",
    ],
    "margen": [
        "margen_filtro_busqueda",
        "margen_filtro_modelo",
        "margen_filtro_bodega",
        "margen_filtro_rotacion",
        "margen_precio_range",
        "margen_precio_desde",
        "margen_precio_hasta",
        "margen_precio_col",
        "margen_exist_range",
        "margen_exist_desde",
        "margen_exist_hasta",
        "margen_filtro_instalacion",
        "margen_filtro_sistema",
        "margen_pct_range",
        "margen_pct_desde",
        "margen_pct_hasta",
        "margen_filtro_margen_col",
    ],
    "auditoria_refs": [
        "aud_refs_txt_busqueda",
        "aud_refs_modelo_txt",
        "aud_refs_sem_sel",
        "aud_refs_rot_sel",
        "aud_precio_range",
        "aud_precio_desde",
        "aud_precio_hasta",
        "aud_precio_col",
        "aud_exist_range",
        "aud_exist_desde",
        "aud_exist_hasta",
        "aud_exist_col",
        "aud_refs_sistema_sel",
        "aud_dias_range",
        "aud_dias_desde",
        "aud_dias_hasta",
        "aud_dias_sig",
        "aud_umbral_var_compra",
        "aud_umbral_var_compra_num",
        "aud_umbral_var_costo",
        "aud_umbral_var_costo_num",
        "aud_solo_significativas",
        "aud_refs_factor_usa_br",
        "aud_refs_factor_otros",
        "aud_refs_top_n",
        "aud_refs_top_grupos",
    ],
}


def _to_jsonable(val: Any) -> Any:
    if isinstance(val, tuple):
        return [_to_jsonable(x) for x in val]
    if isinstance(val, (str, int, float, bool)) or val is None:
        return val
    if isinstance(val, list):
        return [_to_jsonable(x) for x in val]
    try:
        if hasattr(val, "item"):
            return val.item()
    except Exception:
        pass
    return str(val)


def _from_jsonable(key: str, val: Any) -> Any:
    if val is None:
        return None
    if key.endswith("_range") and isinstance(val, list) and len(val) == 2:
        try:
            return (float(val[0]), float(val[1]))
        except (TypeError, ValueError):
            return tuple(val)
    return val


def _read_all_prefs() -> dict[str, Any]:
    if not _PREFS_PATH.exists():
        return {}
    try:
        raw = json.loads(_PREFS_PATH.read_text(encoding="utf-8"))
        return raw if isinstance(raw, dict) else {}
    except Exception:
        return {}


def _write_all_prefs(data: dict[str, Any]) -> None:
    try:
        _PREFS_PATH.parent.mkdir(parents=True, exist_ok=True)
        _PREFS_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    except OSError:
        pass


def load_filter_prefs_into_session() -> None:
    """Hidrata session_state desde disco una sola vez por sesión del navegador."""
    if st.session_state.get("_filter_prefs_hydrated"):
        return
    st.session_state["_filter_prefs_hydrated"] = True
    data = _read_all_prefs()
    for _tab, chunk in data.items():
        if not isinstance(chunk, dict):
            continue
        for k, val in chunk.items():
            if k in st.session_state:
                continue
            st.session_state[k] = _from_jsonable(k, val)


def save_tab_filter_prefs(tab: str) -> None:
    """Guarda en JSON el estado actual de los filtros de una pestaña."""
    keys = TAB_FILTER_KEYS.get(tab)
    if not keys:
        return
    data = _read_all_prefs()
    chunk: dict[str, Any] = {}
    for k in keys:
        if k in st.session_state:
            try:
                chunk[k] = _to_jsonable(st.session_state[k])
            except Exception:
                continue
    data[tab] = chunk
    _write_all_prefs(data)


def clear_tab_filter_prefs(tab: str) -> None:
    """Elimina claves de session_state y borra la entrada en el JSON para esa pestaña."""
    keys = TAB_FILTER_KEYS.get(tab)
    if keys:
        for k in keys:
            st.session_state.pop(k, None)
    data = _read_all_prefs()
    data[tab] = {}
    _write_all_prefs(data)


def render_reset_filters_button(tab: str) -> None:
    """Botón compacto; si se pulsa, limpia filtros de la pestaña y hace rerun."""
    if st.button(
        "Reiniciar filtros",
        key=f"_btn_reset_filters_{tab}",
        type="secondary",
        help="Vuelve a los valores por defecto y actualiza el archivo de preferencias.",
    ):
        clear_tab_filter_prefs(tab)
        st.rerun()
