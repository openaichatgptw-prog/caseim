"""
Utilidades centralizadas para normalizar referencias.

Reglas aplicadas:
- trim y eliminación de tabs/saltos de línea
- "_" -> "-"
- eliminación de caracteres especiales (se conservan . - " / y espacio)
- limpieza de puntos/guiones repetidos
- espacios alrededor de "-" y espacios dobles
- resultado en mayúsculas
"""

from __future__ import annotations

import re


def normalize_reference_text(value: object) -> str:
    s = str(value or "").strip()
    if not s:
        return ""
    s = re.sub(r"\t|\n|\r", "", s)
    s = s.replace("_", "-")
    s = re.sub(r'[^A-Za-z0-9.\-"/ ]', "", s)
    s = re.sub(r"^\.+|\.+$", "", s)
    s = re.sub(r"\.{2,}", ".", s)
    s = re.sub(r"-{2,}", "-", s)
    s = re.sub(r"\s*-\s*", "-", s)
    s = re.sub(r" {2,}", " ", s)
    return s.upper().strip()


def normalize_reference_series_pd(serie):
    import pandas as pd

    if not isinstance(serie, pd.Series):
        serie = pd.Series(serie)
    return serie.astype("string").map(normalize_reference_text)


def normalize_reference_expr_polars(col):
    import polars as pl

    if not isinstance(col, pl.Expr):
        raise TypeError("Se esperaba una expresión de Polars (pl.Expr).")
    return (
        col.cast(pl.Utf8)
        .str.strip_chars()
        .str.replace_all(r"\t|\n|\r", "")
        .str.replace_all(r"_", "-")
        .str.replace_all(r"[^A-Za-z0-9.\-\"/ ]", "")
        .str.replace_all(r"^\.+|\.+$", "")
        .str.replace_all(r"\.{2,}", ".")
        .str.replace_all(r"-{2,}", "-")
        .str.replace_all(r"\s*-\s*", "-")
        .str.replace_all(r" {2,}", " ")
        .str.to_uppercase()
    )


def normalize_reference_expr_sql(campo: str) -> str:
    c = "CAST({campo} AS VARCHAR)".format(campo=campo)
    return (
        "UPPER(trim("
        "regexp_replace("
        "regexp_replace("
        "regexp_replace("
        "regexp_replace("
        "regexp_replace("
        "regexp_replace("
        "replace("
        "trim(replace(replace(replace({c},"
        "chr(9),''),chr(10),''),chr(13),'')"
        "),"
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
