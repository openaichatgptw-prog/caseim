"""
Pipeline de consolidación de precios y disponibilidades por referencia.

Este script:
1. Lee rutas desde archivo config.ini.
2. Informa cómo deben estar construidos los archivos Excel.
3. Lee precios desde:
   - Columna A: referencia
   - Columna I: precio
4. Lee disponibilidades desde:
   - Columna A: referencia
   - Columna I: disp_br
   - Columna L: disp_eur
   - Columna M: disp_usa
5. Consolida precios + disponibilidades.
6. Guarda resultado en DuckDB y CSV.

Salida:
    referencia | precio_usa | precio_br | precio_eur | disp_usa | disp_br | disp_eur
"""

import polars as pl
import duckdb
from pathlib import Path
import configparser


# =========================================================
# Cargar configuración
# =========================================================
BASE_DIR = Path(__file__).parent
CONFIG_PATH = BASE_DIR / "config.ini"

if not CONFIG_PATH.exists():
    raise FileNotFoundError(f"No se encontró el archivo config.ini en: {CONFIG_PATH}")

config = configparser.ConfigParser()
config.read(CONFIG_PATH)

EXCEL_PRECIOS = Path(config["ARCHIVOS"]["excel_precios"])
EXCEL_DISP = Path(config["ARCHIVOS"]["excel_disponibilidad"])

DUCKDB_PATH = BASE_DIR / config["SALIDA"]["duckdb"]
CSV_OUT = BASE_DIR / config["SALIDA"]["csv"]


# =========================================================
# Información al usuario
# =========================================================
def mostrar_instrucciones_archivos():
    print("\n📘 ESTRUCTURA REQUERIDA DE LOS ARCHIVOS EXCEL\n")

    print("📄 Archivo de PRECIOS debe tener:")
    print("  - Hojas llamadas exactamente: USA, BRASIL, EUR")
    print("  - Columnas:")
    print("      Columna A = referencia")
    print("      Columna I = precio")
    print("  - Debe tener encabezados en la primera fila\n")

    print("📄 Archivo de DISPONIBILIDADES debe tener:")
    print("  - Hoja llamada: LISTA AGCS")
    print("  - Columnas:")
    print("      Columna A = referencia")
    print("      Columna I = disp_br")
    print("      Columna L = disp_eur")
    print("      Columna M = disp_usa")
    print("  - Debe tener encabezados en la primera fila\n")


# =========================================================
# Validación de archivos
# =========================================================
def validar_archivos():
    print("Ruta Excel precios:", EXCEL_PRECIOS.resolve())
    print("Ruta Excel disponibilidad:", EXCEL_DISP.resolve())

    if not EXCEL_PRECIOS.exists():
        raise FileNotFoundError(f"No se encontró el archivo de precios: {EXCEL_PRECIOS}")

    if not EXCEL_DISP.exists():
        raise FileNotFoundError(f"No se encontró el archivo de disponibilidad: {EXCEL_DISP}")


# =========================================================
# Lectura de precios
# =========================================================
def leer_hoja_precio(excel_path: Path, sheet_name: str, col_precio_out: str) -> pl.DataFrame:
    """
    Lee una hoja del Excel de precios y devuelve referencia + precio.
    """
    df = pl.read_excel(excel_path, sheet_name=sheet_name, has_header=True)
    cols = df.columns

    return (
        df.select([
            pl.col(cols[0])
              .cast(pl.Utf8)
              .str.strip_chars()
              .alias("referencia"),

            pl.col(cols[8])
              .cast(pl.Float64)
              .alias(col_precio_out)
        ])
        .filter(pl.col("referencia").is_not_null() & (pl.col("referencia") != ""))
        .unique(subset=["referencia"], keep="first")
    )


# =========================================================
# Lectura de disponibilidades
# =========================================================
def leer_disponibilidades(excel_disp_path: Path) -> pl.DataFrame:
    """
    Lee el archivo Excel de disponibilidades por origen.
    """
    df_raw = pl.read_excel(excel_disp_path, sheet_name="LISTA AGCS", has_header=True)
    cols = df_raw.columns

    return (
        df_raw.select([
            pl.col(cols[0])
              .cast(pl.Utf8)
              .str.strip_chars()
              .alias("referencia"),

            pl.col(cols[8]).cast(pl.Int64).alias("disp_br"),
            pl.col(cols[11]).cast(pl.Int64).alias("disp_eur"),
            pl.col(cols[12]).cast(pl.Int64).alias("disp_usa"),
        ])
        .filter(pl.col("referencia").is_not_null() & (pl.col("referencia") != ""))
        .unique(subset=["referencia"], keep="first")
    )


# =========================================================
# Pipeline principal
# =========================================================
def ejecutar_pipeline(excel_precios: Path, excel_disp: Path) -> pl.DataFrame:
    print("🔹 Leyendo precios...")
    df_usa = leer_hoja_precio(excel_precios, "USA", "precio_usa")
    df_br = leer_hoja_precio(excel_precios, "BRASIL", "precio_br")
    df_eur = leer_hoja_precio(excel_precios, "EUR", "precio_eur")

    print("Filas USA :", df_usa.height)
    print("Filas BR  :", df_br.height)
    print("Filas EUR :", df_eur.height)

    df_refs = pl.concat([
        df_usa.select("referencia"),
        df_br.select("referencia"),
        df_eur.select("referencia")
    ]).unique()

    df_precios = (
        df_refs
        .join(df_usa, on="referencia", how="left")
        .join(df_br, on="referencia", how="left")
        .join(df_eur, on="referencia", how="left")
    )

    print("🔹 Leyendo disponibilidades...")
    df_disp = leer_disponibilidades(excel_disp)
    print("Filas disponibilidad:", df_disp.height)

    df_final = (
        df_precios
        .join(df_disp, on="referencia", how="left")
        .sort("referencia")
    )

    return df_final


# =========================================================
# Guardado
# =========================================================
def guardar_resultados(df_final: pl.DataFrame):
    with duckdb.connect(DUCKDB_PATH) as con:
        con.register("tmp_precios", df_final.to_pandas())

        con.execute("""
        CREATE OR REPLACE TABLE precios_consolidados AS
        SELECT * FROM tmp_precios
        """)

        con.execute(f"""
        COPY precios_consolidados
        TO '{CSV_OUT.as_posix()}'
        WITH (HEADER, DELIMITER ',');
        """)

        print("\nMuestra de datos:")
        print(con.execute("SELECT * FROM precios_consolidados LIMIT 10").df())


# =========================================================
# Main
# =========================================================
def main():
    print("🚀 Pipeline de consolidación de precios\n")

    mostrar_instrucciones_archivos()
    validar_archivos()

    df_final = ejecutar_pipeline(EXCEL_PRECIOS, EXCEL_DISP)
    guardar_resultados(df_final)

    print("\n✅ Proceso terminado correctamente")
    print("Archivos generados:")
    print("-", DUCKDB_PATH)
    print("-", CSV_OUT)


if __name__ == "__main__":
    main()