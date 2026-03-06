"""
Pipeline unificado de ítems y precios de lista.

Este script:
1. Lee configuración desde archivo config.ini.
2. Extrae ítems desde SQL Server.
3. Normaliza referencias alternas.
4. Cruza con precios consolidados (DuckDB).
5. Calcula referencia ganadora por familia (precio_familia).
6. Cruza con Excel de precios de lista.
7. Exporta resultado final a Excel.

Salida:
    Precio_De_Lista_con_Precios.xlsx
"""

# =========================================================
# 0. IMPORTS
# =========================================================
import pyodbc
import pandas as pd
import duckdb
import configparser
from pathlib import Path

# =========================================================
# 1. BASE DIR + CONFIG
# =========================================================
BASE_DIR = Path(__file__).parent
CONFIG_PATH = BASE_DIR / "config.ini"

if not CONFIG_PATH.exists():
    raise FileNotFoundError(f"No se encontró el archivo config.ini en: {CONFIG_PATH}")

config = configparser.ConfigParser()
config.read(CONFIG_PATH)

# =========================================================
# 2. PARÁMETROS DESDE CONFIG
# =========================================================
SERVER   = config["SQLSERVER"]["SERVER"]
DATABASE = config["SQLSERVER"]["DATABASE"]
DB_USER  = config["SQLSERVER"]["DB_USER"]
DB_PASS  = config["SQLSERVER"]["DB_PASS"]

DUCKDB_ITEMS   = BASE_DIR / config["RUTAS"]["duckdb_items"]
DUCKDB_PRECIOS = BASE_DIR / config["RUTAS"]["duckdb_precios"]
DUCKDB_MAIN    = BASE_DIR / config["RUTAS"]["duckdb_main"]

EXCEL_LISTA_PATH = Path(config["RUTAS"]["excel_lista"])
EXCEL_OUT        = BASE_DIR / config["RUTAS"]["excel_salida"]

FACTOR_BR  = float(config["FACTORES"]["FACTOR_BR"])
FACTOR_USA = float(config["FACTORES"]["FACTOR_USA"])
FACTOR_EUR = float(config["FACTORES"]["FACTOR_EUR"])

TABLE_ITEMS = config["TABLAS"]["table_items"]
TABLE_ITEMS_PRECIOS = config["TABLAS"]["table_items_precios"]

SQL_QUERY = config["SQL"]["query_items"]

# =========================================================
# 3. EXTRACCIÓN SQL SERVER → DUCKDB
# =========================================================
def extraer_items_sqlserver():
    print("🔹 Conectando a SQL Server...")

    connection = pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={SERVER};"
        f"DATABASE={DATABASE};"
        f"UID={DB_USER};"
        f"PWD={DB_PASS};"
        f"Encrypt=yes;"
        f"TrustServerCertificate=yes;"
    )

    duck = duckdb.connect(DUCKDB_ITEMS)
    duck.execute(f"DROP TABLE IF EXISTS {TABLE_ITEMS}")

    duck.execute(f"""
    CREATE TABLE {TABLE_ITEMS} AS
    SELECT
        CAST(NULL AS INTEGER) AS Item,
        CAST(NULL AS VARCHAR) AS Referencia_Principal,
        CAST(NULL AS VARCHAR) AS Referencia_Alterna,
        CAST(NULL AS VARCHAR) AS Descripcion_Item,
        CAST(NULL AS VARCHAR) AS UM,
        CAST(NULL AS VARCHAR) AS Notas,
        CAST(NULL AS VARCHAR) AS Estado
    WHERE 1=0;
    """)

    print("🔹 Extrayendo por chunks...")

    for chunk in pd.read_sql(SQL_QUERY, connection, chunksize=50_000):
        duck.register("tmp_df", chunk)
        duck.execute(f"INSERT INTO {TABLE_ITEMS} SELECT * FROM tmp_df")

    connection.close()
    duck.close()
    print("✅ Ítems cargados en DuckDB")

# =========================================================
# 4. NORMALIZACIÓN + CRUCE CON PRECIOS
# =========================================================
def normalizar_y_cruzar_precios():
    duck = duckdb.connect(DUCKDB_ITEMS)

    duck.execute(f"ALTER TABLE {TABLE_ITEMS} ADD COLUMN Original VARCHAR;")
    duck.execute(f"""
    UPDATE {TABLE_ITEMS}
    SET Original =
        trim(replace(replace(Referencia_Alterna, chr(10), ''), chr(13), ''));
    """)

    duck.execute(f"ALTER TABLE {TABLE_ITEMS} ADD COLUMN Referencia VARCHAR;")
    duck.execute(f"""
    UPDATE {TABLE_ITEMS}
    SET Referencia =
        trim(
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                replace(Original, '_', '-'),
                                '[^A-Za-z0-9\\.\\-"/ ]','', 'g'
                            ),
                            '^\\.+|\\.+$', '', 'g'
                        ),
                        '\\.{2,}', '.', 'g'
                    ),
                    '\\s*-\\s*', '-', 'g'
                ),
                '[ ]{{3,}}', ' ', 'g'
            )
        );
    """)

    duck.execute(f"ATTACH '{DUCKDB_PRECIOS.as_posix()}' AS precios;")

    duck.execute(f"""
    CREATE OR REPLACE TABLE {TABLE_ITEMS_PRECIOS} AS
    SELECT
        i.*,
        p.precio_usa,
        p.precio_br,
        p.precio_eur,
        p.disp_br,
        p.disp_eur,
        p.disp_usa
    FROM {TABLE_ITEMS} i
    LEFT JOIN precios.precios_consolidados p
        ON i.Referencia = p.referencia;
    """)

    duck.close()
    print("✅ Normalización y cruce con precios completado")

# =========================================================
# 5. PRECIO FAMILIA + CRUCE CON LISTA
# =========================================================
def calcular_precio_lista():
    duck = duckdb.connect(DUCKDB_MAIN)
    duck.execute(f"ATTACH '{DUCKDB_ITEMS.as_posix()}' AS items_db;")

    duck.execute(f"""
    CREATE OR REPLACE TABLE precio_familia AS
    WITH base AS (
        SELECT
            Item,
            Referencia_Principal,
            Original,
            Referencia,
            precio_usa,
            precio_br,
            precio_eur,
            disp_usa,
            disp_br,
            disp_eur,

            COALESCE(disp_usa,0)+COALESCE(disp_br,0)+COALESCE(disp_eur,0) AS suma_disponibilidad,
            COALESCE(precio_usa,0)+COALESCE(precio_br,0)+COALESCE(precio_eur,0) AS suma_precios,

            CASE
                WHEN precio_br IS NULL AND precio_usa IS NULL AND precio_eur IS NULL THEN NULL
                ELSE list_median(
                    list_filter(
                        [
                            precio_br  * {FACTOR_BR},
                            precio_usa * {FACTOR_USA},
                            precio_eur * {FACTOR_EUR}
                        ],
                        x -> x IS NOT NULL
                    )
                )
            END AS precio_rep
        FROM items_db.{TABLE_ITEMS_PRECIOS}
    ),

    familia AS (
        SELECT
            UPPER(TRIM(Referencia_Principal)) AS Referencia_Principal,
            COUNT(*) AS num_ref_activas,
            MAX(precio_rep) / NULLIF(MIN(NULLIF(precio_rep,0)),0) AS ratio_precio,

            '(' || string_agg(UPPER(TRIM(Original)), ',') || ')' AS RefsAlternas,

            '(' || string_agg(
                '(' ||
                  COALESCE(CAST(ROUND(precio_br,2)  AS VARCHAR),'') || ',' ||
                  COALESCE(CAST(ROUND(precio_usa,2) AS VARCHAR),'') || ',' ||
                  COALESCE(CAST(ROUND(precio_eur,2) AS VARCHAR),'') ||
                ')', ','
            ) || ')' AS Precios_BR_USA_EURO,

            '(' || string_agg(
                '(' ||
                  COALESCE(CAST(CAST(disp_br  AS BIGINT) AS VARCHAR),'') || ',' ||
                  COALESCE(CAST(CAST(disp_usa AS BIGINT) AS VARCHAR),'') || ',' ||
                  COALESCE(CAST(CAST(disp_eur AS BIGINT) AS VARCHAR),'') ||
                ')', ','
            ) || ')' AS Dispon_BR_USA_EURO
        FROM base
        GROUP BY UPPER(TRIM(Referencia_Principal))
    ),

    ranked AS (
        SELECT
            b.*,
            f.num_ref_activas,
            f.ratio_precio,
            f.RefsAlternas,
            f.Precios_BR_USA_EURO,
            f.Dispon_BR_USA_EURO,

            ROW_NUMBER() OVER (
                PARTITION BY UPPER(TRIM(b.Referencia_Principal))
                ORDER BY
                    b.suma_disponibilidad DESC,
                    b.suma_precios DESC,
                    CASE
                        WHEN UPPER(TRIM(b.Original)) = UPPER(TRIM(b.Referencia_Principal)) THEN 1
                        ELSE 0
                    END DESC
            ) AS rn
        FROM base b
        JOIN familia f
          ON UPPER(TRIM(b.Referencia_Principal)) = f.Referencia_Principal
    )

    SELECT
        UPPER(TRIM(Referencia_Principal)) AS Referencia_Principal,
        Item,
        UPPER(TRIM(Original)) AS Referencia_Ganadora,
        UPPER(TRIM(Referencia)) AS Referencia_Normalizada_Ganadora,
        precio_usa,
        precio_br,
        precio_eur,
        disp_usa,
        disp_br,
        disp_eur,
        suma_precios,
        suma_disponibilidad,
        num_ref_activas,
        ratio_precio,
        RefsAlternas,
        Precios_BR_USA_EURO,
        Dispon_BR_USA_EURO
    FROM ranked
    WHERE rn = 1;
    """)

    df_lista = pd.read_excel(EXCEL_LISTA_PATH)
    duck.execute("DROP TABLE IF EXISTS precios_lista")
    duck.register("tmp_lista", df_lista)

    duck.execute("""
    CREATE TABLE precios_lista AS
    SELECT
        UPPER(TRIM(Referencia)) AS ref_lista,
        *
    FROM tmp_lista;
    """)

    duck.execute("""
    CREATE OR REPLACE TABLE resultado_precios_lista AS
    SELECT
        pl.*,
        pf.*,
        pf.precio_br  AS "Precio Brasil",
        pf.precio_usa AS "Precio Usa",
        pf.precio_eur AS "Precio Europa"
    FROM precios_lista pl
    LEFT JOIN precio_familia pf
        ON pl.ref_lista = pf.Referencia_Principal;
    """)

    df_out = duck.execute("SELECT * FROM resultado_precios_lista").df()
    df_out.to_excel(EXCEL_OUT, index=False)

    duck.close()
    print(f"📂 Excel final generado: {EXCEL_OUT}")

# =========================================================
# 6. MAIN
# =========================================================
def main():
    print("🚀 Pipeline unificado iniciado")
    extraer_items_sqlserver()
    normalizar_y_cruzar_precios()
    calcular_precio_lista()
    print("✅ Pipeline completo finalizado")

if __name__ == "__main__":
    main()