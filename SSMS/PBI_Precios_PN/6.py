"""
Pipeline de integración de referencias y precios.

Este script construye un archivo maestro de precios combinando tres fuentes:

1) Catálogo técnico de referencias (SQL Server)
   Contiene:
   - La referencia principal del repuesto.
   - Sus referencias alternas equivalentes.
   - Descripción, unidad de medida y estado.
   Este catálogo define las familias técnicas de repuestos.

2) Repositorio de precios internacionales (DuckDB)
   Contiene:
   - Precio del repuesto en Brasil, USA y Europa.
   - Disponibilidad por país.
   - Datos previamente normalizados y consolidados.

3) Maestro comercial de precios (Excel de Precios de Lista)
   Contiene:
   - La referencia comercial.
   - Descripción y atributos comerciales (sistema, línea, equipo, modelo, rotación).
   - Cantidades históricas compradas por país.
   - Participación por país (Part. Brasil / Part. Usa / Part. Europa).

Reglas de negocio principales:
- Las referencias se agrupan por familia técnica (Referencia_Principal).
- Para cada familia se selecciona una única referencia ACTIVA:
    1) Mayor disponibilidad total.
    2) En empate, mayor suma de precios.
    3) En empate, se prioriza la referencia principal.
- El precio sugerido se calcula como un precio prorrateado:
    Precio Brasil  * Part. Brasil
  + Precio USA     * Part. Usa
  + Precio Europa  * Part. Europa
  con reglas de respaldo si faltan países o participaciones.

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
    """
    Extrae el catálogo técnico de referencias desde SQL Server
    y lo almacena en DuckDB para procesamiento local.
    """
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
# 4. NORMALIZACIÓN + CRUCE CON PRECIOS (JOINS BLINDADOS)
# =========================================================
def normalizar_y_cruzar_precios():
    """
    Limpia y estandariza las referencias alternas para poder cruzarlas
    contra la tabla de precios internacionales.
    """
    duck = duckdb.connect(DUCKDB_ITEMS)

    try:
        duck.execute(f"ALTER TABLE {TABLE_ITEMS} ADD COLUMN Original VARCHAR;")
    except:
        pass

    duck.execute(f"""
    UPDATE {TABLE_ITEMS}
    SET Original =
        trim(replace(replace(Referencia_Alterna, chr(10), ''), chr(13), ''));
    """)

    try:
        duck.execute(f"ALTER TABLE {TABLE_ITEMS} ADD COLUMN Referencia VARCHAR;")
    except:
        pass

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
        ON UPPER(TRIM(i.Referencia)) = UPPER(TRIM(p.referencia));
    """)

    duck.close()
    print("✅ Normalización y cruce con precios completado")

# =========================================================
# 5. PRECIO FAMILIA + CRUCE CON LISTA (SALIDA QUERY1)
# =========================================================
def calcular_precio_lista():
    """
    Construye la tabla precio_familia:
    - Agrupa referencias por familia técnica.
    - Selecciona una referencia ACTIVA.
    - Integra precios internacionales.
    Luego cruza contra el maestro comercial (Excel)
    y calcula el Precio Prorrateo.
    """
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
          ON UPPER(TRIM(b.Referencia_Principal)) = UPPER(TRIM(f.Referencia_Principal))
    )

    SELECT
        UPPER(TRIM(Referencia_Principal)) AS Referencia_Principal,
        Item,
        UPPER(TRIM(Original)) AS Referencia_Activa,
        UPPER(TRIM(Referencia)) AS Referencia_Normalizada_Activa,
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
        Referencia AS Referencia_Original,
        UPPER(TRIM(Referencia)) AS Referencia_Limpia,
        *
        EXCLUDE (Referencia)
    FROM tmp_lista;
    """)

    duck.execute("""
    CREATE OR REPLACE TABLE resultado_precios_lista AS
    SELECT
        pl.*,

        pf.Referencia_Principal,
        pf.Referencia_Activa,

        pf.precio_br  AS "Precio Brasil",
        pf.precio_usa AS "Precio Usa",
        pf.precio_eur AS "Precio Europa",

        pf.disp_br,
        pf.disp_usa,
        pf.disp_eur,

        pf.suma_precios,
        pf.suma_disponibilidad,

        pf.num_ref_activas,
        pf.ratio_precio,
        pf.RefsAlternas,
        pf.Precios_BR_USA_EURO,
        pf.Dispon_BR_USA_EURO,

        CASE
            WHEN pf.Referencia_Principal IS NOT NULL THEN 'PRINCIPAL'
            ELSE 'NO_MATCH'
        END AS match_type,

        CASE
            WHEN
                COALESCE(pl."Part. Brasil", 0)
              + COALESCE(pl."Part. Usa", 0)
              + COALESCE(pl."Part. Europa", 0) > 0
            THEN
                COALESCE(pf.precio_br, 0)  * COALESCE(pl."Part. Brasil", 0)
              + COALESCE(pf.precio_usa, 0) * COALESCE(pl."Part. Usa", 0)
              + COALESCE(pf.precio_eur, 0) * COALESCE(pl."Part. Europa", 0)

            WHEN pf.precio_br IS NULL
             AND pf.precio_usa IS NULL
            THEN NULL

            WHEN pf.precio_br IS NULL
            THEN pf.precio_usa

            WHEN pf.precio_usa IS NULL
            THEN pf.precio_br

            ELSE
                (pf.precio_br * 0.5)
              + (pf.precio_usa * 0.5)
        END AS "Precio Prorrateo"

    FROM precios_lista pl
    LEFT JOIN precio_familia pf
        ON UPPER(TRIM(pl.Referencia_Limpia)) = UPPER(TRIM(pf.Referencia_Principal));
    """)

    df_out = duck.execute("SELECT * FROM resultado_precios_lista").df()
    df_out.to_excel(EXCEL_OUT, index=False)

    duck.close()
    print(f"📂 Excel final generado: {EXCEL_OUT}")

# =========================================================
# 6. MAIN
# =========================================================
def main():
    """
    Ejecuta el pipeline completo:
    1) Extrae catálogo técnico desde SQL Server.
    2) Normaliza referencias y cruza con precios.
    3) Calcula precio por familia y cruza con lista comercial.
    4) Exporta Excel final.
    """
    print("🚀 Pipeline unificado iniciado")
    extraer_items_sqlserver()
    normalizar_y_cruzar_precios()
    calcular_precio_lista()
    print("✅ Pipeline completo finalizado")

if __name__ == "__main__":
    main()