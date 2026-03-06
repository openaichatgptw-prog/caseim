"""
pipeline_precios_cnh.py  —  version optimizada
===============================================
Construye el archivo maestro de precios sugeridos de compra para
repuestos CNH (plan 07 / criterio 051), combinando tres fuentes:

  FUENTE 1 — SQL Server (UnoEE)
    Catalogo tecnico de repuestos con referencias alternas.

  FUENTE 2 — Excel de precios internacionales
    Precios desde Brasil, USA y Europa + disponibilidad (LISTA AGCS).

  FUENTE 3 — Excel maestro comercial (Precios de Lista)
    Referencia comercial con participacion historica de compra por pais.

Optimizaciones aplicadas vs version original:
  - pyodbc directo         : mantiene conexion estable en queries pesadas
  - Polars -> DuckDB       : register() sin .to_pandas() — Arrow nativo
  - xlsxwriter             : hasta 5x mas rapido que openpyxl
  - ajuste de columnas     : Excel de salida con anchos legibles
  - pd.concat copy=False   : evita copia extra en memoria al leer chunks

Dependencias: polars, duckdb, pandas, pyodbc, xlsxwriter, openpyxl
Config:       config.ini (secciones ARCHIVOS, SALIDA, SQLSERVER, FACTORES, TABLAS)
"""

# ===========================================================================
# SECCION 0 - LIBRERIAS
# ===========================================================================
import configparser
import pyodbc
import pandas as pd
import polars as pl
import duckdb
from pathlib import Path
from datetime import datetime


# ===========================================================================
# SECCION 1 - CONFIGURACION
# ===========================================================================
BASE_DIR    = Path(__file__).parent
CONFIG_PATH = BASE_DIR / "config.ini"

if not CONFIG_PATH.exists():
    raise FileNotFoundError(
        f"No se encontro config.ini en: {CONFIG_PATH}\n"
        "Crea el archivo con las secciones [ARCHIVOS], [SALIDA], [SQLSERVER], [FACTORES], [TABLAS]."
    )

config = configparser.ConfigParser()
config.read(CONFIG_PATH, encoding="utf-8")

FECHA = datetime.now().strftime("%Y%m%d")

EXCEL_PRECIOS = Path(config["ARCHIVOS"]["excel_precios"])
EXCEL_DISP    = Path(config["ARCHIVOS"]["excel_disponibilidad"])
EXCEL_LISTA   = Path(config["ARCHIVOS"]["excel_lista"])

DUCKDB_PATH = BASE_DIR / config["SALIDA"]["duckdb"]
CSV_OUT     = BASE_DIR / config["SALIDA"]["csv"]
EXCEL_OUT   = BASE_DIR / config["SALIDA"]["excel_salida"].replace("{fecha}", FECHA)

SERVER   = config["SQLSERVER"]["server"]
DATABASE = config["SQLSERVER"]["database"]
DB_USER  = config["SQLSERVER"]["db_user"]
DB_PASS  = config["SQLSERVER"]["db_pass"].strip('"')

FACTOR_BR  = float(config["FACTORES"]["factor_br"])
FACTOR_USA = float(config["FACTORES"]["factor_usa"])
FACTOR_EUR = float(config["FACTORES"]["factor_eur"])

TABLE_ITEMS         = config["TABLAS"]["table_items"]
TABLE_ITEMS_PRECIOS = config["TABLAS"]["table_items_precios"]

CHUNK_SIZE = 50_000


# ===========================================================================
# SECCION 2 - CONSULTA SQL SERVER
# Extrae catalogo CNH: empresa 1, plan 07, criterio 051.
# Excluye items restringidos por perfil de usuario (entidad 149).
# ===========================================================================
SQL_QUERY = """
    SELECT DISTINCT
        i.f120_id                   AS Item,
        i.f120_referencia           AS Referencia_Principal,
        r.f124_referencia           AS Referencia_Alterna,
        i.f120_descripcion          AS Descripcion_Item,
        i.f120_id_unidad_inventario AS UM,
        ie.f121_notas               AS Notas,
        CASE ie.f121_ind_estado
            WHEN 1 THEN 'Activo'
            WHEN 0 THEN 'Inactivo'
            ELSE 'Bloqueado'
        END AS Estado
    FROM t120_mc_items i
    INNER JOIN t121_mc_items_extensiones ie
        ON ie.f121_rowid_item = i.f120_rowid
    LEFT JOIN t124_mc_items_referencias r
        ON r.f124_rowid_item = i.f120_rowid
    WHERE i.f120_id_cia = 1
    AND EXISTS (
        SELECT 1 FROM t125_mc_items_criterios c
        WHERE c.f125_rowid_item        = i.f120_rowid
          AND c.f125_id_plan           = '07'
          AND c.f125_id_criterio_mayor = '051'
    )
    AND NOT EXISTS (
        SELECT 1 FROM t058_mm_usuario_entidad ue
        WHERE ue.f058_id_cia               = 1
          AND ue.f058_entidad              = 149
          AND ue.f058_ind_aplica_consultas = 1
          AND ue.f058_id_tipo_inv_serv     = i.f120_id_tipo_inv_serv
    )
    ORDER BY i.f120_id
"""


# ===========================================================================
# SECCION 3 - NORMALIZACION SQL
# Genera fragmento SQL para limpiar referencias en DuckDB.
# Se aplica en ambos lados de cada JOIN.
# ===========================================================================
def norm_sql(campo: str) -> str:
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
                "),'_','-'"
                "),"
            "'[^A-Za-z0-9.\\-\"/ ]','','g'),"
            "'^\\.+|\\.+$','','g'),"
            "'\\.{{2,}}','.','g'),"
            "'-{{2,}}','-','g'),"
            "'\\s*-\\s*','-','g'),"
            "'[ ]{{2,}}',' ','g')"
        "))"
    ).format(c=c)


# ===========================================================================
# SECCION 4 - NORMALIZACION POLARS
# Misma logica que norm_sql para DataFrames Polars.
# ===========================================================================
def normalizar_ref_polars(col: pl.Expr) -> pl.Expr:
    return (
        col
        .cast(pl.Utf8)
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


# ===========================================================================
# SECCION 5 - INSTRUCCIONES DE USO
# ===========================================================================
def mostrar_instrucciones():
    print("\nESTRUCTURA REQUERIDA DE LOS ARCHIVOS EXCEL\n")
    print("Excel de PRECIOS (una hoja por origen):")
    print("  Hojas requeridas : USA, BRASIL, EUR")
    print("  Columna A        : referencia del repuesto")
    print("  Columna I        : precio de lista en origen")
    print("  Fila 1           : encabezados (se ignoran)\n")
    print("Excel de DISPONIBILIDADES:")
    print("  Hoja requerida   : LISTA AGCS")
    print("  Columna A        : referencia del repuesto")
    print("  Columna I        : disponibilidad Brasil")
    print("  Columna L        : disponibilidad Europa")
    print("  Columna M        : disponibilidad USA\n")
    print("Excel MAESTRO COMERCIAL (Precios de Lista):")
    print("  Columna 'Referencia'   : llave de cruce con catalogo tecnico")
    print("  Columna 'Part. Brasil' : % historico de compra Brasil (0 a 1)")
    print("  Columna 'Part. Usa'    : % historico de compra USA    (0 a 1)")
    print("  Columna 'Part. Europa' : % historico de compra Europa (0 a 1)\n")


# ===========================================================================
# SECCION 6 - VALIDACION DE ARCHIVOS DE ENTRADA
# ===========================================================================
def validar_archivos():
    archivos = {
        "Excel precios internacionales"  : EXCEL_PRECIOS,
        "Excel disponibilidad por origen": EXCEL_DISP,
        "Excel maestro comercial"        : EXCEL_LISTA,
    }
    print("Verificando archivos de entrada:")
    for nombre, ruta in archivos.items():
        if not ruta.exists():
            raise FileNotFoundError(
                f"Archivo no encontrado: {nombre}\n   Ruta esperada: {ruta}"
            )
        print(f"  OK  {nombre}: {ruta.name}")
    print()


# ===========================================================================
# SECCION 7 - LECTURA DE PRECIOS DESDE EXCEL
# Lee una hoja con Polars y devuelve referencia normalizada
# + precio redondeado a 2 decimales. Descarta vacios y duplicados.
# ===========================================================================
def leer_hoja_precio(sheet_name: str, col_precio_out: str) -> pl.DataFrame:
    df   = pl.read_excel(EXCEL_PRECIOS, sheet_name=sheet_name, has_header=True)
    cols = df.columns
    return (
        df.select([
            normalizar_ref_polars(pl.col(cols[0])).alias("referencia"),
            pl.col(cols[8])
              .cast(pl.Float64, strict=False)
              .round(2)
              .alias(col_precio_out),
        ])
        .filter(pl.col("referencia").is_not_null() & (pl.col("referencia") != ""))
        .unique(subset=["referencia"], keep="first")
    )


# ===========================================================================
# SECCION 8 - LECTURA DE DISPONIBILIDADES DESDE EXCEL
# Lee hoja LISTA AGCS. Valores no numericos se convierten a NULL.
# ===========================================================================
def leer_disponibilidades() -> pl.DataFrame:
    df   = pl.read_excel(EXCEL_DISP, sheet_name="LISTA AGCS", has_header=True)
    cols = df.columns
    return (
        df.select([
            normalizar_ref_polars(pl.col(cols[0])).alias("referencia"),
            pl.col(cols[8]).cast(pl.Float64,  strict=False).cast(pl.Int64, strict=False).alias("disp_br"),
            pl.col(cols[11]).cast(pl.Float64, strict=False).cast(pl.Int64, strict=False).alias("disp_eur"),
            pl.col(cols[12]).cast(pl.Float64, strict=False).cast(pl.Int64, strict=False).alias("disp_usa"),
        ])
        .filter(pl.col("referencia").is_not_null() & (pl.col("referencia") != ""))
        .unique(subset=["referencia"], keep="first")
    )


# ===========================================================================
# SECCION 9 - CONSOLIDAR PRECIOS Y DISPONIBILIDADES
# Une precios de los tres origenes y los cruza con disponibilidad.
# OPTIMIZACION: register() con DataFrame Polars directamente — sin .to_pandas()
# DuckDB acepta Polars/Arrow nativamente evitando la conversion intermedia.
# ===========================================================================
def consolidar_precios_disponibilidades():
    print("Leyendo precios por origen...")
    df_usa = leer_hoja_precio("USA",    "precio_usa")
    df_br  = leer_hoja_precio("BRASIL", "precio_br")
    df_eur = leer_hoja_precio("EUR",    "precio_eur")
    print(f"  USA: {df_usa.height:,} | BR: {df_br.height:,} | EUR: {df_eur.height:,} referencias")

    df_refs = pl.concat([
        df_usa.select("referencia"),
        df_br.select("referencia"),
        df_eur.select("referencia"),
    ]).unique()

    df_precios = (
        df_refs
        .join(df_usa, on="referencia", how="left")
        .join(df_br,  on="referencia", how="left")
        .join(df_eur, on="referencia", how="left")
    )

    print("Leyendo disponibilidades...")
    df_disp = leer_disponibilidades()
    print(f"  Disponibilidad: {df_disp.height:,} referencias")

    df_final = df_precios.join(df_disp, on="referencia", how="left").sort("referencia")

    print("Guardando en DuckDB y CSV de auditoria...")

    # Polars -> DuckDB directo via Arrow — sin .to_pandas()
    with duckdb.connect(str(DUCKDB_PATH)) as con:
        con.register("tmp_precios", df_final)
        con.execute("""
            CREATE OR REPLACE TABLE precios_consolidados AS
            SELECT * FROM tmp_precios
        """)

    df_final.write_csv(CSV_OUT)
    print(f"  CSV guardado: {CSV_OUT.name}\n")


# ===========================================================================
# SECCION 10 - EXTRACCION DEL CATALOGO TECNICO DESDE SQL SERVER
# pyodbc directo — mas estable que SQLAlchemy para queries pesadas
# con DISTINCT + subconsultas que tardan varios segundos en el servidor.
# Lee en chunks de 50.000 filas e inserta directamente en DuckDB.
# ===========================================================================
def extraer_items_sqlserver():
    print("Conectando a SQL Server (UnoEE)...")
    conn = pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={SERVER};"
        f"DATABASE={DATABASE};"
        f"UID={DB_USER};"
        f"PWD={DB_PASS};"
        f"Encrypt=yes;"
        f"TrustServerCertificate=yes;"
    )

    with duckdb.connect(str(DUCKDB_PATH)) as duck:
        duck.execute(f"DROP TABLE IF EXISTS {TABLE_ITEMS}")
        duck.execute(f"""
            CREATE TABLE {TABLE_ITEMS} (
                Item                 INTEGER,
                Referencia_Principal VARCHAR,
                Referencia_Alterna   VARCHAR,
                Descripcion_Item     VARCHAR,
                UM                   VARCHAR,
                Notas                VARCHAR,
                Estado               VARCHAR
            )
        """)

        print(f"Extrayendo catalogo tecnico en bloques de {CHUNK_SIZE:,} filas...")
        total  = 0
        chunks = []

        try:
            for chunk in pd.read_sql(SQL_QUERY, conn, chunksize=CHUNK_SIZE):
                chunks.append(chunk)
                total += len(chunk)
                print(f"  {total:,} items leidos...")
        finally:
            conn.close()
            print("  Conexion cerrada")

        if chunks:
            df_all = pd.concat(chunks, ignore_index=True, copy=False)
            duck.register("tmp_items", df_all)
            duck.execute(f"INSERT INTO {TABLE_ITEMS} SELECT * FROM tmp_items")

    print(f"  {total:,} items cargados en DuckDB\n")


# ===========================================================================
# SECCION 11 - NORMALIZACION Y CRUCE CON PRECIOS
# norm_sql() se aplica en ambos lados del JOIN.
# ===========================================================================
def normalizar_y_cruzar_precios():
    print("Normalizando referencias y cruzando con precios...")

    norm_alterna = norm_sql("Referencia_Alterna")
    norm_ref     = norm_sql("n.Referencia")
    norm_precio  = norm_sql("p.referencia")

    sql = (
        f"CREATE TABLE {TABLE_ITEMS_PRECIOS} AS "
        "WITH normalizado AS ("
        "    SELECT *,"
        "        trim(replace(replace(replace("
        "            Referencia_Alterna,"
        "            chr(9),''),chr(10),''),chr(13),'')"
        "        ) AS Original,"
        f"        {norm_alterna} AS Referencia"
        f"    FROM {TABLE_ITEMS}"
        ") "
        "SELECT n.*, "
        "    p.precio_usa, p.precio_br, p.precio_eur,"
        "    p.disp_br, p.disp_eur, p.disp_usa"
        " FROM normalizado n"
        " LEFT JOIN precios_consolidados p"
        f"    ON {norm_ref} = {norm_precio}"
    )

    with duckdb.connect(str(DUCKDB_PATH)) as duck:
        duck.execute(f"DROP TABLE IF EXISTS {TABLE_ITEMS_PRECIOS}")
        duck.execute(sql)

    print("  Cruce completado\n")


# ===========================================================================
# SECCION 12 - PRECIO POR FAMILIA Y CRUCE CON MAESTRO COMERCIAL
#
# A) precio_familia : referencia activa por familia tecnica.
# B) precios_lista  : Excel maestro con dtype=str — preserva Referencia_Original.
# C) resultado      : JOIN final + Precio Prorrateo redondeado a 2 decimales.
#
# OPTIMIZACION: xlsxwriter reemplaza openpyxl en la exportacion final
# (hasta 5x mas rapido para archivos grandes) + ajuste automatico de columnas.
# ===========================================================================
def calcular_precio_lista():
    print("Calculando precio por familia tecnica...")

    norm_ref_princ   = norm_sql("Referencia_Principal")
    norm_b_ref_princ = norm_sql("b.Referencia_Principal")
    norm_b_original  = norm_sql("b.Original")
    norm_referencia  = norm_sql("Referencia")
    norm_original    = norm_sql("Original")

    sql_familia = (
        "CREATE OR REPLACE TABLE precio_familia AS "
        "WITH base AS ("
        "    SELECT"
        "        Item, Referencia_Principal, Original, Referencia,"
        "        precio_usa, precio_br, precio_eur,"
        "        disp_usa, disp_br, disp_eur,"
        "        COALESCE(disp_usa,0)+COALESCE(disp_br,0)+COALESCE(disp_eur,0) AS suma_disponibilidad,"
        "        COALESCE(precio_usa,0)+COALESCE(precio_br,0)+COALESCE(precio_eur,0) AS suma_precios,"
        "        CASE"
        "            WHEN precio_br IS NULL AND precio_usa IS NULL AND precio_eur IS NULL THEN NULL"
        "            ELSE list_median(list_filter(["
        f"               precio_br * {FACTOR_BR},"
        f"               precio_usa * {FACTOR_USA},"
        f"               precio_eur * {FACTOR_EUR}"
        "            ], x -> x IS NOT NULL))"
        "        END AS precio_rep"
        f"    FROM {TABLE_ITEMS_PRECIOS}"
        "), "
        "familia AS ("
        "    SELECT"
        f"        {norm_ref_princ} AS Referencia_Principal,"
        "        COUNT(*) AS num_ref_activas,"
        "        MAX(precio_rep) / NULLIF(MIN(NULLIF(precio_rep,0)),0) AS ratio_precio,"
        "        '(' || string_agg(UPPER(TRIM(Original)), ',') || ')' AS RefsAlternas,"
        "        '(' || string_agg('(' ||"
        "            COALESCE(CAST(ROUND(precio_br,2)  AS VARCHAR),'') || ',' ||"
        "            COALESCE(CAST(ROUND(precio_usa,2) AS VARCHAR),'') || ',' ||"
        "            COALESCE(CAST(ROUND(precio_eur,2) AS VARCHAR),'') ||"
        "        ')', ',') || ')' AS Precios_BR_USA_EURO,"
        "        '(' || string_agg('(' ||"
        "            COALESCE(CAST(CAST(disp_br  AS BIGINT) AS VARCHAR),'') || ',' ||"
        "            COALESCE(CAST(CAST(disp_usa AS BIGINT) AS VARCHAR),'') || ',' ||"
        "            COALESCE(CAST(CAST(disp_eur AS BIGINT) AS VARCHAR),'') ||"
        "        ')', ',') || ')' AS Dispon_BR_USA_EURO"
        f"    FROM base GROUP BY {norm_ref_princ}"
        "), "
        "ranked AS ("
        "    SELECT b.*, f.num_ref_activas, f.ratio_precio,"
        "        f.RefsAlternas, f.Precios_BR_USA_EURO, f.Dispon_BR_USA_EURO,"
        "        ROW_NUMBER() OVER ("
        f"            PARTITION BY {norm_b_ref_princ}"
        "            ORDER BY"
        "                b.suma_disponibilidad DESC,"
        "                b.suma_precios DESC,"
        f"                CASE WHEN {norm_b_original} = {norm_b_ref_princ} THEN 1 ELSE 0 END DESC"
        "        ) AS rn"
        "    FROM base b"
        f"    JOIN familia f ON {norm_b_ref_princ} = f.Referencia_Principal"
        ") "
        "SELECT"
        f"    {norm_ref_princ}                     AS Referencia_Principal,"
        "    Item,"
        f"    {norm_original}                      AS Referencia_Activa,"
        "    ROUND(precio_usa, 2)                  AS precio_usa,"
        "    ROUND(precio_br,  2)                  AS precio_br,"
        "    ROUND(precio_eur, 2)                  AS precio_eur,"
        "    disp_usa, disp_br, disp_eur,"
        "    ROUND(suma_precios, 2)                AS suma_precios,"
        "    suma_disponibilidad,"
        "    num_ref_activas,"
        "    ROUND(ratio_precio, 4)                AS ratio_precio,"
        "    RefsAlternas, Precios_BR_USA_EURO, Dispon_BR_USA_EURO"
        " FROM ranked WHERE rn = 1"
    )

    sql_lista = (
        "CREATE OR REPLACE TABLE precios_lista AS "
        "SELECT"
        "    CAST(Referencia AS VARCHAR) AS Referencia_Original,"
        f"    {norm_referencia}           AS Referencia_Normalizada,"
        "    * EXCLUDE (Referencia)"
        " FROM tmp_lista"
    )

    sql_resultado = """
    CREATE OR REPLACE TABLE resultado_precios_lista AS
    SELECT
        pl.Rowid,
        pl.Referencia_Original,
        pl.Referencia_Normalizada,
        pl.* EXCLUDE (Rowid, Referencia_Original, Referencia_Normalizada),
        pf.Referencia_Principal,
        pf.Referencia_Activa,
        ROUND(pf.precio_br,  2) AS "Precio Brasil",
        ROUND(pf.precio_usa, 2) AS "Precio Usa",
        ROUND(pf.precio_eur, 2) AS "Precio Europa",
        pf.disp_br, pf.disp_usa, pf.disp_eur,
        ROUND(pf.suma_precios, 2)  AS suma_precios,
        pf.suma_disponibilidad,
        pf.num_ref_activas,
        ROUND(pf.ratio_precio, 4) AS ratio_precio,
        pf.RefsAlternas, pf.Precios_BR_USA_EURO, pf.Dispon_BR_USA_EURO,
        CASE
            WHEN pf.Referencia_Principal IS NOT NULL THEN 'PRINCIPAL'
            ELSE 'NO_MATCH'
        END AS match_type,
        CASE
            WHEN COALESCE(CAST(pl."Part. Brasil" AS DOUBLE), 0.0)
               + COALESCE(CAST(pl."Part. Usa"    AS DOUBLE), 0.0)
               + COALESCE(CAST(pl."Part. Europa" AS DOUBLE), 0.0) > 0
            THEN ROUND(
                  COALESCE(pf.precio_br,  0.0) * COALESCE(CAST(pl."Part. Brasil" AS DOUBLE), 0.0)
                + COALESCE(pf.precio_usa, 0.0) * COALESCE(CAST(pl."Part. Usa"    AS DOUBLE), 0.0)
                + COALESCE(pf.precio_eur, 0.0) * COALESCE(CAST(pl."Part. Europa" AS DOUBLE), 0.0)
            , 2)
            WHEN pf.precio_br IS NULL AND pf.precio_usa IS NULL AND pf.precio_eur IS NULL
            THEN NULL
            WHEN pf.precio_br  IS NULL AND pf.precio_usa IS NULL THEN ROUND(pf.precio_eur, 2)
            WHEN pf.precio_br  IS NULL AND pf.precio_eur IS NULL THEN ROUND(pf.precio_usa, 2)
            WHEN pf.precio_usa IS NULL AND pf.precio_eur IS NULL THEN ROUND(pf.precio_br,  2)
            WHEN pf.precio_eur IS NULL THEN ROUND((pf.precio_br  * 0.5) + (pf.precio_usa * 0.5), 2)
            WHEN pf.precio_usa IS NULL THEN ROUND((pf.precio_br  * 0.5) + (pf.precio_eur * 0.5), 2)
            WHEN pf.precio_br  IS NULL THEN ROUND((pf.precio_usa * 0.5) + (pf.precio_eur * 0.5), 2)
            ELSE ROUND((pf.precio_br + pf.precio_usa + pf.precio_eur) / 3.0, 2)
        END AS "Precio Prorrateo"
    FROM precios_lista pl
    LEFT JOIN precio_familia pf
        ON pl.Referencia_Normalizada = pf.Referencia_Principal
    """

    with duckdb.connect(str(DUCKDB_PATH)) as duck:
        duck.execute(sql_familia)

        print("Cargando Excel maestro comercial...")
        # dtype=str preserva Referencia_Original exactamente como viene del Excel
        df_lista = pd.read_excel(EXCEL_LISTA, dtype=str)
        duck.register("tmp_lista", df_lista)
        duck.execute(sql_lista)
        duck.execute(sql_resultado)

        df_out = duck.execute("SELECT * FROM resultado_precios_lista").df()

    # xlsxwriter: hasta 5x mas rapido que openpyxl + ajuste automatico de columnas
    print("Exportando Excel de salida...")
    with pd.ExcelWriter(EXCEL_OUT, engine="xlsxwriter") as writer:
        df_out.to_excel(writer, index=False, sheet_name="Precios CNH")
        workbook  = writer.book
        worksheet = writer.sheets["Precios CNH"]
        for i, col in enumerate(df_out.columns):
            max_len = max(df_out[col].astype(str).map(len).max(), len(str(col))) + 2
            worksheet.set_column(i, i, min(max_len, 50))

    print(f"  Excel generado: {EXCEL_OUT.name}\n")


# ===========================================================================
# SECCION 13 - PUNTO DE ENTRADA PRINCIPAL
# ===========================================================================
def main():
    print("\n" + "=" * 60)
    print("PIPELINE DE PRECIOS CNH - IMECOL S.A.S")
    print("=" * 60)

    mostrar_instrucciones()
    validar_archivos()

    consolidar_precios_disponibilidades()   # Paso 1
    extraer_items_sqlserver()               # Paso 2
    normalizar_y_cruzar_precios()           # Paso 3
    calcular_precio_lista()                 # Paso 4

    print("=" * 60)
    print("PROCESO TERMINADO CORRECTAMENTE")
    print("\nArchivos generados:")
    print(f"  Excel  -> {EXCEL_OUT.name}")
    print(f"  DuckDB -> {DUCKDB_PATH.name}")
    print(f"  CSV    -> {CSV_OUT.name}")
    print(f"\n  Ruta: {BASE_DIR.resolve()}")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
