"""
╔══════════════════════════════════════════════════════════════╗
║        PIPELINE DE PRECIOS CNH — IMECOL S.A.S               ║
║        Integración de referencias, precios y disponibilidad  ║
╚══════════════════════════════════════════════════════════════╝

QUÉ HACE ESTE SCRIPT
─────────────────────
Construye el archivo maestro de precios sugeridos de compra para
repuestos CNH (plan 07 / criterio 051), combinando tres fuentes:

  FUENTE 1 — SQL Server (UnoEE)
    Catálogo técnico de repuestos: cada ítem puede tener varias
    referencias alternas equivalentes (ej: referencia del fabricante,
    referencia cruzada de proveedor). Todas se agrupan bajo una
    Referencia_Principal que define la "familia técnica".

  FUENTE 2 — Excel de precios internacionales
    Precios de lista desde tres orígenes de compra:
      · Brasil (hoja BRASIL, columna I)
      · USA    (hoja USA,    columna I)
      · Europa (hoja EUR,    columna I)
    También incluye la disponibilidad actual por origen
    (hoja LISTA AGCS).

  FUENTE 3 — Excel maestro comercial (Precios de Lista)
    Contiene la referencia comercial con sus atributos
    (sistema, línea, equipo, modelo, rotación) y la
    participación histórica de compra por país:
      · Part. Brasil / Part. Usa / Part. Europa (valores de 0 a 1)

REGLAS DE NEGOCIO
──────────────────
1. Agrupación por familia técnica
   Todas las referencias alternas de un ítem se agrupan bajo
   su Referencia_Principal.

2. Selección de referencia activa (una por familia)
   Se elige la referencia con:
     1°) Mayor disponibilidad total (suma de los tres orígenes)
     2°) En empate: mayor suma de precios
     3°) En empate: se prefiere la que coincide con la referencia principal

3. Precio Prorrateo (precio sugerido de compra)
   Se pondera el precio de cada origen por su participación histórica:
     Precio Prorrateo = precio_br * Part.Brasil
                      + precio_usa * Part.Usa
                      + precio_eur * Part.Europa
   Si faltan participaciones o precios, se aplican reglas de respaldo.
   Todos los precios se redondean a 2 decimales.

COLUMNAS DE REFERENCIA EN EL EXCEL DE SALIDA
─────────────────────────────────────────────
  · Referencia_Original    : valor exacto del Excel maestro (sin tocar, dtype=str)
  · Referencia_Normalizada : Referencia_Original después de limpieza — llave del JOIN
  · Referencia_Principal   : referencia oficial del ítem en UnoEE (normalizada)
  · Referencia_Activa      : alterna ganadora de la familia (normalizada con norm_sql)

  Si Referencia_Normalizada ≠ Referencia_Principal → match_type = NO_MATCH
  Esto indica que la referencia del Excel no cruzó con ningún ítem del catálogo.

NORMALIZACIÓN DE LLAVES DE CRUCE
──────────────────────────────────
Antes de cruzar tablas, todas las referencias pasan por limpieza:
  · Mayúsculas + sin espacios extremos
  · Sin saltos de línea (chr 9/10/13)
  · Solo caracteres permitidos: A-Z 0-9 . - " / espacio
  · Sin puntos al inicio/final
  · Sin puntos, guiones o espacios múltiples consecutivos
  · Guión bajo '_' se convierte en guión '-'

ARCHIVOS DE SALIDA
───────────────────
  · maestro_precios_cnh_YYYYMMDD.xlsx  — resultado final para el área comercial
  · staging_precios_origen.csv         — precios consolidados por origen (auditoría)
  · cnh_repuestos.duckdb               — base de datos local de staging

DEPENDENCIAS
─────────────
  pip install polars duckdb pandas pyodbc openpyxl
  Driver requerido: ODBC Driver 17 for SQL Server
"""

# ============================================================
# SECCIÓN 0 — LIBRERÍAS
# ============================================================
import configparser
import pyodbc
import pandas as pd
import polars as pl
import duckdb
from pathlib import Path
from datetime import datetime


# ============================================================
# SECCIÓN 1 — CONFIGURACIÓN
# Lee rutas, credenciales y parámetros desde config.ini.
# Si el archivo no existe el script para con un mensaje claro.
# ============================================================
BASE_DIR    = Path(__file__).parent
CONFIG_PATH = BASE_DIR / "config.ini"

if not CONFIG_PATH.exists():
    raise FileNotFoundError(
        f"No se encontró config.ini en: {CONFIG_PATH}\n"
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


# ============================================================
# SECCIÓN 2 — CONSULTA SQL SERVER
# Extrae catálogo de repuestos CNH filtrado por:
#   · Empresa 1 (IMECOL S.A.S)
#   · Plan 07 / Criterio mayor 051 (repuestos CNH)
#   · Excluye ítems restringidos por perfil de usuario (entidad 149)
# Incluye todas las referencias alternas de cada ítem.
# ============================================================
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


# ============================================================
# SECCIÓN 3 — NORMALIZACIÓN SQL
# Genera el fragmento SQL para limpiar una referencia en DuckDB.
# Se aplica en AMBOS lados de cada JOIN para garantizar que
# referencias con distintos formatos siempre crucen.
# Usa .format() en lugar de f-string para evitar que Python
# interprete los cuantificadores regex {2,} como variables.
# ============================================================
def norm_sql(campo: str) -> str:
    c = "CAST({campo} AS VARCHAR)".format(campo=campo)
    return (
        "UPPER(trim("
            "regexp_replace("                                      # 8. colapsa espacios múltiples
            "regexp_replace("                                      # 7. elimina espacios alrededor de guión
            "regexp_replace("                                      # 6. colapsa guiones múltiples
            "regexp_replace("                                      # 5. colapsa puntos múltiples
            "regexp_replace("                                      # 4. elimina puntos al inicio/final
            "regexp_replace("                                      # 3. elimina chars no permitidos
                "replace("                                         # 2. guión bajo → guión
                    "trim(replace(replace(replace({c},"            # 1. elimina \t \n \r
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


# ============================================================
# SECCIÓN 4 — NORMALIZACIÓN POLARS
# Misma lógica que norm_sql pero para DataFrames Polars
# al leer los archivos Excel de precios y disponibilidad.
# ============================================================
def normalizar_ref_polars(col: pl.Expr) -> pl.Expr:
    return (
        col
        .cast(pl.Utf8)
        .str.strip_chars()                              # 1. espacios extremos
        .str.replace_all(r"\t|\n|\r", "")              # 2. elimina \t \n \r
        .str.replace_all(r"_", "-")                    # 3. guión bajo → guión
        .str.replace_all(r"[^A-Za-z0-9.\-\"/ ]", "")  # 4. solo chars permitidos
        .str.replace_all(r"^\.+|\.+$", "")             # 5. sin puntos extremos
        .str.replace_all(r"\.{2,}", ".")                # 6. colapsa puntos múltiples
        .str.replace_all(r"-{2,}", "-")                 # 7. colapsa guiones múltiples
        .str.replace_all(r"\s*-\s*", "-")              # 8. espacios alrededor de guión
        .str.replace_all(r" {2,}", " ")                 # 9. colapsa espacios múltiples
        .str.to_uppercase()                             # 10. todo en mayúsculas
    )


# ============================================================
# SECCIÓN 5 — INSTRUCCIONES DE USO
# ============================================================
def mostrar_instrucciones():
    print("\n📘 ESTRUCTURA REQUERIDA DE LOS ARCHIVOS EXCEL\n")
    print("📄 Excel de PRECIOS (una hoja por origen):")
    print("  · Hojas requeridas : USA, BRASIL, EUR")
    print("  · Columna A        : referencia del repuesto")
    print("  · Columna I        : precio de lista en origen")
    print("  · Fila 1           : encabezados (se ignoran)\n")
    print("📄 Excel de DISPONIBILIDADES:")
    print("  · Hoja requerida   : LISTA AGCS")
    print("  · Columna A        : referencia del repuesto")
    print("  · Columna I        : disponibilidad Brasil")
    print("  · Columna L        : disponibilidad Europa")
    print("  · Columna M        : disponibilidad USA")
    print("  · Fila 1           : encabezados (se ignoran)\n")
    print("📄 Excel MAESTRO COMERCIAL (Precios de Lista):")
    print("  · Columna 'Referencia'   : llave de cruce con catálogo técnico")
    print("  · Columna 'Part. Brasil' : % histórico de compra Brasil (0 a 1)")
    print("  · Columna 'Part. Usa'    : % histórico de compra USA    (0 a 1)")
    print("  · Columna 'Part. Europa' : % histórico de compra Europa (0 a 1)\n")


# ============================================================
# SECCIÓN 6 — VALIDACIÓN DE ARCHIVOS DE ENTRADA
# Verifica que los tres Excel existan antes de iniciar.
# Para el proceso con mensaje claro si alguno falta.
# ============================================================
def validar_archivos():
    archivos = {
        "Excel precios internacionales"  : EXCEL_PRECIOS,
        "Excel disponibilidad por origen": EXCEL_DISP,
        "Excel maestro comercial"        : EXCEL_LISTA,
    }
    print("📂 Verificando archivos de entrada:")
    for nombre, ruta in archivos.items():
        if not ruta.exists():
            raise FileNotFoundError(
                f"\n❌ Archivo no encontrado: {nombre}\n   Ruta esperada: {ruta}"
            )
        print(f"  ✅ {nombre}: {ruta.name}")
    print()


# ============================================================
# SECCIÓN 7 — LECTURA DE PRECIOS DESDE EXCEL
# Lee una hoja del Excel de precios y devuelve referencia
# normalizada + precio redondeado a 2 decimales.
# Descarta vacíos y duplicados.
# ============================================================
def leer_hoja_precio(sheet_name: str, col_precio_out: str) -> pl.DataFrame:
    df   = pl.read_excel(EXCEL_PRECIOS, sheet_name=sheet_name, has_header=True)
    cols = df.columns
    return (
        df.select([
            normalizar_ref_polars(pl.col(cols[0])).alias("referencia"),
            pl.col(cols[8])
              .cast(pl.Float64, strict=False)
              .round(2)                          # ← 2 decimales desde la lectura
              .alias(col_precio_out),
        ])
        .filter(pl.col("referencia").is_not_null() & (pl.col("referencia") != ""))
        .unique(subset=["referencia"], keep="first")
    )


# ============================================================
# SECCIÓN 8 — LECTURA DE DISPONIBILIDADES DESDE EXCEL
# Lee hoja LISTA AGCS y devuelve disponibilidad por origen.
# Valores no numéricos se convierten a NULL de forma segura.
# ============================================================
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


# ============================================================
# SECCIÓN 9 — CONSOLIDAR PRECIOS Y DISPONIBILIDADES
# Une precios de los tres orígenes y los cruza con
# disponibilidad. Guarda en DuckDB y CSV de auditoría.
# ============================================================
def consolidar_precios_disponibilidades():
    print("🔹 Leyendo precios por origen...")
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

    print("🔹 Leyendo disponibilidades...")
    df_disp = leer_disponibilidades()
    print(f"  Disponibilidad: {df_disp.height:,} referencias")

    df_final = df_precios.join(df_disp, on="referencia", how="left").sort("referencia")

    print("🔹 Guardando en DuckDB y CSV de auditoría...")
    with duckdb.connect(str(DUCKDB_PATH)) as con:
        con.register("tmp_precios", df_final.to_pandas())
        con.execute("""
            CREATE OR REPLACE TABLE precios_consolidados AS
            SELECT * FROM tmp_precios
        """)

    df_final.write_csv(CSV_OUT)
    print(f"  ✅ CSV guardado: {CSV_OUT.name}\n")


# ============================================================
# SECCIÓN 10 — EXTRACCIÓN DEL CATÁLOGO TÉCNICO DESDE SQL SERVER
# Extrae repuestos CNH con sus referencias alternas en bloques
# de 50.000 filas para no saturar memoria.
# ============================================================
def extraer_items_sqlserver():
    print("🔹 Conectando a SQL Server (UnoEE)...")
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
        duck.execute("DROP TABLE IF EXISTS " + TABLE_ITEMS)
        duck.execute(
            "CREATE TABLE " + TABLE_ITEMS + " ("
            "Item                 INTEGER,"
            "Referencia_Principal VARCHAR,"
            "Referencia_Alterna   VARCHAR,"
            "Descripcion_Item     VARCHAR,"
            "UM                   VARCHAR,"
            "Notas                VARCHAR,"
            "Estado               VARCHAR"
            ")"
        )

        print("🔹 Extrayendo catálogo técnico por bloques de 50.000 filas...")
        total = 0
        for chunk in pd.read_sql(SQL_QUERY, conn, chunksize=50_000):
            duck.register("tmp_chunk", chunk)
            duck.execute("INSERT INTO " + TABLE_ITEMS + " SELECT * FROM tmp_chunk")
            total += len(chunk)
            print(f"  {total:,} ítems cargados...")

    conn.close()
    print(f"  ✅ {total:,} ítems cargados en DuckDB\n")


# ============================================================
# SECCIÓN 11 — NORMALIZACIÓN Y CRUCE CON PRECIOS
# Limpia referencias alternas del catálogo y las cruza con
# precios consolidados. norm_sql() se aplica en ambos lados
# del JOIN para garantizar que formatos distintos crucen.
# ============================================================
def normalizar_y_cruzar_precios():
    print("🔹 Normalizando referencias y cruzando con precios...")

    norm_alterna = norm_sql("Referencia_Alterna")
    norm_ref     = norm_sql("n.Referencia")
    norm_precio  = norm_sql("p.referencia")

    sql = (
        "CREATE TABLE " + TABLE_ITEMS_PRECIOS + " AS "
        "WITH normalizado AS ("
        "    SELECT *,"
        # Original: texto legible sin saltos de línea (para auditoría)
        # NO se normaliza aquí — norm_sql() se aplica en Referencia_Activa
        "        trim(replace(replace(replace("
        "            Referencia_Alterna,"
        "            chr(9),''),chr(10),''),chr(13),'')"
        "        ) AS Original,"
        # Referencia: normalizada completa para el JOIN con precios
        "        " + norm_alterna + " AS Referencia"
        "    FROM " + TABLE_ITEMS +
        ") "
        "SELECT n.*, "
        "    p.precio_usa, p.precio_br, p.precio_eur,"
        "    p.disp_br, p.disp_eur, p.disp_usa"
        " FROM normalizado n"
        " LEFT JOIN precios_consolidados p"
        "    ON " + norm_ref + " = " + norm_precio
    )

    with duckdb.connect(str(DUCKDB_PATH)) as duck:
        duck.execute("DROP TABLE IF EXISTS " + TABLE_ITEMS_PRECIOS)
        duck.execute(sql)

    print("  ✅ Cruce completado\n")


# ============================================================
# SECCIÓN 12 — PRECIO POR FAMILIA Y CRUCE CON MAESTRO COMERCIAL
#
# A) precio_familia: selecciona una referencia activa por familia.
#    Referencia_Activa se normaliza con norm_sql() para consistencia.
#
# B) precios_lista: carga Excel con dtype=str para preservar
#    Referencia_Original exactamente como viene del Excel.
#    Las columnas numéricas se castean explícitamente a DOUBLE
#    en el SQL para evitar conflictos de tipo con VARCHAR.
#
# C) resultado_precios_lista: JOIN final + Precio Prorrateo.
#    Todos los precios se redondean a 2 decimales con ROUND(..., 2).
#    Las participaciones se castean a DOUBLE antes del COALESCE.
# ============================================================
def calcular_precio_lista():
    print("🔹 Calculando precio por familia técnica...")

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
        "    FROM " + TABLE_ITEMS_PRECIOS +
        "), "
        "familia AS ("
        "    SELECT"
        "        " + norm_ref_princ + " AS Referencia_Principal,"
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
        "    FROM base GROUP BY " + norm_ref_princ +
        "), "
        "ranked AS ("
        "    SELECT b.*, f.num_ref_activas, f.ratio_precio,"
        "        f.RefsAlternas, f.Precios_BR_USA_EURO, f.Dispon_BR_USA_EURO,"
        "        ROW_NUMBER() OVER ("
        "            PARTITION BY " + norm_b_ref_princ +
        "            ORDER BY"
        "                b.suma_disponibilidad DESC,"
        "                b.suma_precios DESC,"
        "                CASE WHEN " + norm_b_original + " = " + norm_b_ref_princ + " THEN 1 ELSE 0 END DESC"
        "        ) AS rn"
        "    FROM base b"
        "    JOIN familia f ON " + norm_b_ref_princ + " = f.Referencia_Principal"
        ") "
        "SELECT"
        "    " + norm_ref_princ + "                    AS Referencia_Principal,"
        "    Item,"
        "    " + norm_original + "                     AS Referencia_Activa,"
        # Precios redondeados a 2 decimales en precio_familia
        "    ROUND(precio_usa, 2)                      AS precio_usa,"
        "    ROUND(precio_br,  2)                      AS precio_br,"
        "    ROUND(precio_eur, 2)                      AS precio_eur,"
        "    disp_usa, disp_br, disp_eur,"
        "    ROUND(suma_precios, 2)                    AS suma_precios,"
        "    suma_disponibilidad,"
        "    num_ref_activas,"
        "    ROUND(ratio_precio, 4)                    AS ratio_precio,"
        "    RefsAlternas, Precios_BR_USA_EURO, Dispon_BR_USA_EURO"
        " FROM ranked WHERE rn = 1"
    )

    # ── B) Cargar Excel maestro comercial ───────────────────
    # dtype=str: todos los valores llegan como string puro —
    # preserva Referencia_Original exactamente como está en el Excel.
    # Las columnas numéricas (Part. Brasil/Usa/Europa) se castean
    # explícitamente a DOUBLE en sql_resultado para evitar el error
    # "Cannot mix VARCHAR and INTEGER_LITERAL in COALESCE".
    sql_lista = (
        "CREATE OR REPLACE TABLE precios_lista AS "
        "SELECT"
        "    CAST(Referencia AS VARCHAR) AS Referencia_Original,"
        "    " + norm_referencia + "     AS Referencia_Normalizada,"
        "    * EXCLUDE (Referencia)"
        " FROM tmp_lista"
    )

    # ── C) Resultado final con Precio Prorrateo ─────────────
    # · CAST(...AS DOUBLE) en participaciones: necesario porque
    #   dtype=str las dejó como VARCHAR al leer el Excel.
    # · ROUND(..., 2) en todos los precios y Precio Prorrateo:
    #   garantiza exactamente 2 decimales en el Excel de salida.
    # · JOIN: Referencia_Normalizada = Referencia_Principal
    # · Si no cruza → match_type = NO_MATCH
  
    sql_resultado = """
    CREATE OR REPLACE TABLE resultado_precios_lista AS
    SELECT
        -- Rowid primero como identificador único de cada fila
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
            WHEN pf.precio_br IS NULL AND pf.precio_usa IS NULL THEN ROUND(pf.precio_eur, 2)
            WHEN pf.precio_br IS NULL AND pf.precio_eur IS NULL THEN ROUND(pf.precio_usa, 2)
            WHEN pf.precio_usa IS NULL AND pf.precio_eur IS NULL THEN ROUND(pf.precio_br, 2)
            WHEN pf.precio_eur IS NULL THEN ROUND((pf.precio_br * 0.5) + (pf.precio_usa * 0.5), 2)
            WHEN pf.precio_usa IS NULL THEN ROUND((pf.precio_br * 0.5) + (pf.precio_eur * 0.5), 2)
            WHEN pf.precio_br  IS NULL THEN ROUND((pf.precio_usa * 0.5) + (pf.precio_eur * 0.5), 2)
            ELSE ROUND((pf.precio_br + pf.precio_usa + pf.precio_eur) / 3.0, 2)
        END AS "Precio Prorrateo"
    FROM precios_lista pl
    LEFT JOIN precio_familia pf
        ON pl.Referencia_Normalizada = pf.Referencia_Principal
"""

    with duckdb.connect(str(DUCKDB_PATH)) as duck:
        duck.execute(sql_familia)

        print("🔹 Cargando Excel maestro comercial...")
        df_lista = pd.read_excel(EXCEL_LISTA, dtype=str)
        duck.register("tmp_lista", df_lista)
        duck.execute(sql_lista)
        duck.execute(sql_resultado)

        df_out = duck.execute("SELECT * FROM resultado_precios_lista").df()

    df_out.to_excel(EXCEL_OUT, index=False)
    print(f"  ✅ Excel generado: {EXCEL_OUT.name}\n")


# ============================================================
# SECCIÓN 13 — PUNTO DE ENTRADA PRINCIPAL
# ============================================================
def main():
    print("\n" + "=" * 60)
    print("🚀  PIPELINE DE PRECIOS CNH — IMECOL S.A.S")
    print("=" * 60)

    mostrar_instrucciones()
    validar_archivos()

    consolidar_precios_disponibilidades()   # Paso 1
    extraer_items_sqlserver()               # Paso 2
    normalizar_y_cruzar_precios()           # Paso 3
    calcular_precio_lista()                 # Paso 4

    print("=" * 60)
    print("✅  PROCESO TERMINADO CORRECTAMENTE")
    print("\nArchivos generados:")
    print(f"  📊 Excel  → {EXCEL_OUT.name}")
    print(f"  🗄️  DuckDB → {DUCKDB_PATH.name}")
    print(f"  📄 CSV    → {CSV_OUT.name}")
    print(f"\n  Ruta: {BASE_DIR.resolve()}")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
