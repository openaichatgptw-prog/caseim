# ===========================================================================
# PIPELINE 3 OPTIMIZADO - ORIGEN + PRECIOS CNH (ESTABLE)
# ===========================================================================

import configparser
from pathlib import Path
from datetime import datetime
import pyodbc
import pandas as pd
import duckdb


# ===========================================================================
# CONFIGURACION
# ===========================================================================
BASE_DIR = Path(__file__).parent
config   = configparser.ConfigParser()
config.read(BASE_DIR / "config.ini", encoding="utf-8")

FECHA = datetime.now().strftime("%Y%m%d")

SERVER   = config["SQLSERVER"]["server"]
DATABASE = config["SQLSERVER"]["database"]
DB_USER  = config["SQLSERVER"]["db_user"]
DB_PASS  = config["SQLSERVER"]["db_pass"].strip('"')

DUCKDB_PATH = BASE_DIR / config["SALIDA"]["duckdb"]
EXCEL_OUT   = BASE_DIR / config["SALIDA_ORIGEN"]["excel"].replace("{fecha}", FECHA)
CSV_OUT     = BASE_DIR / config["SALIDA_ORIGEN"]["csv"].replace("{fecha}", FECHA)

FACTOR_BR  = float(config["FACTORES"]["factor_br"])
FACTOR_USA = float(config["FACTORES"]["factor_usa"])
FACTOR_EUR = float(config["FACTORES"]["factor_eur"])

CHUNK = 50_000


# ===========================================================================
# SQL ORIGEN
# ===========================================================================
SQL_ORIGEN = r"""
USE UnoEE;
SET NOCOUNT ON;

IF OBJECT_ID('tempdb..#BaseRefs') IS NOT NULL DROP TABLE #BaseRefs;

CREATE TABLE #BaseRefs (
    Referencia_Principal VARCHAR(50) NOT NULL PRIMARY KEY,
    Referencia_Alternas  VARCHAR(MAX) NULL
);

;WITH RefBase AS (
    SELECT 
        t120.f120_referencia AS RefPrincipal,
        t124.f124_referencia AS RefAlterna
    FROM t124_mc_items_referencias t124
    JOIN t120_mc_items t120 
        ON t124.f124_rowid_item = t120.f120_rowid
    WHERE t124.f124_referencia <> t120.f120_referencia

    UNION ALL

    SELECT 
        t120.f120_referencia,
        t120.f120_referencia
    FROM t120_mc_items t120
)
INSERT INTO #BaseRefs
SELECT 
    f.RefPrincipal,
    STUFF((
        SELECT DISTINCT ', ' + LTRIM(RTRIM(rb2.RefAlterna))
        FROM RefBase rb2
        WHERE rb2.RefPrincipal = f.RefPrincipal
        FOR XML PATH(''), TYPE
    ).value('.', 'NVARCHAR(MAX)'), 1, 2, '') 
FROM RefBase f
GROUP BY f.RefPrincipal;

CREATE INDEX IX_BaseRefs_Principal ON #BaseRefs(Referencia_Principal);

IF OBJECT_ID('tempdb..#Refs051') IS NOT NULL DROP TABLE #Refs051;

SELECT DISTINCT i.f120_referencia
INTO #Refs051
FROM t120_mc_items i
JOIN t125_mc_items_criterios cri
     ON i.f120_rowid = cri.f125_rowid_item
JOIN t106_mc_criterios_item_mayores may
     ON cri.f125_id_criterio_mayor = may.f106_id
WHERE cri.f125_id_plan = '07'
  AND RTRIM(may.f106_id) + ' - ' + may.f106_descripcion = '051 - REPUESTOS CASE';

CREATE INDEX IX_Refs051 ON #Refs051(f120_referencia);

IF OBJECT_ID('tempdb..#EstadoItem') IS NOT NULL DROP TABLE #EstadoItem;

SELECT x.Referencia,
       CASE 
            WHEN x.Estado = 0 THEN 'Inactivo'
            WHEN x.Estado = 2 THEN 'Bloqueado'
            ELSE 'Activo'
       END AS EstadoItem
INTO #EstadoItem
FROM (
    SELECT 
        i.f120_referencia AS Referencia,
        e.f121_ind_estado AS Estado,
        ROW_NUMBER() OVER(PARTITION BY i.f120_referencia ORDER BY e.f121_fecha_actualizacion DESC) AS rn
    FROM t120_mc_items i
    JOIN #Refs051 r ON r.f120_referencia = i.f120_referencia
    JOIN t121_mc_items_extensiones e 
         ON i.f120_rowid = e.f121_rowid_item
) x
WHERE x.rn = 1;

CREATE INDEX IX_EstadoItem ON #EstadoItem(Referencia);

;WITH RefItemExt AS (
    SELECT 
        i.f120_referencia AS Referencia,
        e.f121_rowid AS RowidItemExt
    FROM t120_mc_items i
    JOIN #Refs051 r ON r.f120_referencia = i.f120_referencia
    JOIN t121_mc_items_extensiones e ON i.f120_rowid = e.f121_rowid_item
),
Ingresos AS (
    SELECT 
        mov.f41851_rowid_item_ext AS RowidItemExt,
        CASE 
            WHEN p.f011_id IN ('076','105') THEN 'Brazil'
            WHEN p.f011_id IN ('249','840') THEN 'USA'
            ELSE 'Otros'
        END AS Origen,
        imp.f41850_fecha AS Fecha_Import,
        mov.f41851_cant_entrada AS Cant_Entrada
    FROM t41851_import_movto mov
    JOIN t41850_import_docto imp ON mov.f41851_rowid_docto_import = imp.f41850_rowid
    JOIN t41806_import_origen_destino od ON imp.f41850_rowid_origen = od.f41806_rowid
    JOIN t011_mm_paises p ON od.f41806_id_pais = p.f011_id
    WHERE imp.f41850_ind_estado = 4
      AND imp.f41850_id_tipo_docto = 'IM'
),
Pct AS (
    SELECT 
        r.Referencia AS Referencia_Principal,

        SUM(i.Cant_Entrada) AS Hist_Total,
        SUM(CASE WHEN i.Origen='Brazil' THEN i.Cant_Entrada END) AS Hist_BR_Cant,
        SUM(CASE WHEN i.Origen='USA'    THEN i.Cant_Entrada END) AS Hist_USA_Cant,
        SUM(CASE WHEN i.Origen='Otros'  THEN i.Cant_Entrada END) AS Hist_EUR_Cant,

        CAST(
            SUM(CASE WHEN i.Origen='Brazil' THEN i.Cant_Entrada END) * 100.0
            / NULLIF(SUM(i.Cant_Entrada),0)
        AS DECIMAL(10,4)) AS Hist_BR_Pct,
        CAST(
            SUM(CASE WHEN i.Origen='USA' THEN i.Cant_Entrada END) * 100.0
            / NULLIF(SUM(i.Cant_Entrada),0)
        AS DECIMAL(10,4)) AS Hist_USA_Pct,
        CAST(
            SUM(CASE WHEN i.Origen='Otros' THEN i.Cant_Entrada END) * 100.0
            / NULLIF(SUM(i.Cant_Entrada),0)
        AS DECIMAL(10,4)) AS Hist_EUR_Pct,

        SUM(CASE WHEN i.Fecha_Import >= DATEADD(YEAR,-1,GETDATE()) THEN i.Cant_Entrada END) AS Cant_Vig,
        SUM(CASE WHEN i.Fecha_Import >= DATEADD(YEAR,-2,GETDATE())
                  AND i.Fecha_Import < DATEADD(YEAR,-1,GETDATE()) THEN i.Cant_Entrada END) AS Cant_Ant,
        SUM(CASE WHEN i.Fecha_Import >= DATEADD(YEAR,-3,GETDATE())
                  AND i.Fecha_Import < DATEADD(YEAR,-2,GETDATE()) THEN i.Cant_Entrada END) AS Cant_Año3,

        CAST(
            SUM(CASE WHEN i.Origen='Brazil' AND i.Fecha_Import >= DATEADD(YEAR,-1,GETDATE()) THEN i.Cant_Entrada END) * 100.0
            / NULLIF(SUM(CASE WHEN i.Fecha_Import >= DATEADD(YEAR,-1,GETDATE()) THEN i.Cant_Entrada END),0)
        AS DECIMAL(10,4)) AS Vig_BR_Pct,
        CAST(
            SUM(CASE WHEN i.Origen='USA' AND i.Fecha_Import >= DATEADD(YEAR,-1,GETDATE()) THEN i.Cant_Entrada END) * 100.0
            / NULLIF(SUM(CASE WHEN i.Fecha_Import >= DATEADD(YEAR,-1,GETDATE()) THEN i.Cant_Entrada END),0)
        AS DECIMAL(10,4)) AS Vig_USA_Pct,
        CAST(
            SUM(CASE WHEN i.Origen='Otros' AND i.Fecha_Import >= DATEADD(YEAR,-1,GETDATE()) THEN i.Cant_Entrada END) * 100.0
            / NULLIF(SUM(CASE WHEN i.Fecha_Import >= DATEADD(YEAR,-1,GETDATE()) THEN i.Cant_Entrada END),0)
        AS DECIMAL(10,4)) AS Vig_EUR_Pct,

        CAST(
            SUM(CASE WHEN i.Origen='Brazil'
                      AND i.Fecha_Import >= DATEADD(YEAR,-2,GETDATE())
                      AND i.Fecha_Import < DATEADD(YEAR,-1,GETDATE()) THEN i.Cant_Entrada END) * 100.0
            / NULLIF(SUM(CASE WHEN i.Fecha_Import >= DATEADD(YEAR,-2,GETDATE())
                               AND i.Fecha_Import < DATEADD(YEAR,-1,GETDATE()) THEN i.Cant_Entrada END),0)
        AS DECIMAL(10,4)) AS Ant_BR_Pct,
        CAST(
            SUM(CASE WHEN i.Origen='USA'
                      AND i.Fecha_Import >= DATEADD(YEAR,-2,GETDATE())
                      AND i.Fecha_Import < DATEADD(YEAR,-1,GETDATE()) THEN i.Cant_Entrada END) * 100.0
            / NULLIF(SUM(CASE WHEN i.Fecha_Import >= DATEADD(YEAR,-2,GETDATE())
                               AND i.Fecha_Import < DATEADD(YEAR,-1,GETDATE()) THEN i.Cant_Entrada END),0)
        AS DECIMAL(10,4)) AS Ant_USA_Pct,
        CAST(
            SUM(CASE WHEN i.Origen='Otros'
                      AND i.Fecha_Import >= DATEADD(YEAR,-2,GETDATE())
                      AND i.Fecha_Import < DATEADD(YEAR,-1,GETDATE()) THEN i.Cant_Entrada END) * 100.0
            / NULLIF(SUM(CASE WHEN i.Fecha_Import >= DATEADD(YEAR,-2,GETDATE())
                               AND i.Fecha_Import < DATEADD(YEAR,-1,GETDATE()) THEN i.Cant_Entrada END),0)
        AS DECIMAL(10,4)) AS Ant_EUR_Pct,

        CAST(
            SUM(CASE WHEN i.Origen='Brazil'
                      AND i.Fecha_Import >= DATEADD(YEAR,-3,GETDATE())
                      AND i.Fecha_Import < DATEADD(YEAR,-2,GETDATE()) THEN i.Cant_Entrada END) * 100.0
            / NULLIF(SUM(CASE WHEN i.Fecha_Import >= DATEADD(YEAR,-3,GETDATE())
                               AND i.Fecha_Import < DATEADD(YEAR,-2,GETDATE()) THEN i.Cant_Entrada END),0)
        AS DECIMAL(10,4)) AS Año3_BR_Pct,
        CAST(
            SUM(CASE WHEN i.Origen='USA'
                      AND i.Fecha_Import >= DATEADD(YEAR,-3,GETDATE())
                      AND i.Fecha_Import < DATEADD(YEAR,-2,GETDATE()) THEN i.Cant_Entrada END) * 100.0
            / NULLIF(SUM(CASE WHEN i.Fecha_Import >= DATEADD(YEAR,-3,GETDATE())
                               AND i.Fecha_Import < DATEADD(YEAR,-2,GETDATE()) THEN i.Cant_Entrada END),0)
        AS DECIMAL(10,4)) AS Año3_USA_Pct,
        CAST(
            SUM(CASE WHEN i.Origen='Otros'
                      AND i.Fecha_Import >= DATEADD(YEAR,-3,GETDATE())
                      AND i.Fecha_Import < DATEADD(YEAR,-2,GETDATE()) THEN i.Cant_Entrada END) * 100.0
            / NULLIF(SUM(CASE WHEN i.Fecha_Import >= DATEADD(YEAR,-3,GETDATE())
                               AND i.Fecha_Import < DATEADD(YEAR,-2,GETDATE()) THEN i.Cant_Entrada END),0)
        AS DECIMAL(10,4)) AS Año3_EUR_Pct
    FROM RefItemExt r
    LEFT JOIN Ingresos i ON i.RowidItemExt = r.RowidItemExt
    GROUP BY r.Referencia
)

SELECT
    pct.Referencia_Principal,
    br.Referencia_Alternas,
    est.EstadoItem,
    pct.Hist_Total,
    pct.Hist_BR_Cant,
    pct.Hist_USA_Cant,
    pct.Hist_EUR_Cant,
    pct.Hist_BR_Pct,
    pct.Hist_USA_Pct,
    pct.Hist_EUR_Pct,
    pct.Cant_Vig,
    pct.Cant_Ant,
    pct.Cant_Año3,
    pct.Vig_BR_Pct,
    pct.Vig_USA_Pct,
    pct.Vig_EUR_Pct,
    pct.Ant_BR_Pct,
    pct.Ant_USA_Pct,
    pct.Ant_EUR_Pct,
    pct.Año3_BR_Pct,
    pct.Año3_USA_Pct,
    pct.Año3_EUR_Pct,
    CASE 
        WHEN ISNULL(pct.Hist_Total,0) = 0 THEN 'SIN IMPORTACIONES'
        WHEN 
            (ISNULL(pct.Vig_BR_Pct,0)=0 AND ISNULL(pct.Ant_BR_Pct,0)=0 AND ISNULL(pct.Año3_BR_Pct,0)=0) AND
            (ISNULL(pct.Vig_USA_Pct,0)=0 AND ISNULL(pct.Ant_USA_Pct,0)=0 AND ISNULL(pct.Año3_USA_Pct,0)=0) AND
            (ISNULL(pct.Vig_EUR_Pct,0)=0 AND ISNULL(pct.Ant_EUR_Pct,0)=0 AND ISNULL(pct.Año3_EUR_Pct,0)=0)
        THEN 
            CASE 
                WHEN ISNULL(pct.Hist_BR_Cant,0) > 0 
                 AND ISNULL(pct.Hist_USA_Cant,0) = 0 
                 AND ISNULL(pct.Hist_EUR_Cant,0) = 0 
                THEN 'MONO-BR (HIST)'
                WHEN ISNULL(pct.Hist_USA_Cant,0) > 0 
                 AND ISNULL(pct.Hist_BR_Cant,0) = 0 
                 AND ISNULL(pct.Hist_EUR_Cant,0) = 0 
                THEN 'MONO-USA (HIST)'
                WHEN ISNULL(pct.Hist_EUR_Cant,0) > 0 
                 AND ISNULL(pct.Hist_BR_Cant,0) = 0 
                 AND ISNULL(pct.Hist_USA_Cant,0) = 0 
                THEN 'MONO-EUR (HIST)'
                WHEN 
                    (ISNULL(pct.Hist_BR_Cant,0) > 0 AND ISNULL(pct.Hist_USA_Cant,0) > 0)
                 OR (ISNULL(pct.Hist_BR_Cant,0) > 0 AND ISNULL(pct.Hist_EUR_Cant,0) > 0)
                 OR (ISNULL(pct.Hist_USA_Cant,0) > 0 AND ISNULL(pct.Hist_EUR_Cant,0) > 0)
                THEN 'MIXTO (HIST)'
                ELSE 'SIN ORIGEN HIST'
            END
        WHEN 
            (ISNULL(pct.Vig_BR_Pct,0) + ISNULL(pct.Ant_BR_Pct,0) + ISNULL(pct.Año3_BR_Pct,0)) > 0
            AND (ISNULL(pct.Vig_USA_Pct,0) + ISNULL(pct.Ant_USA_Pct,0) + ISNULL(pct.Año3_USA_Pct,0)) = 0
            AND (ISNULL(pct.Vig_EUR_Pct,0) + ISNULL(pct.Ant_EUR_Pct,0) + ISNULL(pct.Año3_EUR_Pct,0)) = 0
        THEN 'MONO-BR'
        WHEN 
            (ISNULL(pct.Vig_USA_Pct,0) + ISNULL(pct.Ant_USA_Pct,0) + ISNULL(pct.Año3_USA_Pct,0)) > 0
            AND (ISNULL(pct.Vig_BR_Pct,0) + ISNULL(pct.Ant_BR_Pct,0) + ISNULL(pct.Año3_BR_Pct,0)) = 0
            AND (ISNULL(pct.Vig_EUR_Pct,0) + ISNULL(pct.Ant_EUR_Pct,0) + ISNULL(pct.Año3_EUR_Pct,0)) = 0
        THEN 'MONO-USA'
        WHEN 
            (ISNULL(pct.Vig_EUR_Pct,0) + ISNULL(pct.Ant_EUR_Pct,0) + ISNULL(pct.Año3_EUR_Pct,0)) > 0
            AND (ISNULL(pct.Vig_BR_Pct,0) + ISNULL(pct.Ant_BR_Pct,0) + ISNULL(pct.Año3_BR_Pct,0)) = 0
            AND (ISNULL(pct.Vig_USA_Pct,0) + ISNULL(pct.Ant_USA_Pct,0) + ISNULL(pct.Año3_USA_Pct,0)) = 0
        THEN 'MONO-EUR'
        ELSE 'MIXTO'
    END AS Tipo_Origen
FROM Pct pct
LEFT JOIN #BaseRefs br ON br.Referencia_Principal = pct.Referencia_Principal
LEFT JOIN #EstadoItem est ON est.Referencia = pct.Referencia_Principal
ORDER BY pct.Referencia_Principal;
"""


# ===========================================================================
# UTILIDADES
# ===========================================================================
def convertir_chunk(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convierte todas las columnas a float64 salvo las que son
    puramente texto (VARCHAR). Esto evita que DuckDB infiera
    DECIMAL(6,4) al registrar el DataFrame y falle con valores > 99.
    """
    for col in df.columns:
        if df[col].dtype == object:
            # intentar convertir a numérico; si falla deja como str
            converted = pd.to_numeric(df[col], errors="coerce")
            if converted.notna().sum() > 0 and df[col].notna().sum() > 0:
                ratio = converted.notna().sum() / df[col].notna().sum()
                if ratio > 0.9:   # si >90% son numéricos, convertir
                    df[col] = converted
        else:
            # columna ya numérica: forzar float64
            df[col] = df[col].astype("float64", errors="ignore")
    return df


def optimizar_tipos(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.select_dtypes(include=["int64"]).columns:
        df[col] = pd.to_numeric(df[col], downcast="integer")
    return df


# ===========================================================================
# CARGAR ORIGEN POR CHUNKS → DUCKDB
# ===========================================================================
def cargar_origen_en_duckdb():
    print("Conectando a SQL Server (UnoEE)...")
    conn = pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={SERVER};DATABASE={DATABASE};"
        f"UID={DB_USER};PWD={DB_PASS};Encrypt=yes;TrustServerCertificate=yes;"
    )

    with duckdb.connect(str(DUCKDB_PATH)) as con:
        try:
            print("Ejecutando query de origen / importaciones con cursor pyodbc...")

            cur = conn.cursor()
            cur.execute(SQL_ORIGEN)

            # Avanzar hasta el SELECT final
            while True:
                desc = cur.description
                if desc is not None:
                    break
                if not cur.nextset():
                    print("  La consulta no devolvió ningún resultset.")
                    return

            col_names = [c[0] for c in cur.description]

            primera = True
            total = 0
            chunk_index = 0

            while True:
                rows = cur.fetchmany(CHUNK)
                if not rows:
                    break

                chunk = pd.DataFrame.from_records(rows, columns=col_names)

                # ── KEY FIX: forzar float64 antes de registrar en DuckDB ──
                chunk = convertir_chunk(chunk)
                chunk = optimizar_tipos(chunk)

                con.register("origen_chunk", chunk)

                if primera:
                    con.execute(
                        "CREATE OR REPLACE TABLE origen_importaciones AS "
                        "SELECT * FROM origen_chunk"
                    )
                    primera = False
                else:
                    con.execute(
                        "INSERT INTO origen_importaciones "
                        "SELECT * FROM origen_chunk"
                    )

                total += len(chunk)
                chunk_index += 1
                print(f"  Chunk {chunk_index} -> {len(chunk):,} filas (acum: {total:,})")

            if primera:
                print("  El SELECT final no devolvió filas.")
            else:
                print(f"  Tabla origen_importaciones creada ({total:,} filas)")

        finally:
            conn.close()
            print("  Conexion SQL cerrada")


# ===========================================================================
# CALCULO COMPLETO EN DUCKDB
# ===========================================================================
def calcular_en_duckdb():
    with duckdb.connect(str(DUCKDB_PATH)) as con:
        print("\nValidando resultado_precios_lista en DuckDB...")

        existe = con.execute("""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_name = 'resultado_precios_lista'
        """).fetchone()[0]

        if existe == 0:
            raise ValueError("No existe la tabla resultado_precios_lista en DuckDB.")

        print("Creando Ref_Norm y tabla tablero en DuckDB...")

        # MACRO calc_varperm
        con.execute("""
            CREATE OR REPLACE MACRO calc_varperm(minp) AS (
                CASE
                    WHEN minp IS NULL THEN NULL
                    WHEN minp < 1 THEN 10.0
                    WHEN minp <= 3 THEN 1.0
                    WHEN minp <= 10 THEN 0.3
                    WHEN minp <= 100 THEN 0.2
                    WHEN minp <= 2000 THEN 0.1
                    ELSE 0.05
                END
            );
        """)

        # Normalización + cast explícito a DOUBLE (doble garantía)
        con.execute("""
            CREATE OR REPLACE TABLE origen_importaciones_norm AS
            SELECT
                CAST(Referencia_Principal AS VARCHAR) AS Referencia_Principal,
                CAST(Referencia_Alternas  AS VARCHAR) AS Referencia_Alternas,
                CAST(EstadoItem           AS VARCHAR) AS EstadoItem,
                CAST(Hist_Total           AS DOUBLE)  AS Hist_Total,
                CAST(Hist_BR_Cant         AS DOUBLE)  AS Hist_BR_Cant,
                CAST(Hist_USA_Cant        AS DOUBLE)  AS Hist_USA_Cant,
                CAST(Hist_EUR_Cant        AS DOUBLE)  AS Hist_EUR_Cant,
                CAST(Hist_BR_Pct          AS DOUBLE)  AS Hist_BR_Pct,
                CAST(Hist_USA_Pct         AS DOUBLE)  AS Hist_USA_Pct,
                CAST(Hist_EUR_Pct         AS DOUBLE)  AS Hist_EUR_Pct,
                CAST(Cant_Vig             AS DOUBLE)  AS Cant_Vig,
                CAST(Cant_Ant             AS DOUBLE)  AS Cant_Ant,
                CAST(Cant_Año3            AS DOUBLE)  AS Cant_Año3,
                CAST(Vig_BR_Pct           AS DOUBLE)  AS Vig_BR_Pct,
                CAST(Vig_USA_Pct          AS DOUBLE)  AS Vig_USA_Pct,
                CAST(Vig_EUR_Pct          AS DOUBLE)  AS Vig_EUR_Pct,
                CAST(Ant_BR_Pct           AS DOUBLE)  AS Ant_BR_Pct,
                CAST(Ant_USA_Pct          AS DOUBLE)  AS Ant_USA_Pct,
                CAST(Ant_EUR_Pct          AS DOUBLE)  AS Ant_EUR_Pct,
                CAST(Año3_BR_Pct          AS DOUBLE)  AS Año3_BR_Pct,
                CAST(Año3_USA_Pct         AS DOUBLE)  AS Año3_USA_Pct,
                CAST(Año3_EUR_Pct         AS DOUBLE)  AS Año3_EUR_Pct,
                CAST(Tipo_Origen          AS VARCHAR) AS Tipo_Origen,
                UPPER(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(
                                REGEXP_REPLACE(
                                    REGEXP_REPLACE(
                                        REGEXP_REPLACE(
                                            REPLACE(
                                                TRIM(REPLACE(REPLACE(REPLACE(CAST(Referencia_Principal AS VARCHAR),
                                                                              '\t',''),
                                                              '\n',''),
                                                      '\r','')),
                                                '_','-'
                                            ),
                                            '[^A-Za-z0-9.\\-\"/ ]','','g'
                                        ),
                                        '^\\.+|\\.+$','','g'
                                    ),
                                    '\\.{2,}','.',
                                    'g'
                                ),
                                '-{2,}','-',
                                'g'
                            ),
                            '\\s*-\\s*','-',
                            'g'
                        ),
                        ' {2,}',' ',
                        'g'
                    )
                ) AS Ref_Norm
            FROM origen_importaciones;
        """)

        # Tabla final
        con.execute(f"""
            CREATE OR REPLACE TABLE origen_precios_tablero AS
            WITH base AS (
                SELECT
                    o.Ref_Norm,
                    o.Referencia_Principal,
                    o.Referencia_Alternas,
                    o.EstadoItem,
                    o.Hist_Total,
                    o.Hist_BR_Cant,
                    o.Hist_USA_Cant,
                    o.Hist_EUR_Cant,
                    o.Hist_BR_Pct,
                    o.Hist_USA_Pct,
                    o.Hist_EUR_Pct,
                    o.Cant_Vig,
                    o.Cant_Ant,
                    o.Cant_Año3,
                    o.Vig_BR_Pct,
                    o.Vig_USA_Pct,
                    o.Vig_EUR_Pct,
                    o.Ant_BR_Pct,
                    o.Ant_USA_Pct,
                    o.Ant_EUR_Pct,
                    o.Año3_BR_Pct,
                    o.Año3_USA_Pct,
                    o.Año3_EUR_Pct,
                    o.Tipo_Origen,
                    p.disp_br AS "Disponibilidad Brasil",
                    p.disp_usa AS "Disponibilidad USA",
                    p.disp_eur AS "Disponibilidad EUR",
                    p."Precio Brasil" AS "Precio Origen Brasil",
                    p."Precio Usa" AS "Precio Origen USA",
                    p."Precio Europa" AS "Precio Origen Europa"
                FROM origen_importaciones_norm o
                LEFT JOIN resultado_precios_lista p
                    ON o.Ref_Norm = p.Referencia_Normalizada
            ),
            precios_ajustados AS (
                SELECT
                    *,
                    "Precio Origen Brasil" * {FACTOR_BR}  AS "Precio Origen Brasil 1.25%",
                    "Precio Origen USA"    * {FACTOR_USA} AS "Precio Origen USA 1.25%",
                    "Precio Origen Europa" * {FACTOR_EUR} AS "Precio Origen Europa 1.7%"
                FROM base
            ),
            aux_min_prom AS (
                SELECT
                    *,
                    ("Precio Origen Brasil 1.25%" + "Precio Origen USA 1.25%") / 2.0 AS PROM_BR_USA,
                    LEAST(
                        COALESCE("Precio Origen Brasil 1.25%", 1e99),
                        COALESCE("Precio Origen USA 1.25%", 1e99),
                        COALESCE("Precio Origen Europa 1.7%", 1e99)
                    ) AS MINP
                FROM precios_ajustados
            ),
            con_calculos AS (
                SELECT
                    *,
                    CASE 
                        WHEN COALESCE("Disponibilidad Brasil", 0) + 
                             COALESCE("Disponibilidad USA", 0) + 
                             COALESCE("Disponibilidad EUR", 0) > 0 
                        THEN 'SI' 
                        ELSE 'NO' 
                    END AS "DISPONIBLE (SI/NO)",

                    CASE
                        WHEN "Precio Origen Brasil 1.25%" IS NULL
                          OR "Precio Origen USA 1.25%" IS NULL
                        THEN COALESCE(
                            "Precio Origen Brasil 1.25%",
                            "Precio Origen USA 1.25%",
                            "Precio Origen Europa 1.7%"
                        )
                        ELSE
                            CASE
                                WHEN ABS("Precio Origen Brasil 1.25%" - "Precio Origen USA 1.25%")
                                     / NULLIF(LEAST("Precio Origen Brasil 1.25%", "Precio Origen USA 1.25%"), 0)
                                     <= calc_varperm(MINP)
                                THEN
                                    CASE
                                        WHEN "Precio Origen Europa 1.7%" IS NOT NULL
                                         AND ABS("Precio Origen Europa 1.7%" - PROM_BR_USA)
                                             / NULLIF(PROM_BR_USA, 0)
                                             <= calc_varperm(MINP)
                                        THEN ("Precio Origen Brasil 1.25%" +
                                              "Precio Origen USA 1.25%" +
                                              "Precio Origen Europa 1.7%") / 3.0
                                        ELSE ("Precio Origen Brasil 1.25%" +
                                              "Precio Origen USA 1.25%") / 2.0
                                    END
                                ELSE NULL
                            END
                    END AS "P.prorrateo (Precio)",

                    CASE
                        WHEN COALESCE(Hist_BR_Pct, 0) + COALESCE(Hist_USA_Pct, 0) + COALESCE(Hist_EUR_Pct, 0) = 0
                        THEN NULL

                        WHEN
                            (COALESCE(Vig_BR_Pct, 0)  + COALESCE(Vig_USA_Pct, 0)  + COALESCE(Vig_EUR_Pct, 0)  > 0)
                            OR
                            (COALESCE(Ant_BR_Pct, 0)  + COALESCE(Ant_USA_Pct, 0)  + COALESCE(Ant_EUR_Pct, 0)  > 0)
                            OR
                            (COALESCE(Año3_BR_Pct, 0) + COALESCE(Año3_USA_Pct, 0) + COALESCE(Año3_EUR_Pct, 0) > 0)
                        THEN
                            (
                                (
                                    CASE
                                        WHEN COALESCE(Vig_BR_Pct, 0) + COALESCE(Vig_USA_Pct, 0) + COALESCE(Vig_EUR_Pct, 0) > 0
                                        THEN 0.60 * (
                                            COALESCE("Precio Origen Brasil 1.25%", 0) * COALESCE(Vig_BR_Pct, 0) +
                                            COALESCE("Precio Origen USA 1.25%", 0)    * COALESCE(Vig_USA_Pct, 0) +
                                            COALESCE("Precio Origen Europa 1.7%", 0)  * COALESCE(Vig_EUR_Pct, 0)
                                        )
                                        ELSE 0
                                    END
                                )
                                +
                                (
                                    CASE
                                        WHEN COALESCE(Ant_BR_Pct, 0) + COALESCE(Ant_USA_Pct, 0) + COALESCE(Ant_EUR_Pct, 0) > 0
                                        THEN 0.25 * (
                                            COALESCE("Precio Origen Brasil 1.25%", 0) * COALESCE(Ant_BR_Pct, 0) +
                                            COALESCE("Precio Origen USA 1.25%", 0)    * COALESCE(Ant_USA_Pct, 0) +
                                            COALESCE("Precio Origen Europa 1.7%", 0)  * COALESCE(Ant_EUR_Pct, 0)
                                        )
                                        ELSE 0
                                    END
                                )
                                +
                                (
                                    CASE
                                        WHEN COALESCE(Año3_BR_Pct, 0) + COALESCE(Año3_USA_Pct, 0) + COALESCE(Año3_EUR_Pct, 0) > 0
                                        THEN 0.15 * (
                                            COALESCE("Precio Origen Brasil 1.25%", 0) * COALESCE(Año3_BR_Pct, 0) +
                                            COALESCE("Precio Origen USA 1.25%", 0)    * COALESCE(Año3_USA_Pct, 0) +
                                            COALESCE("Precio Origen Europa 1.7%", 0)  * COALESCE(Año3_EUR_Pct, 0)
                                        )
                                        ELSE 0
                                    END
                                )
                            )
                            /
                            NULLIF(
                                (CASE WHEN COALESCE(Vig_BR_Pct, 0)  + COALESCE(Vig_USA_Pct, 0)  + COALESCE(Vig_EUR_Pct, 0)  > 0 THEN 0.60 ELSE 0 END) +
                                (CASE WHEN COALESCE(Ant_BR_Pct, 0)  + COALESCE(Ant_USA_Pct, 0)  + COALESCE(Ant_EUR_Pct, 0)  > 0 THEN 0.25 ELSE 0 END) +
                                (CASE WHEN COALESCE(Año3_BR_Pct, 0) + COALESCE(Año3_USA_Pct, 0) + COALESCE(Año3_EUR_Pct, 0) > 0 THEN 0.15 ELSE 0 END),
                                0
                            )
                            / 100.0

                        ELSE
                            (
                                COALESCE("Precio Origen Brasil 1.25%", 0) * COALESCE(Hist_BR_Pct, 0) +
                                COALESCE("Precio Origen USA 1.25%", 0)    * COALESCE(Hist_USA_Pct, 0) +
                                COALESCE("Precio Origen Europa 1.7%", 0)  * COALESCE(Hist_EUR_Pct, 0)
                            ) / 100.0
                    END AS "P.Hist (0.6/0.25/0.15)"

                FROM aux_min_prom
            ),
            con_variaciones AS (
                SELECT
                    *,
                    CASE
                        WHEN "Precio Origen Brasil 1.25%" IS NULL 
                          OR "Precio Origen USA 1.25%" IS NULL
                        THEN 
                            CASE 
                                WHEN "Precio Origen Brasil 1.25%" IS NOT NULL 
                                  OR "Precio Origen USA 1.25%" IS NOT NULL 
                                  OR "Precio Origen Europa 1.7%" IS NOT NULL
                                THEN 0.0
                                ELSE NULL
                            END
                        WHEN LEAST("Precio Origen Brasil 1.25%", "Precio Origen USA 1.25%") = 0
                        THEN 0.0
                        ELSE ABS("Precio Origen Brasil 1.25%" - "Precio Origen USA 1.25%") /
                             LEAST("Precio Origen Brasil 1.25%", "Precio Origen USA 1.25%")
                    END AS "VAR BR_USA",

                    CASE
                        WHEN "Precio Origen Brasil 1.25%" IS NULL 
                          OR "Precio Origen USA 1.25%" IS NULL 
                          OR "Precio Origen Europa 1.7%" IS NULL
                        THEN NULL
                        ELSE ABS("Precio Origen Europa 1.7%" - PROM_BR_USA)
                             / NULLIF(PROM_BR_USA, 0)
                    END AS "VAR_PROM(USA_BR)_EUR",

                    CASE
                        WHEN "Precio Origen Brasil 1.25%" IS NOT NULL 
                         AND "Precio Origen USA 1.25%" IS NOT NULL 
                         AND "Precio Origen Europa 1.7%" IS NOT NULL
                        THEN
                            CASE
                                WHEN LEAST("Precio Origen Brasil 1.25%",
                                           "Precio Origen USA 1.25%",
                                           "Precio Origen Europa 1.7%") = 0
                                THEN 0.0
                                ELSE (GREATEST("Precio Origen Brasil 1.25%",
                                               "Precio Origen USA 1.25%",
                                               "Precio Origen Europa 1.7%") -
                                      LEAST("Precio Origen Brasil 1.25%",
                                            "Precio Origen USA 1.25%",
                                            "Precio Origen Europa 1.7%")) /
                                     LEAST("Precio Origen Brasil 1.25%",
                                           "Precio Origen USA 1.25%",
                                           "Precio Origen Europa 1.7%")
                            END
                        ELSE NULL
                    END AS "VAR BR_USA_EUR",

                    ABS("Precio Origen Brasil 1.25%" - "Precio Origen USA 1.25%")
                        / NULLIF(LEAST("Precio Origen Brasil 1.25%", "Precio Origen USA 1.25%"), 0)
                    AS VAR_BR_USA_SAFE,

                    CASE
                        WHEN LEAST("Precio Origen Brasil 1.25%",
                                   "Precio Origen USA 1.25%",
                                   "Precio Origen Europa 1.7%") = 0
                        THEN NULL
                        ELSE (GREATEST("Precio Origen Brasil 1.25%",
                                       "Precio Origen USA 1.25%",
                                       "Precio Origen Europa 1.7%") -
                              LEAST("Precio Origen Brasil 1.25%",
                                    "Precio Origen USA 1.25%",
                                    "Precio Origen Europa 1.7%")) /
                             LEAST("Precio Origen Brasil 1.25%",
                                   "Precio Origen USA 1.25%",
                                   "Precio Origen Europa 1.7%")
                    END AS VAR_TOTAL_SAFE

                FROM con_calculos
            ),
            con_porc_var AS (
                SELECT
                    *,
                    CASE
                        WHEN "Precio Origen Brasil 1.25%" IS NULL 
                         AND "Precio Origen USA 1.25%" IS NULL 
                         AND "Precio Origen Europa 1.7%" IS NULL
                        THEN NULL

                        WHEN ("Precio Origen Brasil 1.25%" IS NOT NULL 
                           AND "Precio Origen USA 1.25%" IS NULL 
                           AND "Precio Origen Europa 1.7%" IS NULL)
                          OR ("Precio Origen Brasil 1.25%" IS NULL 
                           AND "Precio Origen USA 1.25%" IS NOT NULL 
                           AND "Precio Origen Europa 1.7%" IS NULL)
                          OR ("Precio Origen Brasil 1.25%" IS NULL 
                           AND "Precio Origen USA 1.25%" IS NULL 
                           AND "Precio Origen Europa 1.7%" IS NOT NULL)
                        THEN 0.0

                        WHEN "Precio Origen Brasil 1.25%" IS NULL OR "Precio Origen USA 1.25%" IS NULL
                        THEN 0.0

                        ELSE
                            CASE
                                WHEN "VAR BR_USA" <= calc_varperm(MINP)
                                THEN
                                    CASE
                                        WHEN "Precio Origen Europa 1.7%" IS NOT NULL 
                                         AND "VAR_PROM(USA_BR)_EUR" <= calc_varperm(MINP)
                                        THEN "VAR BR_USA_EUR"
                                        ELSE "VAR BR_USA"
                                    END
                                ELSE 0.0
                            END
                    END AS "Porc(%) de Var",

                    CASE
                        WHEN "P.prorrateo (Precio)" IS NULL
                        THEN NULL
                        WHEN "Precio Origen Brasil 1.25%" IS NULL 
                         AND "Precio Origen USA 1.25%" IS NULL 
                         AND "Precio Origen Europa 1.7%" IS NULL
                        THEN NULL
                        ELSE calc_varperm(MINP)
                    END AS "Porc(%) Definido Según Precio",

                    CASE
                        WHEN LEAST(
                                COALESCE(VAR_BR_USA_SAFE, 1e99),
                                COALESCE(VAR_TOTAL_SAFE, 1e99)
                             ) = 1e99
                        THEN NULL
                        ELSE LEAST(
                                COALESCE(VAR_BR_USA_SAFE, 1e99),
                                COALESCE(VAR_TOTAL_SAFE, 1e99)
                             )
                    END AS "Porc(%) de Var Tot"

                FROM con_variaciones
            )
            SELECT
                Ref_Norm AS REF,
                Referencia_Principal,
                Referencia_Alternas,
                EstadoItem,
                Hist_Total,
                Hist_BR_Cant,
                Hist_USA_Cant,
                Hist_EUR_Cant,
                Hist_BR_Pct,
                Hist_USA_Pct,
                Hist_EUR_Pct,
                Cant_Vig,
                Cant_Ant,
                Cant_Año3,
                Vig_BR_Pct,
                Vig_USA_Pct,
                Vig_EUR_Pct,
                Ant_BR_Pct,
                Ant_USA_Pct,
                Ant_EUR_Pct,
                Año3_BR_Pct,
                Año3_USA_Pct,
                Año3_EUR_Pct,
                Tipo_Origen,
                "Disponibilidad Brasil",
                "Disponibilidad USA",
                "Disponibilidad EUR",
                "DISPONIBLE (SI/NO)",
                "Precio Origen Brasil",
                "Precio Origen USA",
                "Precio Origen Europa",
                "Precio Origen Brasil 1.25%",
                "Precio Origen USA 1.25%",
                "Precio Origen Europa 1.7%",
                CASE WHEN "P.prorrateo (Precio)" IS NOT NULL THEN 'SI' ELSE 'NO' END AS PRECIO,
                "P.prorrateo (Precio)",
                "P.Hist (0.6/0.25/0.15)",
                "Porc(%) de Var",
                "Porc(%) Definido Según Precio",
                "Porc(%) de Var Tot",
                "VAR BR_USA",
                "VAR_PROM(USA_BR)_EUR",
                "VAR BR_USA_EUR",
                CASE
                    WHEN ("P.prorrateo (Precio)" IS NULL OR "P.prorrateo (Precio)" = 0)
                     AND ("P.Hist (0.6/0.25/0.15)" IS NULL OR "P.Hist (0.6/0.25/0.15)" = 0)
                    THEN 'NO'
                    WHEN "P.prorrateo (Precio)" = 0
                    THEN 'CERO'
                    ELSE 'SI'
                END AS "Tiene. Precio"
            FROM con_porc_var
            ORDER BY REF;
        """)

        print("  Tabla origen_precios_tablero creada")


# ===========================================================================
# EXPORT DESDE DUCKDB
# ===========================================================================
def exportar_desde_duckdb():
    with duckdb.connect(str(DUCKDB_PATH)) as con:
        df = con.execute("SELECT * FROM origen_precios_tablero").df()

    print(f"\nExportando {len(df):,} filas...")

    df.to_csv(CSV_OUT, index=False, encoding="utf-8-sig")
    print(f"  CSV -> {CSV_OUT.name}")

    with pd.ExcelWriter(EXCEL_OUT, engine="xlsxwriter") as w:
        df.to_excel(w, index=False, sheet_name="Origen+Precios")
        ws = w.sheets["Origen+Precios"]
        for i, c in enumerate(df.columns):
            s = df[c].astype("string").fillna("")
            ancho = s.map(len).max()
            ancho = 0 if pd.isna(ancho) else int(ancho)
            ws.set_column(i, i, min(max(ancho, len(str(c))) + 2, 50))

    print(f"  Excel -> {EXCEL_OUT.name}")


# ===========================================================================
# MAIN
# ===========================================================================
def main():
    print("\n" + "=" * 60)
    print("PIPELINE 3 OPTIMIZADO - ORIGEN + PRECIOS CNH (ESTABLE)")
    print("=" * 60 + "\n")

    cargar_origen_en_duckdb()
    calcular_en_duckdb()
    exportar_desde_duckdb()

    print("=" * 60)
    print("PROCESO TERMINADO")
    print(f"  DuckDB -> {DUCKDB_PATH.name}")
    print(f"  Ruta  -> {BASE_DIR.resolve()}")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
