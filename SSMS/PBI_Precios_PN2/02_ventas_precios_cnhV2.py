# ===========================================================================
# SECCION 0 - LIBRERIAS
# ===========================================================================
import configparser
import os
import pyodbc
import pandas as pd
import duckdb
from pathlib import Path
from datetime import datetime, date


# ===========================================================================
# SECCION 1 - CONFIGURACION
# ===========================================================================
BASE_DIR    = Path(__file__).parent
CONFIG_PATH = BASE_DIR / "config.ini"

if not CONFIG_PATH.exists():
    raise FileNotFoundError(f"No se encontro config.ini en: {CONFIG_PATH}")

config = configparser.ConfigParser()
config.read(CONFIG_PATH, encoding="utf-8")

FECHA = datetime.now().strftime("%Y%m%d")

SERVER      = config["SQLSERVER"]["server"]
DATABASE    = config["SQLSERVER"]["database"]
DB_USER     = config["SQLSERVER"]["db_user"]
DB_PASS     = config["SQLSERVER"]["db_pass"].strip('"')

DUCKDB_PATH = Path(os.getenv("PIPELINE_DUCKDB_PATH", str(BASE_DIR / config["SALIDA"]["duckdb"])))
EXCEL_OUT   = BASE_DIR / config["SALIDA_VENTAS"]["excel"].replace("{fecha}", FECHA)
CSV_OUT     = BASE_DIR / config["SALIDA_VENTAS"]["csv"].replace("{fecha}", FECHA)

FECHA_DESDE = date(datetime.now().year - 2, 1, 1).strftime("%Y-%m-%d")
CHUNK_SIZE  = 50_000


# ===========================================================================
# SECCION 2 - NORMALIZACION
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


def norm_pd(serie: pd.Series) -> pd.Series:
    s = (
        serie.astype("string")
        .str.strip()
        .str.replace(r"\t|\n|\r", "", regex=True)
        .str.replace("_", "-", regex=False)
        .str.replace(r'[^A-Za-z0-9.\-"/ ]', "", regex=True)
        .str.replace(r"^\.+|\.+$", "", regex=True)
        .str.replace(r"\.{2,}", ".", regex=True)
        .str.replace(r"-{2,}", "-", regex=True)
        .str.replace(r"\s*-\s*", "-", regex=True)
        .str.replace(r" {2,}", " ", regex=True)
        .str.upper()
        .str.strip()
    )
    return s


def optimizar_duckdb(con: duckdb.DuckDBPyConnection, tablas: list[str]) -> None:
    con.execute("PRAGMA threads = 4")
    for tabla in tablas:
        con.execute(f"ANALYZE {tabla}")


# ===========================================================================
# SECCION 3 - CONSULTA DE VENTAS
# ===========================================================================
def build_query_ventas(fecha_desde: str) -> str:
    return f"""
    SELECT
        t470_cm_movto_invent.f470_rowid                                       AS [Rowid],
        t120_mc_items.f120_rowid                                              AS [Rowid Item],
        t350_co_docto_contable.f350_id_co                                     AS [CO Dcto],
        t470_cm_movto_invent.f470_id_co_movto                                 AS [CO],
        t470_cm_movto_invent.f470_id_co_movto                                 AS [Mvto.],
        nco.Descp_CO                                                          AS [Descp. CO Mvto.],
        t470_cm_movto_invent.f470_id_un_movto                                 AS [UN],
        t281_co_unidades_negocio.f281_descripcion                             AS [Descrip. UN],
        CAST(t461_cm_docto_factura_venta.f461_id_fecha AS DATE)               AS [Fecha Factura],
        t200_mm_terceros_1.f200_razon_social                                  AS [Vendedor],
        t200_mm_terceros.f200_nit                                             AS [Nit Cliente],
        t200_mm_terceros.f200_razon_social                                    AS [Cliente],
        CONCAT(RTRIM(t430_cm_pv_docto.f430_id_tipo_docto), '-',
            REPLICATE('0', 8 - LEN(CONVERT(varchar(10), t430_cm_pv_docto.f430_consec_docto))),
            CONVERT(varchar(10), t430_cm_pv_docto.f430_consec_docto))         AS [Dcto. Pedido],
        CONCAT(RTRIM(t350_co_docto_contable_1.f350_id_tipo_docto), '-',
            REPLICATE('0', 8 - LEN(CONVERT(varchar(10), t350_co_docto_contable_1.f350_consec_docto))),
            CONVERT(varchar(10), t350_co_docto_contable_1.f350_consec_docto)) AS [Dcto. Remision],
        CONCAT(RTRIM(t350_co_docto_contable.f350_id_tipo_docto), '-',
            REPLICATE('0', 8 - LEN(CONVERT(varchar(10), t350_co_docto_contable.f350_consec_docto))),
            CONVERT(varchar(10), t350_co_docto_contable.f350_consec_docto))   AS [Dcto Factura],
        RTRIM(t120_mc_items.f120_referencia)                                  AS [Referencia],
        RTRIM(t120_mc_items.f120_descripcion)                                 AS [Descripcion],
        CASE WHEN t470_cm_movto_invent.f470_ind_naturaleza = 2
             THEN  t470_cm_movto_invent.f470_cant_1
             ELSE -t470_cm_movto_invent.f470_cant_1 END                       AS [Cant.],
        t470_cm_movto_invent.f470_id_unidad_medida                            AS [U.M.],
        RTRIM(t120_mc_items.f120_id_tipo_inv_serv)                            AS [Tipo Inv.],
        t470_cm_movto_invent.f470_id_motivo                                   AS [Motivo],
        t146_mc_motivos.f146_descripcion                                      AS [Descripcion Motivo],
        t150_mc_bodegas.f150_id                                               AS [Bodega],
        t150_mc_bodegas.f150_descripcion                                      AS [Descrip. Bodega],
        t157_mc_instalaciones.f157_id                                         AS [Inst.],
        t157_mc_instalaciones.f157_descripcion                                AS [Descripcion Instalacion],
        t112_mc_listas_precios.f112_id                                        AS [Id Lista],
        t112_mc_listas_precios.f112_descripcion                               AS [Descripcion Lista],
        t470_cm_movto_invent.f470_precio_uni                                  AS [Precio Unit. Venta],
        CASE WHEN t470_cm_movto_invent.f470_ind_naturaleza = 2
             THEN  t470_cm_movto_invent.f470_vlr_bruto
             ELSE -t470_cm_movto_invent.f470_vlr_bruto END
             - (t470_cm_movto_invent.f470_vlr_dscto_linea
             +  t470_cm_movto_invent.f470_vlr_dscto_global)                   AS [Valor Venta],
        t470_cm_movto_invent.f470_vlr_dscto_linea
             + t470_cm_movto_invent.f470_vlr_dscto_global                     AS [Descuento],
        CASE WHEN t470_cm_movto_invent.f470_ind_naturaleza = 2
             THEN  t470_cm_movto_invent.f470_vlr_bruto
                   - t470_cm_movto_invent.f470_costo_prom_tot
             ELSE -t470_cm_movto_invent.f470_vlr_bruto
                   + t470_cm_movto_invent.f470_costo_prom_tot END
             - (t470_cm_movto_invent.f470_vlr_dscto_linea
             +  t470_cm_movto_invent.f470_vlr_dscto_global)                   AS [Utilidad],
        CASE WHEN t470_cm_movto_invent.f470_vlr_bruto <> 0
             THEN (CASE WHEN t470_cm_movto_invent.f470_ind_naturaleza = 2
                        THEN  (t470_cm_movto_invent.f470_vlr_bruto
                              - (t470_cm_movto_invent.f470_vlr_dscto_linea
                              +  t470_cm_movto_invent.f470_vlr_dscto_global))
                              - t470_cm_movto_invent.f470_costo_prom_tot
                        ELSE -(t470_cm_movto_invent.f470_vlr_bruto
                              - (t470_cm_movto_invent.f470_vlr_dscto_linea
                              +  t470_cm_movto_invent.f470_vlr_dscto_global))
                              + t470_cm_movto_invent.f470_costo_prom_tot
                   END)
                   / (t470_cm_movto_invent.f470_vlr_bruto
                      - (t470_cm_movto_invent.f470_vlr_dscto_linea
                      +  t470_cm_movto_invent.f470_vlr_dscto_global))
             ELSE 0 END                                                       AS [Margen],
        t470_cm_movto_invent.f470_costo_prom_uni                              AS [Costo Unit.],
        CASE WHEN t470_cm_movto_invent.f470_ind_naturaleza = 2
             THEN  t470_cm_movto_invent.f470_costo_prom_tot
             ELSE -t470_cm_movto_invent.f470_costo_prom_tot END               AS [Costo Total],
        CONCAT(t106_mc_criterios_item_mayores.f106_id, ' - ',
               t106_mc_criterios_item_mayores.f106_descripcion)               AS [Linea],
        sist.f106_id                                                          AS [Cod. Sistema],
        sist.f106_descripcion                                                 AS [Sistema Precio],
        obj.Margen_obj                                                        AS [Margen Sistema],
        CASE WHEN t150_mc_bodegas.f150_id = 'BEMER' THEN '04' ELSE '09' END   AS [Lista Sugerida],
        ISNULL(lp.f126_precio, 0)                                             AS [Precio Esperado],
        ISNULL(lp.f126_precio, 0)
             * CASE WHEN t470_cm_movto_invent.f470_ind_naturaleza = 2
                    THEN  t470_cm_movto_invent.f470_cant_1
                    ELSE -t470_cm_movto_invent.f470_cant_1 END                AS [Venta Esperada],
        CASE WHEN lp.f126_precio IS NOT NULL
             THEN (CASE WHEN t470_cm_movto_invent.f470_ind_naturaleza = 2
                        THEN  (lp.f126_precio * t470_cm_movto_invent.f470_cant_1)
                              - t470_cm_movto_invent.f470_costo_prom_tot
                        ELSE -(lp.f126_precio * t470_cm_movto_invent.f470_cant_1)
                              + t470_cm_movto_invent.f470_costo_prom_tot
                   END)
                   / NULLIF(lp.f126_precio * t470_cm_movto_invent.f470_cant_1, 0)
             ELSE 0 END                                                       AS [Margen Esperado],
        IIF(ISNULL(lp.f126_precio, 0) = 0, 0,
            (lp.f126_precio
             * CASE WHEN t470_cm_movto_invent.f470_ind_naturaleza = 2
                    THEN  t470_cm_movto_invent.f470_cant_1
                    ELSE -t470_cm_movto_invent.f470_cant_1 END)
            - (CASE WHEN t470_cm_movto_invent.f470_ind_naturaleza = 2
                    THEN  t470_cm_movto_invent.f470_vlr_bruto
                    ELSE -t470_cm_movto_invent.f470_vlr_bruto END
               - (t470_cm_movto_invent.f470_vlr_dscto_linea
               +  t470_cm_movto_invent.f470_vlr_dscto_global))
        )                                                                      AS [Variacion Ventas],
        IIF(lp.f126_precio IS NULL, 'No', 'Si')                               AS [Tiene Precio],
        IIF(t112_mc_listas_precios.f112_id IN ('04','09'), 'Si', 'No')        AS [Uso Lista],
        IIF(t120_mc_items.f120_ind_tipo_item IN (1,3), 'Inv', 'Serv')         AS [Tipo item],
        CASE WHEN RTRIM(rot.f106_id) IN ('AA','AB','BA','BB')
             THEN 'Alta' ELSE 'Baja' END                                      AS [Rotacion],
        IIF(CAST(t461_cm_docto_factura_venta.f461_id_fecha AS DATE)
            >= CAST(DATEADD(day, -DAY(GETDATE())+1,
                            DATEADD(year, -1, GETDATE())) AS date),
            'Ult. 12 Meses', '13 a 24 meses')                                 AS [Ano Comparativo]
    FROM t460_cm_docto_remision_venta
        INNER JOIN t470_cm_movto_invent
            INNER JOIN t461_cm_docto_factura_venta
                ON t470_cm_movto_invent.f470_rowid_docto_fact
                   = t461_cm_docto_factura_venta.f461_rowid_docto
            INNER JOIN t350_co_docto_contable
                ON t461_cm_docto_factura_venta.f461_rowid_docto
                   = t350_co_docto_contable.f350_rowid
            INNER JOIN t200_mm_terceros
                ON  t350_co_docto_contable.f350_rowid_tercero
                    = t200_mm_terceros.f200_rowid
                AND t461_cm_docto_factura_venta.f461_rowid_tercero_fact
                    = t200_mm_terceros.f200_rowid
            INNER JOIN t121_mc_items_extensiones
                ON t470_cm_movto_invent.f470_rowid_item_ext
                   = t121_mc_items_extensiones.f121_rowid
            INNER JOIN t120_mc_items
                ON t121_mc_items_extensiones.f121_rowid_item
                   = t120_mc_items.f120_rowid
            INNER JOIN t150_mc_bodegas
                ON t470_cm_movto_invent.f470_rowid_bodega
                   = t150_mc_bodegas.f150_rowid
            INNER JOIN t200_mm_terceros AS t200_mm_terceros_1
                ON t461_cm_docto_factura_venta.f461_rowid_tercero_vendedor
                   = t200_mm_terceros_1.f200_rowid
        ON t460_cm_docto_remision_venta.f460_rowid_docto_factura
           = t461_cm_docto_factura_venta.f461_rowid_docto
        INNER JOIN t350_co_docto_contable AS t350_co_docto_contable_1
            ON t460_cm_docto_remision_venta.f460_rowid_docto
               = t350_co_docto_contable_1.f350_rowid
        INNER JOIN t157_mc_instalaciones
            ON  t470_cm_movto_invent.f470_id_cia         = t157_mc_instalaciones.f157_id_cia
            AND t470_cm_movto_invent.f470_id_instalacion = t157_mc_instalaciones.f157_id
            AND t150_mc_bodegas.f150_id_cia              = t157_mc_instalaciones.f157_id_cia
            AND t150_mc_bodegas.f150_id_instalacion      = t157_mc_instalaciones.f157_id
        LEFT JOIN t430_cm_pv_docto
            INNER JOIN t431_cm_pv_movto
                ON t430_cm_pv_docto.f430_rowid
                   = t431_cm_pv_movto.f431_rowid_pv_docto
            ON t470_cm_movto_invent.f470_rowid_pv_movto
               = t431_cm_pv_movto.f431_rowid
        LEFT JOIN t106_mc_criterios_item_mayores
            INNER JOIN t125_mc_items_criterios
                ON  t106_mc_criterios_item_mayores.f106_id_cia
                    = t125_mc_items_criterios.f125_id_cia
                AND t106_mc_criterios_item_mayores.f106_id_plan
                    = t125_mc_items_criterios.f125_id_plan
                AND t106_mc_criterios_item_mayores.f106_id
                    = t125_mc_items_criterios.f125_id_criterio_mayor
            ON  t120_mc_items.f120_rowid
                = t125_mc_items_criterios.f125_rowid_item
            AND t125_mc_items_criterios.f125_id_plan = '07'
        INNER JOIN AFAS_NOMBRECO AS nco
            ON nco.f285_id = t470_cm_movto_invent.f470_id_co_movto
        INNER JOIN t281_co_unidades_negocio
            ON t470_cm_movto_invent.f470_id_un_movto
               = t281_co_unidades_negocio.f281_id
        INNER JOIN t112_mc_listas_precios
            ON  t470_cm_movto_invent.f470_id_cia
                = t112_mc_listas_precios.f112_id_cia
            AND t470_cm_movto_invent.f470_id_lista_precio
                = t112_mc_listas_precios.f112_id
        INNER JOIN t146_mc_motivos
            ON  t470_cm_movto_invent.f470_id_cia      = t146_mc_motivos.f146_id_cia
            AND t470_cm_movto_invent.f470_id_concepto = t146_mc_motivos.f146_id_concepto
            AND t470_cm_movto_invent.f470_id_motivo   = t146_mc_motivos.f146_id
        LEFT JOIN (
            SELECT DISTINCT
                t120_mc_items.f120_rowid,
                t106b.f106_id
            FROM t125_mc_items_criterios cr
            INNER JOIN t106_mc_criterios_item_mayores t106b
                ON  cr.f125_id_cia            = t106b.f106_id_cia
                AND cr.f125_id_plan           = t106b.f106_id_plan
                AND cr.f125_id_criterio_mayor = t106b.f106_id
            INNER JOIN t120_mc_items
                ON cr.f125_rowid_item = t120_mc_items.f120_rowid
            WHERE cr.f125_id_cia    = 1
              AND t106b.f106_id_cia = 1
              AND cr.f125_id_plan   = '08'
              AND t120_mc_items.f120_ind_tipo_item IN (1, 3)
        ) AS rot ON rot.f120_rowid = t120_mc_items.f120_rowid
        LEFT JOIN (
            SELECT DISTINCT
                t120_mc_items.f120_rowid,
                t106c.f106_id,
                t106c.f106_descripcion
            FROM t125_mc_items_criterios cr
            INNER JOIN t106_mc_criterios_item_mayores t106c
                ON  cr.f125_id_cia            = t106c.f106_id_cia
                AND cr.f125_id_plan           = t106c.f106_id_plan
                AND cr.f125_id_criterio_mayor = t106c.f106_id
            INNER JOIN t120_mc_items
                ON cr.f125_rowid_item = t120_mc_items.f120_rowid
            WHERE cr.f125_id_cia    = 1
              AND t106c.f106_id_cia = 1
              AND cr.f125_id_plan   = '12'
              AND t120_mc_items.f120_ind_tipo_item IN (1, 3)
        ) AS sist ON sist.f120_rowid = t120_mc_items.f120_rowid
        LEFT JOIN (
            SELECT DISTINCT
                RTRIM(t120_mc_items.f120_referencia)        AS RefLista,
                t126_mc_items_precios.f126_id_lista_precio,
                t126_mc_items_precios.f126_precio
            FROM t126_mc_items_precios
            INNER JOIN t120_mc_items
                ON  t126_mc_items_precios.f126_rowid_item
                    = t120_mc_items.f120_rowid
                AND t126_mc_items_precios.f126_id_unidad_medida
                    = t120_mc_items.f120_id_unidad_inventario
            INNER JOIN t121_mc_items_extensiones
                ON t120_mc_items.f120_rowid
                   = t121_mc_items_extensiones.f121_rowid_item
            WHERE t126_mc_items_precios.f126_id_lista_precio IN ('04', '09')
              AND CONCAT(RTRIM(t120_mc_items.f120_referencia),
                         t126_mc_items_precios.f126_fecha_activacion)
                  IN (
                      SELECT CONCAT(RTRIM(i2.f120_referencia),
                                    MAX(p2.f126_fecha_activacion))
                      FROM t126_mc_items_precios p2
                      INNER JOIN t120_mc_items i2
                          ON  p2.f126_rowid_item       = i2.f120_rowid
                          AND p2.f126_id_unidad_medida = i2.f120_id_unidad_inventario
                      WHERE p2.f126_id_lista_precio IN ('04', '09')
                      GROUP BY RTRIM(i2.f120_referencia)
                  )
        ) AS lp
            ON  lp.RefLista             = RTRIM(t120_mc_items.f120_referencia)
            AND lp.f126_id_lista_precio = CASE WHEN t150_mc_bodegas.f150_id = 'BEMER'
                                              THEN '04' ELSE '09' END
        LEFT JOIN (
            SELECT
                t285_co_centro_op.f285_id,
                col1.f753_dato_texto          AS Cod_Sistema,
                col3.f753_dato_numero / 100.0 AS Margen_obj
            FROM t750_mm_movto_entidad
            INNER JOIN t285_co_centro_op
                ON t750_mm_movto_entidad.f750_rowid
                   = t285_co_centro_op.f285_rowid_movto_entidad
            INNER JOIN t752_mm_movto_entidad_fila fila
                ON t750_mm_movto_entidad.f750_rowid
                   = fila.f752_rowid_movto_entidad
            INNER JOIN t753_mm_movto_entidad_columna col1
                ON  t750_mm_movto_entidad.f750_rowid = col1.f753_rowid_movto_entidad
                AND fila.f752_rowid                  = col1.f753_rowid_movto_entidad_fila
                AND col1.f753_rowid_entidad_atributo = 1154
            INNER JOIN t753_mm_movto_entidad_columna col3
                ON  t750_mm_movto_entidad.f750_rowid = col3.f753_rowid_movto_entidad
                AND fila.f752_rowid                  = col3.f753_rowid_movto_entidad_fila
                AND col3.f753_rowid_entidad_atributo = 1156
            WHERE t285_co_centro_op.f285_id = '001'
        ) AS obj ON sist.f106_id = obj.Cod_Sistema
    WHERE
        t125_mc_items_criterios.f125_id_plan                           = '07'
        AND t461_cm_docto_factura_venta.f461_id_fecha                 >= '{fecha_desde}'
        AND t461_cm_docto_factura_venta.f461_id_fecha                  < CAST(
                DATEADD(day, -DAY(GETDATE())+1, GETDATE()) AS date)
        AND t350_co_docto_contable.f350_ind_estado                     = 1
        AND t470_cm_movto_invent.f470_id_cia                           = 1
        AND t461_cm_docto_factura_venta.f461_id_cia                    = 1
        AND t200_mm_terceros.f200_id_cia                               = 1
        AND t350_co_docto_contable.f350_id_cia                         = 1
        AND t120_mc_items.f120_id_cia                                  = 1
        AND t121_mc_items_extensiones.f121_id_cia                      = 1
        AND t150_mc_bodegas.f150_id_cia                                = 1
        AND t200_mm_terceros_1.f200_id_cia                             = 1
        AND ISNULL(t125_mc_items_criterios.f125_id_cia, 0)             = 1
        AND ISNULL(t106_mc_criterios_item_mayores.f106_id_cia, 1)      = 1
        AND t157_mc_instalaciones.f157_id_cia                          = 1
        AND t460_cm_docto_remision_venta.f460_id_cia                   = 1
        AND t350_co_docto_contable_1.f350_id_cia                       = 1
        AND ISNULL(t431_cm_pv_movto.f431_id_cia, 1)                    = 1
        AND ISNULL(t430_cm_pv_docto.f430_id_cia, 1)                    = 1
        AND t281_co_unidades_negocio.f281_id_cia                       = 1
    ORDER BY [Fecha Factura]
    """


# ===========================================================================
# SECCION 4 - EXTRACCION DE VENTAS (CHUNKS → DuckDB)
# ===========================================================================
def optimizar_tipos(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.select_dtypes(include=["int64"]).columns:
        df[col] = pd.to_numeric(df[col], downcast="integer")
    for col in df.select_dtypes(include=["float64"]).columns:
        df[col] = pd.to_numeric(df[col], downcast="float")
    return df


def extraer_ventas() -> int:
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

    print(f"Extrayendo ventas desde {FECHA_DESDE}...")

    total   = 0
    primera = True

    with duckdb.connect(str(DUCKDB_PATH)) as duck:
        duck.execute("PRAGMA threads = 4")
        try:
            for i, chunk in enumerate(
                pd.read_sql(
                    build_query_ventas(FECHA_DESDE),
                    conn,
                    chunksize=CHUNK_SIZE,
                ),
                start=1,
            ):
                chunk = optimizar_tipos(chunk)

                if "Referencia" not in chunk.columns:
                    raise KeyError('No se encontró la columna "Referencia" en ventas')

                chunk["Referencia"] = norm_pd(chunk["Referencia"]).replace("", pd.NA)
                chunk["Ref_Normalizada"] = chunk["Referencia"]

                duck.register("ventas_chunk", chunk)

                if primera:
                    duck.execute(
                        "CREATE OR REPLACE TABLE ventas_raw AS "
                        "SELECT * FROM ventas_chunk"
                    )
                    primera = False
                else:
                    duck.execute(
                        "INSERT INTO ventas_raw SELECT * FROM ventas_chunk"
                    )

                total += len(chunk)
                print(f"  Chunk {i} cargado: {total:,} filas acumuladas en ventas_raw")

            if not primera:
                duck.execute(
                    'CREATE INDEX IF NOT EXISTS idx_ventas_ref_norm '
                    'ON ventas_raw("Ref_Normalizada")'
                )
                optimizar_duckdb(duck, ["ventas_raw"])

        finally:
            conn.close()
            print("  Conexion cerrada")

    if total == 0:
        print("  La consulta no devolvio filas.")
    else:
        print(f"  Total ventas: {total:,} filas cargadas en DuckDB\n")

    return total


# ===========================================================================
# SECCION 5 - CRUCE CON PRECIO CNH (en DuckDB)
# ===========================================================================
def cruzar_con_precio_cnh() -> pd.DataFrame:
    if not DUCKDB_PATH.exists():
        raise FileNotFoundError(
            f"No se encontro la base DuckDB: {DUCKDB_PATH}\n"
            "Ejecuta primero pipeline_precios_cnh.py para generarla."
        )

    print("Cruzando ventas con Precio CNH (DuckDB)...")

    sql_cruce = f"""
        SELECT
            v.*,
            ROUND(p."Precio Prorrateo", 2) AS "Precio CNH"
        FROM ventas_raw v
        LEFT JOIN resultado_precios_lista p
            ON {norm_sql('v.Ref_Normalizada')} = {norm_sql('p.Referencia_Normalizada')}
    """

    with duckdb.connect(str(DUCKDB_PATH)) as duck:
        duck.execute("PRAGMA threads = 4")
        df_resultado = duck.execute(sql_cruce).df()

    if df_resultado.empty:
        print("  Sin filas en ventas_raw para cruzar.\n")
        return df_resultado

    cols = list(df_resultado.columns)
    if "Ref_Normalizada" in cols and "Referencia" in cols:
        cols.remove("Ref_Normalizada")
        ref_idx = cols.index("Referencia")
        cols.insert(ref_idx + 1, "Ref_Normalizada")
        df_resultado = df_resultado[cols]

    total_match   = df_resultado["Precio CNH"].notna().sum()
    total_nomatch = df_resultado["Precio CNH"].isna().sum()
    print(f"  Con Precio CNH : {total_match:,} filas")
    print(f"  Sin Precio CNH : {total_nomatch:,} filas\n")

    return df_resultado


# ===========================================================================
# SECCION 6 - EXPORTAR RESULTADOS
# ===========================================================================
def exportar_resultados(df: pd.DataFrame) -> None:
    if df.empty:
        print("  Sin datos para exportar.")
        return

    print("Exportando resultados...")

    with pd.ExcelWriter(EXCEL_OUT, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="Detalle Ventas")
        worksheet = writer.sheets["Detalle Ventas"]
        for i, col in enumerate(df.columns):
            serie_str = df[col].astype("string").fillna("")
            ancho = serie_str.map(len).max()
            ancho = 0 if pd.isna(ancho) else int(ancho)
            ancho = min(max(ancho, len(str(col))) + 2, 50)
            worksheet.set_column(i, i, ancho)

    print(f"  Excel -> {EXCEL_OUT.name}\n")


# ===========================================================================
# SECCION 7 - PUNTO DE ENTRADA
# ===========================================================================
def main():
    print("\n" + "=" * 60)
    print("VENTAS + PRECIOS CNH - IMECOL S.A.S")
    print(f"Fecha de corte: desde {FECHA_DESDE} hasta hoy")
    print("=" * 60 + "\n")

    total = extraer_ventas()
    if total == 0:
        print("No hay ventas para procesar.")
        return

    df_resultado = cruzar_con_precio_cnh()
    exportar_resultados(df_resultado)

    print("=" * 60)
    print("PROCESO TERMINADO")
    print(f"  Excel -> {EXCEL_OUT.name}")
    print(f"  CSV   -> {CSV_OUT.name}")
    print(f"  Ruta  -> {BASE_DIR.resolve()}")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
