"""
╔══════════════════════════════════════════════════════════════╗
║   ANÁLISIS VENTAS + PRECIOS CNH — IMECOL S.A.S              ║
║   Ventas desde 2024-01-01 cruzadas con Precio Prorrateo     ║
╚══════════════════════════════════════════════════════════════╝

QUÉ HACE ESTE SCRIPT
─────────────────────
  1. Calcula dinámicamente el año de inicio:
       año_inicio = año_actual - 2  → siempre 01/01 de ese año
       ej: 2026 - 2 = 2024  →  fecha_desde = 2024-01-01

  2. Ejecuta la consulta de ventas directamente en Python
     (sin SP) filtrando desde fecha_desde hasta hoy.

  3. Normaliza la columna Ref de ventas con norm_sql() para
     que cruce correctamente con Referencia_Normalizada del
     pipeline de precios CNH.

  4. Cruza las ventas con resultado_precios_lista (DuckDB)
     usando la llave normalizada y agrega la columna
     "Precio CNH" = Precio Prorrateo del pipeline.

  5. Exporta el resultado a Excel y CSV.

LLAVE DE CRUCE
───────────────
  Ventas  : norm_sql(Ref)          → Ref_Normalizada
  Precios : Referencia_Normalizada (ya normalizada en DuckDB)
  JOIN    : v.Ref_Normalizada = p.Referencia_Normalizada

FECHA DE CORTE
───────────────
  Dinámica: año_actual - 2, mes 01, día 01
  2026 → 2024-01-01
  2027 → 2025-01-01

DEPENDENCIAS
─────────────
  pip install pandas pyodbc openpyxl duckdb
  Driver requerido: ODBC Driver 17 for SQL Server
  Requiere: cnh_repuestos.duckdb generado por pipeline_completo.py
"""

# ============================================================
# SECCIÓN 0 — LIBRERÍAS
# ============================================================
import configparser
import pyodbc
import pandas as pd
import duckdb
from pathlib import Path
from datetime import datetime, date


# ============================================================
# SECCIÓN 1 — CONFIGURACIÓN
# ============================================================
BASE_DIR    = Path(__file__).parent
CONFIG_PATH = BASE_DIR / "config.ini"

if not CONFIG_PATH.exists():
    raise FileNotFoundError(
        f"No se encontró config.ini en: {CONFIG_PATH}"
    )

config = configparser.ConfigParser()
config.read(CONFIG_PATH, encoding="utf-8")

FECHA = datetime.now().strftime("%Y%m%d")

SERVER   = config["SQLSERVER"]["server"]
DATABASE = config["SQLSERVER"]["database"]
DB_USER  = config["SQLSERVER"]["db_user"]
DB_PASS  = config["SQLSERVER"]["db_pass"].strip('"')

DUCKDB_PATH = BASE_DIR / config["SALIDA"]["duckdb"]

EXCEL_OUT = BASE_DIR / config["SALIDA_VENTAS"]["excel"].replace("{fecha}", FECHA)
CSV_OUT   = BASE_DIR / config["SALIDA_VENTAS"]["csv"].replace("{fecha}", FECHA)

# ── Fecha dinámica: 01/01 del año (hoy - 2 años) ──────────
FECHA_DESDE = date(datetime.now().year - 2, 1, 1).strftime("%Y-%m-%d")
print(f"  📅 Fecha de corte ventas: desde {FECHA_DESDE}")


# ============================================================
# SECCIÓN 2 — NORMALIZACIÓN SQL (misma función del pipeline)
# Se usa tanto para el SELECT de ventas como para el JOIN
# en DuckDB. Garantiza que Ref de ventas cruce con
# Referencia_Normalizada del pipeline de precios.
# ============================================================
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


# ============================================================
# SECCIÓN 3 — CONSULTA DE VENTAS (reemplaza el SP)
# Misma lógica del SP original pero como consulta Python
# con fecha dinámica y sin tablas temporales.
# norm_sql se aplica sobre Ref para normalizar la llave.
# ============================================================
def build_query_ventas(fecha_desde: str) -> str:
    return f"""
    SELECT
        t470_cm_movto_invent.f470_rowid,
        t350_co_docto_contable.f350_id_co,
        t470_cm_movto_invent.f470_id_co_movto,
        nco.Descp_CO,
        t470_cm_movto_invent.f470_id_un_movto,
        t281_co_unidades_negocio.f281_descripcion,
        CAST(t461_cm_docto_factura_venta.f461_id_fecha AS DATE) AS Fecha_Fact,
        t200_mm_terceros_1.f200_razon_social AS Vendedor,
        t200_mm_terceros.f200_nit,
        t200_mm_terceros.f200_razon_social,
        CONCAT(RTRIM(t430_cm_pv_docto.f430_id_tipo_docto), '-',
            REPLICATE('0', 8 - LEN(CONVERT(varchar(10), t430_cm_pv_docto.f430_consec_docto))),
            CONVERT(varchar(10), t430_cm_pv_docto.f430_consec_docto)) AS Pedido,
        CONCAT(RTRIM(t350_co_docto_contable_1.f350_id_tipo_docto), '-',
            REPLICATE('0', 8 - LEN(CONVERT(varchar(10), t350_co_docto_contable_1.f350_consec_docto))),
            CONVERT(varchar(10), t350_co_docto_contable_1.f350_consec_docto)) AS Remision,
        CONCAT(RTRIM(t350_co_docto_contable.f350_id_tipo_docto), '-',
            REPLICATE('0', 8 - LEN(CONVERT(varchar(10), t350_co_docto_contable.f350_consec_docto))),
            CONVERT(varchar(10), t350_co_docto_contable.f350_consec_docto)) AS Factura,
        RTRIM(t120_mc_items.f120_referencia) AS Ref,
        RTRIM(t120_mc_items.f120_descripcion) AS Descp,
        CASE WHEN t470_cm_movto_invent.f470_ind_naturaleza = 2
             THEN t470_cm_movto_invent.f470_cant_1
             ELSE -t470_cm_movto_invent.f470_cant_1 END AS Cantidad,
        t470_cm_movto_invent.f470_id_unidad_medida,
        RTRIM(t120_mc_items.f120_id_tipo_inv_serv) AS f120_id_tipo_inv_serv,
        t470_cm_movto_invent.f470_id_motivo,
        t146_mc_motivos.f146_descripcion,
        t150_mc_bodegas.f150_id,
        t150_mc_bodegas.f150_descripcion,
        t157_mc_instalaciones.f157_id,
        t157_mc_instalaciones.f157_descripcion,
        t112_mc_listas_precios.f112_id,
        t112_mc_listas_precios.f112_descripcion,
        IIF(t112_mc_listas_precios.f112_id IN ('04','09'), 'Si', 'No') AS Uso_Lista,
        t470_cm_movto_invent.f470_precio_uni,
        CASE WHEN t470_cm_movto_invent.f470_ind_naturaleza = 2
             THEN  t470_cm_movto_invent.f470_vlr_bruto
             ELSE -t470_cm_movto_invent.f470_vlr_bruto END
             - (t470_cm_movto_invent.f470_vlr_dscto_linea
             +  t470_cm_movto_invent.f470_vlr_dscto_global) AS Vr_Venta,
        t470_cm_movto_invent.f470_vlr_dscto_linea
             + t470_cm_movto_invent.f470_vlr_dscto_global AS Descuento,
        CASE WHEN t470_cm_movto_invent.f470_ind_naturaleza = 2
             THEN  t470_cm_movto_invent.f470_vlr_bruto - t470_cm_movto_invent.f470_costo_prom_tot
             ELSE -t470_cm_movto_invent.f470_vlr_bruto + t470_cm_movto_invent.f470_costo_prom_tot END
             - (t470_cm_movto_invent.f470_vlr_dscto_linea
             +  t470_cm_movto_invent.f470_vlr_dscto_global) AS Utilidad,
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
             ELSE 0 END AS Margen,
        t470_cm_movto_invent.f470_costo_prom_uni,
        CASE WHEN t470_cm_movto_invent.f470_ind_naturaleza = 2
             THEN  t470_cm_movto_invent.f470_costo_prom_tot
             ELSE -t470_cm_movto_invent.f470_costo_prom_tot END AS f470_costo_prom_tot,
        CONCAT(t106_mc_criterios_item_mayores.f106_id, ' - ',
               t106_mc_criterios_item_mayores.f106_descripcion) AS Linea,
        CASE WHEN t150_mc_bodegas.f150_id = 'BEMER' THEN '04' ELSE '09' END AS Lista_Sugerida,
        t470_cm_movto_invent.f470_ind_naturaleza,
        IIF(t120_mc_items.f120_ind_tipo_item IN (1,3), 'Inv', 'Serv') AS Tipo_item,
        t120_mc_items.f120_rowid AS cod_item,
        IIF(CAST(t461_cm_docto_factura_venta.f461_id_fecha AS DATE)
            >= CAST(DATEADD(day, -DAY(GETDATE())+1, DATEADD(year, -1, GETDATE())) AS date),
            'Ult. 12 Meses', '13 a 24 meses') AS ano_comp,
        -- Rotación (plan 08)
        CASE WHEN RTRIM(rot.f106_id) IN ('AA','AB','BA','BB') THEN 'Alta' ELSE 'Baja' END AS rotacion,
        -- Sistema de precios (plan 12)
        sist.f106_id   AS Cod_sistema,
        sist.f106_descripcion AS sistema_precio,
        -- Precio lista vigente (04 o 09)
        ISNULL(lp.f126_precio, 0) AS Precio_list,
        ISNULL(lp.f126_precio, 0) * CASE WHEN t470_cm_movto_invent.f470_ind_naturaleza = 2
             THEN  t470_cm_movto_invent.f470_cant_1
             ELSE -t470_cm_movto_invent.f470_cant_1 END AS Venta_Lista,
        CASE WHEN lp.f126_precio IS NOT NULL
             THEN (CASE WHEN t470_cm_movto_invent.f470_ind_naturaleza = 2
                        THEN (lp.f126_precio * t470_cm_movto_invent.f470_cant_1)
                             - t470_cm_movto_invent.f470_costo_prom_tot
                        ELSE -(lp.f126_precio * t470_cm_movto_invent.f470_cant_1)
                             + t470_cm_movto_invent.f470_costo_prom_tot
                   END)
                  / NULLIF(lp.f126_precio * t470_cm_movto_invent.f470_cant_1, 0)
             ELSE 0 END AS Margen_Esperado,
        IIF(ISNULL(lp.f126_precio,0) = 0, 0,
            (lp.f126_precio * CASE WHEN t470_cm_movto_invent.f470_ind_naturaleza = 2
             THEN  t470_cm_movto_invent.f470_cant_1
             ELSE -t470_cm_movto_invent.f470_cant_1 END)
            - (CASE WHEN t470_cm_movto_invent.f470_ind_naturaleza = 2
                    THEN  t470_cm_movto_invent.f470_vlr_bruto
                    ELSE -t470_cm_movto_invent.f470_vlr_bruto END
                    - (t470_cm_movto_invent.f470_vlr_dscto_linea
                    +  t470_cm_movto_invent.f470_vlr_dscto_global))
        ) AS Var_Venta,
        IIF(lp.f126_precio IS NULL, 'No', 'Si') AS Tiene_Precio,
        -- Margen objetivo (entidad configurada en UnoEE)
        obj.Margen_obj
    FROM t460_cm_docto_remision_venta
        INNER JOIN t470_cm_movto_invent
            INNER JOIN t461_cm_docto_factura_venta
                ON t470_cm_movto_invent.f470_rowid_docto_fact = t461_cm_docto_factura_venta.f461_rowid_docto
            INNER JOIN t350_co_docto_contable
                ON t461_cm_docto_factura_venta.f461_rowid_docto = t350_co_docto_contable.f350_rowid
            INNER JOIN t200_mm_terceros
                ON  t350_co_docto_contable.f350_rowid_tercero        = t200_mm_terceros.f200_rowid
                AND t461_cm_docto_factura_venta.f461_rowid_tercero_fact = t200_mm_terceros.f200_rowid
            INNER JOIN t121_mc_items_extensiones
                ON t470_cm_movto_invent.f470_rowid_item_ext = t121_mc_items_extensiones.f121_rowid
            INNER JOIN t120_mc_items
                ON t121_mc_items_extensiones.f121_rowid_item = t120_mc_items.f120_rowid
            INNER JOIN t150_mc_bodegas
                ON t470_cm_movto_invent.f470_rowid_bodega = t150_mc_bodegas.f150_rowid
            INNER JOIN t200_mm_terceros AS t200_mm_terceros_1
                ON t461_cm_docto_factura_venta.f461_rowid_tercero_vendedor = t200_mm_terceros_1.f200_rowid
        ON t460_cm_docto_remision_venta.f460_rowid_docto_factura = t461_cm_docto_factura_venta.f461_rowid_docto
        INNER JOIN t350_co_docto_contable AS t350_co_docto_contable_1
            ON t460_cm_docto_remision_venta.f460_rowid_docto = t350_co_docto_contable_1.f350_rowid
        INNER JOIN t157_mc_instalaciones
            ON  t470_cm_movto_invent.f470_id_cia         = t157_mc_instalaciones.f157_id_cia
            AND t470_cm_movto_invent.f470_id_instalacion = t157_mc_instalaciones.f157_id
            AND t150_mc_bodegas.f150_id_cia              = t157_mc_instalaciones.f157_id_cia
            AND t150_mc_bodegas.f150_id_instalacion      = t157_mc_instalaciones.f157_id
        LEFT JOIN t430_cm_pv_docto
            INNER JOIN t431_cm_pv_movto
                ON t430_cm_pv_docto.f430_rowid = t431_cm_pv_movto.f431_rowid_pv_docto
            ON t470_cm_movto_invent.f470_rowid_pv_movto = t431_cm_pv_movto.f431_rowid
        LEFT JOIN t106_mc_criterios_item_mayores
            INNER JOIN t125_mc_items_criterios
                ON  t106_mc_criterios_item_mayores.f106_id_cia    = t125_mc_items_criterios.f125_id_cia
                AND t106_mc_criterios_item_mayores.f106_id_plan   = t125_mc_items_criterios.f125_id_plan
                AND t106_mc_criterios_item_mayores.f106_id        = t125_mc_items_criterios.f125_id_criterio_mayor
            ON  t120_mc_items.f120_rowid = t125_mc_items_criterios.f125_rowid_item
            AND t125_mc_items_criterios.f125_id_plan = '07'
        INNER JOIN AFAS_NOMBRECO AS nco
            ON nco.f285_id = t470_cm_movto_invent.f470_id_co_movto
        INNER JOIN t281_co_unidades_negocio
            ON t470_cm_movto_invent.f470_id_un_movto = t281_co_unidades_negocio.f281_id
        INNER JOIN t112_mc_listas_precios
            ON  t470_cm_movto_invent.f470_id_cia          = t112_mc_listas_precios.f112_id_cia
            AND t470_cm_movto_invent.f470_id_lista_precio  = t112_mc_listas_precios.f112_id
        INNER JOIN t146_mc_motivos
            ON  t470_cm_movto_invent.f470_id_cia      = t146_mc_motivos.f146_id_cia
            AND t470_cm_movto_invent.f470_id_concepto = t146_mc_motivos.f146_id_concepto
            AND t470_cm_movto_invent.f470_id_motivo   = t146_mc_motivos.f146_id
        -- Rotación (plan 08) — LEFT para no perder ventas sin rotación
        LEFT JOIN (
            SELECT DISTINCT t120_mc_items.f120_rowid, t106b.f106_id
            FROM t125_mc_items_criterios cr
            INNER JOIN t106_mc_criterios_item_mayores t106b
                ON  cr.f125_id_cia          = t106b.f106_id_cia
                AND cr.f125_id_plan         = t106b.f106_id_plan
                AND cr.f125_id_criterio_mayor = t106b.f106_id
            INNER JOIN t120_mc_items ON cr.f125_rowid_item = t120_mc_items.f120_rowid
            WHERE cr.f125_id_cia  = 1
              AND t106b.f106_id_cia = 1
              AND cr.f125_id_plan  = '08'
              AND t120_mc_items.f120_ind_tipo_item IN (1,3)
        ) AS rot ON rot.f120_rowid = t120_mc_items.f120_rowid
        -- Sistema de precios (plan 12) — LEFT para no perder ventas sin sistema
        LEFT JOIN (
            SELECT DISTINCT t120_mc_items.f120_rowid, t106c.f106_id, t106c.f106_descripcion
            FROM t125_mc_items_criterios cr
            INNER JOIN t106_mc_criterios_item_mayores t106c
                ON  cr.f125_id_cia            = t106c.f106_id_cia
                AND cr.f125_id_plan           = t106c.f106_id_plan
                AND cr.f125_id_criterio_mayor = t106c.f106_id
            INNER JOIN t120_mc_items ON cr.f125_rowid_item = t120_mc_items.f120_rowid
            WHERE cr.f125_id_cia   = 1
              AND t106c.f106_id_cia = 1
              AND cr.f125_id_plan  = '12'
              AND t120_mc_items.f120_ind_tipo_item IN (1,3)
        ) AS sist ON sist.f120_rowid = t120_mc_items.f120_rowid
        -- Precio de lista vigente (04 o 09) — LEFT para no perder ventas sin precio
        LEFT JOIN (
            SELECT DISTINCT
                RTRIM(t120_mc_items.f120_referencia) AS RefLista,
                t126_mc_items_precios.f126_id_lista_precio,
                t126_mc_items_precios.f126_precio
            FROM t126_mc_items_precios
            INNER JOIN t120_mc_items
                ON  t126_mc_items_precios.f126_rowid_item       = t120_mc_items.f120_rowid
                AND t126_mc_items_precios.f126_id_unidad_medida = t120_mc_items.f120_id_unidad_inventario
            INNER JOIN t121_mc_items_extensiones
                ON t120_mc_items.f120_rowid = t121_mc_items_extensiones.f121_rowid_item
            WHERE t126_mc_items_precios.f126_id_lista_precio IN ('04','09')
              AND CONCAT(RTRIM(t120_mc_items.f120_referencia), t126_mc_items_precios.f126_fecha_activacion)
                  IN (
                      SELECT CONCAT(RTRIM(i2.f120_referencia), MAX(p2.f126_fecha_activacion))
                      FROM t126_mc_items_precios p2
                      INNER JOIN t120_mc_items i2
                          ON  p2.f126_rowid_item       = i2.f120_rowid
                          AND p2.f126_id_unidad_medida = i2.f120_id_unidad_inventario
                      WHERE p2.f126_id_lista_precio IN ('04','09')
                      GROUP BY RTRIM(i2.f120_referencia)
                  )
        ) AS lp
            ON  lp.RefLista               = RTRIM(t120_mc_items.f120_referencia)
            AND lp.f126_id_lista_precio   = CASE WHEN t150_mc_bodegas.f150_id = 'BEMER' THEN '04' ELSE '09' END
        -- Margen objetivo
        LEFT JOIN (
            SELECT
                t285_co_centro_op.f285_id,
                col1.f753_dato_texto AS Cod_Sistema,
                col3.f753_dato_numero / 100.0 AS Margen_obj
            FROM t750_mm_movto_entidad
            INNER JOIN t285_co_centro_op
                ON t750_mm_movto_entidad.f750_rowid = t285_co_centro_op.f285_rowid_movto_entidad
            INNER JOIN t752_mm_movto_entidad_fila fila
                ON t750_mm_movto_entidad.f750_rowid = fila.f752_rowid_movto_entidad
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
        t125_mc_items_criterios.f125_id_plan              = '07'
        AND t461_cm_docto_factura_venta.f461_id_fecha    >= '{fecha_desde}'
        AND t461_cm_docto_factura_venta.f461_id_fecha     < CAST(DATEADD(day, -DAY(GETDATE())+1, GETDATE()) AS date)
        AND t350_co_docto_contable.f350_ind_estado        = 1
        AND t470_cm_movto_invent.f470_id_cia              = 1
        AND t461_cm_docto_factura_venta.f461_id_cia       = 1
        AND t200_mm_terceros.f200_id_cia                  = 1
        AND t350_co_docto_contable.f350_id_cia            = 1
        AND t120_mc_items.f120_id_cia                     = 1
        AND t121_mc_items_extensiones.f121_id_cia         = 1
        AND t150_mc_bodegas.f150_id_cia                   = 1
        AND t200_mm_terceros_1.f200_id_cia                = 1
        AND ISNULL(t125_mc_items_criterios.f125_id_cia,0) = 1
        AND ISNULL(t106_mc_criterios_item_mayores.f106_id_cia, 1) = 1
        AND t157_mc_instalaciones.f157_id_cia             = 1
        AND t460_cm_docto_remision_venta.f460_id_cia      = 1
        AND t350_co_docto_contable_1.f350_id_cia          = 1
        AND ISNULL(t431_cm_pv_movto.f431_id_cia, 1)       = 1
        AND ISNULL(t430_cm_pv_docto.f430_id_cia, 1)       = 1
        AND t281_co_unidades_negocio.f281_id_cia          = 1
    ORDER BY Fecha_Fact
    """


# ============================================================
# SECCIÓN 4 — EXTRACCIÓN DE VENTAS DESDE SQL SERVER
# Lee la consulta en bloques de 50.000 filas para no
# saturar memoria con 2 años de transacciones.
# ============================================================
def extraer_ventas() -> pd.DataFrame:
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

    print(f"🔹 Extrayendo ventas desde {FECHA_DESDE}...")
    query  = build_query_ventas(FECHA_DESDE)
    chunks = []
    total  = 0

    try:
        for chunk in pd.read_sql(query, conn, chunksize=50_000):
            chunks.append(chunk)
            total += len(chunk)
            print(f"  {total:,} filas leídas...")
    finally:
        conn.close()
        print("  🔒 Conexión cerrada")

    if not chunks:
        print("  ⚠️  La consulta no devolvió filas.")
        return pd.DataFrame()

    df = pd.concat(chunks, ignore_index=True)
    print(f"  ✅ Total ventas: {total:,} filas\n")
    return df


# ============================================================
# SECCIÓN 5 — NORMALIZACIÓN Y CRUCE CON PRECIO CNH
# 1. Registra el DataFrame de ventas en DuckDB
# 2. Aplica norm_sql() sobre la columna Ref de ventas
#    para generar Ref_Normalizada
# 3. Cruza con resultado_precios_lista usando:
#      v.Ref_Normalizada = p.Referencia_Normalizada
# 4. Agrega columna "Precio CNH" = "Precio Prorrateo"
# ============================================================
def cruzar_con_precio_cnh(df_ventas: pd.DataFrame) -> pd.DataFrame:
    if df_ventas.empty:
        return df_ventas

    if not DUCKDB_PATH.exists():
        raise FileNotFoundError(
            f"\n❌ No se encontró la base DuckDB: {DUCKDB_PATH}\n"
            "   Ejecuta primero pipeline_completo.py para generarla."
        )

    print("🔹 Cruzando ventas con Precio CNH (DuckDB)...")

    norm_ref = norm_sql("v.Ref")

    sql_cruce = f"""
        SELECT
            v.*,
            {norm_ref} AS Ref_Normalizada,
            ROUND(p."Precio Prorrateo", 2) AS "Precio CNH"
        FROM ventas_tmp v
        LEFT JOIN resultado_precios_lista p
            ON {norm_ref} = p.Referencia_Normalizada
    """

    with duckdb.connect(str(DUCKDB_PATH)) as duck:
        duck.register("ventas_tmp", df_ventas)
        df_resultado = duck.execute(sql_cruce).df()

    # Mover Ref_Normalizada junto a Ref para mejor legibilidad
    cols = list(df_resultado.columns)
    ref_idx = cols.index("Ref")
    cols.remove("Ref_Normalizada")
    cols.insert(ref_idx + 1, "Ref_Normalizada")
    df_resultado = df_resultado[cols]

    total_match   = df_resultado["Precio CNH"].notna().sum()
    total_nomatch = df_resultado["Precio CNH"].isna().sum()
    print(f"  ✅ Con Precio CNH   : {total_match:,} filas")
    print(f"  ⚠️  Sin Precio CNH  : {total_nomatch:,} filas (referencia sin cruce)\n")

    return df_resultado


# ============================================================
# SECCIÓN 6 — EXPORTAR RESULTADOS
# ============================================================
def exportar_resultados(df: pd.DataFrame) -> None:
    if df.empty:
        print("  ⚠️  Sin datos para exportar.")
        return

    print("🔹 Exportando resultados...")

    df.to_csv(CSV_OUT, index=False, encoding="utf-8-sig")
    print(f"  ✅ CSV   → {CSV_OUT.name}")

    with pd.ExcelWriter(EXCEL_OUT, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Ventas")
        ws = writer.sheets["Ventas"]
        for col_cells in ws.columns:
            max_len = max(
                len(str(cell.value)) if cell.value is not None else 0
                for cell in col_cells
            )
            ws.column_dimensions[col_cells[0].column_letter].width = min(max_len + 2, 50)

    print(f"  ✅ Excel → {EXCEL_OUT.name}\n")


# ============================================================
# SECCIÓN 7 — PUNTO DE ENTRADA PRINCIPAL
# ============================================================
def main():
    print("\n" + "=" * 60)
    print("🚀  VENTAS + PRECIOS CNH — IMECOL S.A.S")
    print("=" * 60 + "\n")

    df_ventas    = extraer_ventas()
    df_resultado = cruzar_con_precio_cnh(df_ventas)
    exportar_resultados(df_resultado)

    print("=" * 60)
    print("✅  PROCESO TERMINADO CORRECTAMENTE")
    print("\nArchivos generados:")
    print(f"  📊 Excel → {EXCEL_OUT.name}")
    print(f"  📄 CSV   → {CSV_OUT.name}")
    print(f"\n  Ruta: {BASE_DIR.resolve()}")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
