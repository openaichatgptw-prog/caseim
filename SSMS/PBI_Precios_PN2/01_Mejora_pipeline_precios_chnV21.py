"""
pipeline_precios_cnh.py
─────────────────────────────────────────────────────────────────────────────
Pipeline ETL — Consolidación de precios CNH para IMECOL S.A.S

Flujo:
  Paso 1 │ Lee Excels de precios (USA/Brasil/Europa) y disponibilidad AGCS
         │ → DuckDB: precios_consolidados (incluye ref_norm precalculada)
         │ + CSV maestro referencias
  Paso 2 │ Extrae maestro de ítems + referencias alternas desde SQL Server
         │ → DuckDB: maestro (incluye Ref_Norm)
         │           referencias_alternas (incluye Ref_Norm + Ref_Alt_Norm)
  Paso 3 │ Cruza precios por familia de referencias usando columnas normalizadas
         │ precalculadas — sin recalcular norm_sql en runtime.
         │ Elige ganador y calcula Precio Prorrateo ponderado.
         │ → Excel final + tablas precio_familia y resultado_precios_lista

Configuración: config.ini (secciones ARCHIVOS, SALIDA, SQLSERVER, FACTORES)
─────────────────────────────────────────────────────────────────────────────
"""
import configparser, pyodbc, pandas as pd, polars as pl, duckdb, os
from pathlib import Path
from datetime import datetime
from ref_normalization import (
    normalize_reference_expr_polars,
    normalize_reference_expr_sql,
)


# ===========================================================================
# CONFIGURACION
# ===========================================================================
BASE_DIR    = Path(__file__).parent
config      = configparser.ConfigParser()
config.read(BASE_DIR / "config.ini", encoding="utf-8")

FECHA       = datetime.now().strftime("%Y%m%d")
EXCEL_PREC  = Path(config["ARCHIVOS"]["excel_precios"])
EXCEL_DISP  = Path(config["ARCHIVOS"]["excel_disponibilidad"])
DUCKDB_PATH = Path(os.getenv("PIPELINE_DUCKDB_PATH", str(BASE_DIR / config["SALIDA"]["duckdb"])))
CSV_OUT     = BASE_DIR / config["SALIDA"]["csv"]
EXCEL_OUT   = BASE_DIR / config["SALIDA"]["excel_salida"].replace("{fecha}", FECHA)
SERVER      = config["SQLSERVER"]["server"]
DATABASE    = config["SQLSERVER"]["database"]
DB_USER     = config["SQLSERVER"]["db_user"]
DB_PASS     = config["SQLSERVER"]["db_pass"].strip('"')
FACTOR_BR   = float(config["FACTORES"]["factor_br"])
FACTOR_USA  = float(config["FACTORES"]["factor_usa"])
FACTOR_EUR  = float(config["FACTORES"]["factor_eur"])
CHUNK       = 50_000

# Participación e importaciones: desde 01/01 del (año actual − 2), hasta hoy.
_ANIO_PART_DESDE = datetime.now().year - 2
FECHA_PART_DESDE = f"{_ANIO_PART_DESDE}-01-01"
FECHA_PART_HASTA = datetime.now().strftime("%Y-%m-%d")


def optimizar_duckdb(con: duckdb.DuckDBPyConnection, tablas: list[str]) -> None:
    con.execute("PRAGMA threads = 4")
    for tabla in tablas:
        con.execute(f"ANALYZE {tabla}")


# ===========================================================================
# QUERY PRINCIPAL
# ===========================================================================
SQL = f"""
SET NOCOUNT ON;

WITH
cri AS (
    SELECT * FROM (
        SELECT i.f120_rowid AS rowid_item, c.f125_id_plan,
               {{fn CONCAT({{fn CONCAT(RTRIM(m.f106_id),' - ')}}, m.f106_descripcion)}} AS valor
        FROM   t106_mc_criterios_item_mayores m
        JOIN   t125_mc_items_criterios c
               ON m.f106_id_cia=c.f125_id_cia AND m.f106_id_plan=c.f125_id_plan
               AND m.f106_id=c.f125_id_criterio_mayor
        JOIN   t120_mc_items i ON c.f125_rowid_item=i.f120_rowid
        WHERE  m.f106_id_cia=1 AND c.f125_id_cia=1
               AND c.f125_id_plan IN ('01','03','04','07','08','12')
    ) s PIVOT (MAX(valor) FOR f125_id_plan IN ([01],[03],[04],[07],[08],[12])) p
),
ult_venta AS (
    SELECT * FROM (
        SELECT
            i.f120_rowid                                                 AS rowid_item,
            CAST(f.f461_id_fecha AS DATE)                                AS Fecha_Fact,
            cli.f200_razon_social                                        AS Cliente,
            mv.f470_precio_uni,
            pv.f430_tasa_local,
            pv.f430_id_moneda_docto,
            (mv.f470_vlr_bruto - mv.f470_vlr_dscto_linea
             + mv.f470_vlr_dscto_global) / mv.f470_cant_1                AS Precio_init_COP,
            lp.f112_id,
            CASE WHEN mv.f470_vlr_bruto <> 0
                 THEN ((mv.f470_vlr_bruto
                        - (mv.f470_vlr_dscto_linea + mv.f470_vlr_dscto_global))
                       - mv.f470_costo_prom_tot)
                      / (mv.f470_vlr_bruto
                         - (mv.f470_vlr_dscto_linea + mv.f470_vlr_dscto_global))
                 ELSE 0 END                                              AS Margen,
            ROW_NUMBER() OVER (PARTITION BY i.f120_rowid
                               ORDER BY mv.f470_rowid DESC)              AS rn
        FROM   t460_cm_docto_remision_venta rem
        JOIN   t470_cm_movto_invent mv
        JOIN   t461_cm_docto_factura_venta f
               ON mv.f470_rowid_docto_fact = f.f461_rowid_docto
        JOIN   t350_co_docto_contable dc
               ON f.f461_rowid_docto = dc.f350_rowid
        JOIN   t200_mm_terceros cli
               ON dc.f350_rowid_tercero = cli.f200_rowid
               AND f.f461_rowid_tercero_fact = cli.f200_rowid
        JOIN   t121_mc_items_extensiones ie
               ON mv.f470_rowid_item_ext = ie.f121_rowid
        JOIN   t120_mc_items i
               ON ie.f121_rowid_item = i.f120_rowid
        JOIN   t150_mc_bodegas b
               ON mv.f470_rowid_bodega = b.f150_rowid
        JOIN   t200_mm_terceros vend
               ON f.f461_rowid_tercero_vendedor = vend.f200_rowid
               ON rem.f460_rowid_docto_factura = f.f461_rowid_docto
        JOIN   t350_co_docto_contable dcr
               ON rem.f460_rowid_docto = dcr.f350_rowid
        JOIN   t157_mc_instalaciones ins
               ON mv.f470_id_cia = ins.f157_id_cia
               AND mv.f470_id_instalacion = ins.f157_id
               AND b.f150_id_cia = ins.f157_id_cia
               AND b.f150_id_instalacion = ins.f157_id
        LEFT JOIN t430_cm_pv_docto pv
        JOIN   t431_cm_pv_movto pm
               ON pv.f430_rowid = pm.f431_rowid_pv_docto
               ON mv.f470_rowid_pv_movto = pm.f431_rowid
        JOIN   t281_co_unidades_negocio un ON mv.f470_id_un_movto = un.f281_id
        JOIN   t112_mc_listas_precios lp
               ON mv.f470_id_cia = lp.f112_id_cia
               AND mv.f470_id_lista_precio = lp.f112_id
        JOIN   t146_mc_motivos mo
               ON mv.f470_id_cia = mo.f146_id_cia
               AND mv.f470_id_concepto = mo.f146_id_concepto
               AND mv.f470_id_motivo = mo.f146_id
        WHERE  f.f461_id_fecha BETWEEN
                   CAST(DATEADD(day,-DAY(GETDATE())+1,DATEADD(year,-3,GETDATE())) AS date)
               AND CAST(DATEADD(day,-DAY(GETDATE()),GETDATE()) AS date)
          AND dc.f350_ind_estado = 1
          AND mv.f470_ind_naturaleza = 2
          AND i.f120_ind_tipo_item IN (1,3)
          AND mv.f470_id_cia=1  AND f.f461_id_cia=1   AND cli.f200_id_cia=1
          AND dc.f350_id_cia=1  AND i.f120_id_cia=1   AND ie.f121_id_cia=1
          AND b.f150_id_cia=1   AND vend.f200_id_cia=1 AND ins.f157_id_cia=1
          AND rem.f460_id_cia=1 AND dcr.f350_id_cia=1
          AND ISNULL(pm.f431_id_cia,1)=1
          AND ISNULL(pv.f430_id_cia,1)=1
          AND un.f281_id_cia=1
    ) v WHERE rn=1
),
ult_import AS (
    SELECT * FROM (
        SELECT
            i.f120_rowid                                                 AS rowid_item,
            im.f41851_rowid,
            CAST(d.f41850_fecha AS DATE)                                 AS Fecha_import,
            oc.f421_precio_unitario,
            ter.f200_razon_social                                        AS Proveedor,
            im.f41851_cant_entrada,
            CASE WHEN p.f011_descripcion='EEUU'                THEN 'USA'
                 WHEN p.f011_descripcion IN('FRANCIA','ITALIA') THEN 'EUROPA'
                 WHEN d.f41850_consec_docto=3559               THEN 'ARGENTINA'
                 ELSE p.f011_descripcion END                            AS pais,
            ROW_NUMBER() OVER (PARTITION BY i.f120_rowid
                               ORDER BY d.f41850_fecha DESC)            AS rn
        FROM   t421_cm_oc_movto oc
        JOIN   t41851_import_movto im  ON oc.f421_rowid=im.f41851_rowid_oc_movto
        JOIN   t41850_import_docto d   ON d.f41850_rowid=im.f41851_rowid_docto_import
        JOIN   t121_mc_items_extensiones ie ON im.f41851_rowid_item_ext=ie.f121_rowid
        JOIN   t120_mc_items i         ON i.f120_rowid=ie.f121_rowid_item
        JOIN   t150_mc_bodegas bo      ON oc.f421_rowid_bodega=bo.f150_rowid
        JOIN   t420_cm_oc_docto ocd    ON oc.f421_rowid_oc_docto=ocd.f420_rowid
        JOIN   t200_mm_terceros ter    ON ocd.f420_rowid_tercero_prov=ter.f200_rowid
        JOIN   t41807_import_vias vi   ON d.f41850_rowid_via=vi.f41807_rowid
        JOIN   t41806_import_origen_destino ori ON d.f41850_rowid_origen=ori.f41806_rowid
        JOIN   t125_mc_items_criterios c   ON i.f120_rowid=c.f125_rowid_item
        JOIN   t106_mc_criterios_item_mayores m
               ON c.f125_id_cia=m.f106_id_cia AND c.f125_id_plan=m.f106_id_plan
               AND c.f125_id_criterio_mayor=m.f106_id
        LEFT JOIN t011_mm_paises p ON ori.f41806_id_pais=p.f011_id
        WHERE  d.f41850_fecha BETWEEN '{FECHA_PART_DESDE}' AND '{FECHA_PART_HASTA}'
          AND  d.f41850_ind_estado=4
          AND  c.f125_id_plan='07'
          AND  m.f106_id IN ('051','019','102','074')
    ) x WHERE rn=1
),
costos AS (
    SELECT * FROM (
        SELECT f41853_rowid_movto_import, f41853_rowid_cpto_import,
               SUM(f41853_costo_local_aplicado) AS monto
        FROM   t41853_import_mov_costo_acum
        WHERE  f41853_rowid_cpto_import BETWEEN 1 AND 5
        GROUP BY f41853_rowid_movto_import, f41853_rowid_cpto_import
    ) s PIVOT (SUM(monto) FOR f41853_rowid_cpto_import IN ([1],[2],[3],[4],[5])) p
),
part AS (
    SELECT rowid_item,
           ISNULL([USA],0)    AS Cant_Usa,
           ISNULL([BRASIL],0) AS Cant_Brasil,
           ISNULL([EUROPA],0) AS Cant_Europa,
           ISNULL([USA],0)+ISNULL([BRASIL],0)+ISNULL([EUROPA],0) AS Total
    FROM (
        SELECT i.f120_rowid AS rowid_item,
               CASE WHEN p.f011_descripcion='EEUU'                 THEN 'USA'
                    WHEN p.f011_descripcion IN('FRANCIA','ITALIA') THEN 'EUROPA'
                    WHEN d.f41850_consec_docto=3559                THEN 'ARGENTINA'
                    ELSE p.f011_descripcion END AS pais,
               im.f41851_cant_entrada
        FROM   t421_cm_oc_movto oc
        JOIN   t41851_import_movto im  ON oc.f421_rowid=im.f41851_rowid_oc_movto
        JOIN   t41850_import_docto d   ON d.f41850_rowid=im.f41851_rowid_docto_import
        JOIN   t121_mc_items_extensiones ie ON im.f41851_rowid_item_ext=ie.f121_rowid
        JOIN   t120_mc_items i         ON i.f120_rowid=ie.f121_rowid_item
        JOIN   t150_mc_bodegas bo      ON oc.f421_rowid_bodega=bo.f150_rowid
        JOIN   t420_cm_oc_docto ocd    ON oc.f421_rowid_oc_docto=ocd.f420_rowid
        JOIN   t200_mm_terceros ter    ON ocd.f420_rowid_tercero_prov=ter.f200_rowid
        JOIN   t41807_import_vias vi   ON d.f41850_rowid_via=vi.f41807_rowid
        JOIN   t41806_import_origen_destino ori ON d.f41850_rowid_origen=ori.f41806_rowid
        JOIN   t125_mc_items_criterios c   ON i.f120_rowid=c.f125_rowid_item
        JOIN   t106_mc_criterios_item_mayores m
               ON c.f125_id_cia=m.f106_id_cia AND c.f125_id_plan=m.f106_id_plan
               AND c.f125_id_criterio_mayor=m.f106_id
        LEFT JOIN t011_mm_paises p ON ori.f41806_id_pais=p.f011_id
        WHERE  d.f41850_fecha BETWEEN '{FECHA_PART_DESDE}' AND '{FECHA_PART_HASTA}'
          AND  d.f41850_ind_estado=4
          AND  c.f125_id_plan='07'
          AND  m.f106_id IN ('051','019','102','074')
    ) s PIVOT (SUM(f41851_cant_entrada) FOR pais IN ([USA],[BRASIL],[EUROPA])) p
),
obj AS (
    SELECT col1.f753_dato_texto  AS Cod_Sistema,
           col3.f753_dato_numero AS Margen
    FROM   t750_mm_movto_entidad e
    JOIN   t285_co_centro_op co  ON e.f750_rowid=co.f285_rowid_movto_entidad
    JOIN   t752_mm_movto_entidad_fila fi ON e.f750_rowid=fi.f752_rowid_movto_entidad
    JOIN   t753_mm_movto_entidad_columna col1
           ON e.f750_rowid=col1.f753_rowid_movto_entidad
           AND fi.f752_rowid=col1.f753_rowid_movto_entidad_fila
           AND col1.f753_rowid_entidad_atributo=1154
    JOIN   t753_mm_movto_entidad_columna col3
           ON e.f750_rowid=col3.f753_rowid_movto_entidad
           AND fi.f752_rowid=col3.f753_rowid_movto_entidad_fila
           AND col3.f753_rowid_entidad_atributo=1156
    WHERE  co.f285_id='001'
)

SELECT
    i.f120_rowid                                                 AS Rowid,
    RTRIM(i.f120_referencia)                                     AS Referencia,
    i.f120_descripcion                                           AS Descripcion_Item,
    i.f120_id_unidad_inventario                                  AS UM,
    ie.f121_notas                                                AS Notas,
    CASE ie.f121_ind_estado
        WHEN 0 THEN 'Inactivo' WHEN 2 THEN 'Bloqueado' ELSE 'Activo'
    END                                                          AS Estado,
    cri.[07]                                                     AS Linea,
    cri.[04]                                                     AS Sistema,
    cri.[12]                                                     AS Sistema_Precio,
    LEFT(cri.[12], NULLIF(CHARINDEX(' -', cri.[12]), 0) - 1)     AS Cod_Sistema,
    cri.[01]                                                     AS Equipo,
    cri.[03]                                                     AS Modelo,
    cri.[08]                                                     AS Rotacion,
    obj.Margen / 100                                             AS Ma_Margen,
    v.Fecha_Fact,
    v.f470_precio_uni,
    v.Precio_init_COP,
    v.f112_id,
    v.Margen,
    v.Cliente,
    v.f430_id_moneda_docto,
    v.f430_tasa_local,
    ui.f421_precio_unitario,
    ui.Fecha_import,
    ui.pais                                                      AS f011_descripcion,
    ui.f41851_rowid,
    TRY_CONVERT(DECIMAL(38,10),
        CASE
            WHEN NULLIF(TRY_CONVERT(DECIMAL(38,10), c.[1]), 0) IS NULL THEN NULL
            ELSE
                (
                    (COALESCE(TRY_CONVERT(DECIMAL(38,10), c.[2]), 0)
                     + COALESCE(TRY_CONVERT(DECIMAL(38,10), c.[3]), 0)
                     + COALESCE(TRY_CONVERT(DECIMAL(38,10), c.[4]), 0)
                     + COALESCE(TRY_CONVERT(DECIMAL(38,10), c.[5]), 0))
                    / NULLIF(TRY_CONVERT(DECIMAL(38,10), c.[1]), 0)
                ) * 100
        END
    )                                                           AS Factor,
    TRY_CONVERT(DECIMAL(38,10),
        CASE
            WHEN NULLIF(TRY_CONVERT(DECIMAL(38,10), ui.f41851_cant_entrada), 0) IS NULL THEN NULL
            ELSE
                (
                    COALESCE(TRY_CONVERT(DECIMAL(38,10), c.[1]), 0)
                    + COALESCE(TRY_CONVERT(DECIMAL(38,10), c.[2]), 0)
                    + COALESCE(TRY_CONVERT(DECIMAL(38,10), c.[3]), 0)
                    + COALESCE(TRY_CONVERT(DECIMAL(38,10), c.[4]), 0)
                    + COALESCE(TRY_CONVERT(DECIMAL(38,10), c.[5]), 0)
                ) / NULLIF(TRY_CONVERT(DECIMAL(38,10), ui.f41851_cant_entrada), 0)
        END
    )                                                           AS Vr_unit_local,
    ui.Proveedor,
    ISNULL(p.Cant_Brasil,0)                                      AS Cant_Brasil,
    ISNULL(p.Cant_Usa,   0)                                      AS Cant_Usa,
    ISNULL(p.Cant_Europa,0)                                      AS Cant_Europa,
    TRY_CONVERT(DECIMAL(38,10),
        CASE
            WHEN NULLIF(TRY_CONVERT(DECIMAL(38,10), p.Total), 0) IS NULL THEN 0
            ELSE COALESCE(TRY_CONVERT(DECIMAL(38,10), p.Cant_Brasil), 0)
                 / NULLIF(TRY_CONVERT(DECIMAL(38,10), p.Total), 0)
        END
    )                                                           AS [Part. Brasil],
    TRY_CONVERT(DECIMAL(38,10),
        CASE
            WHEN NULLIF(TRY_CONVERT(DECIMAL(38,10), p.Total), 0) IS NULL THEN 0
            ELSE COALESCE(TRY_CONVERT(DECIMAL(38,10), p.Cant_Usa), 0)
                 / NULLIF(TRY_CONVERT(DECIMAL(38,10), p.Total), 0)
        END
    )                                                           AS [Part. Usa],
    TRY_CONVERT(DECIMAL(38,10),
        CASE
            WHEN NULLIF(TRY_CONVERT(DECIMAL(38,10), p.Total), 0) IS NULL THEN 0
            ELSE COALESCE(TRY_CONVERT(DECIMAL(38,10), p.Cant_Europa), 0)
                 / NULLIF(TRY_CONVERT(DECIMAL(38,10), p.Total), 0)
        END
    )                                                           AS [Part. Europa],
    ISNULL(p.Total,0)                                            AS Total
FROM       t120_mc_items i
JOIN       t121_mc_items_extensiones ie  ON i.f120_rowid = ie.f121_rowid_item
LEFT JOIN  cri           ON cri.rowid_item = i.f120_rowid
LEFT JOIN  ult_venta v   ON v.rowid_item   = i.f120_rowid
LEFT JOIN  ult_import ui ON ui.rowid_item  = i.f120_rowid
LEFT JOIN  costos c      ON c.f41853_rowid_movto_import = ui.f41851_rowid
LEFT JOIN  part p        ON p.rowid_item   = i.f120_rowid
LEFT JOIN  obj           ON obj.Cod_Sistema =
                            LEFT(cri.[12], NULLIF(CHARINDEX(' -',cri.[12]),0) - 1)
WHERE  i.f120_id_cia = 1
  AND  ie.f121_id_cia = 1
  AND  i.f120_ind_tipo_item IN (1,3)
  AND  EXISTS (
        SELECT 1 FROM t125_mc_items_criterios c2
        WHERE  c2.f125_rowid_item = i.f120_rowid
          AND  c2.f125_id_plan = '07'
          AND  c2.f125_id_criterio_mayor IN ('051','019','102','074'))
ORDER BY i.f120_rowid
"""


# ===========================================================================
# QUERY REFERENCIAS ALTERNAS
# ===========================================================================
SQL_REFS = """
SELECT RTRIM(i.f120_referencia) AS Referencia,
       RTRIM(r.f124_referencia) AS Referencia_Alterna
FROM   t124_mc_items_referencias r
JOIN   t120_mc_items i ON r.f124_rowid_item = i.f120_rowid
WHERE  r.f124_id_cia = 1
  AND  i.f120_id_cia = 1
"""


def leer_hoja(path, sheet, cols):
    df = pl.read_excel(path, sheet_name=sheet, has_header=True)
    n  = df.columns
    a0 = next(iter(cols))
    return (
        df.select(
            [normalize_reference_expr_polars(pl.col(n[cols[a0]])).alias(a0)] +
            [
                # DNET suele venir como texto; mantenerlo como string para parsearlo luego.
                (
                    pl.col(n[v]).cast(pl.Utf8, strict=False).alias(k)
                    if str(k).strip().upper().startswith("DNET")
                    else pl.col(n[v]).cast(pl.Float64, strict=False).alias(k)
                )
                for k, v in list(cols.items())[1:]
            ]
        )
        .filter(pl.col(a0).is_not_null() & (pl.col(a0) != ""))
        .unique(subset=[a0], keep="first")
    )

            
def exportar_excel(df, path, sheet):
    out_path = Path(path)
    try:
        with pd.ExcelWriter(out_path, engine="xlsxwriter") as w:
            df.to_excel(w, index=False, sheet_name=sheet)
            ws = w.sheets[sheet]
            for i, c in enumerate(df.columns):
                serie_str = df[c].astype("string").fillna("")
                ancho = serie_str.map(len).max()
                ancho = 0 if pd.isna(ancho) else int(ancho)
                ws.set_column(i, i, min(max(ancho, len(str(c))) + 2, 50))
        return
    except PermissionError:
        # Típico: el archivo destino está abierto en Excel/OneDrive.
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        alt = out_path.with_name(f"{out_path.stem}_{ts}{out_path.suffix}")
        with pd.ExcelWriter(alt, engine="xlsxwriter") as w:
            df.to_excel(w, index=False, sheet_name=sheet)
            ws = w.sheets[sheet]
            for i, c in enumerate(df.columns):
                serie_str = df[c].astype("string").fillna("")
                ancho = serie_str.map(len).max()
                ancho = 0 if pd.isna(ancho) else int(ancho)
                ws.set_column(i, i, min(max(ancho, len(str(c))) + 2, 50))
        print(f"[WARN] No se pudo escribir '{out_path.name}' (archivo en uso). Se guardó como: {alt.name}")


# ===========================================================================
# PASOS
# ===========================================================================
def paso1_precios_excel():
    """
    Lee precios y disponibilidades desde Excel.
    Persiste ref_norm como columna calculada en precios_consolidados
    para que paso3 la use directamente sin recalcular.
    """
    print("Paso 1 — Leyendo precios y disponibilidades desde Excel...")

    df_usa  = leer_hoja(EXCEL_PREC, "USA",    {"r": 0, "precio_usa": 8})
    df_br   = leer_hoja(EXCEL_PREC, "BRASIL", {"r": 0, "precio_br":  8})
    df_eur  = leer_hoja(EXCEL_PREC, "EUR",    {"r": 0, "precio_eur": 8})
    df_disp = leer_hoja(EXCEL_DISP, "LISTA AGCS",
                        {
                            "r": 0,
                            # Disponibilidad por origen (conteos)
                            "disp_br": 8,
                            "disp_eur": 11,
                            "disp_usa": 12,
                            # Columnas adicionales solicitadas (DNET)
                            "DNET BRA USD": 5,
                            "DNET USA USD": 6,
                            "DNET EUR EURO": 7,
                        })
    # LISTA AGCS trae PN repetidos; vigente = fila más reciente por Fecha (orden Excel).
    # Aseguramos ese criterio con sort explícito y luego unique(first).
    df_disp = (
        df_disp
        .with_columns(
            pl.col("Fecha").cast(pl.Date, strict=False).alias("_fecha_agcs")
            if "Fecha" in df_disp.columns
            else pl.lit(None).cast(pl.Date).alias("_fecha_agcs")
        )
        .sort(["r", "_fecha_agcs"], descending=[False, True])
        .unique(subset=["r"], keep="first")
        .drop(["_fecha_agcs"])
    )

    todas_refs = pl.concat([
        df_usa.select("r"),
        df_br.select("r"),
        df_eur.select("r"),
        df_disp.select("r"),
    ]).unique()

    df = (
        todas_refs
        .join(df_usa,  on="r", how="left")
        .join(df_br,   on="r", how="left")
        .join(df_eur,  on="r", how="left")
        .join(df_disp, on="r", how="left")
        .rename({"r": "referencia"})
        .with_columns([
            pl.col("precio_usa").round(2),
            pl.col("precio_br").round(2),
            pl.col("precio_eur").round(2),
            pl.col("disp_br").cast(pl.Int64,  strict=False),
            pl.col("disp_eur").cast(pl.Int64, strict=False),
            pl.col("disp_usa").cast(pl.Int64, strict=False),
            # DNET viene frecuentemente como texto (comas/símbolos); limpiamos y convertimos.
            pl.col("DNET BRA USD")
              .cast(pl.Utf8, strict=False)
              .str.replace_all(",", ".")
              .str.replace_all(r"[^0-9.\-]", "")
              .cast(pl.Float64, strict=False)
              .round(2),
            pl.col("DNET USA USD")
              .cast(pl.Utf8, strict=False)
              .str.replace_all(",", ".")
              .str.replace_all(r"[^0-9.\-]", "")
              .cast(pl.Float64, strict=False)
              .round(2),
            pl.col("DNET EUR EURO")
              .cast(pl.Utf8, strict=False)
              .str.replace_all(",", ".")
              .str.replace_all(r"[^0-9.\-]", "")
              .cast(pl.Float64, strict=False)
              .round(2),
            normalize_reference_expr_polars(pl.col("referencia")).alias("ref_norm"),
        ])
        .sort("referencia")
    )
    with duckdb.connect(str(DUCKDB_PATH)) as con:
        con.execute("PRAGMA threads = 4")
        con.register("tmp", df)
        con.execute("CREATE OR REPLACE TABLE precios_consolidados AS SELECT * FROM tmp")
        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_precios_consolidados_ref_norm "
            "ON precios_consolidados(ref_norm)"
        )
        optimizar_duckdb(con, ["precios_consolidados"])

    df.write_csv(CSV_OUT)
    print(f"  {df.height:,} referencias consolidadas -> {CSV_OUT.name}\n")


def paso2_sqlserver():
    """
    Extrae maestro y referencias alternas desde SQL Server.
    Persiste Ref_Norm en maestro y (Ref_Norm + Ref_Alt_Norm) en referencias_alternas
    para que paso3 los use directamente sin recalcular.
    """
    print("Paso 2 — Extrayendo desde SQL Server...")
    print(f"  Rango participación: {FECHA_PART_DESDE} → {FECHA_PART_HASTA}")

    conn_sql = pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SERVER};"
        f"DATABASE={DATABASE};UID={DB_USER};PWD={DB_PASS};"
        f"Encrypt=yes;TrustServerCertificate=yes;"
    )

    with duckdb.connect(str(DUCKDB_PATH)) as con:
        con.execute("PRAGMA threads = 4")
        try:
            total   = 0
            primera = True

            for i, chunk in enumerate(
                pd.read_sql(SQL, conn_sql, chunksize=CHUNK), start=1
            ):
                chunk.columns = [c.strip() for c in chunk.columns]
                con.register("chunk_tmp", chunk)

                if primera:
                    con.execute(
                        "CREATE OR REPLACE TABLE maestro AS SELECT * FROM chunk_tmp"
                    )
                    primera = False
                else:
                    con.execute("INSERT INTO maestro SELECT * FROM chunk_tmp")

                total += len(chunk)
                print(f"  Chunk {i} cargado: {total:,} filas acumuladas en maestro")

            if primera:
                raise ValueError("El query principal no retornó datos.")

            # Persiste Ref_Norm en maestro — calculada una sola vez
            con.execute("ALTER TABLE maestro ADD COLUMN Ref_Norm VARCHAR")
            con.execute(f"UPDATE maestro SET Ref_Norm = {normalize_reference_expr_sql('Referencia')}")

            print("  Cargando referencias alternas...")
            df_refs = pd.read_sql(SQL_REFS, conn_sql)
            df_refs.columns = [c.strip() for c in df_refs.columns]
            con.register("tmp_refs", df_refs)

            # Persiste Ref_Norm + Ref_Alt_Norm en referencias_alternas
            con.execute(f"""
                CREATE OR REPLACE TABLE referencias_alternas AS
                SELECT *,
                       {normalize_reference_expr_sql('Referencia')}        AS Ref_Norm,
                       {normalize_reference_expr_sql('Referencia_Alterna')} AS Ref_Alt_Norm
                FROM tmp_refs
            """)
            con.execute(
                "CREATE INDEX IF NOT EXISTS idx_maestro_ref_norm "
                "ON maestro(Ref_Norm)"
            )
            con.execute(
                "CREATE INDEX IF NOT EXISTS idx_referencias_alternas_ref_norm "
                "ON referencias_alternas(Ref_Norm)"
            )
            con.execute(
                "CREATE INDEX IF NOT EXISTS idx_referencias_alternas_ref_alt_norm "
                "ON referencias_alternas(Ref_Alt_Norm)"
            )
            optimizar_duckdb(con, ["maestro", "referencias_alternas"])

            d1 = con.execute("""
                SELECT COUNT(*)            AS total,
                       COUNT(Fecha_Fact)   AS con_venta,
                       COUNT(Fecha_import) AS con_importacion
                FROM maestro
            """).df()
            d2 = con.execute(
                "SELECT COUNT(*) AS total_alternas FROM referencias_alternas"
            ).df()


            print(f"\n  Maestro:\n{d1.to_string(index=False)}")
            print(f"  Alternas:\n{d2.to_string(index=False)}\n")

            bad = con.execute("""
                SELECT COUNT(*)
                FROM maestro
                WHERE COALESCE(CAST("Part. Brasil" AS DOUBLE), 0) < 0
                   OR COALESCE(CAST("Part. Brasil" AS DOUBLE), 0) > 1
                   OR COALESCE(CAST("Part. Usa"    AS DOUBLE), 0) < 0
                   OR COALESCE(CAST("Part. Usa"    AS DOUBLE), 0) > 1
                   OR COALESCE(CAST("Part. Europa" AS DOUBLE), 0) < 0
                   OR COALESCE(CAST("Part. Europa" AS DOUBLE), 0) > 1
            """).fetchone()[0]

            if bad > 0:
                raise ValueError(
                    f"{bad} filas con participaciones fuera de rango 0-1 — "
                    f"revisar query SQL Server antes de continuar."
                )

        finally:
            conn_sql.close()


def paso3_cruzar_y_exportar():
    """
    Cruza precios usando columnas normalizadas precalculadas.
    Sin recalcular norm_sql en runtime — todos los JOINs usan
    ref_norm / Ref_Norm / Ref_Alt_Norm directamente.
    """
    print("Paso 3 — Cruzando precios, calculando ganador y exportando...")

    with duckdb.connect(str(DUCKDB_PATH)) as con:
        con.execute("PRAGMA threads = 4")

        con.execute(f"""
        CREATE OR REPLACE TABLE precio_familia AS
        WITH
        candidatos AS (
            -- JOIN directo sobre columnas precalculadas — sin norm_sql en runtime
            SELECT
                m.Rowid,
                m.Referencia,
                m.Ref_Norm                              AS Ref_Principal_Norm,
                COALESCE(a.Ref_Alt_Norm, m.Ref_Norm)    AS ref_candidata,
                a.Referencia_Alterna
            FROM maestro m
            LEFT JOIN referencias_alternas a
                   ON m.Ref_Norm = a.Ref_Norm
        ),
        con_precios AS (
            SELECT
                c.Rowid,
                c.Referencia,
                c.Ref_Principal_Norm,
                c.ref_candidata,
                c.Referencia_Alterna,
                p.precio_br,  p.precio_usa,  p.precio_eur,
                p.disp_br,    p.disp_usa,    p.disp_eur,
                p."DNET BRA USD"             AS dnet_bra_usd,
                p."DNET USA USD"             AS dnet_usa_usd,
                p."DNET EUR EURO"            AS dnet_eur_euro,
                COALESCE(p.disp_usa, 0) + COALESCE(p.disp_br, 0)
                    + COALESCE(p.disp_eur, 0)                     AS suma_disp,
                COALESCE(p.precio_usa, 0) + COALESCE(p.precio_br, 0)
                    + COALESCE(p.precio_eur, 0)                   AS suma_prec,
                CASE WHEN p.precio_br IS NULL AND p.precio_usa IS NULL
                          AND p.precio_eur IS NULL THEN NULL
                     ELSE list_median(list_filter([
                            p.precio_br  * {FACTOR_BR},
                            p.precio_usa * {FACTOR_USA},
                            p.precio_eur * {FACTOR_EUR}
                        ], x -> x IS NOT NULL))
                END                                               AS precio_rep
            FROM candidatos c
            -- JOIN sobre ref_norm precalculada — sin recalcular
            LEFT JOIN precios_consolidados p
                   ON c.ref_candidata = p.ref_norm
        ),
        fam AS (
            SELECT
                Ref_Principal_Norm,
                COUNT(*)                                           AS num_refs,
                MAX(precio_rep) / NULLIF(MIN(NULLIF(precio_rep,0)),0) AS ratio,
                '('||string_agg(
                    UPPER(TRIM(COALESCE(Referencia_Alterna, Referencia))), ','
                )||')'                                             AS RefsAlternas,
                '('||string_agg(
                    '('||COALESCE(CAST(ROUND(precio_br, 2) AS VARCHAR), '')||','
                       ||COALESCE(CAST(ROUND(precio_usa,2) AS VARCHAR), '')||','
                       ||COALESCE(CAST(ROUND(precio_eur,2) AS VARCHAR), '')||')', ','
                )||')'                                             AS Precios_BR_USA_EURO,
                '('||string_agg(
                    '('||COALESCE(CAST(CAST(disp_br  AS BIGINT) AS VARCHAR), '')||','
                       ||COALESCE(CAST(CAST(disp_usa AS BIGINT) AS VARCHAR), '')||','
                       ||COALESCE(CAST(CAST(disp_eur AS BIGINT) AS VARCHAR), '')||')', ','
                )||')'                                             AS Dispon_BR_USA_EURO
            FROM con_precios
            GROUP BY Ref_Principal_Norm
        ),
        ranked AS (
            SELECT
                f.Ref_Principal_Norm,
                cp.Rowid,
                cp.ref_candidata,
                cp.precio_br,  cp.precio_usa,  cp.precio_eur,
                cp.disp_br,    cp.disp_usa,    cp.disp_eur,
                cp.dnet_bra_usd, cp.dnet_usa_usd, cp.dnet_eur_euro,
                cp.suma_prec,  cp.suma_disp,
                f.num_refs,    f.ratio,
                f.RefsAlternas, f.Precios_BR_USA_EURO, f.Dispon_BR_USA_EURO,
                ROW_NUMBER() OVER (
                    PARTITION BY cp.Ref_Principal_Norm
                    ORDER BY
                        cp.suma_disp DESC,
                        cp.suma_prec DESC,
                        CASE WHEN cp.ref_candidata = cp.Ref_Principal_Norm
                             THEN 1 ELSE 0 END DESC
                ) AS rn
            FROM con_precios cp
            JOIN fam f ON cp.Ref_Principal_Norm = f.Ref_Principal_Norm
        )
        SELECT
            Ref_Principal_Norm          AS Referencia_Principal,
            Rowid                       AS Item,
            ref_candidata               AS Referencia_Activa,
            ROUND(precio_usa, 2)        AS precio_usa,
            ROUND(precio_br,  2)        AS precio_br,
            ROUND(precio_eur, 2)        AS precio_eur,
            disp_usa, disp_br, disp_eur,
            ROUND(dnet_bra_usd, 2)      AS DNET_BRA_USD,
            ROUND(dnet_usa_usd, 2)      AS DNET_USA_USD,
            ROUND(dnet_eur_euro, 2)     AS DNET_EUR_EURO,
            ROUND(suma_prec, 2)         AS suma_precios,
            suma_disp                   AS suma_disponibilidad,
            num_refs                    AS num_ref_activas,
            ROUND(ratio, 4)             AS ratio_precio,
            RefsAlternas, Precios_BR_USA_EURO, Dispon_BR_USA_EURO
        FROM ranked
        WHERE rn = 1
        """)

        con.execute(f"""
        CREATE OR REPLACE TABLE resultado_precios_lista AS
        SELECT
            m.Rowid,
            m.Referencia                             AS Referencia_Original,
            m.Ref_Norm                               AS Referencia_Normalizada,
            m.Descripcion_Item                       AS "Descripción",
            m.Cod_Sistema                            AS "Cod. Sistema",
            m.Sistema_Precio                         AS "Sistema Precio",
            m.Linea                                  AS "Línea",
            m.Sistema                                AS "Sistema CNH",
            m.Equipo,
            m.Modelo,
            m.Rotacion                               AS "Rotación",
            m.Estado,
            m.Fecha_Fact                             AS "Fecha Ult. Venta",
            m.f430_id_moneda_docto                   AS "Moneda",
            m.f430_tasa_local                        AS "Tasa Local",
            m.f470_precio_uni                        AS "Ult. Precio Venta",
            m.f112_id                                AS "Lista Venta",
            m.Margen,
            m.Ma_Margen                              AS "Margen Objetivo",
            m.Cliente,
            m.Fecha_import                           AS "Ult. Fecha Compra",
            m.f421_precio_unitario                   AS "Último Valor (USD)",
            -- Misma lógica que AFAS_VENTAS_Base_Precios (precio lista.txt):
            -- Valor liquidado COP = costo importación por unidad (no Precio_init_COP de ventas).
            m.Vr_unit_local                          AS "Valor Liq. (COP)",
            m.Precio_init_COP                        AS "Precio venta COP",
            m.Factor                                 AS "Factor Import.",
            m.Proveedor,
            m.Cant_Brasil                            AS "Cant. Brasil",
            m.Cant_Usa                               AS "Cant. Usa",
            m.Cant_Europa                            AS "Cant Europa",
            m.Total                                  AS "Cant. Total",
            m."Part. Brasil",
            m."Part. Usa",
            m."Part. Europa",
            pf.Referencia_Principal,
            pf.Referencia_Activa,
            ROUND(pf.precio_br,  2)                  AS "Precio Brasil",
            ROUND(pf.precio_usa, 2)                  AS "Precio Usa",
            ROUND(pf.precio_eur, 2)                  AS "Precio Europa",
            pf.disp_br,
            pf.disp_usa,
            pf.disp_eur,
            pf.DNET_BRA_USD                           AS "DNET BRA USD",
            pf.DNET_USA_USD                           AS "DNET USA USD",
            pf.DNET_EUR_EURO                          AS "DNET EUR EURO",
            pf.suma_precios,
            pf.suma_disponibilidad,
            pf.num_ref_activas,
            pf.ratio_precio,
            pf.RefsAlternas,
            pf.Precios_BR_USA_EURO,
            pf.Dispon_BR_USA_EURO,
            CASE WHEN pf.Referencia_Principal IS NOT NULL
                 THEN 'PRINCIPAL' ELSE 'NO_MATCH' END  AS match_type,
            CASE
                WHEN COALESCE(CAST(m."Part. Brasil" AS DOUBLE), 0)
                   + COALESCE(CAST(m."Part. Usa"    AS DOUBLE), 0)
                   + COALESCE(CAST(m."Part. Europa" AS DOUBLE), 0) > 0
                THEN ROUND(
                      COALESCE(pf.precio_br,  0) * COALESCE(CAST(m."Part. Brasil" AS DOUBLE), 0)
                    + COALESCE(pf.precio_usa, 0) * COALESCE(CAST(m."Part. Usa"    AS DOUBLE), 0)
                    + COALESCE(pf.precio_eur, 0) * COALESCE(CAST(m."Part. Europa" AS DOUBLE), 0)
                , 2)
                WHEN pf.precio_br  IS NULL AND pf.precio_usa IS NULL
                     AND pf.precio_eur IS NULL             THEN NULL
                WHEN pf.precio_br  IS NULL AND pf.precio_usa IS NULL
                     THEN ROUND(pf.precio_eur, 2)
                WHEN pf.precio_br  IS NULL AND pf.precio_eur IS NULL
                     THEN ROUND(pf.precio_usa, 2)
                WHEN pf.precio_usa IS NULL AND pf.precio_eur IS NULL
                     THEN ROUND(pf.precio_br,  2)
                WHEN pf.precio_eur IS NULL
                     THEN ROUND((pf.precio_br *0.5)+(pf.precio_usa*0.5), 2)
                WHEN pf.precio_usa IS NULL
                     THEN ROUND((pf.precio_br *0.5)+(pf.precio_eur*0.5), 2)
                WHEN pf.precio_br  IS NULL
                     THEN ROUND((pf.precio_usa*0.5)+(pf.precio_eur*0.5), 2)
                ELSE ROUND((pf.precio_br+pf.precio_usa+pf.precio_eur)/3.0, 2)
            END                                       AS "Precio Prorrateo"
        FROM maestro m
        -- JOIN sobre Ref_Norm precalculada — sin recalcular
        LEFT JOIN precio_familia pf ON m.Ref_Norm = pf.Referencia_Principal
        """)
        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_precio_familia_ref_principal "
            "ON precio_familia(Referencia_Principal)"
        )
        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_resultado_ref_norm "
            "ON resultado_precios_lista(Referencia_Normalizada)"
        )
        optimizar_duckdb(con, ["precio_familia", "resultado_precios_lista"])

        df_out = con.execute("SELECT * FROM resultado_precios_lista").df()

    exportar_excel(df_out, EXCEL_OUT, "Precios CNH")
    print(f"  {len(df_out):,} filas -> {EXCEL_OUT.name}\n")


# ===========================================================================
# MAIN
# ===========================================================================
def main():
    for ruta in [EXCEL_PREC, EXCEL_DISP]:
        if not ruta.exists():
            raise FileNotFoundError(f"Archivo no encontrado: {ruta}")

    print("\n" + "=" * 60)
    print("PIPELINE DE PRECIOS CNH - IMECOL S.A.S")
    print(f"  Participación: {FECHA_PART_DESDE} → {FECHA_PART_HASTA}")
    print("=" * 60 + "\n")

    paso1_precios_excel()
    paso2_sqlserver()
    paso3_cruzar_y_exportar()

    print("=" * 60)
    print(f"  Excel  -> {EXCEL_OUT.name}")
    print(f"  DuckDB -> {DUCKDB_PATH.name}")
    print(f"  CSV    -> {CSV_OUT.name}")
    print(f"  Ruta   -> {BASE_DIR.resolve()}")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
