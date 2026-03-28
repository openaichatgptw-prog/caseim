"""
00_Reportes_SQL.py
--------------------------------------------------------------------------
Pipeline SQL (Paso 00) - Carga de reportes base a DuckDB

Flujo:
  1) Ejecuta 00_precio_margen_SIESA.sql en SQL Server
  2) Ejecuta 00_atributos_referencias.sql en SQL Server
  3) Persiste resultados en DuckDB:
       - margen_siesa_raw
       - atributos_referencias_raw

No hay limite de filas en el total: se lee el resultset completo en bloques
(CHUNK) solo para no saturar memoria en una sola lectura.

Configuracion: config.ini (secciones SQLSERVER y SALIDA)
--------------------------------------------------------------------------
"""

import configparser
import os
from pathlib import Path
from datetime import datetime
import re

import duckdb
import pandas as pd
import pyodbc
from ref_normalization import normalize_reference_series_pd


# ===========================================================================
# CONFIGURACION
# ===========================================================================
BASE_DIR = Path(__file__).parent
CONFIG_PATH = BASE_DIR / "config.ini"

if not CONFIG_PATH.exists():
    raise FileNotFoundError(f"No se encontro config.ini en: {CONFIG_PATH}")

config = configparser.ConfigParser()
config.read(CONFIG_PATH, encoding="utf-8")

FECHA = datetime.now().strftime("%Y%m%d")

SERVER = config["SQLSERVER"]["server"]
DATABASE = config["SQLSERVER"]["database"]
DB_USER = config["SQLSERVER"]["db_user"]
DB_PASS = config["SQLSERVER"]["db_pass"].strip('"')

DUCKDB_PATH = Path(os.getenv("PIPELINE_DUCKDB_PATH", str(BASE_DIR / config["SALIDA"]["duckdb"])))
CHUNK = 50_000

SQL_PRECIO_MARGEN_SIESA = """
/*
Reporte de inventario del grupo 051 – Repuestos CASE por bodega,
con indicadores de rentabilidad por lista y control de antigüedad del stock.

NombreQuery: Reporte_Inventario_CASE_Rentabilidad_Stock
*/

USE UnoEE;
SET NOCOUNT ON;

------------------------------------------------------------
-- 1 BASE DE REFERENCIAS PRINCIPAL + TODAS SUS ALTERNAS
------------------------------------------------------------
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

------------------------------------------------------------
-- 2 REFERENCIAS DEL GRUPO 051 – REPUESTOS CASE
------------------------------------------------------------
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

------------------------------------------------------------
-- 3 ROTACION (PLAN 08)
------------------------------------------------------------
IF OBJECT_ID('tempdb..#Rotacion') IS NOT NULL DROP TABLE #Rotacion;

SELECT
    i.f120_referencia,
    {fn CONCAT({fn CONCAT(RTRIM(may.f106_id), ' - ')}, may.f106_descripcion)} AS Rotacion
INTO #Rotacion
FROM t106_mc_criterios_item_mayores may
JOIN t125_mc_items_criterios cri
     ON may.f106_id_cia = cri.f125_id_cia
    AND may.f106_id_plan = cri.f125_id_plan
    AND may.f106_id = cri.f125_id_criterio_mayor
JOIN t120_mc_items i
     ON cri.f125_rowid_item = i.f120_rowid
WHERE cri.f125_id_plan = '08';

CREATE INDEX IX_Rotacion ON #Rotacion(f120_referencia);

------------------------------------------------------------
-- 4 EXISTENCIAS + COSTOS + FECHAS + PRECIOS
------------------------------------------------------------
;WITH BaseExistencias AS (
    SELECT
        i.f120_referencia                                AS Referencia,
        RTRIM(i.f120_descripcion)                        AS Descripcion,
        i.f120_id_unidad_inventario                      AS Unidad,

        CASE
            WHEN e.f121_ind_estado = 0 THEN 'Inactivo'
            WHEN e.f121_ind_estado = 2 THEN 'Bloqueado'
            ELSE 'Activo'
        END AS EstadoItem,

        x.f400_id_instalacion                            AS Instalacion,
        RTRIM(inst.f157_descripcion)                     AS Nom_Instalacion,

        b.f150_id                                        AS Bodega,
        RTRIM(b.f150_descripcion)                        AS Nom_Bodega,

        x.f400_cant_existencia_1                         AS Existencia,
        (x.f400_cant_existencia_1 - x.f400_cant_comprometida_1)
                                                        AS Disponible,

        CAST(ins.f132_costo_prom_uni AS DECIMAL(18,6))   AS Costo_Prom_Inst,

        x.f400_fecha_ult_compra,
        x.f400_fecha_ult_venta,
        x.f400_fecha_ult_salida,
        x.f400_fecha_ult_entrada,

        -- Lista 09
        (
            SELECT TOP 1 CAST(p.f126_precio AS DECIMAL(18,6))
            FROM t126_mc_items_precios p
            WHERE p.f126_rowid_item = i.f120_rowid
              AND p.f126_id_lista_precio = '09'
            ORDER BY p.f126_fecha_activacion DESC
        ) AS Precio_Lista_09,

        -- Lista 04
        (
            SELECT TOP 1 CAST(p.f126_precio AS DECIMAL(18,6))
            FROM t126_mc_items_precios p
            WHERE p.f126_rowid_item = i.f120_rowid
              AND p.f126_id_lista_precio = '04'
            ORDER BY p.f126_fecha_activacion DESC
        ) AS Precio_Lista_04
    FROM t120_mc_items i
    JOIN #Refs051 r ON r.f120_referencia = i.f120_referencia
    JOIN t121_mc_items_extensiones e
         ON i.f120_rowid = e.f121_rowid_item
    JOIN t400_cm_existencia x
         ON e.f121_rowid = x.f400_rowid_item_ext
    JOIN t150_mc_bodegas b
         ON x.f400_rowid_bodega = b.f150_rowid
    JOIN t157_mc_instalaciones inst
         ON inst.f157_id_cia = b.f150_id_cia
        AND inst.f157_id     = x.f400_id_instalacion
    JOIN t132_mc_items_instalacion ins
         ON e.f121_rowid = ins.f132_rowid_item_ext
        AND x.f400_id_instalacion = ins.f132_id_instalacion
)

------------------------------------------------------------
-- 5 SALIDA FINAL CON TODO
------------------------------------------------------------
SELECT
    be.Referencia,
    br.Referencia_Alternas,
    be.Descripcion,
    be.Unidad,
    be.EstadoItem,
    rot.Rotacion,
    be.Instalacion,
    be.Nom_Instalacion,
    be.Bodega,
    be.Nom_Bodega,
    be.Existencia,
    be.Disponible,
    CAST(be.Costo_Prom_Inst AS DECIMAL(18,2)) AS Costo_Prom_Inst,
    CAST(be.Precio_Lista_09 AS DECIMAL(18,2)) AS Precio_Lista_09,
    CAST(
        CASE WHEN be.Precio_Lista_09 = 0 THEN NULL
             ELSE ((be.Precio_Lista_09 - be.Costo_Prom_Inst) / be.Precio_Lista_09) * 100
        END AS DECIMAL(18,2)
    ) AS Margen09,
    CAST(be.Precio_Lista_04 AS DECIMAL(18,2)) AS Precio_Lista_04,
    CAST(
        CASE WHEN be.Precio_Lista_04 = 0 THEN NULL
             ELSE ((be.Precio_Lista_04 - be.Costo_Prom_Inst) / be.Precio_Lista_04) * 100
        END AS DECIMAL(18,2)
    ) AS Margen04,
    be.f400_fecha_ult_compra,
    be.f400_fecha_ult_venta,
    be.f400_fecha_ult_salida,
    be.f400_fecha_ult_entrada,
    Fecha_Max =
        COALESCE(
            (
                SELECT MAX(f)
                FROM (VALUES
                        (be.f400_fecha_ult_compra),
                        (be.f400_fecha_ult_venta),
                        (be.f400_fecha_ult_salida)
                     ) AS Fechas(f)
            ),
            be.f400_fecha_ult_entrada
        ),
    Dias_Desde_Fecha_Max =
        DATEDIFF(
            DAY,
            COALESCE(
                (
                    SELECT MAX(f)
                    FROM (VALUES
                            (be.f400_fecha_ult_compra),
                            (be.f400_fecha_ult_venta),
                            (be.f400_fecha_ult_salida)
                         ) AS Fechas(f)
                ),
                be.f400_fecha_ult_entrada
            ),
            GETDATE()
        )
FROM BaseExistencias be
LEFT JOIN #BaseRefs  br  ON br.Referencia_Principal = be.Referencia
LEFT JOIN #Rotacion  rot ON rot.f120_referencia = be.Referencia
--WHERE be.Existencia > 1
ORDER BY be.Referencia, be.Bodega;
"""

SQL_ATRIBUTOS_REFERENCIAS = """
SELECT
    i.f120_referencia                 AS Referencia,
    RTRIM(i.f120_descripcion)         AS Descripcion_Item,

    Linea_Item.Cod_Linea,
    Linea_Item.Linea_Item,

    Sistema_CNH.Sistema_CNH,
    Equipo_CNH.Equipo_CNH,
    Modelo_CNH.Modelo_CNH,
    Rotacion_Item.Clasificacion_Rotacion,

    Sistema_Precio.Cod_Sistema_Precio,
    Sistema_Precio.Sistema_Precio_Item,

    Margen_Objetivo.Margen_Objetivo_Sistema

FROM t120_mc_items i

/* ========== LINEA ITEM (PLAN 07) ========== */
OUTER APPLY (
    SELECT TOP 1
        RTRIM(may.f106_id) AS Cod_Linea,
        RTRIM(may.f106_id) + ' - ' + may.f106_descripcion AS Linea_Item
    FROM t125_mc_items_criterios cri
    JOIN t106_mc_criterios_item_mayores may
        ON may.f106_id_plan = cri.f125_id_plan
       AND may.f106_id = cri.f125_id_criterio_mayor
    WHERE cri.f125_rowid_item = i.f120_rowid
      AND cri.f125_id_plan = '07'
) AS Linea_Item

/* ========== SISTEMA CNH (PLAN 04) ========== */
OUTER APPLY (
    SELECT TOP 1
        RTRIM(may.f106_id) + ' - ' + may.f106_descripcion AS Sistema_CNH
    FROM t125_mc_items_criterios cri
    JOIN t106_mc_criterios_item_mayores may
        ON may.f106_id_plan = cri.f125_id_plan
       AND may.f106_id = cri.f125_id_criterio_mayor
    WHERE cri.f125_rowid_item = i.f120_rowid
      AND cri.f125_id_plan = '04'
) AS Sistema_CNH

/* ========== EQUIPO (PLAN 01) ========== */
OUTER APPLY (
    SELECT TOP 1
        RTRIM(may.f106_id) + ' - ' + may.f106_descripcion AS Equipo_CNH
    FROM t125_mc_items_criterios cri
    JOIN t106_mc_criterios_item_mayores may
        ON may.f106_id_plan = cri.f125_id_plan
       AND may.f106_id = cri.f125_id_criterio_mayor
    WHERE cri.f125_rowid_item = i.f120_rowid
      AND cri.f125_id_plan = '01'
) AS Equipo_CNH

/* ========== MODELO (PLAN 03) ========== */
OUTER APPLY (
    SELECT TOP 1
        RTRIM(may.f106_id) + ' - ' + may.f106_descripcion AS Modelo_CNH
    FROM t125_mc_items_criterios cri
    JOIN t106_mc_criterios_item_mayores may
        ON may.f106_id_plan = cri.f125_id_plan
       AND may.f106_id = cri.f125_id_criterio_mayor
    WHERE cri.f125_rowid_item = i.f120_rowid
      AND cri.f125_id_plan = '03'
) AS Modelo_CNH

/* ========== ROTACION (PLAN 08) ========== */
OUTER APPLY (
    SELECT TOP 1
        RTRIM(may.f106_id) + ' - ' + may.f106_descripcion AS Clasificacion_Rotacion
    FROM t125_mc_items_criterios cri
    JOIN t106_mc_criterios_item_mayores may
        ON may.f106_id_plan = cri.f125_id_plan
       AND may.f106_id = cri.f125_id_criterio_mayor
    WHERE cri.f125_rowid_item = i.f120_rowid
      AND cri.f125_id_plan = '08'
) AS Rotacion_Item

/* ========== SISTEMA PRECIO (PLAN 12) ========== */
OUTER APPLY (
    SELECT TOP 1
        RTRIM(may.f106_id) AS Cod_Sistema_Precio,
        RTRIM(may.f106_id) + ' - ' + may.f106_descripcion AS Sistema_Precio_Item
    FROM t125_mc_items_criterios cri
    JOIN t106_mc_criterios_item_mayores may
        ON may.f106_id_plan = cri.f125_id_plan
       AND may.f106_id = cri.f125_id_criterio_mayor
    WHERE cri.f125_rowid_item = i.f120_rowid
      AND cri.f125_id_plan = '12'
) AS Sistema_Precio

/* ========== MARGEN OBJETIVO POR SISTEMA ========== */
OUTER APPLY (
    SELECT TOP 1
        t753_b.f753_dato_numero / 100 AS Margen_Objetivo_Sistema
    FROM t750_mm_movto_entidad me
    JOIN t285_co_centro_op co
        ON me.f750_rowid = co.f285_rowid_movto_entidad
    JOIN t752_mm_movto_entidad_fila fila
        ON me.f750_rowid = fila.f752_rowid_movto_entidad
    JOIN t753_mm_movto_entidad_columna t753_a
        ON fila.f752_rowid = t753_a.f753_rowid_movto_entidad_fila
    JOIN t753_mm_movto_entidad_columna t753_b
        ON fila.f752_rowid = t753_b.f753_rowid_movto_entidad_fila
    WHERE co.f285_id = '001'
      AND t753_a.f753_rowid_entidad_atributo = 1154
      AND t753_b.f753_rowid_entidad_atributo = 1156
      AND t753_a.f753_dato_texto = Sistema_Precio.Cod_Sistema_Precio
) AS Margen_Objetivo

WHERE
    i.f120_id_cia = 1
    AND i.f120_ind_tipo_item IN (1,3);
"""


def _factores_logistica_config() -> tuple[str, str, str]:
    """Literales para DECLARE en SQL_AUDITORIA; deben coincidir con [FACTORES] en config.ini / Config.ini."""
    defaults = (1.25, 1.25, 1.5)
    br, usa, eur = defaults
    for candidate in (BASE_DIR / "config.ini", BASE_DIR / "Config.ini"):
        if not candidate.exists():
            continue
        cfg = configparser.ConfigParser()
        cfg.read(candidate, encoding="utf-8")
        if "FACTORES" not in cfg:
            continue
        sec = cfg["FACTORES"]
        try:
            br = float(sec.get("factor_br", str(defaults[0])))
            usa = float(sec.get("factor_usa", str(defaults[1])))
            eur = float(sec.get("factor_eur", str(defaults[2])))
        except ValueError:
            br, usa, eur = defaults
        break

    def _fmt(v: float) -> str:
        s = f"{v:.10f}".rstrip("0").rstrip(".")
        return s if s else "0"

    return (_fmt(br), _fmt(usa), _fmt(eur))


def _inyectar_factores_en_sql_auditoria(sql: str) -> str:
    br, usa, eur = _factores_logistica_config()
    return sql.replace("__FACTOR_BR__", br).replace("__FACTOR_USA__", usa).replace("__FACTOR_EUR__", eur)


SQL_AUDITORIA_TEMPLATE = """
USE [UnoEE];
GO
SET NOCOUNT ON;

-------------------------------------------------------------------------------
-- PARÁMETROS CONFIGURABLES
-------------------------------------------------------------------------------
DECLARE @ReferenciaAlternaFiltro   VARCHAR(50)  = NULL;

-- UMBRALES DE VARIACIÓN PORCENTUAL (DECIMAL: 0.10 = 10%)
DECLARE @UmbralPctCritico          DECIMAL(5,4) = 0.30;
DECLARE @UmbralPctModAlto          DECIMAL(5,4) = 0.20;
DECLARE @UmbralPctModBajo          DECIMAL(5,4) = 0.10;

-- Factores logísticos (precio compra USD×TRM comparable a costo de bodega; [FACTORES] en config.ini)
DECLARE @FactorBR  DECIMAL(9,4) = __FACTOR_BR__;
DECLARE @FactorUSA DECIMAL(9,4) = __FACTOR_USA__;
DECLARE @FactorEUR DECIMAL(9,4) = __FACTOR_EUR__;

-------------------------------------------------------------------------------
-- LIMPIEZA
-------------------------------------------------------------------------------
DROP TABLE IF EXISTS #RefsFiltradas;
DROP TABLE IF EXISTS #BaseRefs;
DROP TABLE IF EXISTS #Existencias;
DROP TABLE IF EXISTS #ExistenciaTotalRef;
DROP TABLE IF EXISTS #BodegasPorCosto;
DROP TABLE IF EXISTS #FactPricing;
DROP TABLE IF EXISTS #VarCuartiles;
DROP TABLE IF EXISTS #VarStats;

-------------------------------------------------------------------------------
-- 1) BASE DE REFERENCIAS PRINCIPAL + ALTERNAS
-------------------------------------------------------------------------------
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
        t120.f120_referencia AS RefPrincipal,
        t120.f120_referencia AS RefAlterna
    FROM t120_mc_items t120
)
SELECT 
    f.RefPrincipal AS Referencia_Principal,
    STUFF((
        SELECT DISTINCT ', ' + LTRIM(RTRIM(rb2.RefAlterna))
        FROM RefBase rb2
        WHERE rb2.RefPrincipal = f.RefPrincipal
        FOR XML PATH(''), TYPE
    ).value('.', 'NVARCHAR(MAX)'), 1, 2, '') AS Referencias_Alternas
INTO #BaseRefs
FROM RefBase f
GROUP BY f.RefPrincipal;

CREATE CLUSTERED INDEX IX_BaseRefs ON #BaseRefs(Referencia_Principal);

-------------------------------------------------------------------------------
-- 2) FILTRO UNIVERSO REPUESTOS CASE (PLAN 07 / 051)
-------------------------------------------------------------------------------
SELECT DISTINCT i.f120_referencia
INTO #RefsFiltradas
FROM t120_mc_items i
JOIN t125_mc_items_criterios cri
  ON i.f120_rowid = cri.f125_rowid_item
JOIN t106_mc_criterios_item_mayores may
  ON cri.f125_id_criterio_mayor = may.f106_id
WHERE cri.f125_id_plan = '07'
  AND RTRIM(may.f106_id) + ' - ' + may.f106_descripcion = '051 - REPUESTOS CASE';

CREATE CLUSTERED INDEX IX_RefsFiltradas ON #RefsFiltradas(f120_referencia);

-------------------------------------------------------------------------------
-- 3) EXISTENCIAS + COSTO PROM INST + PRECIO LISTA 09 + U.M.
-------------------------------------------------------------------------------
SELECT 
  i.f120_referencia              AS Referencia,
  RTRIM(i.f120_descripcion)      AS Descripcion,
  i.f120_id_unidad_inventario    AS [U.M.],
  b.f150_id                      AS Bodega,
  x.f400_id_instalacion          AS Instalacion,
  x.f400_cant_existencia_1       AS Existencia,
  (x.f400_cant_existencia_1 - x.f400_cant_comprometida_1) AS Disponible,
  CAST(ins.f132_costo_prom_uni AS DECIMAL(18,6))  AS Costo_Prom_Inst,
  CAST(L09.Precio_Lista_09 AS DECIMAL(18,6))      AS Precio_Lista_09
INTO #Existencias
FROM t120_mc_items i
JOIN #RefsFiltradas rf ON i.f120_referencia = rf.f120_referencia
JOIN t121_mc_items_extensiones e ON i.f120_rowid = e.f121_rowid_item
JOIN t400_cm_existencia x        ON e.f121_rowid = x.f400_rowid_item_ext
JOIN t150_mc_bodegas b           ON x.f400_rowid_bodega = b.f150_rowid
JOIN t132_mc_items_instalacion ins
     ON e.f121_rowid = ins.f132_rowid_item_ext
    AND x.f400_id_instalacion = ins.f132_id_instalacion
OUTER APPLY (
  SELECT TOP 1 CAST(p.f126_precio AS DECIMAL(18,6)) AS Precio_Lista_09
  FROM t126_mc_items_precios p
  WHERE p.f126_rowid_item = i.f120_rowid
    AND p.f126_id_lista_precio = '09'
  ORDER BY p.f126_fecha_activacion DESC
) AS L09
WHERE 1 = 1
/*__AUDITORIA_FILTER_BODEGAS__*/;

CREATE NONCLUSTERED INDEX IX_Existencias_Ref ON #Existencias(Referencia);

-------------------------------------------------------------------------------
-- 3A) TOTAL EXISTENCIA SIESA (t400_cm_existencia.f400_cant_existencia_1) POR REF.
--     Existencia_Total en la salida usa este agregado, no la suma de existencias
--     por nivel de costo (min / intermedio / max).
-------------------------------------------------------------------------------
SELECT
    Referencia,
    CAST(SUM(Existencia) AS DECIMAL(18, 4)) AS Existencia_Total_Siesa
INTO #ExistenciaTotalRef
FROM #Existencias
GROUP BY Referencia;

CREATE NONCLUSTERED INDEX IX_ExistenciaTotalRef ON #ExistenciaTotalRef(Referencia);

-------------------------------------------------------------------------------
-- 3B) PRE-CALCULAR BODEGAS POR (REFERENCIA, COSTO) SOLO CON EXISTENCIA > 0
-------------------------------------------------------------------------------
SELECT
    e.Referencia,
    e.Costo_Prom_Inst,
    STUFF((
        SELECT ', ' + x.Bodega
        FROM (
            SELECT DISTINCT LTRIM(RTRIM(e2.Bodega)) AS Bodega
            FROM #Existencias e2
            WHERE e2.Referencia      = e.Referencia
              AND e2.Costo_Prom_Inst = e.Costo_Prom_Inst
              AND e2.Existencia      > 0
        ) x
        FOR XML PATH(''), TYPE
    ).value('.', 'NVARCHAR(MAX)'), 1, 2, '') AS Bodegas_Nombre
INTO #BodegasPorCosto
FROM #Existencias e
WHERE e.Existencia > 0
GROUP BY e.Referencia, e.Costo_Prom_Inst;

CREATE CLUSTERED INDEX IX_BodegasPorCosto ON #BodegasPorCosto(Referencia, Costo_Prom_Inst);

-------------------------------------------------------------------------------
-- 4) COSTOS MIN / INTERMEDIO / MAX
-------------------------------------------------------------------------------
;WITH Universo AS (
    SELECT 
        e.*,
        MAX(CASE WHEN e.Existencia > 0 THEN 1 ELSE 0 END) 
            OVER (PARTITION BY e.Referencia) AS HayStock
    FROM #Existencias e
),
BaseCosto AS (
    SELECT *
    FROM Universo
    WHERE (HayStock = 1 AND Existencia > 0)
       OR (HayStock = 0)
),
CostosMarcados AS (
    SELECT 
        bc.*,
        CASE WHEN bc.Costo_Prom_Inst > 0 THEN 1 ELSE 0 END AS EsCostoValido
    FROM BaseCosto bc
),
Stats AS (
    SELECT 
        Referencia,
        COUNT(DISTINCT CASE WHEN EsCostoValido = 1 THEN Costo_Prom_Inst END) AS NumCostosValidos,
        MIN(CASE WHEN EsCostoValido = 1 THEN Costo_Prom_Inst END) AS Costo_Min_Valido,
        MAX(CASE WHEN EsCostoValido = 1 THEN Costo_Prom_Inst END) AS Costo_Max_Valido
    FROM CostosMarcados
    GROUP BY Referencia
),
Extremos AS (
    SELECT 
        c.Referencia,
        MAX(c.Descripcion) AS Descripcion,
        CASE 
            WHEN s.NumCostosValidos = 0 THEN 'MIN'
            WHEN c.Costo_Prom_Inst = s.Costo_Min_Valido THEN 'MIN'
            WHEN c.Costo_Prom_Inst = s.Costo_Max_Valido THEN 'MAX'
        END AS Tipo_Registro,

        CAST(
            CASE 
                WHEN s.NumCostosValidos = 0 THEN 0
                ELSE c.Costo_Prom_Inst
            END AS DECIMAL(18,2)
        ) AS Costo_Prom_Inst,

        SUM(CASE WHEN c.Existencia > 0 THEN c.Existencia  ELSE 0 END) AS Existencia,
        SUM(CASE WHEN c.Existencia > 0 THEN c.Disponible ELSE 0 END) AS Disponible,
        COUNT(DISTINCT CASE WHEN c.Existencia > 0 THEN c.Bodega END)  AS Nro_Bodegas,

        MAX(c.Precio_Lista_09)   AS Precio_Lista_09,
        MAX(s.NumCostosValidos)  AS NumCostosValidos,
        MAX(CASE WHEN c.Existencia > 0 THEN bpc.Bodegas_Nombre END) AS Bodegas_Nombre
    FROM CostosMarcados c
    JOIN Stats s ON c.Referencia = s.Referencia
    LEFT JOIN #BodegasPorCosto bpc
           ON bpc.Referencia      = c.Referencia
          AND bpc.Costo_Prom_Inst = c.Costo_Prom_Inst
    WHERE 
        (s.NumCostosValidos = 0)
        OR
        (c.EsCostoValido = 1 AND c.Costo_Prom_Inst IN (s.Costo_Min_Valido, s.Costo_Max_Valido))
    GROUP BY 
        c.Referencia,
        c.Costo_Prom_Inst,
        s.NumCostosValidos,
        s.Costo_Min_Valido,
        s.Costo_Max_Valido
),
Intermedio AS (
    SELECT 
        c.Referencia,
        MAX(c.Descripcion) AS Descripcion,
        'INTERMEDIO' AS Tipo_Registro,

        CAST(
            SUM(CASE WHEN c.Existencia > 0 
                     THEN c.Costo_Prom_Inst * c.Existencia 
                END) / NULLIF(SUM(CASE WHEN c.Existencia > 0 THEN c.Existencia END),0)
        AS DECIMAL(18,2)) AS Costo_Prom_Inst,

        SUM(CASE WHEN c.Existencia > 0 THEN c.Existencia  ELSE 0 END) AS Existencia,
        SUM(CASE WHEN c.Existencia > 0 THEN c.Disponible ELSE 0 END) AS Disponible,
        COUNT(DISTINCT CASE WHEN c.Existencia > 0 THEN c.Bodega END)  AS Nro_Bodegas,
        MAX(c.Precio_Lista_09)   AS Precio_Lista_09,
        MAX(s.NumCostosValidos)  AS NumCostosValidos,
        MAX(CASE WHEN c.Existencia > 0 THEN bpc.Bodegas_Nombre END) AS Bodegas_Nombre
    FROM CostosMarcados c
    JOIN Stats s ON c.Referencia = s.Referencia
    LEFT JOIN #BodegasPorCosto bpc
           ON bpc.Referencia      = c.Referencia
          AND bpc.Costo_Prom_Inst = c.Costo_Prom_Inst
    WHERE s.NumCostosValidos >= 3
      AND c.EsCostoValido = 1
      AND c.Costo_Prom_Inst NOT IN (s.Costo_Min_Valido, s.Costo_Max_Valido)
    GROUP BY c.Referencia
),
ResumenCostos AS (
    SELECT * FROM Extremos
    UNION ALL
    SELECT * FROM Intermedio
),
UM_Por_Ref AS (
    SELECT Referencia, MAX([U.M.]) AS [U.M.]
    FROM #Existencias
    GROUP BY Referencia
),
BasePivot AS (
    SELECT 
        b.Referencia_Principal AS Referencia,
        MAX(r.Descripcion)     AS Descripcion,
        b.Referencias_Alternas,
        MAX(um.[U.M.])         AS [U.M.],

        MAX(CASE WHEN r.Tipo_Registro = 'MIN'        THEN r.Costo_Prom_Inst END) AS Costo_Min,
        MAX(CASE WHEN r.Tipo_Registro = 'INTERMEDIO' THEN r.Costo_Prom_Inst END) AS Costo_Intermedio,
        COALESCE(
            MAX(CASE WHEN r.Tipo_Registro = 'MAX' THEN r.Costo_Prom_Inst END),
            MAX(CASE WHEN r.Tipo_Registro = 'MIN' THEN r.Costo_Prom_Inst END)
        ) AS Costo_Max,

        MAX(CASE WHEN r.Tipo_Registro = 'MIN' THEN r.Bodegas_Nombre END) AS Bodega_CostoMin,
        MAX(CASE WHEN r.Tipo_Registro = 'MAX' THEN r.Bodegas_Nombre END) AS Bodega_CostoMax,

        SUM(CASE WHEN r.Tipo_Registro = 'MIN'        THEN r.Existencia END) AS Existencia_Min,
        CASE WHEN MAX(CASE WHEN r.Tipo_Registro = 'INTERMEDIO' THEN 1 END) = 1
             THEN SUM(CASE WHEN r.Tipo_Registro = 'INTERMEDIO' THEN r.Existencia END)
        END AS Existencia_Intermedio,
        COALESCE(
            SUM(CASE WHEN r.Tipo_Registro = 'MAX' THEN r.Existencia END),
            SUM(CASE WHEN r.Tipo_Registro = 'MIN' THEN r.Existencia END)
        ) AS Existencia_Max,

        MAX(et.Existencia_Total_Siesa) AS Existencia_Total_Siesa,

        SUM(CASE WHEN r.Tipo_Registro = 'MIN'        THEN r.Disponible END) AS Disponible_Min,
        CASE WHEN MAX(CASE WHEN r.Tipo_Registro = 'INTERMEDIO' THEN 1 END) = 1
             THEN SUM(CASE WHEN r.Tipo_Registro = 'INTERMEDIO' THEN r.Disponible END)
        END AS Disponible_Intermedio,
        COALESCE(
            SUM(CASE WHEN r.Tipo_Registro = 'MAX' THEN r.Disponible END),
            SUM(CASE WHEN r.Tipo_Registro = 'MIN' THEN r.Disponible END)
        ) AS Disponible_Max,

        MAX(CASE WHEN r.Tipo_Registro = 'MIN' THEN r.Nro_Bodegas END) AS NroBod_Min,
        CASE WHEN MAX(CASE WHEN r.Tipo_Registro = 'INTERMEDIO' THEN 1 END) = 1
             THEN MAX(CASE WHEN r.Tipo_Registro = 'INTERMEDIO' THEN r.Nro_Bodegas END)
        END AS NroBod_Intermedio,
        COALESCE(
            MAX(CASE WHEN r.Tipo_Registro = 'MAX' THEN r.Nro_Bodegas END),
            MAX(CASE WHEN r.Tipo_Registro = 'MIN' THEN r.Nro_Bodegas END)
        ) AS NroBod_Max,

        MAX(r.Precio_Lista_09) AS Precio_Lista_09,
        MAX(r.NumCostosValidos) AS NumCostosValidos
    FROM ResumenCostos r
    JOIN #BaseRefs b        ON r.Referencia = b.Referencia_Principal
    LEFT JOIN UM_Por_Ref um ON um.Referencia = b.Referencia_Principal
    LEFT JOIN #ExistenciaTotalRef et ON et.Referencia = b.Referencia_Principal
    GROUP BY b.Referencia_Principal, b.Referencias_Alternas
),

-------------------------------------------------------------------------------
-- 6) COMPRAS (ÚLTIMA Y PENÚLTIMA)
-- REGLA:
--   POR CADA DOCUMENTO DE IMPORTACIÓN:
--   - TRM = MAX(f451_tasa_local)
--   - FECHA_COMPRA = MAX(f350_fecha)
-------------------------------------------------------------------------------
ComprasLimpias AS (
    SELECT
        dcpr.f451_rowid_docto_import,
        i.f120_referencia,
        RTRIM(i.f120_descripcion) AS Descripcion_Item,
        CAST(imp.f41850_fecha AS DATE) AS Fecha_Importacion,
        CAST(dc.f350_fecha AS DATE)    AS Fecha_Contable,
        CASE 
            WHEN p.f011_id IN ('076','105') THEN 'Brazil'
            WHEN p.f011_id IN ('249','840') THEN 'USA'
            ELSE 'Otros'
        END AS Pais_Origen,
        prov.f200_razon_social          AS Proveedor,
        oc.f420_usuario_aprobacion      AS Comprador,
        CAST(movoc.f421_precio_unitario AS DECIMAL(18,4)) AS Precio_Unitario_USD,
        CAST(dcpr.f451_tasa_local AS DECIMAL(18,6)) AS TRM_Registro
    FROM t451_cm_docto_compras dcpr
    JOIN t41850_import_docto imp
         ON dcpr.f451_rowid_docto_import = imp.f41850_rowid
    JOIN t350_co_docto_contable dc
         ON dcpr.f451_rowid_docto = dc.f350_rowid
    JOIN t470_cm_movto_invent mov
         ON dc.f350_rowid = mov.f470_rowid_docto
    JOIN t121_mc_items_extensiones e
         ON mov.f470_rowid_item_ext = e.f121_rowid
    JOIN t120_mc_items i
         ON e.f121_rowid_item = i.f120_rowid
    JOIN #RefsFiltradas rf
         ON i.f120_referencia = rf.f120_referencia
    JOIN t41806_import_origen_destino od
         ON imp.f41850_rowid_origen = od.f41806_rowid
    JOIN t011_mm_paises p
         ON od.f41806_id_pais = p.f011_id
    JOIN t41851_import_movto movimp
         ON movimp.f41851_rowid_docto_import = imp.f41850_rowid
        AND movimp.f41851_rowid_item_ext = e.f121_rowid
    JOIN t421_cm_oc_movto movoc
         ON movimp.f41851_rowid_oc_movto = movoc.f421_rowid
    JOIN t420_cm_oc_docto oc
         ON movoc.f421_rowid_oc_docto = oc.f420_rowid
    JOIN t200_mm_terceros prov
         ON oc.f420_rowid_tercero_prov = prov.f200_rowid
    WHERE imp.f41850_ind_estado = 4
      AND imp.f41850_id_tipo_docto = 'IM'
      AND dcpr.f451_ind_forma_conv = 1
),
ComprasPorImport AS (
    SELECT
        f451_rowid_docto_import,
        f120_referencia,
        MAX(Descripcion_Item)   AS Descripcion_Item,
        MAX(Fecha_Importacion)  AS Fecha_Importacion,
        MAX(Fecha_Contable)     AS Fecha_Compra,
        MAX(Pais_Origen)        AS Pais_Origen,
        MAX(Proveedor)          AS Proveedor,
        MAX(Comprador)          AS Comprador,
        MAX(Precio_Unitario_USD) AS Precio_Unitario_USD,
        MAX(TRM_Registro)        AS TRM_Original,
        CAST(MAX(Precio_Unitario_USD) * MAX(TRM_Registro) AS DECIMAL(18,2)) AS Precio_Unitario_COP
    FROM ComprasLimpias
    GROUP BY
        f451_rowid_docto_import,
        f120_referencia
),
ComprasRankeadas AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY f120_referencia
            ORDER BY Fecha_Compra DESC, f451_rowid_docto_import DESC
        ) AS rn
    FROM ComprasPorImport
),
Ultima AS (
    SELECT * FROM ComprasRankeadas WHERE rn = 1
),
Penultima AS (
    SELECT * FROM ComprasRankeadas WHERE rn = 2
),

-------------------------------------------------------------------------------
-- 7) #FactPricing (variaciones en COP, %, ABSVar_Costo y ABSVar_Costo_Pct)
-------------------------------------------------------------------------------
FactBase AS (
    SELECT 
        bp.Referencia,
        bp.[U.M.],
        bp.Descripcion,
        bp.Referencias_Alternas,
        bp.Costo_Min,
        bp.Bodega_CostoMin,
        bp.Costo_Intermedio,
        bp.Costo_Max,
        bp.Bodega_CostoMax,
        bp.Existencia_Min, bp.Existencia_Intermedio, bp.Existencia_Max,
        bp.Existencia_Total_Siesa,
        bp.Disponible_Min, bp.Disponible_Intermedio, bp.Disponible_Max,
        bp.NroBod_Min, bp.NroBod_Intermedio, bp.NroBod_Max,
        bp.Precio_Lista_09,
        bp.NumCostosValidos,
        CAST(CASE WHEN bp.NumCostosValidos = 1 THEN 1 ELSE 0 END AS BIT) AS EsCostoUnico,
        u1.Fecha_Compra        AS Fecha_Ultima_Compra,
        u1.Pais_Origen         AS Pais_Ultima,
        u1.Proveedor           AS Proveedor_Ultima,
        u1.Comprador           AS Comprador_Ultima,
        u1.Precio_Unitario_USD AS Precio_USD_Ultima,
        u1.Precio_Unitario_COP AS Precio_COP_Ultima,
        u1.TRM_Original        AS TRM_Ultima,
        u2.Fecha_Compra        AS Fecha_Penultima_Compra,
        u2.Pais_Origen         AS Pais_Penultima,
        u2.Proveedor           AS Proveedor_Penultima,
        u2.Comprador           AS Comprador_Penultima,
        u2.Precio_Unitario_USD AS Precio_USD_Penultima,
        u2.Precio_Unitario_COP AS Precio_COP_Penultima,
        u2.TRM_Original        AS TRM_Penultima
    FROM BasePivot bp
    LEFT JOIN Ultima    u1 ON bp.Referencia = u1.f120_referencia
    LEFT JOIN Penultima u2 ON bp.Referencia = u2.f120_referencia
),
-- Precio COP × factor (solo para Var_* / Dif_* / ABSVar; no se exponen columnas extra al resultado final)
FactParaVar AS (
    SELECT
        fb.*,
        CAST(
            CASE
                WHEN fb.Precio_COP_Ultima IS NULL THEN NULL
                ELSE fb.Precio_COP_Ultima * CASE fb.Pais_Ultima
                    WHEN 'Brazil' THEN @FactorBR
                    WHEN 'USA' THEN @FactorUSA
                    WHEN 'Otros' THEN @FactorEUR
                    ELSE @FactorEUR
                END
            END AS DECIMAL(18,2)
        ) AS _PxUltVar,
        CAST(
            CASE
                WHEN fb.Precio_COP_Penultima IS NULL THEN NULL
                ELSE fb.Precio_COP_Penultima * CASE fb.Pais_Penultima
                    WHEN 'Brazil' THEN @FactorBR
                    WHEN 'USA' THEN @FactorUSA
                    WHEN 'Otros' THEN @FactorEUR
                    ELSE @FactorEUR
                END
            END AS DECIMAL(18,2)
        ) AS _PxPenVar
    FROM FactBase fb
),
VariacionesBase AS (
    SELECT
        f.Referencia,
        f.[U.M.],
        f.Descripcion,
        f.Referencias_Alternas,
        f.Costo_Min,
        f.Bodega_CostoMin,
        f.Costo_Intermedio,
        f.Costo_Max,
        f.Bodega_CostoMax,
        f.Existencia_Min,
        f.Existencia_Intermedio,
        f.Existencia_Max,
        f.Existencia_Total_Siesa,
        f.Disponible_Min,
        f.Disponible_Intermedio,
        f.Disponible_Max,
        f.NroBod_Min,
        f.NroBod_Intermedio,
        f.NroBod_Max,
        f.Precio_Lista_09,
        f.NumCostosValidos,
        f.EsCostoUnico,
        f.Fecha_Ultima_Compra,
        f.Pais_Ultima,
        f.Proveedor_Ultima,
        f.Comprador_Ultima,
        f.Precio_USD_Ultima,
        f.Precio_COP_Ultima,
        f.TRM_Ultima,
        f.Fecha_Penultima_Compra,
        f.Pais_Penultima,
        f.Proveedor_Penultima,
        f.Comprador_Penultima,
        f.Precio_USD_Penultima,
        f.Precio_COP_Penultima,
        f.TRM_Penultima,
        CAST(CASE WHEN f.Costo_Min > 0 AND f.Precio_Lista_09 > 0
                  THEN ((f.Precio_Lista_09 - f.Costo_Min) / f.Precio_Lista_09) * 100
             END AS DECIMAL(18,2)) AS Margen_Min_Pct,
        CAST(CASE WHEN f.Costo_Intermedio > 0 AND f.Precio_Lista_09 > 0
                  THEN ((f.Precio_Lista_09 - f.Costo_Intermedio) / f.Precio_Lista_09) * 100
             END AS DECIMAL(18,2)) AS Margen_Intermedio_Pct,
        CAST(CASE WHEN f.Costo_Max > 0 AND f.Precio_Lista_09 > 0
                  THEN ((f.Precio_Lista_09 - f.Costo_Max) / f.Precio_Lista_09) * 100
             END AS DECIMAL(18,2)) AS Margen_Max_Pct,
        DATEDIFF(DAY, f.Fecha_Penultima_Compra, f.Fecha_Ultima_Compra) AS Dias_Entre_Compras,
        CAST(CASE WHEN f.Precio_USD_Penultima IS NULL OR f.Precio_USD_Penultima = 0 THEN NULL
                  ELSE (f.Precio_USD_Ultima - f.Precio_USD_Penultima) / f.Precio_USD_Penultima
             END AS DECIMAL(18,4)) AS Var_PrecioUSD,
        CAST(CASE WHEN f._PxPenVar IS NULL OR f._PxPenVar = 0 THEN NULL
                  WHEN f._PxUltVar IS NULL THEN NULL
                  ELSE (f._PxUltVar - f._PxPenVar) / f._PxPenVar
             END AS DECIMAL(18,4)) AS Var_PrecioCOP,
        CAST(CASE WHEN f.TRM_Penultima IS NULL OR f.TRM_Penultima = 0 THEN NULL
                  ELSE (f.TRM_Ultima - f.TRM_Penultima) / f.TRM_Penultima
             END AS DECIMAL(18,4)) AS Var_TRM,
        CAST(CASE WHEN f._PxUltVar IS NULL OR f.Costo_Min IS NULL OR f.Costo_Min = 0 THEN NULL
                  ELSE (f._PxUltVar - f.Costo_Min) / f.Costo_Min
             END AS DECIMAL(18,4)) AS VarMinPct,
        CAST(CASE WHEN f._PxUltVar IS NULL OR f.Costo_Max IS NULL OR f.Costo_Max = 0 THEN NULL
                  ELSE (f._PxUltVar - f.Costo_Max) / f.Costo_Max
             END AS DECIMAL(18,4)) AS VarMaxPct,
        CAST(CASE WHEN f._PxUltVar IS NULL OR f.Costo_Min IS NULL THEN NULL
                  ELSE f.Costo_Min - f._PxUltVar
             END AS DECIMAL(18,2)) AS Dif_CostoMin_COP,
        CAST(CASE WHEN f._PxUltVar IS NULL OR f.Costo_Max IS NULL THEN NULL
                  ELSE f.Costo_Max - f._PxUltVar
             END AS DECIMAL(18,2)) AS Dif_CostoMax_COP
    FROM FactParaVar f
),
VariacionesExtremas AS (
    SELECT
        vb.*,
        CAST(ABS(ISNULL(vb.Dif_CostoMin_COP, 0)) AS DECIMAL(18,2)) AS AbsDifMin,
        CAST(ABS(ISNULL(vb.Dif_CostoMax_COP, 0)) AS DECIMAL(18,2)) AS AbsDifMax,
        CAST(ABS(ISNULL(vb.VarMinPct, 0)) AS DECIMAL(18,4)) AS AbsVarMinPct,
        CAST(ABS(ISNULL(vb.VarMaxPct, 0)) AS DECIMAL(18,4)) AS AbsVarMaxPct
    FROM VariacionesBase vb
),
FactPricingFinal AS (
    SELECT
        ve.Referencia,
        ve.[U.M.],
        ve.Descripcion,
        ve.Referencias_Alternas,
        ve.Costo_Min,
        ve.Bodega_CostoMin,
        ve.Costo_Intermedio,
        ve.Costo_Max,
        ve.Bodega_CostoMax,
        ve.Existencia_Min, ve.Existencia_Intermedio, ve.Existencia_Max,
        ve.Existencia_Total_Siesa,
        ve.Disponible_Min, ve.Disponible_Intermedio, ve.Disponible_Max,
        ve.NroBod_Min, ve.NroBod_Intermedio, ve.NroBod_Max,
        ve.Precio_Lista_09,
        ve.NumCostosValidos,
        ve.EsCostoUnico,
        ve.Margen_Min_Pct,
        ve.Margen_Intermedio_Pct,
        ve.Margen_Max_Pct,
        ve.Fecha_Ultima_Compra,
        ve.Pais_Ultima,
        ve.Proveedor_Ultima,
        ve.Comprador_Ultima,
        ve.Precio_USD_Ultima,
        ve.Precio_COP_Ultima,
        ve.TRM_Ultima,
        ve.Fecha_Penultima_Compra,
        ve.Pais_Penultima,
        ve.Proveedor_Penultima,
        ve.Comprador_Penultima,
        ve.Precio_USD_Penultima,
        ve.Precio_COP_Penultima,
        ve.TRM_Penultima,
        ve.Dias_Entre_Compras,
        ve.Var_PrecioUSD,
        ve.Var_PrecioCOP,
        ve.Var_TRM,
        ve.VarMinPct AS Var_CostoMin_PrecioCOP,
        ve.VarMaxPct AS Var_CostoMax_PrecioCOP,
        ve.Dif_CostoMin_COP,
        ve.Dif_CostoMax_COP,
        CAST(CASE 
            WHEN ve.Precio_COP_Ultima IS NULL THEN NULL
            WHEN ve.AbsDifMin >= ve.AbsDifMax THEN ve.AbsDifMin
            ELSE ve.AbsDifMax
        END AS DECIMAL(18,2)) AS ABSVar_Costo,
        CAST(CASE
            WHEN ve.Precio_COP_Ultima IS NULL THEN NULL
            WHEN ve.AbsVarMinPct >= ve.AbsVarMaxPct THEN ve.AbsVarMinPct
            ELSE ve.AbsVarMaxPct
        END AS DECIMAL(18,4)) AS ABSVar_Costo_Pct
    FROM VariacionesExtremas ve
)
SELECT *
INTO #FactPricing
FROM FactPricingFinal;

CREATE CLUSTERED INDEX IX_FactPricing_Referencia ON #FactPricing(Referencia);

-------------------------------------------------------------------------------
-- 8) Estadísticos de variación absoluta en COP (cuartiles robustos)
-------------------------------------------------------------------------------
;WITH VarBase AS (
    SELECT 
        ABSVar_Costo AS DifAbsExtrema
    FROM #FactPricing
    WHERE ABSVar_Costo IS NOT NULL
),
VarPercentiles AS (
    SELECT
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY DifAbsExtrema) OVER () AS Q1,
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY DifAbsExtrema) OVER () AS Q2,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY DifAbsExtrema) OVER () AS Q3
    FROM VarBase
)
SELECT
    MIN(Q1) AS Q1,
    MIN(Q2) AS Q2,
    MIN(Q3) AS Q3,
    MIN(Q3) - MIN(Q1) AS IQR
INTO #VarStats
FROM VarPercentiles;

-------------------------------------------------------------------------------
-- 9) DIM + SEMÁFORO + SALIDA FINAL
-------------------------------------------------------------------------------
;WITH CriteriosRaw AS (
    SELECT
        i.f120_referencia AS Referencia,
        cri.f125_id_plan AS IdPlan,
        RTRIM(may.f106_id) AS Cod_Criterio,
        RTRIM(may.f106_id) + ' - ' + may.f106_descripcion AS Valor_Criterio
    FROM t120_mc_items i
    JOIN #FactPricing f ON f.Referencia = i.f120_referencia
    JOIN t125_mc_items_criterios cri
        ON cri.f125_rowid_item = i.f120_rowid
    JOIN t106_mc_criterios_item_mayores may
        ON may.f106_id_plan = cri.f125_id_plan
       AND may.f106_id = cri.f125_id_criterio_mayor
    WHERE i.f120_id_cia = 1
      AND i.f120_ind_tipo_item IN (1,3)
      AND cri.f125_id_plan IN ('01','03','04','07','08','12')
),
CriteriosPivot AS (
    SELECT
        Referencia,
        MAX(CASE WHEN IdPlan = '07' THEN Cod_Criterio END)   AS Cod_Linea,
        MAX(CASE WHEN IdPlan = '07' THEN Valor_Criterio END) AS Linea_Item,
        MAX(CASE WHEN IdPlan = '04' THEN Valor_Criterio END) AS Sistema_CNH,
        MAX(CASE WHEN IdPlan = '01' THEN Valor_Criterio END) AS Equipo_CNH,
        MAX(CASE WHEN IdPlan = '03' THEN Valor_Criterio END) AS Modelo_CNH,
        MAX(CASE WHEN IdPlan = '08' THEN Valor_Criterio END) AS Clasificacion_Rotacion,
        MAX(CASE WHEN IdPlan = '12' THEN Cod_Criterio END)   AS Cod_Sistema_Precio,
        MAX(CASE WHEN IdPlan = '12' THEN Valor_Criterio END) AS Sistema_Precio_Item
    FROM CriteriosRaw
    GROUP BY Referencia
),
MargenObjetivo AS (
    SELECT
        t753_a.f753_dato_texto AS Cod_Sistema_Precio,
        MAX(t753_b.f753_dato_numero / 100.0) AS Margen_Objetivo_Sistema
    FROM t750_mm_movto_entidad me
    JOIN t285_co_centro_op co
        ON me.f750_rowid = co.f285_rowid_movto_entidad
    JOIN t752_mm_movto_entidad_fila fila
        ON me.f750_rowid = fila.f752_rowid_movto_entidad
    JOIN t753_mm_movto_entidad_columna t753_a
        ON fila.f752_rowid = t753_a.f753_rowid_movto_entidad_fila
    JOIN t753_mm_movto_entidad_columna t753_b
        ON fila.f752_rowid = t753_b.f753_rowid_movto_entidad_fila
    WHERE co.f285_id = '001'
      AND t753_a.f753_rowid_entidad_atributo = 1154
      AND t753_b.f753_rowid_entidad_atributo = 1156
    GROUP BY t753_a.f753_dato_texto
),
Dim_Item AS (
    SELECT
        p.Referencia,
        p.Cod_Linea,
        p.Linea_Item,
        p.Sistema_CNH,
        p.Equipo_CNH,
        p.Modelo_CNH,
        p.Clasificacion_Rotacion,
        p.Cod_Sistema_Precio,
        p.Sistema_Precio_Item,
        mo.Margen_Objetivo_Sistema
    FROM CriteriosPivot p
    LEFT JOIN MargenObjetivo mo
        ON mo.Cod_Sistema_Precio = p.Cod_Sistema_Precio
),
FinalConSemaforo AS (
    SELECT
        f.*,
        d.Cod_Linea,
        d.Linea_Item,
        d.Sistema_CNH,
        d.Equipo_CNH,
        d.Modelo_CNH,
        d.Clasificacion_Rotacion,
        d.Cod_Sistema_Precio,
        d.Sistema_Precio_Item,
        d.Margen_Objetivo_Sistema,
        vs.Q1,
        vs.Q2,
        vs.Q3,
        vs.IQR,
        CASE 
            WHEN f.ABSVar_Costo IS NULL
                 THEN 'SIN DATOS'
            ELSE
            CASE 
                WHEN f.ABSVar_Costo >= vs.Q3 + 3 * vs.IQR
                 AND f.ABSVar_Costo_Pct >= @UmbralPctCritico
                    THEN 'CRÍTICO'

                WHEN f.ABSVar_Costo >= vs.Q3
                 AND f.ABSVar_Costo_Pct >= @UmbralPctModAlto
                    THEN 'MODERADO ALTO'

                WHEN f.ABSVar_Costo >= vs.Q2
                 AND f.ABSVar_Costo_Pct >= @UmbralPctModBajo
                    THEN 'MODERADO BAJO'

                ELSE 'NO CRÍTICO'
            END
        END AS Semaforo_Variacion
    FROM #FactPricing f
    LEFT JOIN Dim_Item d ON d.Referencia = f.Referencia
    CROSS JOIN #VarStats vs
    WHERE
        (
            @ReferenciaAlternaFiltro IS NULL
            OR EXISTS (
                SELECT 1
                FROM t124_mc_items_referencias t124
                JOIN t120_mc_items t120 ON t124.f124_rowid_item = t120.f120_rowid
                WHERE t120.f120_referencia = f.Referencia
                  AND t124.f124_referencia = @ReferenciaAlternaFiltro
            )
            OR f.Referencia = @ReferenciaAlternaFiltro
        )
)

SELECT
    Referencia,
    [U.M.],
    Descripcion,
    Referencias_Alternas,

    Costo_Min,
    Bodega_CostoMin,
    Costo_Intermedio,
    Costo_Max,
    Bodega_CostoMax,

    Existencia_Min,    Existencia_Intermedio,    Existencia_Max,
    COALESCE(Existencia_Total_Siesa, 0) AS Existencia_Total,
    Disponible_Min,    Disponible_Intermedio,    Disponible_Max,
    NroBod_Min,        NroBod_Intermedio,        NroBod_Max,

    Precio_Lista_09,
    Margen_Min_Pct,
    Margen_Intermedio_Pct,
    Margen_Max_Pct,

    Fecha_Ultima_Compra,
    Pais_Ultima,
    Proveedor_Ultima,
    Comprador_Ultima,
    Precio_USD_Ultima,
    Precio_COP_Ultima,
    TRM_Ultima,

    Fecha_Penultima_Compra,
    Pais_Penultima,
    Proveedor_Penultima,
    Comprador_Penultima,
    Precio_USD_Penultima,
    Precio_COP_Penultima,
    TRM_Penultima,

    Dias_Entre_Compras,
    Var_PrecioUSD,
    Var_PrecioCOP,
    Var_TRM,

    Var_CostoMin_PrecioCOP,
    Var_CostoMax_PrecioCOP,
    ABSVar_Costo,
    ABSVar_Costo_Pct,
    NumCostosValidos,
    EsCostoUnico,
    Semaforo_Variacion,

    Cod_Linea,
    Linea_Item,
    Sistema_CNH,
    Equipo_CNH,
    Modelo_CNH,
    Clasificacion_Rotacion,
    Cod_Sistema_Precio,
    Sistema_Precio_Item,
    Margen_Objetivo_Sistema

FROM FinalConSemaforo
ORDER BY
    CASE Semaforo_Variacion
        WHEN 'CRÍTICO'        THEN 1
        WHEN 'MODERADO ALTO'  THEN 2
        WHEN 'MODERADO BAJO'  THEN 3
        WHEN 'NO CRÍTICO'     THEN 4
        ELSE 5
    END,
    Referencia;
"""

SQL_AUDITORIA = _inyectar_factores_en_sql_auditoria(SQL_AUDITORIA_TEMPLATE)

SQL_REPORTS = [
    ("consulta_interna_precio_margen_siesa", "margen_siesa_raw", SQL_PRECIO_MARGEN_SIESA),
    ("consulta_interna_atributos_referencias", "atributos_referencias_raw", SQL_ATRIBUTOS_REFERENCIAS),
    ("consulta_interna_auditoria", "auditoria_raw", SQL_AUDITORIA),
]


# ===========================================================================
# UTILIDADES
# ===========================================================================
def _columna_referencia_texto_fijo(nombre: str) -> bool:
    """No promover a numérico: Referencia y listas de alternas (códigos alfanuméricos)."""
    c = str(nombre).strip().lower().replace(" ", "_")
    if c in ("referencia", "referencia_alternas"):
        return True
    if "referencia_altern" in c or "referencias_altern" in c:
        return True
    return False


def _es_columna_de_referencia(nombre: str) -> bool:
    c = str(nombre).strip().lower().replace(" ", "_")
    return "referencia" in c or "ref_" in c


def dataframe_para_duckdb(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fuerza float64 en columnas numéricas para que DuckDB no infiera DECIMAL estrecho
    en el primer chunk (falla con valores como -277443375.00 fuera de DECIMAL(10,2)).

    Las columnas object que son casi todas números (montos, márgenes) también pasan a
    float64. Referencia / alternas se dejan en texto.
    """
    df = df.copy()
    for col in df.columns:
        if _es_columna_de_referencia(col):
            df[col] = normalize_reference_series_pd(df[col]).replace("", pd.NA)

    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            continue
        if _columna_referencia_texto_fijo(col):
            continue
        if pd.api.types.is_numeric_dtype(df[col]):
            df[col] = df[col].astype("float64")
        elif df[col].dtype == object:
            converted = pd.to_numeric(df[col], errors="coerce")
            nn = int(df[col].notna().sum())
            if nn > 0 and converted.notna().sum() / nn > 0.9:
                df[col] = converted.astype("float64")
    return df


def conectar_sqlserver() -> pyodbc.Connection:
    return pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={SERVER};"
        f"DATABASE={DATABASE};"
        f"UID={DB_USER};"
        f"PWD={DB_PASS};"
        f"Encrypt=yes;"
        f"TrustServerCertificate=yes;"
    )


def optimizar_duckdb_post_carga() -> None:
    with duckdb.connect(str(DUCKDB_PATH)) as con:
        con.execute("PRAGMA threads = 4")
        con.execute("CREATE INDEX IF NOT EXISTS idx_margen_siesa_ref ON margen_siesa_raw(Referencia)")
        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_atributos_ref ON atributos_referencias_raw(Referencia)"
        )
        con.execute("CREATE INDEX IF NOT EXISTS idx_auditoria_ref ON auditoria_raw(Referencia)")
        con.execute("ANALYZE margen_siesa_raw")
        con.execute("ANALYZE atributos_referencias_raw")
        con.execute("ANALYZE auditoria_raw")


def ejecutar_sql_a_duckdb(sql_text: str, table_name: str) -> int:
    total = 0

    conn_sql = conectar_sqlserver()
    with duckdb.connect(str(DUCKDB_PATH)) as con:
        try:
            cur = conn_sql.cursor()
            # `GO` es un separador de lotes de SSMS, no una sentencia SQL válida
            # para pyodbc. Se elimina para ejecutar el mismo script desde Python.
            sql_ejecutable = re.sub(
                r"(?im)^[ \t]*GO[ \t]*(?:--.*)?$",
                "",
                sql_text,
            )
            cur.execute(sql_ejecutable)

            # Si el script tiene varias sentencias, avanzar al primer resultset tabular.
            while cur.description is None:
                if not cur.nextset():
                    raise ValueError(
                        f"El SQL para {table_name} no devolvio un resultset final."
                    )

            cols = [c[0].strip() if c[0] else f"col_{i}" for i, c in enumerate(cur.description)]
            primera = True
            chunks = 0

            while True:
                rows = cur.fetchmany(CHUNK)
                if not rows:
                    break

                df = dataframe_para_duckdb(
                    pd.DataFrame.from_records(rows, columns=cols)
                )
                con.register("tmp_chunk", df)

                if primera:
                    con.execute(
                        f"CREATE OR REPLACE TABLE {table_name} AS "
                        "SELECT * FROM tmp_chunk"
                    )
                    primera = False
                else:
                    con.execute(
                        f"INSERT INTO {table_name} "
                        "SELECT * FROM tmp_chunk"
                    )

                con.unregister("tmp_chunk")
                total += len(df)
                chunks += 1
                print(f"  {table_name} - chunk {chunks}: {total:,} filas acumuladas")

            if primera:
                col_defs = ", ".join([f'"{c}" VARCHAR' for c in cols])
                con.execute(f"CREATE OR REPLACE TABLE {table_name} ({col_defs})")
                print(f"  {table_name}: sin filas, tabla vacia creada")
            else:
                print(f"  {table_name}: {total:,} filas cargadas")

        finally:
            conn_sql.close()

    return total


# ===========================================================================
# PIPELINE 00
# ===========================================================================
def paso00_reportes_sql():
    print("Paso 00 - Ejecutando reportes SQL y cargando en DuckDB...")

    for sql_name, table_name, sql_text in SQL_REPORTS:
        print(f"\nEjecutando: {sql_name}")
        ejecutar_sql_a_duckdb(sql_text, table_name)

    print("\nAplicando optimizaciones en DuckDB (indices y estadisticas)...")
    optimizar_duckdb_post_carga()


# ===========================================================================
# MAIN
# ===========================================================================
def main():
    print("\n" + "=" * 60)
    print("PIPELINE 00 - REPORTES SQL BASE")
    print("=" * 60 + "\n")

    paso00_reportes_sql()

    print("\n" + "=" * 60)
    print("PROCESO TERMINADO")
    print(f"  DuckDB -> {DUCKDB_PATH.name}")
    print(f"  Ruta   -> {BASE_DIR.resolve()}")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
