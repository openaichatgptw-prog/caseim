# Referencias Compras Y1–Y3 (UnoEE)

Este repositorio contiene el notebook **`referencias_compras_Y1_Y3.ipynb`** que:
1) Normaliza una lista de **referencias principales y alternas** provenientes de `referencias.csv` contra UnoEE,  
2) Calcula compras de importación por **año actual (Y1)** y **últimos 3 años (Y3)**,  
3) Construye métricas de **unidades**, **valor USD** y **porcentajes por origen (USA, BR, OTROS)**,  
4) Exporta el resultado a `resultado_referencias_Y1_Y3.xlsx`.

> **Motivación:** En insumos reales, el archivo de entrada puede contener **referencias alternas**. La versión anterior del query (basada en `COALESCE(MIN/MAX)`) no devolvía resultados cuando la entrada era alterna. Aquí se corrige con la lógica **`DetectRef`** para que **siempre** se resuelva (Principal, Alterna) correctamente.

---

## 🧭 Índice
- [Arquitectura de la solución](#arquitectura-de-la-solución)
- [Archivos y estructura](#archivos-y-estructura)
- [Prerrequisitos](#prerrequisitos)
- [Variables de configuración](#variables-de-configuración)
- [Formato de entrada (`referencias.csv`)](#formato-de-entrada-referenciascsv)
- [Flujo de ejecución](#flujo-de-ejecución)
- [Consulta SQL completa (con `DetectRef`)](#consulta-sql-completa-con-detectref)
- [Diccionario de datos de salida](#diccionario-de-datos-de-salida)
- [Validaciones y pruebas rápidas](#validaciones-y-pruebas-rápidas)
- [Rendimiento y buenas prácticas](#rendimiento-y-buenas-prácticas)
- [Solución de problemas (FAQ)](#solución-de-problemas-faq)
- [Extensiones futuras](#extensiones-futuras)
- [Licencia](#licencia)

---

## Arquitectura de la solución

```mermaid
flowchart LR
  A[referencias.csv] --> B[Notebook: referencias_compras_Y1_Y3.ipynb]
  B --> C[build_query(): SQL con DetectRef]
  C --> D[(SQL Server UnoEE)]
  D --> E[DataFrame Pandas]
  E --> F[resultado_referencias_Y1_Y3.xlsx]
```

- **Entrada:** `referencias.csv` con una columna `Ref` (puede contener principales **o** alternas).
- **Normalización:** `DetectRef` identifica si la referencia de entrada es principal o alterna y asigna su par (Principal, Alterna).
- **Cálculo:** Compras por `Anho` y `Origen`, agregadas a Y1 y Y3, además de porcentajes.
- **Salida:** Archivo Excel con métricas y banderas (`TieneCompras_Y1`, `TieneCompras_Y3`).

---

## Archivos y estructura

```
.
├─ referencias_compras_Y1_Y3.ipynb     # Notebook principal
├─ referencias.csv                      # Entrada: referencias (principal o alterna)
├─ config.json                          # Credenciales DB: DB_USER, DB_PASS
└─ resultado_referencias_Y1_Y3.xlsx     # Salida: métricas Y1/Y3 (generado)
```

---

## Prerrequisitos

- **Python** 3.8 o superior.
- Paquetes:
  - `pandas`
  - `pyodbc`
  - `openpyxl` (para `.xlsx`)
- **SQL Server** accesible desde la red local con el **ODBC Driver 17** instalado.
- Permisos de lectura sobre las tablas UnoEE usadas.

Instalación sugerida:
```bash
pip install pandas pyodbc openpyxl
```

---

## Variables de configuración

Archivo `config.json` (no versionar en repos públicos):
```json
{
  "DB_USER": "usuario_sql",
  "DB_PASS": "password_fuerte"
}
```

Parámetros de conexión definidos en el notebook:
```python
server = "10.75.71.10,1433"
database = "UnoEE"
```

> **Seguridad:** No imprimas ni subas credenciales. Usa variables de entorno o `config.json` ignorado por Git.

---

## Formato de entrada `referencias.csv`

- Debe contener una columna **`Ref`**.
- Puede incluir **principales** y/o **alternas**; la lógica `DetectRef` resuelve ambas.

Ejemplo mínimo:
```csv
Ref
12345-A
ABC-001
XYZ-ALT
```
> Se eliminan nulos y duplicados antes de ejecutar.

---

## Flujo de ejecución

1. **Cargar CSV:** lee `referencias.csv` → lista única `refs_list`.
2. **Chunking:** divide la lista en bloques de **1000** para evitar `VALUES` gigantes en SQL.
3. **`build_query(subset)`:** arma el SQL con `InputRefs` y la lógica `DetectRef`.
4. **Ejecución en SQL:** corre el `WITH ... SELECT final` contra UnoEE vía `pyodbc`.
5. **Concatenación y exportación:** agrupa resultados de todos los chunks y exporta a `resultado_referencias_Y1_Y3.xlsx`.
6. **Verificación rápida:** imprime `head()` en consola.

---

## Consulta SQL completa (con `DetectRef`)

> Esta es la consulta exacta que construye `build_query(subset)` dentro del notebook. Copiar/pegar para ejecutar directo en SQL Server (reemplazando `InputRefs` por una tabla temporal si se desea).

```sql
SET NOCOUNT ON;
DECLARE @YearNow INT = YEAR(GETDATE());

-- InputRefs con lista de referencias (generado por Python con VALUES (...))
WITH
InputRefs AS (
    SELECT Ref FROM (VALUES -- ( 'ref1' ), ( 'ref2' ), ... ) 
    v(Ref)
),
RefBase AS (
    -- Alternas distintas a la principal
    SELECT DISTINCT
        t124.f124_referencia AS Referencia_Alterna,
        t120.f120_referencia AS Referencia_Principal
    FROM t124_mc_items_referencias t124
    JOIN t120_mc_items t120 ON t124.f124_rowid_item = t120.f120_rowid
    WHERE t124.f124_referencia <> t120.f120_referencia

    UNION
    -- Principales sin alterna → alterna = principal
    SELECT 
        t120.f120_referencia AS Referencia_Alterna,
        t120.f120_referencia AS Referencia_Principal
    FROM t120_mc_items t120
    WHERE NOT EXISTS (
        SELECT 1
        FROM t124_mc_items_referencias r
        WHERE r.f124_rowid_item = t120.f120_rowid
          AND r.f124_referencia <> t120.f120_referencia
    )
),
DetectRef AS (
    SELECT r.Ref,
           CASE 
             WHEN EXISTS (SELECT 1 FROM RefBase WHERE Referencia_Principal = r.Ref) THEN 1 
             ELSE 0 
           END AS FlagPrincipal
    FROM InputRefs r
),
BaseRefs AS (
    SELECT 
        CASE 
            WHEN d.FlagPrincipal = 1 
                THEN r.Ref                  -- si es principal: la misma
            ELSE rb.Referencia_Principal   -- si es alterna: buscamos su principal
        END AS Referencia_Principal,

        CASE 
            WHEN d.FlagPrincipal = 1 
                THEN r.Ref   -- si es principal: se usa como alterna también
            ELSE r.Ref      -- si es alterna: se queda solo esa alterna
        END AS Referencia_Alterna
    FROM InputRefs r
    JOIN DetectRef d ON r.Ref = d.Ref
    LEFT JOIN RefBase rb 
           ON r.Ref = rb.Referencia_Alterna AND d.FlagPrincipal = 0
),
-- =======================================
-- Compras y agregaciones (Y1 y Y3)
-- =======================================
Compras AS (
    SELECT 
        i.f120_referencia AS Referencia_Principal,
        YEAR(imp.f41850_fecha) AS Anho,
        CASE 
            WHEN p.f011_id IN ('249','840') THEN 'USA'
            WHEN p.f011_id IN ('076','105') THEN 'BR'
            ELSE 'OTROS'
        END AS Origen,
        SUM(mov.f41851_cant_entrada) AS Unidades,
        SUM(mov.f41851_cant_entrada * movoc.f421_precio_unitario) AS Valor_USD
    FROM t41851_import_movto          AS mov
    JOIN t41850_import_docto          AS imp   ON mov.f41851_rowid_docto_import = imp.f41850_rowid
    JOIN t41806_import_origen_destino AS od    ON imp.f41850_rowid_origen = od.f41806_rowid
    JOIN t011_mm_paises               AS p     ON od.f41806_id_pais = p.f011_id
    JOIN t121_mc_items_extensiones    AS e     ON mov.f41851_rowid_item_ext = e.f121_rowid
    JOIN t120_mc_items                AS i     ON e.f121_rowid_item = i.f120_rowid
    JOIN t421_cm_oc_movto             AS movoc ON mov.f41851_rowid_oc_movto = movoc.f421_rowid
    JOIN BaseRefs                     AS br    ON br.Referencia_Principal = i.f120_referencia
    WHERE imp.f41850_ind_estado    = 4
      AND imp.f41850_id_tipo_docto = 'IM'
    GROUP BY i.f120_referencia,
             YEAR(imp.f41850_fecha),
             CASE 
                WHEN p.f011_id IN ('249','840') THEN 'USA'
                WHEN p.f011_id IN ('076','105') THEN 'BR'
                ELSE 'OTROS'
             END
),
AggPer AS (
    SELECT Referencia_Principal, 'Y1' AS Periodo, Origen,
           SUM(Unidades) AS U, SUM(Valor_USD) AS VUSD
    FROM Compras
    WHERE Anho = @YearNow
    GROUP BY Referencia_Principal, Origen
    UNION ALL
    SELECT Referencia_Principal, 'Y3', Origen,
           SUM(Unidades), SUM(Valor_USD)
    FROM Compras
    WHERE Anho BETWEEN @YearNow-2 AND @YearNow
    GROUP BY Referencia_Principal, Origen
),
TotPer AS (
    SELECT Referencia_Principal, Periodo,
           SUM(U) AS U_TOT, SUM(VUSD) AS VUSD_TOT
    FROM AggPer
    GROUP BY Referencia_Principal, Periodo
),
TotWide AS (
    SELECT
      t.Referencia_Principal,
      MAX(CASE WHEN t.Periodo='Y1' THEN t.U_TOT END) AS Unidades_Y1,
      MAX(CASE WHEN t.Periodo='Y3' THEN t.U_TOT END) AS Unidades_Y3,
      MAX(CASE WHEN t.Periodo='Y1' THEN t.VUSD_TOT END) AS ValorUSD_Y1,
      MAX(CASE WHEN t.Periodo='Y3' THEN t.VUSD_TOT END) AS ValorUSD_Y3
    FROM TotPer t
    GROUP BY t.Referencia_Principal
),
Pct AS (
    SELECT a.Referencia_Principal, a.Periodo, a.Origen,
           ISNULL(CAST(100.0*a.U/NULLIF(t.U_TOT,0) AS DECIMAL(10,2)),0) AS PctUnid,
           ISNULL(CAST(100.0*a.VUSD/NULLIF(t.VUSD_TOT,0) AS DECIMAL(10,2)),0) AS PctValor
    FROM AggPer a
    JOIN TotPer t
      ON t.Referencia_Principal = a.Referencia_Principal
     AND t.Periodo = a.Periodo
),
PctWide AS (
    SELECT p.Referencia_Principal,
      MAX(CASE WHEN Periodo='Y1' AND Origen='USA'   THEN PctUnid END) AS PctUnid_Y1_USA,
      MAX(CASE WHEN Periodo='Y1' AND Origen='BR'    THEN PctUnid END) AS PctUnid_Y1_BR,
      MAX(CASE WHEN Periodo='Y1' AND Origen='OTROS' THEN PctUnid END) AS PctUnid_Y1_OTROS,
      MAX(CASE WHEN Periodo='Y3' AND Origen='USA'   THEN PctUnid END) AS PctUnid_Y3_USA,
      MAX(CASE WHEN Periodo='Y3' AND Origen='BR'    THEN PctUnid END) AS PctUnid_Y3_BR,
      MAX(CASE WHEN Periodo='Y3' AND Origen='OTROS' THEN PctUnid END) AS PctUnid_Y3_OTROS,
      MAX(CASE WHEN Periodo='Y1' AND Origen='USA'   THEN PctValor END) AS PctValor_Y1_USA,
      MAX(CASE WHEN Periodo='Y1' AND Origen='BR'    THEN PctValor END) AS PctValor_Y1_BR,
      MAX(CASE WHEN Periodo='Y1' AND Origen='OTROS' THEN PctValor END) AS PctValor_Y1_OTROS,
      MAX(CASE WHEN Periodo='Y3' AND Origen='USA'   THEN PctValor END) AS PctValor_Y3_USA,
      MAX(CASE WHEN Periodo='Y3' AND Origen='BR'    THEN PctValor END) AS PctValor_Y3_BR,
      MAX(CASE WHEN Periodo='Y3' AND Origen='OTROS' THEN PctValor END) AS PctValor_Y3_OTROS
    FROM Pct p
    GROUP BY p.Referencia_Principal
)
SELECT
    br.Referencia_Principal,
    br.Referencia_Alterna,
    ISNULL(tw.Unidades_Y1,0) AS Unidades_Y1,
    ISNULL(tw.Unidades_Y3,0) AS Unidades_Y3,
    ISNULL(tw.ValorUSD_Y1,0) AS ValorUSD_Y1,
    ISNULL(tw.ValorUSD_Y3,0) AS ValorUSD_Y3,
    ISNULL(pw.PctUnid_Y1_USA,0) AS PctUnid_Y1_USA,
    ISNULL(pw.PctUnid_Y1_BR,0) AS PctUnid_Y1_BR,
    ISNULL(pw.PctUnid_Y1_OTROS,0) AS PctUnid_Y1_OTROS,
    ISNULL(pw.PctUnid_Y3_USA,0) AS PctUnid_Y3_USA,
    ISNULL(pw.PctUnid_Y3_BR,0) AS PctUnid_Y3_BR,
    ISNULL(pw.PctUnid_Y3_OTROS,0) AS PctUnid_Y3_OTROS,
    ISNULL(pw.PctValor_Y1_USA,0) AS PctValor_Y1_USA,
    ISNULL(pw.PctValor_Y1_BR,0) AS PctValor_Y1_BR,
    ISNULL(pw.PctValor_Y1_OTROS,0) AS PctValor_Y1_OTROS,
    ISNULL(pw.PctValor_Y3_USA,0) AS PctValor_Y3_USA,
    ISNULL(pw.PctValor_Y3_BR,0) AS PctValor_Y3_BR,
    ISNULL(pw.PctValor_Y3_OTROS,0) AS PctValor_Y3_OTROS,
    CAST(CASE WHEN ISNULL(tw.Unidades_Y1,0) > 0 THEN 1 ELSE 0 END AS BIT) AS TieneCompras_Y1,
    CAST(CASE WHEN ISNULL(tw.Unidades_Y3,0) > 0 THEN 1 ELSE 0 END AS BIT) AS TieneCompras_Y3
FROM BaseRefs br
LEFT JOIN TotWide tw ON tw.Referencia_Principal = br.Referencia_Principal
LEFT JOIN PctWide pw ON pw.Referencia_Principal = br.Referencia_Principal
ORDER BY br.Referencia_Principal;
```

> **Nota:** El bloque `InputRefs` es generado dinámicamente por Python como `VALUES ('ref1'),('ref2'),...` en cada chunk.

---

## Diccionario de datos de salida

| Columna | Descripción |
|---|---|
| `Referencia_Principal` | Referencia normalizada de `t120_mc_items` (o principal de la alterna). |
| `Referencia_Alterna` | Si el input fue principal → misma ref; si fue alterna → la alterna de entrada. |
| `Unidades_Y1` | Unidades importadas en el año actual. |
| `Unidades_Y3` | Unidades importadas en el período [año actual - 2, año actual]. |
| `ValorUSD_Y1` | Valor en USD del año actual. |
| `ValorUSD_Y3` | Valor en USD de los últimos 3 años. |
| `PctUnid_Y1_USA` / `PctUnid_Y1_BR` / `PctUnid_Y1_OTROS` | % unidades por origen en Y1 (0–100, `DECIMAL(10,2)`). |
| `PctUnid_Y3_USA` / `PctUnid_Y3_BR` / `PctUnid_Y3_OTROS` | % unidades por origen en Y3. |
| `PctValor_Y1_*` / `PctValor_Y3_*` | % valor USD por origen en Y1/Y3. |
| `TieneCompras_Y1` | `BIT` (1/0): indica si hubo compras en Y1. |
| `TieneCompras_Y3` | `BIT` (1/0): indica si hubo compras en Y3. |

---

## Validaciones y pruebas rápidas

1. **¿Entradas alternas responden?**  
   - Incluir una alterna conocida en `referencias.csv`.  
   - Confirmar que el resultado contenga la fila `(Principal, Alterna)` y métricas.

2. **Cruce con una sola principal sin alternas:**  
   - Debe devolver `(Ref, Ref)` y métricas si existen compras.

3. **Muestra manual por una ref específica:**  
   ```sql
   -- Ejemplo: validar una referencia puntual
   SELECT TOP 50 *
   FROM t41851_import_movto m
   JOIN t41850_import_docto d ON m.f41851_rowid_docto_import = d.f41850_rowid
   -- ... resto de joins según la consulta principal ...
   WHERE YEAR(d.f41850_fecha) >= YEAR(GETDATE())-2
     AND <filtro por la referencia principal que esperas>
   ORDER BY d.f41850_fecha DESC;
   ```

---

## Rendimiento y buenas prácticas

- **Chunking (1000):** evita queries con `VALUES` enormes; ajusta según tu ODBC/SQL Server.
- **Índices recomendados (si usas tablas temporales en versiones futuras):**
  - `#BaseRefs(Referencia_Principal)`
  - `t124_mc_items_referencias(f124_rowid_item, f124_referencia)`
  - `t121_mc_items_extensiones(f121_rowid, f121_rowid_item)`
  - `t41851_import_movto(f41851_rowid_docto_import, f41851_rowid_item_ext)`
  - `t41850_import_docto(f41850_rowid, f41850_fecha, f41850_id_tipo_docto, f41850_ind_estado)`
- **Filtros adicionales:** si tu volumen es muy alto, considera restringir por rango de fechas en `Compras`.
- **Tipos decimales:** los `%` están como `DECIMAL(10,2)`; ajusta si requieres mayor precisión.

---

## Solución de problemas (FAQ)

- **“No devuelve filas para ciertas refs”**  
  - Verifica que existan en `t120_mc_items` o `t124_mc_items_referencias`.
  - Confirma que la ref del CSV no tenga espacios extra o caracteres invisibles.
  - Revisa si hay compras con `f41850_ind_estado = 4` y tipo `IM` (condiciones del WHERE).

- **“Error ODBC / timeout”**  
  - Reduce `chunk_size` (ej. 300–500).
  - Valida conectividad a `server` y permisos del usuario.

- **“Excel vacío o columnas NaN”**  
  - Revisa que `dfs` tenga al menos un chunk con filas.
  - Imprime `df_chunk.shape` por chunk para detectar dónde se cae.

---

## Extensiones futuras

- **Parámetros de año**: permitir Y1/Y3 custom (ej. años fiscales).
- **Más orígenes**: regionales adicionales o normalizados por país.
- **Enriquecimiento de catálogo**: unir descripciones de item o familias (`t120`/`t121`).

---

## Licencia

Uso interno. Si se publica, definir una licencia (MIT/Apache-2.0) y remover credenciales/hosts sensibles.