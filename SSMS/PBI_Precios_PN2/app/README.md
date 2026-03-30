# App Streamlit — Precios CNH

Interfaz de consulta sobre **DuckDB** (`pipeline_read.duckdb` / `pipeline.duckdb` en la carpeta **`PBI_Precios_PN2`**) y disparo de **pipelines** y **cargas SQL 00**. Código principal: `app.py`.

---

## Requisitos

- **Python** 3.10+ recomendado.
- **Paquetes** (no hay `requirements.txt` en este repo; instala explícitamente):

```bash
pip install streamlit pandas duckdb plotly
```

`plotly` es **opcional**: sin él, algunos gráficos no se muestran.

---

## Cómo ejecutar

Desde el directorio **`app`** (donde está `app.py`), para que resuelva el paquete `services`:

```bash
cd SSMS/PBI_Precios_PN2/app
python -m streamlit run app.py
```

La app espera las bases y `Config.ini` en **`PBI_Precios_PN2`** (un nivel por encima de `app`), según `services/data_access.py`.

---

## Pestañas principales (orden en pantalla)

| Pestaña | Contenido resumido |
|---------|-------------------|
| **Consulta referencias** | Sub-pestañas **Consulta individual** (búsqueda, ficha HTML, existencia SIESA, últimas ventas, KPIs costo/precio reposición con TRM y margen de pantalla) y **Consulta en lote (CSV)** (catálogo / CSV / referencia rápida, mejor origen, tabla **Columnas para cotizar**, descargas CSV). |
| **Resumen de ventas** | Tablero y detalle desde `ventas_raw` (pipeline 02). |
| **Reporte margen SIESA** | KPIs y tablas desde `margen_siesa_raw` (SQL 001). |
| **Auditoría referencias** | Vista principal y reportes gráficos desde auditoría (SQL 003 + lógica en app). |

**Cabecera:** popover **Actualizar datos** — modos **Rápida** (scripts 01, 02, 03), **Completa** (+ cargas SQL 001–003) y **Personalizada**; filtro de bodegas cuando aplica a SQL 003. Menú **⚙️** con auditoría de cargas y **Laboratorio SQL** (solo lectura sobre DuckDB).

---

## Consulta en lote: «Columnas para cotizar»

En **Consulta en lote (CSV)**, después de la tabla principal, la sección **Columnas para cotizar** exporta un subconjunto en **orden fijo** (alineado a la hoja de trabajo / Power BI): identificación de referencias (entrada, original, normalizada, cruce, alternas, descripción) → **estado y tipo de coincidencia** → precios BR/USA/EUR y prorrateo → disponibilidades → **USD lista** y **mejor precio ajustado** / mejor origen → tipo/disponible → costos mín/máx → **brecha** mín/máx inventario → valor líquido lista y existencias → **reposición y piso** (COP) y brechas → lista 09 y brecha vs reposición → última venta y fechas → lista OC → **última compra (auditoría)**.

Las columnas **Costo reposición**, **Precio reposición**, **P. piso inventario**, **Valor inventario** y las **brechas %** se calculan al exportar usando parámetros **parametrizables**:

| Parámetro | Dónde se define |
|-----------|-----------------|
| TRM (COP/USD), margen venta %, margen piso % | Por defecto en **`Config.ini`** → sección **`[COLUMNAS_PARA_COTIZAR]`** (`trm`, `margen_venta_pct`, `margen_piso_pct`). En la app: expander **«Parámetros de exportación (TRM y márgenes %)»** (persisten en la sesión mientras uses la app). |
| Factores importación USA/BR y EUR | Sliders **Factor importación USA/BR** y **Factor importación EURO** del mismo bloque de consulta masiva (mismo criterio que el **mejor origen**). |

Si falta `[COLUMNAS_PARA_COTIZAR]` en el INI, se usan los mismos valores numéricos que antes (4200 / 25 / 40) hasta que edites el archivo o la pantalla.

**Costo reposición (COP)** = USD base (mejor precio ajustado o, si no hay, último USD lista × factor país) × TRM. **Precio reposición (COP)** = mismo costo ÷ (1 − margen venta). **P. piso inventario** = Costo_Min ÷ (1 − margen piso) cuando hay existencias y costo mínimo. **Valor inventario** = Existencia_Total × Costo_Min (aproximación operativa). Las brechas % usan |A − B| ÷ max(A, B).

Puedes descargar **todo el resultado de la consulta** o solo **Columnas para cotizar** (CSV con ese orden y columnas).

---

## Documentación ampliada

- **[Guía detallada (pestaña a pestaña)](GUIA_DETALLADA_APP_PRECIOS_CNH.md)** — reglas, tablas DuckDB y glosario. Si hubiera discrepancia puntual con una versión muy reciente de `app.py`, **prima el código**.

---

## Referencia rápida de datos

| Artefacto | Ubicación típica |
|-----------|------------------|
| DuckDB maestro / lectura | `PBI_Precios_PN2/pipeline.duckdb`, `pipeline_read.duckdb` |
| Factores BR/USA/EUR, rutas y `[COLUMNAS_PARA_COTIZAR]` | `PBI_Precios_PN2/Config.ini` |
| Pipelines Python | `01_Mejora_pipeline_precios_chnV21.py`, `02_ventas_precios_cnhV2.py`, `03_Maestro_historico.py` (raíz `PBI_Precios_PN2`) |
