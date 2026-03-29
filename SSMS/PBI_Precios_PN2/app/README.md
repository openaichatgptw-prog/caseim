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
| **Consulta referencias** | Sub-pestañas **Consulta individual** (búsqueda, ficha HTML, existencia SIESA, últimas ventas, KPIs costo/precio reposición con TRM y margen de pantalla) y **Consulta en lote (CSV)** (catálogo / CSV / referencia rápida, mejor origen, **cotizador COP**, alertas, descargas). |
| **Resumen de ventas** | Tablero y detalle desde `ventas_raw` (pipeline 02). |
| **Reporte margen SIESA** | KPIs y tablas desde `margen_siesa_raw` (SQL 001). |
| **Auditoría referencias** | Vista principal y reportes gráficos desde auditoría (SQL 003 + lógica en app). |

**Cabecera:** popover **Actualizar datos** — modos **Rápida** (scripts 01, 02, 03), **Completa** (+ cargas SQL 001–003) y **Personalizada**; filtro de bodegas cuando aplica a SQL 003. Menú **⚙️** con auditoría de cargas y **Laboratorio SQL** (solo lectura sobre DuckDB).

---

## Cotizador (consulta masiva): COP, columnas y alertas

El **USD base** alimenta el cotizador: **Mejor precio ajustado** del cruce, o si falta **último USD lista × factor país** (BR/USA, Europa u otros). Ese valor ya viene **ajustado**; sobre él solo se aplican **TRM** y **margen** de los sliders de la pantalla.

| Concepto (COP) | Fórmula |
|----------------|---------|
| Costo reposición (importación, sin margen de venta) | USD base × TRM |
| Precio reposición / P experto (*m* = margen objetivo sobre venta) | USD base × TRM ÷ (1 − *m*) |
| Piso inventario | Costo_Min ÷ (1 − *X*) |
| Precio recomendado | max(P experto, P piso) cuando aplica: **prioriza reposición con margen**; el **piso inventario** solo eleva el precio cuando el costo de stock obliga a no vender por debajo de esa lógica de importación. Se **anula** si el **score** entra en revisión o bloqueo (ver abajo) |

**Score → estado** (suma de puntos por señales; columna **Score cotización** en la tabla):

| Score | Estado | P. recomendado |
|------:|--------|----------------|
| 0 | OK | Visible |
| 1 | OK (con observaciones) | Visible |
| 2–4 | Revisar manual | **Anulado** (siguen experto, piso, alertas) |
| ≥ 5 | Precio no calculable automáticamente | Anulado |
| (sin USD base ni costo mín.) | Precio no calculable automáticamente | Anulado |

**Lista 09 vs precio reposición** se exporta como **Lista 09 vs repo (ref. urg. %)**: referencia de **qué tan desactualizada** está la lista frente a reposición; **no suma al score** ni condiciona el precio recomendado.

**Mercado vs reposición** (señal de score): solo **últ. venta** y **últ. compra (COP)** (`Precio_COP_Ultima`), con brecha relativa (|A − B| ÷ max(A, B)):

- **Últ. venta** → **precio reposición** (= P experto). Requiere USD base.
- **Últ. compra (COP)** → **USD base × TRM** (sin margen de venta).

Si la **mayor** de las dos brechas disponibles supera el umbral del slider, suma **+2**. Si **venta y compra** existen y **ambas** superan el umbral, suma **+1** adicional.

La exportación incluye **lista ref. urg.**, **brecha mercado máx. (%)** (solo venta/compra), **mercado: guías > umbral** y **margen implícito vs costo mín. (%)** sobre el precio calculado.

**Otras señales del score** (resumen): **costo mín. vs máx.** (slider %); **dispersión entre orígenes USD** (sliders moderado/crítico); **experto vs piso** cuando el recomendado es el piso (slider: % del piso por debajo del cual el experto dispara alerta); falta total de USD base y costo mín. **La existencia baja no suma score**. Ver `_consulta_masiva_cotizador_alertas` en `app.py`.

---

## Documentación ampliada

- **[Guía detallada (pestaña a pestaña)](GUIA_DETALLADA_APP_PRECIOS_CNH.md)** — reglas, tablas DuckDB y glosario. Si hubiera discrepancia puntual con una versión muy reciente de `app.py`, **prima el código**.

---

## Referencia rápida de datos

| Artefacto | Ubicación típica |
|-----------|------------------|
| DuckDB maestro / lectura | `PBI_Precios_PN2/pipeline.duckdb`, `pipeline_read.duckdb` |
| Factores BR/USA/EUR y rutas | `PBI_Precios_PN2/Config.ini` |
| Pipelines Python | `01_Mejora_pipeline_precios_chnV21.py`, `02_ventas_precios_cnhV2.py`, `03_Maestro_historico.py` (raíz `PBI_Precios_PN2`) |
