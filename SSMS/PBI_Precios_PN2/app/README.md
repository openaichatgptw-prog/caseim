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
| Precio recomendado | max(P experto, P piso) cuando aplica; puede **anularse** por score de riesgo |

**Brechas que usan umbral** (|A − B| ÷ max(A, B)):

- **Lista 09** y **últ. venta** → referencia **precio reposición** (= P experto, misma fórmula de arriba). Requieren USD base.
- **Últ. compra (COP)** (`Precio_COP_Ultima` en el cruce) → referencia **USD base × TRM** (costo importación en COP, sin margen).

**Otras señales del score** (resumen): inventario muy justo; costo mín. vs máx.; dispersión entre orígenes USD (Brasil/USA; Europa según mejor origen); piso que domina con experto muy bajo; falta total de USD base y costo mín. Ver implementación en `_consulta_masiva_cotizador_alertas` en `app.py`.

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
