# Guía detallada — App Streamlit «Consulta de referencia - Precios CNH»

Documento orientado al **usuario final** y al **analista**: qué hace cada pestaña, cada control y cómo se interpretan los campos calculados.  
**Código fuente principal:** `app/app.py` · **Acceso a datos:** `app/services/data_access.py` · **SQL de auditoría / cargas 00:** `00_Reportes_SQL.py` (raíz `PBI_Precios_PN2`).

---

## Tabla de contenidos

1. [Arquitectura rápida](#1-arquitectura-rápida)
2. [Cabecera, actualización de datos y menú ⚙️](#2-cabecera-actualización-de-datos-y-menú-️)
3. [Pestaña «Consulta referencias»](#3-pestaña-consulta-referencias)
4. [Pestaña «Resumen de ventas»](#4-pestaña-resumen-de-ventas)
5. [Pestaña «Reporte margen SIESA»](#5-pestaña-reporte-margen-siesa)
6. [Pestaña «Auditoría referencias»](#6-pestaña-auditoría-referencias)
7. [Auditoría de cargas (popover) y Laboratorio SQL](#7-auditoría-de-cargas-popover-y-laboratorio-sql)
8. [Persistencia de filtros y rendimiento (fragmentos)](#8-persistencia-de-filtros-y-rendimiento-fragmentos)
9. [Tablas DuckDB más usadas por la UI](#9-tablas-duckdb-más-usadas-por-la-ui)
10. [Glosario de medidas y campos calculados](#10-glosario-de-medidas-y-campos-calculados)

---

## 1. Arquitectura rápida

| Concepto | Descripción |
|----------|-------------|
| **`pipeline.duckdb`** | Base **maestra**. Los pipelines y la carga SQL 00 escriben aquí (o en una copia de trabajo durante la actualización, según el flujo del runner). |
| **`pipeline_read.duckdb`** | Copia **solo lectura** para la interfaz. Antes de cada conexión, la app intenta **sincronizar** desde la maestra si el archivo maestro cambió (`mtime`), para ver datos nuevos sin reiniciar Streamlit. |
| **Pipelines Python** | `01_Mejora_pipeline_precios_chnV21.py`, `02_ventas_precios_cnhV2.py`, `03_Maestro_historico.py` — alimentan precios, ventas y maestro/origen en DuckDB. |
| **SQL 00 (embebido)** | Desde `00_Reportes_SQL.py` se cargan tablas raw en DuckDB: margen SIESA, atributos, **auditoría de referencias** (`auditoria_raw`), etc. |
| **`Config.ini`** | Rutas de Excels de salida, nombre del DuckDB de salida, **factores BR/USA/EUR** (`[FACTORES]`) y conexión SQL Server para las cargas 00. La app de consulta usa en parte los mismos factores en UI (consulta masiva / cotizador). |

Si falta una tabla, el mensaje en pantalla suele indicar **qué actualización** ejecutar (p. ej. «SQL 001» o «pipeline 02»).

---

## 2. Cabecera, actualización de datos y menú ⚙️

### 2.1 Bloque superior

- **Título y subtítulo:** contexto de la app (precio, disponibilidad, última compra, ventas).
- **Texto de ayuda:** recuerda actualizar la base antes de consultar si necesitas datos al día.

### 2.2 Popover «Actualizar datos»

| Control | Efecto |
|---------|--------|
| **Modo — Rápida** | Ejecuta solo los **scripts Python 01, 02 y 03**. **No** lanza las consultas SQL 00 contra SQL Server. Útil si solo necesitas refrescar DuckDB desde Excels/pipelines ya alimentados. |
| **Modo — Completa** | Ejecuta **01 + 02 + 03** y además las **tres cargas SQL 00** (margen SIESA, atributos, auditoría → tablas raw en DuckDB). |
| **Modo — Personalizada** | Dos multiselect independientes: qué **pipelines .py** correr y qué **consultas SQL 00** correr. Si ambos quedan vacíos, no hay trabajo que ejecutar. |
| **Vista previa** | Texto generado a partir de la selección actual (solo lectura). |

#### Filtro de bodegas (solo si la selección incluye **SQL 003 — Auditoría**)

- **Multiselect «Bodegas a incluir en auditoría»:** se inyecta en el SQL de auditoría como filtro de existencias por bodega. **Vacío = todas las bodegas.**
- **Botón «↻ Bodegas»:** reconstruye el catálogo `bodegas_dim` desde `margen_siesa_raw` **sin** ejecutar pipelines completos.
- **Aviso en pantalla:** este filtro **solo afecta** a la carga de **`auditoria_raw`**, no a SQL 001/002 ni a los scripts 01/02/03.

| Botón | Efecto |
|--------|--------|
| **Ejecutar actualización** | Lanza el runner con la configuración elegida; al finalizar sincroniza la copia de lectura y muestra toast de éxito o errores. |

### 2.3 «Ver log de ejecución»

- Checkbox que abre un **contenedor con el log** de la última actualización (texto plano).
- **Cerrar log** limpia el estado visible del log.

### 2.4 Popover ⚙️ (esquina)

Dos sub-pestañas embebidas:

1. **Cruces** — misma pantalla que la función interna de **auditoría de cargas** (métricas de tablas, archivos, flags de puente RPL/alternas, etc.).
2. **Laboratorio SQL** — editor SQL de solo lectura sobre `pipeline_read.duckdb` (ver sección 7).

### 2.5 Estado «_actualizando»

Mientras corre una actualización, varias pestañas muestran un **aviso** de que las consultas están pausadas; puedes cambiar de pestaña sin cancelar el proceso.

---

## 3. Pestaña «Consulta referencias»

Tiene **dos sub-pestañas**: **Consulta individual** y **Consulta en lote (CSV)**.

### 3.1 Consulta individual

#### Búsqueda y selección

| Control | Comportamiento |
|---------|----------------|
| **Referencia o descripción** | Texto libre. Dispara `buscar_referencias()` sobre DuckDB: busca por referencia original, normalizada o texto en descripción (límite de coincidencias en servicio). |
| **Coincidencia (selectbox)** | Lista etiquetada `Original \| Normalizada \| Descripción`. La clave interna del widget cambia con un hash del texto buscado para **no mezclar** la selección al cambiar la consulta. |
| **Umbral de disponibilidad (slider)** | Solo afecta al **panel visual** de orígenes (Brasil / USA / Europa): se consideran candidatos solo regiones con disponibilidad **estrictamente mayor** al umbral; entre ellas se resalta la de **menor precio ajustado**. Si ninguna supera el umbral, no hay «mejor precio» en ese sentido. |

#### Tras elegir una referencia

1. **`obtener_resumen_referencia(ref_norm)`** — datos desde `resultado_precios_lista` (y columnas relacionadas).
2. **Enriquecimiento opcional** con la misma lógica masiva (`obtener_resumen_referencias_masivo`) para campos como **Costo_Min, Costo_Max, Existencia_Total, disponibilidad consolidada** cuando existan en el merge aud/inventario.

#### Panel HTML («ficha»)

- Muestra **última compra**, **orígenes USD**, **DNET**, **costos**, **existencias** según el resumen; el umbral del slider participa en el cálculo visual del mejor origen.
- Textos de ayuda (íconos **i**) explican diferencias entre fuentes (lista OC vs auditoría, etc.).

#### Existencia por bodega (Siesa)

- Tabla desde **`margen_siesa_raw`** (carga **SQL 001**). Sin recálculos de margen en Python: formato de números y porcentajes para lectura.
- Si no hay tabla o filas: mensaje para ejecutar actualización con **SQL 001**.

#### Últimas ventas

- Hasta **20** movimientos desde `ventas_raw` (pipeline **02**), con formato monetario y fechas.
- Ayuda contextual con el rango de fechas disponible en `ventas_raw`.

#### Preferencias

- Al salir de flujos relevantes se llama `save_tab_filter_prefs("consulta")` para recordar filtros en disco (ver sección 8).

---

### 3.2 Consulta en lote (CSV)

#### Origen de referencias (tres modos)

| Modo | Descripción |
|------|-------------|
| **Catálogo completo** | Toggle «Analizar todas las referencias»: toma el catálogo cacheado desde márgenes (`obtener_dataset_margenes` / referencias únicas). Opcional **límite numérico** de cuántas procesar. |
| **Consulta rápida** | Una referencia en text input + botón **Consultar**; el valor se persiste en `session_state` para sobrevivir reruns. |
| **CSV** | `file_uploader`; detecta separador o trata el archivo como una columna de códigos. **Selectbox** elige qué columna del CSV es la referencia. Normalización de refs (incluye limpieza de `12345.0` tipo Excel). |

#### Parámetros globales del lote (columnas superiores)

| Control | Regla de negocio |
|---------|------------------|
| **Umbral de disponibilidad** | Slider + número enlazados. Solo entran en la competencia de «mejor origen» las filas con **disp** estrictamente **>** umbral por región (Brasil / USA / Europa). |
| **Factor USA/Brasil** y **Factor Europa** | Multiplican el precio USD de cada origen al comparar y al calcular **Mejor_Precio_Ajustado**. Por defecto pueden alinearse con `Config.ini`; la UI permite sobrescribirlos. |
| **Procesar en lotes** | Para catálogos grandes: divide en chunks con barra de progreso; usa `obtener_resumen_referencias_masivo` por lote si existe, o fallback fila a fila. |

#### Cálculo «Mejor origen» (después del cruce SQL)

Para cada fila, entre orígenes con disponibilidad válida:

- Se calcula precio ajustado: **Brasil/USA** × factor USA-Br, **Europa** × factor EUR.
- **Mejor_Origen** = región con **menor** precio ajustado.
- Si ningún origen cumple disponibilidad: sin mejor origen / precios ajustados nulos según columnas.

#### Vista de tabla principal

- **`_consulta_masiva_preparar_vista`** — ordena columnas (entrada, cruce, alternas, estado, etc.) y oculta en UI refs duplicadas internas si aplica.
- **Toggle «Todas las columnas»** — añade bloque inventario/venta y bloque auditoría (última compra SQL 003) con etiquetas cortas «lista (OC)» vs «aud.».
- **Toggle + multiselect «Seleccionar columnas»** — personaliza columnas visibles; botón **«Solo columnas estratégicas»** restaura un subconjunto definido en código (`_CONSULTA_MASIVA_PRIORIDAD_ESTRATEGICA`).
- **Fragmento Streamlit** (`st.fragment` si existe): al cambiar columnas solo se redibuja ese bloque + tabla.

#### Cotizador automático

El cotizador trabaja **fila a fila** sobre el mismo resultado que la tabla de consulta masiva (tras calcular **Mejor origen** y columnas de inventario/auditoría si las activaste). Todo lo que sigue está implementado en `app.py` (`_consulta_masiva_cotizador_df`, `_consulta_masiva_cotizador_alertas`).

##### Precios COP que calcula la app

| Campo en datos (antes de etiquetas UI) | Nombre en pantalla (típico) | Fórmula / regla |
|----------------------------------------|------------------------------|-----------------|
| `USD_base` | **USD base (cotiz.)** | Si existe **Mejor_Precio_Ajustado** (USD ya multiplicado por factor BR/USA/EUR de la consulta masiva), ese valor. Si no, **Último Valor USD** de la lista de precios × **factor logístico** según **País última compra** (Brasil/USA → factor USA-Brasil; Europa → factor EUR; otros países → mismo factor que EUR en código). |
| `USD_base_fuente` | **Fuente USD** | Texto: de dónde salió el USD base (`Mejor_Precio_Ajustado` o respaldo lista ajustada). |
| `USD_base_unidades_disp` | **Und. disp. origen USD base** | Disponibilidad asociada al origen ganador (`Mejor_Disponibilidad`) cuando el USD base viene del mejor origen. |
| `Factor_ultima_compra_aplicado` | (si visible) | Solo cuando el USD base viene del respaldo por **última compra USD**: qué factor se aplicó. |
| `P_venta_experto_COP` | **P. venta experto (COP)** | COP derivado del USD base y la TRM; fórmula explícita en el recuadro [①](#recuadro-fórmulas-cotizador-cop) debajo de esta tabla. |
| `P_piso_inventario_COP` | **P. piso inventario (COP)** | Piso desde **Costo_Min** y el margen piso; fórmula [②](#recuadro-fórmulas-cotizador-cop). |
| `P_recomendado_COP` | **P. recomendado (COP)** | Si hay experto **y** piso: **máximo** de ambos (no vender por debajo del piso ni por debajo del experto cuando el piso domina). Si solo hay uno, ese valor. Puede quedar **vacío** si el motor **anula** el recomendado por riesgo (ver alertas). |
| `Regla_precio` | **Regla precio** | Texto que indica cuál tramo mandó: experto, piso, empate, solo experto, solo piso. |

**Margen % (cot.)** (`Margen_pct_cot`) y **TRM (cot.)** (`TRM_cot`) son los **parámetros** usados en esa fila (no son margen contable de SIESA): sirven para auditoría de “con qué supuestos se calculó”.

<a id="recuadro-fórmulas-cotizador-cop"></a>
###### Recuadro — fórmulas cotizador (COP)

*(Notación en texto plano: se lee bien en GitHub, VS Code y PDF; símbolos × ÷ en lugar de LaTeX.)*

```text
①  P_experto  =  USD_base × TRM ÷ (1 − m)

    m  = margen objetivo como fracción del precio de VENTA
         Ejemplo: 25 %  →  m = 0,25  →  divisor (1 − m) = 0,75
    TRM y m salen de los sliders / cajas de la pantalla.

②  P_piso  =  Costo_Min ÷ (1 − X)

    X  = margen «piso inventario» en fracción (slider 5 % … 80 %)
    Costo_Min = misma columna que en consulta masiva (cruce inventario).
```

##### «Guía Δ lista vs repo (%)» y «Guía Δ venta vs repo (%)»

En la app, **“repo”** (reposición) significa el **precio recomendado en COP** (`P_recomendado_COP`) **antes** de anularlo por alertas — es la referencia contra la que se mide qué tan lejos están la lista y la última venta.

**Guía Δ lista vs repo (%)** (`Guia_lista09_vs_repo_pct`):

```text
Guía_lista (%)  =  | Precio_Lista_09 − P_rec |  ÷  max(Precio_Lista_09 , P_rec)  ×  100
```

**Guía Δ venta vs repo (%)** (`Guia_venta_vs_repo_pct`):

```text
Guía_venta (%)  =  | Ult_venta − P_rec |  ÷  max(Ult_venta , P_rec)  ×  100
```

(`Ult_venta` = **Últ. Precio Venta** del cruce masivo; `P_rec` = precio recomendado en COP.)

Solo se calculan si **ambos** operandos existen y son &gt; 0. Son **métricas de brecha / alineación** respecto al precio de reposición **propuesto por el cotizador**, no márgenes contables.

**Valores vacíos (—):** si no hay `P_rec` usable, o falta lista 09 / última venta, la guía correspondiente no se calcula (`None`).

**Si el estado anula el recomendado** («Precio no calculable…»), en pantalla puede seguir apareciendo **P. recomendado** vacío pero **guía %** con número: las guías se evalúan con el **P_rec intermedio** antes de anularlo, para que veas qué tan lejos iban lista y venta de ese precio propuesto (solo diagnóstico).

**Uso en alertas:** si la guía lista supera **35 %** o la guía venta supera **40 %** (umbrales fijos en código), suman **score** y texto en **Alertas** (“muy distinto del precio reposición”). Eso **no** cambia la fórmula de la guía; solo dispara revisión.

##### Estado de cotización y alertas (score)

`_consulta_masiva_cotizador_alertas` suma un **score** por condiciones (todas son **orientativas**, no política comercial escrita en piedra):

| Señal (resumen) | Efecto en score (típico) |
|-----------------|---------------------------|
| Existencia total ∈ (0, 3] unidades | +1 |
| Costo máx. vs costo mín.: **(máx − mín) ÷ mín** &gt; 35 % | +2 |
| ≥ 2 orígenes USD válidos y dispersión **(máx − mín) ÷ mín** &gt; 35 % (o &gt; 55 % → más peso) | +2 o +4 |
| Guía lista vs repo &gt; 35 % | +2 |
| Guía venta vs repo &gt; 40 % | +1 |
| Piso domina y experto &lt; 50 % del piso | +1 |
| Sin USD base **y** sin costo mín. | Estado bloqueado, sin recomendación |

| Estado mostrado | Condición resumida |
|-----------------|-------------------|
| **OK** | Sin alertas. |
| **OK (con observaciones)** | Hay alertas pero score bajo. |
| **Revisar manual** | Score ≥ 2 **o** ≥ 2 alertas. |
| **Precio no calculable automáticamente** | Score ≥ 5 **o** falta total de insumos (USD y costo mín.); se **anula** `P_recomendado` y las guías % pueden quedar sin sentido de negocio. |

**Filtro rápido de riesgo:** deja solo filas con `Existencia_Total ≤ X` **y** estado «Revisar manual» o «Precio no calculable…».

**Vista analítica del cotizador:** además del **núcleo** de columnas, muestra el **bloque analítico** (Estado de coincidencia, Costo máx., Precio lista 09, última venta guía, guías %, margen/TRM usados, fuente USD, regla, alertas) y el **multiselect** de columnas extra del `df_out` de consulta. Atajos: **Solo columnas estratégicas** y **Todas las columnas de consulta**. Bloque en **fragmento** Streamlit cuando está disponible.

**Descargas CSV:** resultado crudo de consulta masiva vs cotizador **tal como quedó** tras merges y columnas visibles del fragmento.

---

## 4. Pestaña «Resumen de ventas»

**Fuente:** `ventas_raw` (pipeline **02**).

### 4.1 Filtros del tablero

Multiselects (vacío = **sin filtro** en esa dimensión, según implementación en `data_access`):

- Sede, Sistema precio, Rotación (plan 08, alineado con margen SIESA), Descrip. UN, Cliente, Vendedor, Línea.
- **Filtrar modelo (texto)** acota el desplegable **Modelo** (plan 03).
- **Año** y **Mes** multiselect.

**Limpiar filtros** resetea claves de `session_state` de ventas. **Actualizar vista** fuerza rerun.

### 4.2 Contenido agregado

- KPIs y gráficos (Plotly) según `obtener_dashboard_ventas`: comparativos por año, modelo, vendedor, cliente, etc.
- Textos de ayuda cuando faltan años o dimensiones (p. ej. ejecutar de nuevo pipeline 02).

### 4.3 Detalle operativo

- Carga hasta **200.000** filas filtradas para tabla en pantalla.
- **Descargar CSV (vista actual)** — lo que ves.
- **Preparar CSV completo (sin tope)** — recarga con `limite=None` y segundo botón de descarga con conteo de filas.
- Si la tabla supera un tope de celdas para el **Styler** de pandas, se muestra sin formato enriquecido para evitar errores de render.

---

## 5. Pestaña «Reporte margen SIESA»

**Fuente:** `margen_siesa_raw` (**SQL 001**).

### 5.1 Requisitos

- Si no existe la tabla o está vacía: mensajes para ejecutar **Actualizar datos** con SQL 001.

### 5.2 «Margen a analizar»

- Multiselect con **máximo 1** elemento: **Margen09** o **Margen04** si existen.
- Según la elección, el precio de lista usado en KPIs/detalle es **Precio_Lista_09** o **Precio_Lista_04**.

### 5.3 Filtros completos (`_margen_ui_filtros_completos`)

Incluye (según columnas disponibles): búsqueda de texto, rangos de margen, bodega, rotación, instalación, sistema precio, líneas de crédito, etc. Botón de reset de filtros si está configurado en el módulo de preferencias.

### 5.4 KPIs (franja superior)

| Métrica | Cálculo (resumen) |
|---------|-------------------|
| **Refs** | `nunique` de código de referencia interno (`_ref_codigo`) en el conjunto filtrado. |
| **Inv** | Suma de **valor inventario** = existencia × costo prom. instalación (por fila), formateado resumido COP. |
| **Exist** | Suma de existencias (columna Existencia o Disponible). |
| **Margen %** | Margen global ponderado; fórmula en [recuadro margen KPI](#recuadro-margen-kpi). |
| **Margen $** | Ver [recuadro margen KPI](#recuadro-margen-kpi) (lógica análoga a DAX en comentarios del código). |
| **Costo Bodega** | Costo promedio ponderado por existencia. |

<a id="recuadro-margen-kpi"></a>
**Recuadro — KPIs de margen (franja):**

```text
Margen %  =  100 × ( 1 −  ( Σ (Q × Costo_prom) ) ÷ ( Σ (Q × Precio_lista) ) )

Margen $  =  Σ (Q × Precio_lista)  −  Σ (Q × Costo_prom)
```

(`Q` = existencia por fila; agregación sobre el conjunto filtrado.)

### 5.5 Sub-pestaña «Detalle filtrado»

- Orden: margen ascendente, luego valor inventario descendente (prioriza refs con margen bajo y mucho valor).
- Columna **Margen09_Max:** máximo Margen09 por referencia calculado sobre el **dataset base completo** con existencia > 0 (no solo el filtro actual), para comparar política vs situación filtrada.
- Formato de referencia sin sufijo `.0` espurio.

### 5.6 Sub-pestaña «Segmentación»

- Agrupación por dimensión (sistema, equipo, modelo, bodega, rotación…) con agregados (promedio, mediana, min/max margen, negativos, valor inventario, % negativos).

### 5.7 «Reporte gráfico»

- Visualizaciones adicionales sobre el mismo dataset filtrado (según implementación en la función `_render_tab_margen` parte gráfica).

---

## 6. Pestaña «Auditoría referencias»

**Fuente:** `auditoria_raw` — resultado del **SQL 003** embebido en `00_Reportes_SQL.py` (carga desde SQL Server a DuckDB).

### 6.1 Expander «Cómo se calcula el semáforo»

Reproduce la lógica en lenguaje natural (la app inserta el mismo texto desde `_auditoria_doc_semaforo_markdown()`). Resumen:

- El semáforo responde si la **última compra en COP**, ajustada por **factor logístico por país** (Brasil / USA / EUR según `Config.ini`), está alineada con **costo mínimo y máximo** de inventario en bodega.
- **`ABSVar_Costo`:** en COP, la **mayor** de las dos brechas absolutas (vs costo mín y vs costo máx) según el SQL de `FactPricing`.
- **`ABSVar_Costo_Pct`:** la **mayor** desviación relativa entre las dos rutas (min/max).
- **Cuartiles Q1, Q2, Q3 e IQR** se calculan sobre **todas las filas** del mismo resultado del reporte (misma corrida / mismos filtros de bodega en SQL 003).
- **Reglas fijas de % (en SQL):**  
  - Crítico: `ABSVar_Costo ≥ Q3 + 3×IQR` **y** `ABSVar_Costo_Pct ≥ 0,30`  
  - Moderado alto: `ABSVar_Costo ≥ Q3` **y** `≥ 0,20`  
  - Moderado bajo: `ABSVar_Costo ≥ Q2` **y** `≥ 0,10`  
  - Si no: **NO CRÍTICO** (en UI suele mostrarse como **Alineado**).

**Importante:** esto **no** es lo mismo que **Var_PrecioCOP** (salto entre penúltima y última compra).

### 6.2 Filtros (barra superior)

Incluyen: búsqueda texto, modelo, multiselect semáforo (con **etiquetas de negocio**: Crítico, Moderado alto, Alineado…), rotación, sistema precio, sliders de precio lista y existencia total, **umbrales |Δ compra|** y **|Δ vs costo|** (porcentajes), checkbox **«Solo variación fuerte»** (exige que **ambos** |Δ| superen sus umbrales), y más según columnas disponibles.

### 6.3 Columnas derivadas en la app (no siempre en SQL con el mismo nombre)

| Columna interna | Significado |
|-----------------|-------------|
| **`_abs_var_compra`** | Valor absoluto de la variación de compra elegida (p. ej. `Var_PrecioCOP` en %). |
| **`_abs_var_costo`** | Prioriza variación **última compra × factor logístico vs costo prom. inventario** (`_ABS_Ultima_vs_CostoLog_Pct`) si existe; si no, valor absoluto de `ABSVar_Costo_Pct`. |
| **`_score_alerta`** | `0,55 × _abs_var_compra + 0,45 × _abs_var_costo` — prioriza conjuntamente «problema entre compras» y «problema vs inventario». |
| **`_Existencia_suma_niveles`** | Suma de existencias por niveles si el SQL trae columnas min/interm/max (helper en Python). |

### 6.4 Métricas del «cuadro de mando» (fila de KPIs)

- Referencias distintas, conteo **semáforo crítico**, % de filas sobre umbral |Δ compra|, % sobre umbral |Δ vs costo|, **valor inventario expuesto** (suma aproximada costo×existencia en el slice).

### 6.5 Sub-pestaña «Vista principal»

#### Vista estratégica

- **Top N** configurable: tabla ordenada por **`_score_alerta`** descendente.
- Columnas en orden lógico: identificación → score → días entre compras → variaciones → costo prom. inv. → |Δ| → magnitud (lista, stock, costos) → contexto (sistema, modelo, margen objetivo).

#### Vista táctica

- Una fila por referencia con **multiselect de columnas visibles** y botón **«Solo columnas estratégicas»**.
- Bloque decorado con **`st.fragment`** (Streamlit ≥ 1.33): cambiar columnas **no** rerun completo de la pestaña.

#### Vista operativa

- Varios **bloques** temáticos (identificación, semáforo/score, problema 1 compras, problema 2 vs inventario, stock/márgenes, facturas, logística…) cada uno con tabla angosta y guía de lectura.

#### Segmentación

- Agrupación por semáforo, sistema o modelo: conteos, score máximo, etc.

### 6.6 Sub-pestaña «Reporte gráfico»

- Múltiples gráficos Plotly: dispersión score vs magnitudes, distribución de semáforo, tendencias por octiles de días, **plan de acción** (top 20 por índice de prioridad combinando |Δ compra|, |Δ costo| y score, con pesos y multiplicadores si semáforo crítico o moderado alto), etc.

---

## 7. Auditoría de cargas (popover) y Laboratorio SQL

### 7.1 Auditoría de cargas (`_render_tab_auditoria`)

- Llama a `obtener_auditoria_dashboard()`: flags (puente RPL, origen completo, atributos, alternas, auditoría), tablas de archivos, estado de tablas, **cobertura de cruces** con métricas y % sobre margen.

### 7.2 Laboratorio SQL

- **Solo** sentencias de lectura: `SELECT`, `WITH`, `SHOW`, `DESCRIBE`, `EXPLAIN` (validación en servicio).
- **`pipeline_read.duckdb`** con límite máximo de filas configurable.
- Resultado en tabla renombrada con etiquetas de negocio (`_renombrar_negocio`).

---

## 8. Persistencia de filtros y rendimiento (fragmentos)

- **`filter_prefs`:** al usar ciertas pestañas se guardan preferencias en JSON (`user_filter_prefs.json` en la carpeta de la app) para restaurar multiselects y textos en la próxima sesión.
- **`st.fragment` / `_streamlit_fragment_optional()`:** si la versión de Streamlit expone `st.fragment`, se usa para que **cambios en multiselects de columnas** (consulta masiva, cotizador analítico, vista táctica auditoría) **redibujen solo ese fragmento**; si no, el decorador es neutro (función normal).

---

## 9. Tablas DuckDB más usadas por la UI

| Tabla | Origen típico | Uso en UI |
|-------|----------------|-----------|
| `resultado_precios_lista` | Pipeline 01 / DuckDB | Consulta individual, base del cruce masivo |
| `margen_siesa_raw` | SQL 001 | Margen, existencia por bodega en consulta, catálogo bodegas auditoría |
| `auditoria_raw` | SQL 003 | Pestaña auditoría referencias |
| `atributos_referencias_raw` | SQL 002 | Cruces en consultas masivas / joins |
| `referencias_alternas` | Pipelines / DuckDB | Cruce principal ↔ alternas |
| `ventas_raw` | Pipeline 02 | Resumen ventas y últimas ventas |
| `origen_precios_tablero` / maestros | Pipeline 03 | Contexto de orígenes (según pantallas que lo consulten) |
| `bodegas_dim` | Derivada de margen | Selector de bodegas para SQL 003 |

---

## 10. Glosario de medidas y campos calculados

Referencias cruzadas: **SQL** = definido en `00_Reportes_SQL.py` / motor SQL Server al generar la tabla; **Python** = calculado solo en `app.py` o `data_access.py`. Los nombres en pantalla pueden diferir por `BUSINESS_LABELS` / `_renombrar_negocio`.

### 10.1 Consulta masiva (tabla principal, antes del cotizador)

| Medida / campo | Origen | Qué es |
|----------------|--------|--------|
| **Referencia Entrada** | Python/cruce | Código que envió el usuario (CSV, catálogo o rápido). |
| **Ref. cruce** | Cruce | Referencia principal del maestro que ganó el match. |
| **Ref. alternas** | Pipeline | Texto de alternas asociado al ítem. |
| **Estado** | Cruce | «OK» si hubo coincidencia en el modelo de datos; variantes «Sin coincidencia» según reglas del SQL/Python. |
| **Mejor_Origen** | Python | Región (Brasil / USA / Europa) con **menor** precio USD **ajustado** entre las que tienen disponibilidad **&gt; umbral**. |
| **Mejor_Disponibilidad** | Python | Disponibilidad del origen elegido. |
| **Mejor_Precio_Ajustado** | Python | Precio USD del ganador × factor región (mismo criterio que comparación). |
| **Mejor_Precio_Sin_Factor** | Python | Precio USD bruto del ganador (antes de factor). |
| **Precio Brasil / Usa / Europa** + **disp_*** | DuckDB | Precio y disponibilidad por región desde el pipeline de precios. |
| **Costo_Min, Costo_Max, Existencia_Total** | Merge consulta | Inventario consolidado; prioriza datos de auditoría/inventario según `COALESCE` en `data_access` (lista vs SQL 003). |
| **Precio_Lista_09, Últ. Precio Venta** | Cruce | Para cotizador y guías; vienen del join a márgenes/ventas cuando existen. |
| **Bloque «lista (OC)» vs «aud.»** | Etiquetas UI | Misma referencia puede mostrar **última compra lista** (importación OC) y **última compra auditoría** (SQL 003); **no** son el mismo documento contable. |

### 10.2 Cotizador (resumen; detalle en §3.2)

| Medida | Qué es |
|--------|--------|
| **P. recomendado (COP)** | `max(experto, piso)` con experto = USD×TRM/(1−m) y piso = Costo_Min/(1−X). |
| **Guía Δ lista vs repo (%)** | `|PL09 − P_rec| ÷ max(PL09, P_rec) × 100` (detalle en §3.2). |
| **Guía Δ venta vs repo (%)** | `|UltVenta − P_rec| ÷ max(UltVenta, P_rec) × 100` (detalle en §3.2). |
| **Estado cotización** | Resultado del score de alertas (OK → bloqueado). |
| **Alertas** | Texto concatenado de las reglas de `_consulta_masiva_cotizador_alertas`. |

### 10.3 Consulta individual (ficha HTML)

- **Mejor precio / orígenes:** respeta el **umbral de disponibilidad** del slider; lógica análoga a la masiva pero en el componente visual de la ficha.
- **KPIs de última compra:** valores tal cual vienen del resumen en DuckDB (sin recalcular TRM o márgenes en el HTML salvo formato).

### 10.4 Reporte margen SIESA

| Medida | Cálculo en UI |
|--------|----------------|
| **Valor inventario (fila)** | Existencia (o Disponible) × **Costo prom. inv. (COP)** por línea. |
| **Margen % (franja)** | Igual que §5.4: `100 × (1 − Σ(Q×C_prom) ÷ Σ(Q×P_lista))` con `Q` = existencia. |
| **Margen $ (franja)** | `Σ(Q×P_lista) − Σ(Q×C_prom)`. |
| **Costo Bodega** | `Σ(Q×C_prom) ÷ Σ(Q)` (promedio ponderado por unidades). |
| **Margen09_Max** | Por cada ref., el **máximo Margen09** en todo el dataset base con existencia &gt; 0 (no solo el filtro actual). |

Los **Margen09 / Margen04** y **Precio lista** en la tabla son los del **reporte SIESA** cargado en SQL 001; la app no recalcula esos porcentajes desde cero, solo agrega y filtra.

### 10.5 Auditoría referencias

| Campo | Origen | Qué es |
|-------|--------|--------|
| **Semaforo_Variacion** | SQL | Categoría por **ABSVar_Costo** vs cuartiles del lote **y** **ABSVar_Costo_Pct** vs umbrales 10 % / 20 % / 30 % (ver §6.1 y expander en app). |
| **ABSVar_Costo** | SQL | En COP: la **mayor** brecha absoluta entre última compra (lógica SQL) vs costo mín. **o** vs costo máx. |
| **ABSVar_Costo_Pct** | SQL | La **mayor** desviación **relativa** entre esas dos rutas. |
| **Var_PrecioUSD / Var_PrecioCOP / Var_TRM** | SQL | Variación **penúltima → última** compra (problema 1). |
| **Var_CostoMin_PrecioCOP** / **Var_CostoMax_PrecioCOP** | SQL | Variación última compra COP vs costos extremos (problema 2, distinto del semáforo si la definición SQL difiere en detalle). |
| **Dias_Entre_Compras** | SQL | Días entre fechas de última y penúltima compra. |
| **NumCostosValidos, EsCostoUnico** | SQL | Diagnóstico de cuántos costos de bodega son válidos y si hay un solo costo. |
| **_abs_var_compra** | Python | `|Var_PrecioCOP|` (o la columna de variación de compra detectada). |
| **_abs_var_costo** | Python | Preferentemente variación % **última compra ajustada logísticamente vs costo prom. inventario** si existe columna; si no, `|ABSVar_Costo_Pct|`. |
| **_score_alerta** | Python | `0,55 × |Δ compra| + 0,45 × |Δ vs costo|` (sobre las columnas derivadas anteriores). |
| **_Existencia_suma_niveles** | Python | Si no hay `Existencia_Total` en SQL: suma Existencia_Min + Intermedio + Max. |
| **Valor inv. expuesto (KPI auditoría)** | Python | `Σ Valor_Inventario` si existe; si no, `Σ (Existencia_Intermedio × Costo_Intermedio)` en el dataframe filtrado. |
| **% ≥ umbral \|Δ compra\|** | Python | % de **filas** (no refs únicas) con `_abs_var_compra` ≥ umbral del slider. |
| **% ≥ umbral \|Δ vs costo\|** | Python | Igual para `_abs_var_costo`. |
| **Índice plan de acción (gráfico)** | Python | `0,60 × g_compra + 0,25 × g_costo + 0,15 × score`, con multiplicadores ×1,5 si crítico y ×1,2 si moderado alto (ver código cerca de `Plan de acción`). |

**Etiquetas UI del semáforo:** «Alineado» = valor SQL `NO CRÍTICO` (no confundir con “sin problema comercial”).

### 10.6 Resumen de ventas

- KPIs y series agregadas vienen de **`obtener_dashboard_ventas`** sobre `ventas_raw`: totales de **Valor Venta**, **Margen**, comparativos por año/modelo/cliente, etc. Cada gráfico tiene caption en la app; el detalle de fórmulas agregadas está en `data_access.py` (agrupaciones `sum` / `mean` por dimensión).
- **Detalle operativo:** filas de movimientos con columnas de factura, precio unitario, costo, **Precio CNH**, **Margen Sistema**, etc., según el SELECT del pipeline 02.

### 10.7 Auditoría de cargas (popover Cruces)

- **Pct_sobre_margen** y **Valor** en tablas de cobertura: métricas de **cuántas referencias** cruzan contra márgenes / atributos, expresadas como conteos y porcentajes sobre un denominador definido en `obtener_auditoria_dashboard` (ver `data_access.py`).

---

## Mantenimiento del documento

Al añadir pestañas, filtros o fórmulas en `app.py`, conviene actualizar esta guía en el mismo PR. Para el detalle **campo a campo del SQL** de auditoría, seguir ampliando `readmetecnico.md` y `00_Reportes_SQL.py` (comentarios y CTEs).

---

*Generado como documentación de usuario para el proyecto IMECOL — Precios CNH.*
