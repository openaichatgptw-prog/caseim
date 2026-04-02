# Reglas del monitor de mejor precio y costo

**Audiencia:** dirección y equipos estratégicos (lectura sin conocimiento técnico).  
**Herramienta:** script que transforma un archivo de datos de entrada en un informe con columnas de decisión.

---

## 1. Para qué sirve

El sistema responde, **por cada referencia de producto**, a tres preguntas de negocio:

1. **¿Con qué costo deberíamos trabajar** para evaluar si el precio de lista es sostenible?
2. **¿Ese margen cumple la política** que la empresa definió como objetivo (incluida la variación por **categoría**)?
3. **¿Podemos confiar en el número automático** o hay situaciones que obligan a **revisión humana**?

No sustituye el criterio comercial: **acota errores** cuando los datos son incoherentes y **documenta el motivo** cuando no se puede calcular un costo único con seguridad.

---

## 2. Qué información usa (por fila)

Cada línea del archivo representa un producto y aporta, como mínimo:

| Concepto | Significado de negocio |
|----------|-------------------------|
| Costo de inventario (mínimo y máximo) | Distintos “tramos” o lotes en bodega pueden tener costos distintos. |
| Costo de reposición USA y Brasil | Precio al que podrías volver a comprar en cada origen. |
| Disponibilidad en USA y Brasil | Unidades o señal de stock disponible en ese proveedor. |
| Disponibilidad en tramos de inventario (opcional) | Cuánto hay asociado al costo mínimo y al máximo en bodega. |
| Precio de lista | Precio de venta al público o lista vigente. |
| **Categoría (obligatoria)** | Texto que debe coincidir con la política de margen y, si aplica, con el umbral de dispersión de inventario definidos en el programa (mismo nombre, sin importar mayúsculas). |

Si faltan columnas opcionales de disponibilidad en inventario, el modelo asume ceros donde corresponde y lo indica en consola al procesar.

---

## 3. Política de margen sobre lista

El margen que se calcula es el **margen bruto sobre precio de lista**:

\[
\text{Margen (\%)} = \frac{\text{Precio de lista} - \text{Costo usado}}{\text{Precio de lista}} \times 100
\]

### 3.1 Valores por defecto (cuando la categoría no está en la tabla del programa)

En el código existen **valores globales** de referencia:

- **Margen objetivo por defecto:** **40 %** (`MARGEN_OBJETIVO_PCT`).
- **Tolerancia por defecto:** **15 puntos porcentuales** debajo del objetivo (`MARGEN_TOLERANCIA_PCT`).
- **Piso por defecto:** objetivo menos tolerancia → **25 %**.

Si el margen es **≥ piso**, **OK_MARGEN_OBJETIVO** es **SÍ**; si es **< piso**, **NO**.

### 3.2 Márgenes por categoría (parametrización principal)

La política **por categoría** se define en un **solo bloque de texto** en el código (`MARGEN_POLITICA_POR_CATEGORIA_TEXTO`). Ahí se listan líneas del tipo:

- `NombreCategoria: objetivo%, tolerancia_pp` — tolerancia explícita para esa categoría.
- `NombreCategoria: objetivo%` — si no se indica tolerancia, se usa la **tolerancia global** (`MARGEN_TOLERANCIA_PCT`).

Se pueden separar entradas con **salto de línea**, **punto y coma** o **corchetes**; las líneas que empiezan por `#` son comentarios.

**Ejemplo compacto (ilustrativo):**

`[ GENERAL:40%,15 ; FILTROS:35%,12 ; ACIET:30% ; MANGUERA:35%,10 ]`

Los nombres deben alinearse con lo que viene en la columna **Categoria** del CSV. La coincidencia **no distingue mayúsculas**.

**Valores de referencia en el repositorio** (sujetos a cambio en código): por ejemplo General 40 % / 15 pp, Filtros 35 % / 12 pp, Lubricantes 45 % / 10 pp. El **piso** de cada fila es siempre **objetivo − tolerancia** de esa categoría (o de los globales si la categoría no figura en la tabla).

### 3.3 Columna PRECIO_CALCULADO

Para las filas con **MEJOR_COSTO** calculado, se añade **PRECIO_CALCULADO**: precio de lista **teórico** que implicaría un margen bruto **igual al objetivo de esa categoría** sobre ese costo:

\[
\text{PRECIO\_CALCULADO} = \frac{\text{MEJOR\_COSTO}}{1 - \text{objetivo}_{\text{categoría}}/100}
\]

Si no hay costo válido (por ejemplo fila **NO_CALCULABLE**), esta columna queda vacía.

---

## 4. Cómo se elige el “mejor costo”

La lógica intenta **un solo costo representativo** por fila, llamado **MEJOR_COSTO**, y dice **ORIGEN** (de dónde sale ese número).

### 4.1 Orden general

1. Primero se comprueba si la fila entra en algún caso **no calculable** (ver sección 7). Si es así, no se fuerza un costo único.
2. Si **no hay datos de reposición** pero sí inventario, se usa el costo de inventario (con una advertencia de que no hay repos en datos).
3. Si hay reposición (USA y/o Brasil), se determina un **costo de reposición de referencia** según las reglas de estabilidad y disponibilidad (sección 5).
4. Luego se compara ese costo de reposición con el **costo máximo de inventario** (el más conservador del stock):
   - Si el inventario es **más caro** que la reposición, **gana el inventario** como costo (porque es lo que ya tienes cargado en bodega).
   - Si la reposición es **más cara o igual** en la lógica implementada, se usa el costo de reposición elegido.
5. **Excepción importante:** cuando el inventario tiene **mucha dispersión** entre costo mínimo y máximo, normalmente eso bloquearía el cálculo; pero si los dos repos (USA y BR) son **estables** y el costo máximo de bodega está **suficientemente alineado** con los precios de reposición, el sistema **prioriza el costo de reposición** para no quedar atrapado en un “no calculable” cuando en la práctica los proveedores coinciden con el techo de bodega.

---

## 5. Reposición USA frente a Brasil

### 5.1 Repos “estables”

Si la diferencia entre el costo USA y el BR es **pequeña** en términos relativos **y** la diferencia en dinero no supera un tope que depende del tamaño del costo (cuartiles del propio archivo), los dos orígenes se consideran **estables**. En ese caso se elige **el más barato** sin complicaciones.

### 5.2 Repos no estables

Si no son estables, la elección **no** es solo “el más barato”: entra en juego la **disponibilidad**. Cada origen debe superar un **piso de disponibilidad** que depende del **tramo de precio de lista** del artículo (artículos más caros exigen más disponibilidad mínima) y de la **distribución de disponibilidades** en el archivo completo.

- Si **ambos** superan el piso, se toma el **más barato**.
- Si solo uno lo supera, se toma **ese**.
- Si **ninguno** lo supera, se hace un desempate prudente según disponibilidad y datos presentes.

Así se evita basar la decisión en un precio teórico de un origen **sin stock real**.

---

## 6. Dispersión del costo en inventario y categorías

Se mide qué tan “abierto” está el rango de costos en bodega: comparación entre el **máximo** y el **mínimo** respecto al máximo.

- Hay un **umbral por defecto** para considerar que la dispersión es demasiado alta (hoy **45 %** del máximo en configuración típica).
- En el código existe un diccionario **SPREAD_MAX_POR_CATEGORIA** (umbrales distintos por categoría). Si el nombre de **Categoria** coincide, se usa ese umbral; si no, el global.

Si la dispersión supera el umbral **y** no aplica la excepción de “repos estables + costo máximo alineado”, el resultado es **no calculable** (código **NC_INV_RANGO_AMPLIO**): un solo número automático **no defendería** el precio frente a la realidad de bodegas.

---

## 7. Casos en los que no se calcula (NO_CALCULABLE)

En estas situaciones el sistema **prefiere no inventar** un costo único. La columna **CODIGO** resume el motivo; la nota lleva el prefijo **[REVISAR MANUAL]**.

| Código | Qué significa en negocio |
|--------|---------------------------|
| **NC_INV_RANGO_AMPLIO** | Los costos de inventario entre el tramo bajo y el alto están tan dispersos que no hay un costo único defendible sin criterio adicional. |
| **NC_CONFLICTO_ABASTECIMIENTO** | Un origen es mucho más barato que el otro, pero el barato tiene **muy poca** disponibilidad mientras el caro tiene mejor cobertura; el ratio de precios supera el umbral de conflicto. La política evita “elegir” un costo que ignore un riesgo de abastecimiento. |
| **NC_REPOS_DISP_INSUFICIENTE** | USA y BR están **muy lejos** en precio **y** ninguno cumple el piso de disponibilidad del tramo: no hay base sólida para automatizar. |
| **NC_SIN_DATOS** | No hay costo de inventario ni de reposición utilizables en el archivo para esa fila. |

---

## 8. Señal “LISTA_EN_RANGO”

Es una **lectura única** para dirección: ¿el **precio de lista** está en un rango coherente con el **piso de margen** frente a dos referencias?

- El **piso** es el de la **categoría de esa fila** (objetivo − tolerancia según la tabla de márgenes o los globales).
- Compara el margen usando el **costo máximo de inventario** (si existe).
- Compara el margen usando el **repos más barato** disponible (si hay datos de reposición).

Valores posibles:

| Valor | Interpretación |
|-------|----------------|
| **SI** | Contra el costo máximo de bodega **y** contra el repos barato, el margen cumple el piso (cuando ambos datos existen). |
| **NO** | En ambas miradas el margen queda por debajo del piso. |
| **REVISAR** | Solo una de las dos miradas cumple; hay tensión entre stock y reposición. |
| **N/A** | Faltan datos para una o ambas comparaciones. |

---

## 9. Nivel de CONFIANZA

Resumen ejecutivo de qué tan “limpia” es la fila para decidir sin mirar el detalle:

| CONFIANZA | Cuándo |
|-----------|--------|
| **ALTA** | Cálculo normal, sin alertas de atención en la nota, margen en política. |
| **MEDIA** | Cálculo hecho pero con **[ATENCION]** en la nota, o solo inventario sin reposición en datos. |
| **BAJA** | No calculable **o** margen por debajo del piso. |

---

## 10. Avisos en la columna de nota (NOTA_DECISION)

- **[REVISAR MANUAL]** aparece solo en filas **NO_CALCULABLE**, con el código entre corchetes al final.
- **[ATENCION]** aparece cuando hay riesgo operativo: repos inestables, excepción de usar reposición pese a dispersión de inventario, o patrones como **mucho stock en el tramo de costo mínimo** mientras el costo efectivo analizado es mayor, **repos externos con poca disponibilidad e inventario alto**, o **USA/BR muy divergentes** cuando la pareja se clasifica como inestable.

En filas calculadas, la nota indica el **objetivo y tolerancia** aplicados a esa fila (según categoría).

Estos textos están pensados para **priorizar** qué líneas lleva compras o pricing a una mesa de revisión.

---

## 11. Parámetros de política (referencia)

Los números están en el código (`mejor_precio.py`, sección de hiperparámetros). Aquí solo una **guía**; si cambian en el programa, prevalece el código.

| Tema | Dónde se define | Idea |
|------|-----------------|------|
| Márgenes por categoría | Bloque **MARGEN_POLITICA_POR_CATEGORIA_TEXTO** | Objetivo y tolerancia por nombre de categoría. |
| Margen / tolerancia por defecto | **MARGEN_OBJETIVO_PCT**, **MARGEN_TOLERANCIA_PCT** | Si la categoría no está en la tabla de texto. |
| Dispersión inventario por categoría | **SPREAD_MAX_POR_CATEGORIA** + **PCT_SPREAD_MAX_COSTO_INV** | Umbral de dispersión (max−min)/max. |
| Estabilidad USA/BR (relativa) | **PCT_ESTABILIDAD_REPOS** (ej. 20 %) | Por debajo de esta brecha relativa, repos “estables” (junto con tope en dinero). |
| Alineación inventario–repos (excepción) | **PCT_INV_MAX_ALINEADO_REPO** | Si el máximo de bodega está cerca de los repos, puede usarse reposición pese a dispersión. |
| Ratio de conflicto de precios | **RATIO_REPOS_CONFLICTO** | Si el caro cuesta el doble o más que el barato y el barato tiene poca disponibilidad, se bloquea. |
| Disponibilidad mínima en origen barato | **DISP_MIN_ORIGEN_BARATO** | Unidades mínimas en el origen más barato para evitar conflicto artificial. |
| Divergencia extrema repos + disponibilidad baja | **PCT_DIF_REPOS_EXTREMA_NC** | Si ambos repos están muy lejos y ninguno cumple disponibilidad mínima del tramo. |
| Divergencia que sugiere revisión (inestable) | **PCT_DIF_REPOS_REVISION** | Aviso en nota si los repos están muy separados. |
| “Mucho stock” en tramo mínimo | **UMBRAL_DISPINV_TRAMO_MIN** | Dispara aviso en contexto de tramos distintos de costo. |
| Inventario alto con repos externos flojos | **UMBRAL_INV_UNIDAD_EXT_DISP** | Combinado con disponibilidad externa bajo umbral. |

Los **pisos de disponibilidad** por tramo de precio de lista combinan valores fijos (cuartiles de precio en el lote) con la distribución de disponibilidades del archivo.

---

## 12. Salida y modo auditoría

El archivo de salida **repite las columnas de entrada** en un orden fijado por la herramienta (incluida **Categoria**) y **añade** columnas de resultado, entre otras:

- **ESTADO**, **CODIGO**, **MEJOR_COSTO**, **PRECIO_CALCULADO**, **ORIGEN**, **MARGEN_PCT_LISTA**, **OK_MARGEN_OBJETIVO**, **LISTA_EN_RANGO**, **NOTA_DECISION**, **CONFIANZA**.

Si se activa el modo **auditoría**, se añade una columna breve con **trazas internas** para quien deba contrastar la decisión sin releer toda la lógica.

---

## 13. Limitaciones que conviene tener presentes

- Los resultados dependen de la **calidad y consistencia** de los datos de entrada (costos, disponibilidades, precio de lista y **categoría**).
- Los umbrales son **políticos y configurables**: deben alinearse con la estrategia comercial y de abastecimiento de la empresa.
- Un resultado **calculado** no implica que el precio sea el óptimo de mercado; solo indica coherencia interna frente a los costos y reglas cargadas.

---

*Documento alineado con la lógica del monitor en la carpeta `06_MEJOR_PRECIO`. Si cambian **MARGEN_POLITICA_POR_CATEGORIA_TEXTO**, **SPREAD_MAX_POR_CATEGORIA** u otros hiperparámetros en `mejor_precio.py`, conviene actualizar las secciones 3, 6 y 11 de este archivo.*
