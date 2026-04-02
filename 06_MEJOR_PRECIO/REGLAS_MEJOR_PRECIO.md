# Reglas del monitor de mejor precio y costo

**Audiencia:** dirección y equipos estratégicos (lectura sin conocimiento técnico).  
**Herramienta:** script que transforma un archivo de datos de entrada en un informe con columnas de decisión.

---

## 1. Para qué sirve

El sistema responde, **por cada referencia de producto**, a tres preguntas de negocio:

1. **¿Con qué costo deberíamos trabajar** para evaluar si el precio de lista es sostenible?
2. **¿Ese margen sobre lista cumple la política** (objetivo y tolerancia, que pueden **depender de la categoría**)?
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

## 3. Margen sobre lista y política por categoría (una sola fórmula)

**No hay dos tipos de margen.** En todo el informe el margen es siempre el **mismo indicador**: **margen bruto sobre precio de lista** (costo frente a lista de venta).

\[
\text{Margen (\%)} = \frac{\text{Precio de lista} - \text{Costo usado}}{\text{Precio de lista}} \times 100
\]

Lo que **sí cambia según la categoría** (o según la línea **Default** del texto si el nombre no coincide con ninguna entrada) es solo la **política**: **objetivo %**, **tolerancia en puntos** y el **piso** (objetivo − tolerancia) para decidir **OK_MARGEN_OBJETIVO** y para **PRECIO_CALCULADO**. No se define otro “margen por categoría” distinto del margen sobre lista.

### 3.1 Línea `Default` (obligatoria en el bloque de texto)

En `MARGEN_POLITICA_POR_CATEGORIA_TEXTO` debe existir una línea del tipo **`Default: objetivo%, tolerancia_pp`**. Esa política se usa cuando la categoría de la fila **no** coincide con ninguna otra entrada del bloque (por ejemplo categoría desconocida o vacía).

Ejemplo de referencia: **Default: 40 %, 15 puntos** → piso **25 %**. Si el margen sobre lista es **≥ piso**, **OK_MARGEN_OBJETIVO** es **SÍ**; si es **< piso**, **NO**.

### 3.2 Tabla de política por categoría (`MARGEN_POLITICA_POR_CATEGORIA_TEXTO`)

En el **mismo bloque de texto** se listan las categorías con su **objetivo** y, si se desea, **tolerancia** (misma fórmula de margen arriba; solo cambian los umbrales):

- `NombreCategoria: objetivo%, tolerancia_pp` — tolerancia explícita.
- `NombreCategoria: objetivo%` — tolerancia = la de la línea **Default**.

Separadores: salto de línea, `;` o corchetes; `#` = comentario.

**Ejemplo:** `[ GENERAL:40%,15 ; FILTROS:35%,12 ; ACIET:30% ; MANGUERA:35%,10 ]`

Los nombres deben coincidir con la columna **Categoria** (sin importar mayúsculas). Ejemplo de referencia: General 40 % / 15 pp, Filtros 35 % / 12 pp, Lubricantes 45 % / 10 pp.

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

- El umbral máximo de dispersión admisible **(max−min)/max** se configura en el bloque de texto **`SPREAD_MAX_POR_CATEGORIA_TEXTO`**, con una línea obligatoria **`Default: …`** (p. ej. `45 %` o `0,45`) y líneas por categoría (`General: 40 %`, etc.).
- Si el nombre de **Categoria** no coincide con ninguna entrada, se usa el **Default** del mismo bloque.

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

Los números están en `mejor_precio.py` (bloque **HIPERPARAMETROS**). El código incluye un **mapa por grupos [A]–[H]** para que no se mezclen reglas: **margen** y **spread de inventario** van en texto por categoría; **estabilidad USA–BR** y **alineación inventario–repos** son umbrales distintos (aunque a veces compartan el mismo valor por defecto, como 20 %).

| Tema | Constante / bloque | Idea |
|------|---------------------|------|
| Margen por categoría | **MARGEN_POLITICA_POR_CATEGORIA_TEXTO** | Obligatorio `Default:` + líneas por categoría. |
| Spread inventario por categoría | **SPREAD_MAX_POR_CATEGORIA_TEXTO** | Obligatorio `Default:` + líneas; no es margen. |
| Estabilidad USA vs BR (%) | **PCT_ESTABILIDAD_REPOS** + **FACTOR_NOMINAL_CUARTILES** | % relativo y tope en $ por cuartil de costo repo. |
| Dispersión inv. vs repos (bypass NC) | **PCT_INV_MAX_ALINEADO_REPO** | Otra regla que no es la estabilidad USA–BR. |
| Piso disp. según precio lista | **DISP_MIN_POR_TRAMO_PRECIO** | Cuatro pisos por tramo de precio (cuartiles del lote). |
| Conflicto caro/barato | **RATIO_REPOS_CONFLICTO**, **DISP_MIN_ORIGEN_BARATO** | Ratio de precios + stock mínimo en el barato. |
| Divergencia USA–BR | **PCT_DIF_REPOS_EXTREMA_NC** vs **PCT_DIF_REPOS_REVISION** | NC duro vs aviso en nota (umbrales distintos). |
| Frases en nota | **UMBRAL_DISPINV_TRAMO_MIN**, **UMBRAL_INV_UNIDAD_EXT_DISP** | Stock en tramo mínimo; inventario alto y repos flojos. |

Los **pisos de disponibilidad** por tramo de precio combinan **DISP_MIN_POR_TRAMO_PRECIO** con la distribución de disponibilidades del archivo.

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

*Documento alineado con la lógica del monitor en la carpeta `06_MEJOR_PRECIO`. Si cambian los bloques de texto o los grupos [A]–[H] en `mejor_precio.py`, conviene actualizar las secciones 3, 6 y 11 de este archivo.*
