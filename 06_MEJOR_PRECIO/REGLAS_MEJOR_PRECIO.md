# Reglas del monitor de mejor precio y costo

**Audiencia:** dirección y equipos estratégicos (lectura sin conocimiento técnico).  
**Herramienta:** script que transforma un archivo de datos de entrada en un informe con columnas de decisión.

---

## 1. Para qué sirve

El sistema responde, **por cada referencia de producto**, a tres preguntas de negocio:

1. **¿Con qué costo deberíamos trabajar** para evaluar si el precio de lista es sostenible?
2. **¿Ese margen cumple la política** que la empresa definió como objetivo?
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
| Categoría (opcional) | Permite aplicar reglas de dispersión de inventario distintas por tipo de producto. |

Si faltan columnas opcionales, el modelo asume ceros donde corresponde y lo indica en consola al procesar.

---

## 3. Política de margen sobre lista

El margen que se calcula es el **margen bruto sobre precio de lista**:

\[
\text{Margen (\%)} = \frac{\text{Precio de lista} - \text{Costo usado}}{\text{Precio de lista}} \times 100
\]

La política configurable distingue:

- **Margen objetivo:** referencia deseada (hoy **40 %** en la configuración del sistema).
- **Tolerancia:** cuántos puntos porcentuales por debajo del objetivo siguen siendo aceptables (hoy **15 puntos**).
- **Piso efectivo:** objetivo menos tolerancia → hoy **25 %**.  
  - Si el margen es **≥ 25 %**, la columna **OK_MARGEN_OBJETIVO** indica **SÍ**.  
  - Si es **< 25 %**, indica **NO**.

Por encima del objetivo (40 %) el resultado sigue siendo favorable; la tolerancia solo define **hasta dónde abajo** aún se considera “dentro de política”.

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

- Hay un **umbral por defecto** para considerar que la dispersión es demasiado alta (hoy **45 %** del máximo).
- Si existe la columna **Categoría** y coincide con nombres configurados, se pueden usar **umbrales distintos** por categoría (por ejemplo, productos más homogéneos permiten más dispersión o menos, según lo definido en política).

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

Estos textos están pensados para **priorizar** qué líneas lleva compras o pricing a una mesa de revisión.

---

## 11. Parámetros de política (valores actuales de referencia)

Los números siguientes están centralizados en la configuración del programa y pueden ajustarse; aquí figuran **solo como referencia** a la fecha de redacción del documento:

| Tema | Valor de referencia | Idea |
|------|---------------------|------|
| Margen objetivo | 40 % | Meta de margen bruto sobre lista. |
| Tolerancia | 15 puntos | Banda por debajo del objetivo aún aceptable. |
| Piso de margen | 25 % | Objetivo menos tolerancia. |
| Estabilidad USA/BR (relativa) | 20 % | Por debajo de esta brecha relativa, los repos se tratan como estables (junto con tope en dinero). |
| Umbral dispersión inventario (defecto) | 45 % | Máxima dispersión aceptable entre min y max de inventario. |
| Alineación inventario–repos (excepción) | 20 % | Si el máximo de bodega está cerca de los repos, puede usarse reposición pese a dispersión. |
| Ratio de conflicto de precios | 2× | Si el caro cuesta el doble o más que el barato y el barato tiene poca disponibilidad, se bloquea. |
| Disponibilidad mínima en origen barato (conflicto) | 5 unidades | Por debajo, con ratio alto, entra conflicto. |
| Divergencia extrema repos + disponibilidad baja | 55 % | Si ambos repos están muy lejos y ninguno cumple disponibilidad mínima del tramo. |
| Divergencia que sugiere revisión (inestable) | 35 % | Aviso en nota si los repos están muy separados. |
| “Mucho stock” en tramo mínimo | ≥ 50 unidades | Dispara aviso en contexto de tramos distintos de costo. |
| Inventario alto con repos externos flojos | inventario > 30 unidades | Combinado con disponibilidad externa bajo umbral. |

Los **pisos de disponibilidad** por tramo de precio de lista combinan valores fijos (por ejemplo **5, 8, 10 y 15** unidades según cuartil de precio) con información del propio archivo (distribución de disponibilidades), de modo que el umbral **se adapta al lote** sin dejar de tener mínimos claros.

---

## 12. Salida y modo auditoría

El archivo de salida **conserva las columnas de entrada** (en un orden fijado por la herramienta) y **añade** columnas de resultado: estado, código, mejor costo, origen, margen, cumplimiento de política, lista en rango, nota y confianza.

Si se activa el modo **auditoría**, se añade una columna breve con **trazas internas** para quien deba contrastar la decisión sin releer toda la lógica.

---

## 13. Limitaciones que conviene tener presentes

- Los resultados dependen de la **calidad y consistencia** de los datos de entrada (costos, disponibilidades y precio de lista).
- Los umbrales son **políticos y configurables**: deben alinearse con la estrategia comercial y de abastecimiento de la empresa.
- Un resultado **calculado** no implica que el precio sea el óptimo de mercado; solo indica coherencia interna frente a los costos y reglas cargadas.

---

*Documento alineado con la lógica del monitor en la carpeta `06_MEJOR_PRECIO`. Si se cambian parámetros en el código, conviene actualizar la sección 11.*
