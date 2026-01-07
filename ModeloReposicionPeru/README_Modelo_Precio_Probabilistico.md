# README – Modelo de Precio de Reposición Probabilístico Multiorigen

## 1. Objetivo del modelo
Determinar un **precio de reposición estable, prudente y defendible**, combinando precios y disponibilidades de múltiples orígenes (USA, BR, EURO), controlando explícitamente:
- Volatilidad de precios
- Concentración de compra en un solo origen
- Sobreestimación de disponibilidad real
- Complejidad operativa innecesaria

El modelo prioriza **estabilidad y consistencia temporal** sobre la minimización puntual del costo.

---

## 2. Variables base
- **T, U, V**: disponibilidad por origen (USA, BR, EURO)
- **W = T + U + V**: disponibilidad total
- **X, Y, Z**: precios por origen
- **E**: demanda
- **Qᵢ**: cantidad asignada al origen *i*

---

## 3. Indicador de dispersión de precios (D)

### Definición
Mide la **desalineación relativa de precios** entre orígenes.

D = σ(Pᵢ) / μ(Pᵢ)

### Impacto
- No asigna cantidades
- Alimenta la decisión de activar prudencia

---

## 4. Régimen prudente (AD2 / AE2)

### AD2 – Volatilidad tolerable
Se activa cuando:
D > AD2

### AE2 – Spread extremo
Se activa cuando:
Max(Pᵢ) / Min(Pᵢ) > 1 + AE2

### Impacto
La activación de cualquiera de los dos **cambia las reglas de asignación**, no detiene el modelo.

---

## 5. Reglas de disponibilidad

### G2 / G3 / G4 – Acceso real al stock
Reducen la disponibilidad reportada por origen desde el inicio.

Disponibilidad válidaᵢ = Disponibilidad reportadaᵢ × Gᵢ

---

### AI2 – Stock mínimo operativo
Si la disponibilidad válida de un origen es menor a AI2:
- El origen queda excluido del reparto
- No participa en el promedio de precios

---

## 6. Reglas de asignación de cantidades

### AH2 – Exposición máxima por origen
Actúa **solo bajo régimen prudente**.

Qᵢ ≤ Disponibilidadᵢ × AH2

Evita concentración total de la demanda en un solo origen.

---

### AB2 – Participación mínima
Elimina orígenes cuya participación es marginal respecto a la disponibilidad total.

Si:
Disponibilidadᵢ / W < AB2  
→ el origen no participa.

Simplifica el reparto y evita ruido operativo.

---

### AF2 – Suficiencia de stock vs demanda
AF2 **no es precio** y **no adelanta orígenes**.

Actúa cuando un origen, ya evaluado por orden de precio, tiene stock holgado:

Disponibilidadᵢ ≥ E × (1 + AF2)

#### Impacto real
- Habilita cubrir la demanda directamente desde ese origen
- Evita reparto innecesario
- **No fuerza compras a precios no óptimos**
- El orden por precio siempre se respeta

---

## 7. Cálculo del precio final

AJ = (Precio promedio ponderado × V2) + (Precio máximo × V3)

- **V2** controla eficiencia/agresividad
- **V3** introduce protección conservadora

---

## 8. Interpretación final
- La volatilidad se mide **solo en precios**
- Las cantidades son mecanismos de control de riesgo
- El modelo adapta su comportamiento según el contexto del mercado
- La decisión es trazable, consistente y auditable