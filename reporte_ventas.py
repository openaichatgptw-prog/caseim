from bokeh.layouts import column, row
from bokeh.models import ColumnDataSource, TapTool, CustomJS
from bokeh.models import HoverTool
from bokeh.plotting import figure, output_file, save
import pandas as pd
import numpy as np

# ============================================
# 1. Datos simulados
# ============================================
np.random.seed(42)

fechas = pd.date_range("2024-01-01", periods=180)
categorias = ["Repuestos", "Servicios", "Equipos"]

df = pd.DataFrame({
    "Fecha": fechas,
    "Ventas": np.random.randint(100, 700, len(fechas)),
    "Categoria": np.random.choice(categorias, len(fechas))
})

# base completa para filtros
full = df.copy()

# agrupación para las barras
df_cat = df.groupby("Categoria")["Ventas"].sum().reset_index()

# columnas fuentes
src_full = ColumnDataSource(full)
src_cat = ColumnDataSource(df_cat)
src_filtered = ColumnDataSource(full.copy())

# ============================================
# 2. Gráfico de barras (clicable)
# ============================================
p_cat = figure(
    title="Click para filtrar por categoría",
    x_range=df_cat["Categoria"],
    width=400, height=300,
    tools="tap"
)
p_cat.vbar(x="Categoria", top="Ventas", width=0.8, source=src_cat)
p_cat.add_tools(HoverTool(tooltips=[("Categoria", "@Categoria"), ("Ventas", "@Ventas")]))

# ============================================
# 3. Otras 3 gráficas que reaccionan al clic
# ============================================

# Línea
p_line = figure(title="Ventas Diarias", x_axis_type="datetime", width=400, height=300)
p_line.line("Fecha", "Ventas", source=src_filtered, line_width=2)

# Dispersión
p_scatter = figure(title="Scatter Ventas", width=400, height=300)
p_scatter.circle("Ventas", "Fecha", source=src_filtered, size=6)

# Histograma (simple: barras verticales)
p_hist = figure(title="Histograma Ventas", width=400, height=300)
p_hist.vbar(x="Ventas", top=1, width=5, source=src_filtered)

# ============================================
# 4. Callback JS: clic en barra = filtrar todo
# ============================================
callback = CustomJS(
    args=dict(
        src_cat=src_cat,
        src_full=src_full,
        src_filtered=src_filtered
    ),
    code="""
    const indices = cb_obj.indices;

    // Si no hay selección, mostrar todo
    if (indices.length === 0) {
        src_filtered.data = {...src_full.data};
        src_filtered.change.emit();
        return;
    }

    // Barra seleccionada
    const index = indices[0];
    const categoria_seleccionada = src_cat.data['Categoria'][index];

    // Filtrar datos completos
    const Fecha = [];
    const Ventas = [];
    const Categoria = [];

    for (let i = 0; i < src_full.data['Categoria'].length; i++) {
        if (src_full.data['Categoria'][i] === categoria_seleccionada) {
            Fecha.push(src_full.data['Fecha'][i]);
            Ventas.push(src_full.data['Ventas'][i]);
            Categoria.push(src_full.data['Categoria'][i]);
        }
    }

    src_filtered.data = {
        Fecha: Fecha,
        Ventas: Ventas,
        Categoria: Categoria
    };

    src_filtered.change.emit();
"""
)

p_cat.select(TapTool).callback = callback

# ============================================
# 5. Layout + exportar a HTML standalone
# ============================================
dashboard = column(
    p_cat,
    row(p_line, p_scatter),
    p_hist
)

output_file("dashboard_click.html")
save(dashboard)

print("✔ Dashboard generado: dashboard_click.html")
