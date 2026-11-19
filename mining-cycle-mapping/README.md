# Mining Fleet Cycle Mapping with SQL Server, R and Power BI

**Etiqueta: Personal / demostración.** Flujo end-to-end para transformar coordenadas UTM de ciclos de acarreo en SQL Server, construir rutas con R/Leaflet y publicar una visualización operacional en Power BI. Une modelado relacional, geoespacial y consumo analítico en una sola pieza.

## Arquitectura

```text
SQL Server: ciclos + ubicaciones
             |
             v
fn_utm2latlong + vista de año actual
             |
             v
R + Leaflet (rutas, puntos, leyenda)
             |
             v
Power BI custom visual / app
```

## Piezas seleccionadas

- `fn_utm2latlong.sql`: función de conversión UTM a latitud/longitud.
- `sp_hist_cycle_hp_current_year_phase.sql`: procedimiento de preparación.
- `CDPV6-sp_hist_cycle_hp_current_year_phase.sql`: variante del procedimiento.
- El script R y los artefactos de Power BI se presentan en el catálogo del proyecto (`projects.js`); este README se concentra en el flujo y la reproducibilidad.

## Requisitos

- SQL Server con tablas de ciclos que incluyan zona UTM, hemisferio, este, norte, origen/destino y toneladas.
- R 4.x con `DBI`, `odbc`, `dplyr`, `leaflet` y `htmlwidgets`.
- Power BI Desktop y, para publicar, permisos sobre un workspace.
- Un DSN o cadena de conexión administrada por el entorno; no se guarda en el código.

## Dataset ficticio reproducible

Construye 50,000 ciclos con 20 camiones, 8 ubicaciones y 7 días de datos. Genera coordenadas UTM dentro de una caja de 5 km, una fase (`load`, `haul`, `dump`) y toneladas entre 80 y 240. Usa una semilla fija para que la misma ejecución produzca el mismo conjunto.

Parámetros del flujo:

```text
@year=2026
@site_zone=19
@hemisphere='S'
@from_date='2026-01-01'
@to_date='2026-01-08'
```

## Ejecución

1. Crea un esquema de demostración y carga las filas sintéticas.
2. Despliega `fn_utm2latlong.sql` y el procedimiento seleccionado.
3. Ejecuta la vista/procedimiento con las fechas y zona del dataset.
4. En R, consulta solo las columnas preparadas, convierte a `sf` y dibuja rutas y marcadores.
5. Abre el resultado en Power BI, valida filtros por camión/fase y publica solo si las pruebas pasan.

## Pruebas y manejo de errores

- Rechazar coordenadas fuera del rango UTM o zonas/hemisferios no configurados.
- Validar que cada ciclo tenga origen, destino y `cycle_id` único.
- Comparar filas SQL con filas entregadas a R y contar ciclos descartados.
- Sustituir errores de conexión por un mensaje de diagnóstico; no devolver un mapa vacío como éxito.
- Probar un caso con coordenada nula, una fase desconocida y un ciclo duplicado.

## Métricas de benchmark (ficticias y reproducibles)

Con 50,000 ciclos y 20 camiones, el objetivo es que SQL prepare los datos en **12 s**, R renderice **50,000 puntos en menos de 8 s** y la vista filtrada de Power BI responda en **menos de 3 s** en un equipo de referencia. La conversión espacial evita repetir lógica en el visual y reduce el dataset entregado al mapa en aproximadamente **35%**. Mide filas, ciclos válidos, descarte, duración SQL/R y tiempo de interacción.

## Decisiones de diseño

La conversión y validación permanecen en SQL Server para que distintos consumidores compartan el mismo contrato. R se reserva para la visualización espacial y Power BI para filtros, distribución y publicación. En producción añadiría índices sobre fecha, `truck_id` y `cycle_id`, además de límites de puntos por consulta.
