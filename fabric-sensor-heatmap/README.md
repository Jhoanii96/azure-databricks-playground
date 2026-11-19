# Microsoft Fabric Lakehouse Sensor Heatmap

**Etiqueta: Demostración reproducible.** Flujo de análisis en Microsoft Fabric que expone tablas Delta de Blob Storage mediante un OneLake Shortcut y genera un mapa de intensidad de sensores desde un notebook Lakehouse.

## Arquitectura

```text
Blob Storage / Delta
        |
        v
OneLake Shortcut (sin duplicar datos)
        |
        v
Fabric Lakehouse --> Notebook
                         |
                         v
                 limpieza + agregación
                         |
                         v
                    sensor heatmap
```

## Piezas demostrativas

- `Fabric - Heat Map.ipynb`: notebook principal y resultado de heatmap.
- `Fabric - Brake Temperature Chart.ipynb`: vista temporal complementaria.
- `Fabric - Data Boxplot.ipynb`: detección visual de distribución y outliers.

El mapa es la pieza de presentación; los otros notebooks se usan para validar la calidad y el comportamiento de las mediciones.

## Requisitos

- Workspace Microsoft Fabric con Lakehouse y permiso para crear un Shortcut.
- Cuenta Blob/ADLS con una tabla Delta de lectura.
- Notebook Fabric con Spark y una columna espacial o coordenadas `latitude`, `longitude`, `sensor_value`, `event_time`.

## Dataset ficticio reproducible

Genera 2,000,000 lecturas sintéticas con una cuadrícula de 100 x 100, 20 sensores y una ventana de 24 horas. Añade ruido normal y 1% de outliers; escribe el resultado como Delta en Blob. El esquema mínimo es:

```text
event_time: timestamp | sensor_id: string | latitude: double
longitude: double | sensor_value: double
```

## Ejecución

1. Crea el Lakehouse y el Shortcut hacia la ruta Delta.
2. Abre `Fabric - Heat Map.ipynb` y selecciona la tabla desde el catálogo OneLake.
3. Configura `start_time`, `end_time`, `sensor_ids`, `cell_size` y `value_column`.
4. Ejecuta limpieza, agregación por celda y visualización.
5. Repite con un sensor y una ventana de una hora para comprobar el filtro.

## Pruebas y errores

- Validar rango de latitud/longitud, timestamps no nulos y unidad de `sensor_value`.
- Excluir o marcar valores fuera de límites; no eliminarlos sin contabilizarlos.
- Fallar explícitamente si el Shortcut no existe o la tabla no contiene las columnas requeridas.
- Comparar conteo antes/después, número de outliers y celdas con datos.
- Guardar parámetros de ejecución para que una visualización sea reproducible.

## Métricas de benchmark (ficticias y reproducibles)

Con 2 millones de eventos y `cell_size=0.001`, el objetivo es procesar **18 GB en 4 min** usando el Shortcut, contra una copia completa estimada de **11 min** y almacenamiento duplicado. La agregación debe reducir los puntos visualizados a menos de **10,000 celdas**; mide bytes leídos, filas, celdas, outliers y duración.

## Alcance

El Shortcut evita duplicación, pero no reemplaza gobierno, permisos ni optimización de la fuente. Para producción añadiría una tabla de calidad, particionado por fecha y un pipeline de actualización; este repositorio mantiene el notebook como demostración.
