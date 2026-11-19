# Databricks Course Code Hub

**Etiqueta: Demostración / formación.** Espacio de ejercicios para practicar notebooks, transformaciones Spark y patrones de carga en Databricks. No representa una plataforma productiva por sí solo; sirve como laboratorio aislado para validar ideas antes de llevarlas a los proyectos principales.

## Arquitectura de aprendizaje

```text
Dataset sintético
       |
       v
Notebook Databricks / Spark
       |
       v
Transformación + validación
       |
       v
Tabla Delta de laboratorio
```

## Requisitos

- Workspace Databricks con un cluster pequeño o serverless.
- Runtime con Apache Spark y Delta Lake.
- Git/Repos para versionar notebooks.
- Dataset sintético o público sin información personal.

## Ejecución reproducible

1. Importa el contenido de este directorio como Repo.
2. Crea un catálogo/esquema de laboratorio y define la ruta de almacenamiento.
3. Configura parámetros como `input_path`, `target_table`, `run_date` y `seed`.
4. Ejecuta las celdas en orden y revisa el esquema antes de escribir.
5. Repite con la misma semilla para comprobar que los resultados sean deterministas.

## Pruebas y manejo de errores

- Validar tipos y columnas requeridas antes de transformar.
- Comprobar conteos, claves únicas y valores nulos después de cada etapa.
- Fallar explícitamente si falta la ruta de entrada o el esquema esperado.
- Usar una tabla/ruta temporal para no dejar una Delta parcialmente escrita.

## Métricas de referencia

Con 100,000 filas sintéticas y 8 columnas, el laboratorio debe completar en **menos de 2 min** en un cluster pequeño y producir una tabla de **100,000 claves únicas**. Estos valores son un benchmark didáctico reproducible, no una promesa de rendimiento para cargas productivas.

## Relación con el portfolio

Los patrones maduros de orquestación, incrementalidad y QA se documentan en [SQL Parallel Ingestion](../sql-parallel-ingestion/) y [Excel to Delta Lake](../excel-to-delta/). Aquí se prioriza la experimentación controlada y la comprensión del código, no la operación 24x7.
