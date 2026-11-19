# Data Engineering Portfolio

Portfolio de ingeniería de datos orientado a plataformas Azure y analítica operacional. Las piezas muestran cuatro años de experiencia aplicada: ingestión paralela, cargas incrementales, gobierno de clusters, lakehouses y entrega de datos para SQL Server, R y Power BI.

> **Cómo leer este repositorio:** cada proyecto selecciona artefactos demostrativos en vez de listar todos los notebooks. Las etiquetas distinguen el contexto de la pieza:
> - **Producción:** patrón diseñado para operación, con parametrización, observabilidad y controles.
> - **Demostración:** implementación reproducible para explicar una técnica.
> - **Personal:** exploración de una visualización o integración.
>
> Las métricas marcadas como **benchmark reproducible** son escenarios ficticios que pueden ejecutarse ajustando los parámetros; no representan datos confidenciales ni resultados de un cliente.

## Mapa de capacidades

| Proyecto | Capacidad demostrada | Etiqueta |
| --- | --- | --- |
| [SQL Parallel Ingestion](./sql-parallel-ingestion/) | ADF + Databricks, paralelismo, full copy e incremental Delta | Producción |
| [Excel to Delta Lake](./excel-to-delta/) | Excel/SharePoint, metadata-driven ingestion y Delta Lake | Producción |
| [Cluster Dependencies](./cluster-dependencies/) | Unity Catalog Volumes, drivers e init scripts | Producción |
| [Fabric Sensor Heatmap](./fabric-sensor-heatmap/) | Microsoft Fabric Lakehouse, OneLake Shortcut y análisis espacial | Demostración |
| [Mining Cycle Mapping](./mining-cycle-mapping/) | SQL Server, R/Leaflet y Power BI para analítica operacional | Personal / demostración |

## Arquitectura transversal

```text
Fuentes SQL / Excel / sensores
            |
            v
ADF o OneLake Shortcut -----> Databricks / Fabric notebooks
            |                         |
            v                         v
      Blob Storage -------------> Delta Lake / Lakehouse
                                      |
                                      v
                           SQL Server / R / Power BI
```

## Requisitos comunes

- Azure Data Factory y una cuenta de almacenamiento con Blob/ADLS Gen2.
- Databricks Runtime compatible con Delta Lake; Unity Catalog para el proyecto de dependencias.
- Microsoft Fabric para el proyecto Lakehouse.
- Python 3.10+, Jupyter/Databricks notebooks y Git.
- SQL Server, R con `sf`, `leaflet` y `ggplot2`, y Power BI Desktop solo para el proyecto de minería.
- Credenciales mediante Key Vault, secret scopes o variables de entorno; nunca se incluyen secretos en este repositorio.

## Calidad y forma de trabajo

Los patrones incluyen parámetros de entorno, grupos de ingestión, watermarks, validaciones de esquema, reintentos y separación de zonas. Las pruebas recomendadas son:

1. Ejecutar primero con el dataset sintético de cada README.
2. Validar conteos, claves, nulls y duplicados antes de promover a Silver/Gold.
3. Reprocesar el mismo lote para comprobar idempotencia.
4. Forzar un archivo inválido o una fuente no disponible y verificar que el error queda registrado y el lote se marca como fallido.

## Métricas de referencia

El escenario común usa 12 tablas SQL, 3 archivos Excel y 2 millones de lecturas de sensores. Los README de cada proyecto detallan volumen, duración esperada, paralelismo y comparación de coste. Ajusta `rows`, `batch_size`, `parallelism` y `watermark` para reproducir el benchmark en tu capacidad de Azure.

## Orden sugerido

1. Comenzar por [SQL Parallel Ingestion](./sql-parallel-ingestion/) para el patrón de plataforma.
2. Continuar con [Excel to Delta Lake](./excel-to-delta/) para fuentes semiestructuradas.
3. Revisar [Cluster Dependencies](./cluster-dependencies/) para operación y gobierno.
4. Explorar [Fabric Sensor Heatmap](./fabric-sensor-heatmap/) y [Mining Cycle Mapping](./mining-cycle-mapping/) para consumo analítico.
