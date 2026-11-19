# Excel to Delta Lake Ingestion

**Etiqueta: Producción (patrón reutilizable).** Framework metadata-driven para llevar Excel desde SharePoint a Blob Storage y convertirlo en tablas Delta mediante Azure Data Factory y Databricks. Soporta carga completa e incremental sin codificar una pipeline por archivo.

## Arquitectura

```text
CSV de configuración
        |
        v
ADF filtra IngestionGroup
        |
        v
SharePoint Excel --copy--> Blob staging --invoke--> Databricks
                                                   |
                               full copy o merge por clave/watermark
                                                   v
                                             Delta Lake
```

## Piezas demostrativas

- `adf_pipelines/Azure Data Factory - SharePoint to Blob - Read All Folder.json`: descubrimiento y staging.
- `notebooks/Delta FullCopy Ingestion.ipynb`: creación inicial de la tabla.
- `notebooks/Delta Incremental Ingestion.ipynb`: merge de cambios.
- `notebooks/Functions.ipynb`: funciones de lectura y normalización.
- `notebooks/Cluster Ingestion.ipynb`: entrada parametrizada para orquestación.

La configuración es la pieza central; los notebooks se muestran como implementación de etapas, no como entregables aislados.

## Requisitos y parámetros

- SharePoint con permisos de lectura, ADF, Blob Storage y workspace Databricks.
- Unity Catalog o una ruta Delta con permisos de escritura.
- CSV con `site`, `file_path`, `worksheet`, `range`, `header`, `key_column`, `watermark_column`, `target_table`, `load_type` e `ingestion_group`.
- Secretos en Key Vault/linked service, nunca en el CSV.

Ejemplo ficticio:

```csv
site,file_path,worksheet,range,header,key_column,watermark_column,target_table,load_type,ingestion_group
demo,/Shared/demo/orders.xlsx,Orders,A1:F1000,true,order_id,updated_at,silver.orders,incremental,demo
```

## Ejecución reproducible

1. Genera `orders.xlsx` y `customers.xlsx` con 1000 filas cada uno y las columnas declaradas en el CSV.
2. Sube ambos archivos a una carpeta SharePoint de prueba.
3. Configura `IngestionGroup=demo`, la ruta Blob de staging y el catálogo/esquema Delta.
4. Ejecuta el pipeline ADF; primero se copiará el archivo y después se invocará el notebook correspondiente.
5. Modifica 50 filas, agrega 25 y cambia `updated_at`; vuelve a ejecutar el incremental.
6. Comprueba que la tabla final contiene 1025 claves y conserva una auditoría por archivo.

## Pruebas y errores

- Rechazar extensiones distintas de `.xlsx` y hojas/rangos inexistentes con un mensaje accionable.
- Validar encabezados, tipos, clave no nula y duplicados antes del merge.
- Registrar `file_path`, `etag`, tamaño, filas leídas, filas escritas, watermark y `run_id`.
- Hacer el merge idempotente usando `key_column`; un reintento del mismo `etag` no duplica filas.
- Enviar a `quarantine/` los archivos inválidos y marcar el lote como `Failed`, sin publicar una tabla parcial.

## Métricas de benchmark (ficticias y reproducibles)

Para 3 libros, 30 hojas y 120,000 filas (aprox. 240 MB), con `ForEach` de 4 y un cluster pequeño, el objetivo es **7 min** frente a **19 min** secuencial. El staging incremental evita reprocesar un 80% de bytes en ejecuciones diarias; el ahorro estimado es **42%**. Mide los resultados con `etag`, bytes, filas, duración y número de hojas.

## Seguridad y operación

Usa identidad administrada/Key Vault, limita el acceso a la carpeta de staging y aplica retención a `quarantine`. Promueve cambios de metadata por entorno; no guardes tokens ni archivos reales en el repositorio.
