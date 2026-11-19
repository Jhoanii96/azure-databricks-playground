# SQL Parallel Ingestion Platform

**Etiqueta: Producción (patrón reutilizable).** Ingesta paralela de SQL Server/Oracle con Azure Data Factory (ADF), Databricks y Delta Lake. El diseño separa orquestación, extracción, transformación y publicación para poder cambiar de fuente sin duplicar notebooks.

## Arquitectura

```text
SQL Server / Oracle
        |
        v
ADF metadata + ForEach (parallelism=6)
        |                     \
        v                      v
Parquet Bronze ---------> Databricks notebooks
                               |
                               v
                    Delta Silver (full/incremental)
                               |
                               v
                    QA + consumo analítico
```

## Qué demuestra

- `PL_Ingestion_004_SqlParallel.json`: pipeline ADF parametrizado con actividades paralelas.
- `notebooks/Ingestion/Global/SQL FullCopy Load.py`: carga inicial.
- `notebooks/Ingestion/Global/SQL Incremental Load.py`: lectura por watermark.
- `notebooks/Ingestion/Global/Delta Incremental Load.py`: merge idempotente a Delta.
- `notebooks/Ingestion/Global/ClusterIngestion/Cluster Ingestion.py`: composición de las etapas SQL y Delta.
- `docs/PL_Ingestion_004_SqlParallel.png`: vista resumida del pipeline.

Los notebooks anteriores son las piezas demostrativas; el resto del árbol contiene variantes de soporte y QA, no una lista de entregables independientes.

## Requisitos

- ADF con linked services a SQL/Oracle, ADLS Gen2 y Databricks.
- Databricks Runtime con Delta Lake y permisos de escritura en Bronze/Silver.
- Una tabla fuente con clave estable y columna `updated_at` para incremental.
- Secretos en Key Vault/secret scope.

## Parámetros

El contrato mínimo del pipeline es `SiteName`, `IngestionGroup`, `ActualDateTime`, `WatermarkSchema` y `WatermarkTable`. La metadata debe incluir `source_table`, `target_table`, `load_type`, `primary_key` y `watermark_column`.

Ejemplo de configuración ficticia:

```json
{
  "source_table": "sales.orders",
  "target_table": "silver.orders",
  "load_type": "incremental",
  "primary_key": "order_id",
  "watermark_column": "updated_at"
}
```

## Ejecución reproducible

1. Importa los notebooks en un workspace Databricks y configura el linked service de ADF.
2. Crea `sales.orders` con 100000 filas sintéticas y `updated_at`; usa `order_id` como clave.
3. Publica el JSON de `adf_pipelines/` y ejecuta con `IngestionGroup=demo`.
4. Ejecuta una segunda vez con 2500 filas modificadas para comprobar el incremental.
5. Valida en Delta: conteo, unicidad de `order_id`, watermark máximo y ausencia de duplicados.

Para generar datos sin exponer información real:

```sql
CREATE TABLE sales.orders AS
SELECT id AS order_id, id % 50 AS customer_id,
       CAST('2026-01-01' AS datetime2) AS updated_at
FROM GENERATE_SERIES(1, 100000) AS g(id);
```

## Pruebas y manejo de errores

- **Contrato:** rechazar tablas sin clave o watermark y registrar la tabla en `ingestion_errors`.
- **Idempotencia:** repetir el mismo watermark no debe aumentar el número de filas.
- **Fallo parcial:** una tabla fallida no debe ocultar el estado de las demás; ADF conserva `run_id`, tabla y mensaje.
- **Reintento:** reintentar solo la actividad fallida después de corregir conectividad.
- **QA:** `Data QA.py` valida conteos, nulls y duplicados antes de promover.

## Métricas de benchmark (ficticias y reproducibles)

Con 12 tablas y 2,000,000 filas (aprox. 18 GB Parquet), `parallelism=6` produce una duración objetivo de **22 min**, frente a **105 min** secuencial: 4.8x de mejora. El coste estimado del escenario es **58% menor** al apagar el cluster tras el job y usar cargas incrementales posteriores. Son valores de referencia; mídelo con `run_id`, filas, bytes, duración y DBU.

## Límites y evolución

La estrategia es batch, no streaming. Para mayor escala se puede particionar por fecha, controlar límites de origen y añadir una tabla de auditoría con `run_id`, `source_count`, `target_count`, `started_at`, `finished_at` y `status`.
