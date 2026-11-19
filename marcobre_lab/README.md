# Marcobre Lab - Plataforma Bronze, Silver y Gold

**Etiqueta: Producción (patrón reutilizable).** Plataforma de ingeniería de datos en Databricks para procesar información operacional de Marcobre desde archivos Excel y fuentes SQL hasta tablas Delta gobernadas en Unity Catalog. El flujo separa ingesta, curación y consumo funcional para que cada etapa pueda validarse y reprocesarse de forma independiente.

## Arquitectura

```text
Excel / SQL / File Server
          |
          v
  Blob Storage: prelanding
          |
          v
 Bronze landing (Delta + metadata)
          |
          v
 Silver: hist / dia / dataset_master
          |
          v
 Gold: configuraciones + KPI
          |
          v
 Reporting / Power BI / consumo analítico
```

### Capas y responsabilidades

| Capa | Ubicación lógica | Responsabilidad |
| --- | --- | --- |
| **Bronze pre-landing** | `{environment}_bronze_prelanding_minsur` | Recibir archivos y conservar la fuente antes de transformar. |
| **Bronze landing** | `{environment}_bronze_landing_minsur` | Interpretar Excel/SQL, normalizar nombres y persistir Delta con evolución de esquema. |
| **Silver** | `{environment}_silver_curated_minsur` | Limpiar tipos, fechas y columnas; generar datasets históricos, diarios y consolidados. |
| **Gold** | `{environment}_gold_functional_minsur` | Aplicar configuraciones de escenario y producir tablas funcionales para KPI. |

`environment` se obtiene desde el secret scope y evita cambiar el código entre `dev`, `test` y `prod`.

## Estructura relevante

```text
Principal/
├── Ingesta/
│   ├── Funciones/Ingesta Funciones.ipynb
│   └── Procesos/
│       ├── Leer Configuracion.ipynb
│       ├── Leer Configuracion SQL.ipynb
│       ├── CSV Configuration.ipynb
│       ├── Ingesta Completa.ipynb
│       └── Ingesta Incremental*.ipynb
├── Silver/
│   ├── Parametros.ipynb
│   ├── Silver dataset_master*.ipynb
├── Gold/
│   ├── Parametros.ipynb
│   ├── Funciones.ipynb
│   ├── Transformacion/...
│   └── Transformacion Comentario/...
├── Librerias/
└── Utilidades/
    ├── Mounting.ipynb
    └── Prueba Secret Scope.ipynb
```

## Dependencias del cluster

El directorio `Principal/Librerias` contiene los artefactos de referencia para un cluster con Python 3.11:

- `pandas` y `numpy`: lectura, normalización y transformación intermedia.
- `openpyxl-3.1.5-...whl`: lectura de `.xlsx`, `.xlsm` y rangos complejos.
- `pyodbc-4.0.39-...whl`: conectividad ODBC desde Python.
- `msodbcsql17_...deb`: driver Microsoft ODBC para SQL Server.
- `delta-spark`: operaciones Delta (`saveAsTable`, `DeltaTable`, merge); debe ser compatible con el Databricks Runtime.
- `python-dateutil`: parseo de fechas y watermarks.
- `pyspark`: provisto por el runtime.

### Preinstalación al crear o iniciar el cluster

Configura las librerías desde un Unity Catalog Volume versionado o desde la ruta de Repo. No instales paquetes manualmente en una sesión interactiva.

```text
/Volumes/<catalog>/<schema>/dependencies/openpyxl-3.1.5-py2.py3-none-any.whl
/Volumes/<catalog>/<schema>/dependencies/pyodbc-4.0.39-cp311-*.whl
/Volumes/<catalog>/<schema>/dependencies/msodbcsql17_17.10.1.1-1_amd64.deb
```

El `.deb` y el wheel de `pyodbc` deben coincidir con el sistema operativo y la versión de Python del Runtime. Para un init script, instala el driver antes del wheel y termina con código distinto de cero si falla una instalación. Después del arranque ejecuta este smoke test:

```python
import numpy
import pandas
import openpyxl
import pyodbc
from delta.tables import DeltaTable

print("cluster dependencies: OK")
```

Registra en los logs el Runtime, versión de Python, versión de cada paquete y el `exit code`. No guardes credenciales ni secretos dentro del init script.

## Utilidad de mounting

`Principal/Utilidades/Mounting.ipynb` define `mount_blob(container)` y crea:

```text
/mnt/landing
/mnt/prelanding
```

apuntando al storage account `mcstoragelab` mediante `wasbs://<container>@<storage>.blob.core.windows.net`. El notebook evita montar dos veces el mismo punto y muestra `dbutils.fs.mounts()` para validar el resultado.

### Configuración segura recomendada

El notebook contiene el patrón histórico de mounting. Antes de usarlo en un workspace:

1. Sustituye el valor vacío de `fs.azure.account.key...` por una referencia a `dbutils.secrets.get(...)`; nunca escribas la clave en el notebook.
2. Usa un secret scope respaldado por Key Vault y permisos mínimos para el cluster.
3. Parametriza `storage_name`, `containers` y `mount_root` por entorno.
4. Comprueba con `dbutils.fs.ls("/mnt/prelanding")` y `dbutils.fs.ls("/mnt/landing")`.
5. Si el workspace usa Unity Catalog, prioriza Volumes/External Locations sobre mounts DBFS para nuevas cargas; conserva el mounting solo para compatibilidad con el file server existente.

La utilidad `Prueba Secret Scope.ipynb` sirve únicamente para validar acceso. Su salida debe permanecer redactada y no debe imprimir el valor de ningún secreto.

## Ejecución completa

### 1. Preparar workspace

1. Crea catálogos/esquemas para `bronze_prelanding_minsur`, `bronze_landing_minsur`, `silver_curated_minsur` y `gold_functional_minsur`.
2. Configura el secret scope con `Entorno` y las conexiones requeridas.
3. Instala las librerías del cluster y ejecuta el smoke test.
4. Ejecuta `Mounting.ipynb` solo si el workspace mantiene el acceso por mount; para Unity Catalog, valida primero el Volume.

### 2. Ejecutar Bronze

1. `Leer Configuracion.ipynb` o `Leer Configuracion SQL.ipynb` obtiene las reglas de ingesta.
2. `CSV Configuration.ipynb` lee la configuración desde:
   `/Volumes/dev_bronze_prelanding_minsur/marcobre_fileserver/marcobre_fileserver/configuracion/sharepoint_ingesta.csv`.
3. `Ingesta Completa.ipynb` procesa Excel desde pre-landing y escribe tablas Delta en landing.
4. `Ingesta Incremental.ipynb` usa `IdColumns`, `WatermarkColumn` y `WatermarkLookBack` para leer solo cambios.
5. `Ingesta Incremental SQL.ipynb` aplica el mismo contrato a fuentes SQL.

Parámetros principales de los notebooks de ingesta:

```text
Environment, ExcelPath, TableName, SheetName, DataRange,
IndexColumns, CustomColumnNames, ReportDate, ForecastName,
ColumnsToSkip, RowsToSkip, IsHorizontal, MergeNHeaders, Datetime
```

Para incremental también se requieren `Id`, `IdColumns`, `WatermarkColumn` y `WatermarkLookBack`.

### 3. Ejecutar Silver

1. Ejecuta `Silver dataset_master.ipynb` para limpiar y estandarizar el dataset maestro.
2. Ejecuta `Silver dataset_master_hist.ipynb` para el histórico.
3. Ejecuta `Silver dataset_master_dia.ipynb` para el período diario y el corte por `DateKey`.
4. Ejecuta `Silver dataset_master_union.ipynb` para unir `dataset_master_hist` y `dataset_master_dia` mediante `FULL JOIN` por `DateKey`.

La tabla consolidada queda en:

```text
{environment}_silver_curated_minsur.marcobre_fileserver.dataset_master
```

### 4. Ejecutar Gold

1. `Gold/Parametros.ipynb` resuelve los catálogos/esquemas del entorno.
2. `Gold/Transformacion/Configuracion.ipynb` y `Transformacion Comentario/Configuracion Comentario.ipynb` cargan configuraciones de escenarios.
3. `Gold/Transformacion/Tranformacion.ipynb` y su variante de comentarios leen la configuración, calculan la ventana `Dia` y generan las tablas funcionales.
4. `Funciones.ipynb` concentra carga de tablas, widgets, dimensiones y escritura Delta.

Parámetros funcionales principales:

```text
Escenario, Dia, ScenarioName
```

## Pruebas y manejo de errores

- **Bronze:** validar extensión, hoja, rango, encabezados, claves, filas leídas y esquema antes de escribir.
- **Idempotencia:** repetir el mismo `Id`/watermark no debe duplicar registros.
- **Silver:** comparar conteos Bronze/Silver, nulos de `DateKey`, duplicados y cobertura de fechas.
- **Unión:** validar que el `FULL JOIN` no pierda días presentes solo en histórico o solo en diario.
- **Gold:** comprobar que `Escenario` existe, `Dia` es numérico y las tablas de configuración tienen esquema válido.
- **Errores:** registrar `run_id`, notebook, entorno, tabla, watermark, filas de entrada/salida, duración y mensaje; no convertir un error en una tabla vacía exitosa.
- **Reintentos:** reintentar la etapa fallida después de corregir la causa, no ejecutar todo el flujo indiscriminadamente.

## Dataset ficticio y benchmark

Para una ejecución reproducible, genera 30 archivos Excel sintéticos con 1,000 filas cada uno, 90 días de histórico y 10 escenarios Gold. Incluye una actualización incremental de 200 filas, una hoja con encabezados multinivel y un archivo inválido para probar `quarantine`.

Métricas objetivo de referencia, no resultados de datos reales:

| Etapa | Volumen ficticio | Objetivo |
| --- | ---: | ---: |
| Bronze full | 30,000 filas / 30 archivos | 6-10 min |
| Bronze incremental | 200 cambios | menos de 2 min |
| Silver | 30,000 filas | menos de 3 min |
| Gold | 10 escenarios | menos de 2 min |

Mide por etapa filas, bytes, archivos, duración, reintentos, registros rechazados y coste del cluster. La principal optimización esperada es no reprocesar archivos sin cambios y ejecutar Silver/Gold solo después de una ingesta Bronze exitosa.

## Alcance

Este laboratorio conserva notebooks de demostración y patrones operativos. Antes de promoverlo a producción se deben externalizar secretos, versionar metadata, añadir una tabla de auditoría, configurar alertas y reemplazar mounts heredados por Unity Catalog External Locations cuando sea posible.
