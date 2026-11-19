# Databricks Cluster Dependencies

**Etiqueta: Producción (patrón operativo).** Configuración reproducible de librerías Python y drivers ODBC mediante Unity Catalog Volumes e init scripts. El objetivo es que un cluster nuevo tenga el mismo runtime que uno existente sin instalaciones manuales.

## Arquitectura

```text
Wheel / .deb / script versionados
              |
              v
Unity Catalog Volume (gobernado)
              |
              v
Cluster init script ---> Python + ODBC runtime
              |
              v
Jobs y notebooks consumen dependencias
```

## Piezas seleccionadas

- `pyodbc-install.sh`: instalación y configuración del driver.
- `pyodbc-4.0.39-...whl`: wheel de referencia para Python 3.11.
- `openpyxl-3.1.5-...whl`: lectura de Excel.
- `msodbcsql17_...deb`: driver ODBC de SQL Server.

Las versiones son un ejemplo; deben coincidir con el Databricks Runtime y la arquitectura del cluster.

## Requisitos

- Workspace con Unity Catalog, Volume dedicado y permisos `READ VOLUME`.
- Cluster compatible con la versión de Python de los wheels.
- Identidad administrada o secret scope para conexiones, sin credenciales en el script.
- Prueba de conectividad al destino SQL Server desde la subred autorizada.

## Configuración y ejecución

1. Crea un Volume, por ejemplo `main.shared.dependencies`, con acceso de escritura restringido.
2. Sube los wheels, el `.deb` y `pyodbc-install.sh`; fija versiones y registra un checksum.
3. En **Compute > Advanced > Init scripts**, selecciona el script desde el Volume.
4. Reinicia el cluster y valida el log de init antes de ejecutar notebooks.
5. Ejecuta un smoke test: `import openpyxl, pyodbc` y una consulta `SELECT 1`.

Ejemplo de parámetros operativos:

```text
DEPENDENCY_VOLUME=/Volumes/main/shared/dependencies
PYTHON_WHEEL=pyodbc-4.0.39-cp311-cp311-*.whl
ODBC_PACKAGE=msodbcsql17_17.10.1.1-1_amd64.deb
```

## Pruebas y manejo de errores

- Validar existencia, checksum y compatibilidad de cada artefacto antes de instalar.
- Hacer que el script termine con código distinto de cero si falla `apt`, `pip` o la configuración ODBC.
- Guardar logs de instalación por cluster y bloquear jobs si el smoke test no pasa.
- Probar un cluster limpio y un reinicio; ambos deben producir el mismo conjunto de versiones.
- Actualizar una dependencia en una ruta versionada y promoverla después de la prueba, nunca sobrescribir silenciosamente.

## Métricas de benchmark (ficticias y reproducibles)

En 10 clusters, la instalación centralizada reduce el onboarding de **30 a 8 min por cluster** y elimina aproximadamente **220 min** de trabajo manual por ciclo. Con un init script de 2 minutos y artefactos cacheados, el objetivo es un **95% de instalaciones consistentes** en el smoke test. Mide duración, versión instalada, exit code y número de reinicios.

## Seguridad y mantenimiento

El Volume debe seguir las reglas de Unity Catalog y mínimo privilegio. No uses `dbutils.secrets.get` para imprimir valores ni subas certificados o cadenas de conexión. Revisa las versiones por runtime antes de actualizar `pyodbc`, el driver o los `.deb`.
