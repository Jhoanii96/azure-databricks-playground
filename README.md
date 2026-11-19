# Projects

Collection of databricks and analytics projects focused on Azure Data Factory, Databricks, Microsoft Fabric, SQL Server, R and Power BI.

## [SQL Parallel Ingestion Platform](./sql-parallel-ingestion/)

Azure Data Factory and Databricks pipeline for parallel SQL Server ingestion.

- Dynamically extracts multiple SQL tables concurrently.
- Writes source data to Parquet.
- Loads reusable Delta Lake destinations through Databricks notebooks.
- Uses ADF pipeline definitions and supporting SQL scripts.

## [Excel to Delta Lake Ingestion](./excel-to-delta/)

Metadata-driven workflow for ingesting Excel files from SharePoint.

- Azure Data Factory stages files from SharePoint in Blob Storage.
- Databricks notebooks perform full or incremental ingestion.
- Metadata determines the source-to-target loading configuration.

## [Databricks Cluster Dependencies](./cluster-dependencies/)

Centralized pattern for managing Databricks cluster dependencies.

- Python libraries and ODBC drivers are stored in Unity Catalog Volumes.
- Cluster init scripts install dependencies automatically at startup.
- Provides repeatable and governed dependency setup across clusters.

## [Microsoft Fabric Lakehouse Sensor Heatmap](./fabric-sensor-heatmap/)

Sensor data analysis workflow built with Microsoft Fabric.

- Uses a OneLake shortcut to connect Blob Storage Delta tables.
- Processes sensor data with a Fabric notebook.
- Generates a heatmap for operational analysis.

## [Mining Fleet Cycle Mapping with SQL Server, R and Power BI](./mining-cycle-mapping/)

End-to-end mining fleet haul-cycle visualization.

- SQL Server converts UTM coordinates.
- R and Leaflet render haul routes.
- The compiled custom visual is published through Power BI.