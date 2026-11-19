# Excel to Delta Lake Ingestion

Metadata-driven ingestion workflow for Excel files stored in SharePoint.

## Overview

Azure Data Factory stages Excel files from SharePoint into Blob Storage and invokes Databricks notebooks to load the data into Delta Lake. The workflow supports both full and incremental ingestion.

## Structure

- `adf_pipelines/`: Azure Data Factory pipeline definitions.
- `notebooks/`: Databricks notebooks for full and incremental loading.

