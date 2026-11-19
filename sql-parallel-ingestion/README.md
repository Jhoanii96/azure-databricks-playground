# SQL Parallel Ingestion Platform

Azure Data Factory and Databricks pipeline for parallel SQL Server data ingestion.

## Overview

This project uses dynamic, metadata-driven Azure Data Factory pipelines to extract multiple SQL tables concurrently, write the data to Parquet, and load it into Delta Lake tables through reusable Databricks notebooks.

## Structure

- `adf_pipelines/`: Azure Data Factory pipeline definitions.
- `notebooks/`: Databricks notebooks used during ingestion.

