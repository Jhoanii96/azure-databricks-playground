# Databricks notebook source
import multiprocessing
from delta.tables import *
from pyspark.sql.functions import *
from multiprocessing.pool import ThreadPool
from multiprocessing import cpu_count

# COMMAND ----------

dbutils.widgets.text("SiteName", "", "Site Name")
dbutils.widgets.text("IngestionGroup", "", "Ingestion Group")
dbutils.widgets.text("ActualDateTime", "", "Actual Date Time")

# COMMAND ----------

site_name = dbutils.widgets.get("SiteName")
ingestion_group = dbutils.widgets.get("IngestionGroup")
actual_date_time = dbutils.widgets.get("ActualDateTime")
fullcopy_notebook = '../SQL FullCopy Load'
incremental_notebook = '../SQL Incremental Load'

print('Site Name: ' + site_name)
print('Ingestion Group: ' + ingestion_group)
print('Actual DateTime: ' + actual_date_time)

# COMMAND ----------

# DBTITLE 1,Load Tables
url = dbutils.secrets.get(scope="AkvSecretScope01", key='azure-sql-dw-conn-str')
df_DataPipeline = spark.read.jdbc(url, "[KPI].[DataPipeline]")
df_DataPipeline = df_DataPipeline.filter(df_DataPipeline.DeletedSetDate.isNull())
df_DataPipeline = df_DataPipeline.filter(df_DataPipeline.Site == site_name)
df_DataPipeline = df_DataPipeline.filter(df_DataPipeline.IngestionGroup == ingestion_group)
display(df_DataPipeline)

# COMMAND ----------

df_count = df_DataPipeline.count()
if df_count > 0:
  pool = ThreadPool(df_count)
  print('Number of processors: ' + str(df_count))

# COMMAND ----------

if df_count > 0:
  data_datapipeline = df_DataPipeline.collect()
  pool.map(lambda i: 
           dbutils.notebook.run(fullcopy_notebook, 3600, { "ParquetPath" : 'raw/' + i.DestinationFolder + '/' + i.TableSourceName + '/' + actual_date_time + '/' + i.TableSourceName + '.parquet', "Schema": str(i.TargetSchema or ''), "TableName": str(i.TableSourceName or ''), "DatabaseTargetName": str(i.DatabaseTargetName or '') }) if (i.IngestionType == "FullCopy") else dbutils.notebook.run(incremental_notebook, 3600 , { "ParquetPath" : 'raw/' + i.DestinationFolder + '/' + i.TableSourceName + '/' + actual_date_time + '/' + i.TableSourceName + '.parquet', "Schema": str(i.TargetSchema or ''), "Tablename": str(i.TableSourceName or ''), "Watermark": str(i.WatermarkVal or ''), "ColumnId": str(i.ColumnId or ''), "DatabaseTargetName": str(i.DatabaseTargetName or ''), "IngestionType": str(i.IngestionType or ''), "IncrementalColumn": str(i.IncrementalColumn or '') })
           , data_datapipeline)

# COMMAND ----------

if df_count > 0:
  pool.terminate()