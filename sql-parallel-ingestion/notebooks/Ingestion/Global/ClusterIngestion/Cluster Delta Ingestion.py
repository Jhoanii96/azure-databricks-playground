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
dbutils.widgets.text("WatermarkSchema", "", "Watermark Schema")
dbutils.widgets.text("WatermarkTable", "", "Watermark Table")

# COMMAND ----------

site_name = dbutils.widgets.get("SiteName")
ingestion_group = dbutils.widgets.get("IngestionGroup")
actual_date_time = dbutils.widgets.get("ActualDateTime")
watermark_schema = dbutils.widgets.get("WatermarkSchema")
watermark_table = dbutils.widgets.get("WatermarkTable")
fullcopy_notebook = '../Delta FullCopy Load'
incremental_notebook = '../Delta Incremental Load'

print('Site Name: ' + site_name)
print('Ingestion Group: ' + ingestion_group)
print('Actual DateTime: ' + actual_date_time)
print('Watermark Schema: ' + watermark_schema)
print('Watermark Table: ' + watermark_table)
watermark_tablename = watermark_schema + '.' + watermark_table
print('Watermark Table Name: ' + watermark_tablename)

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
           dbutils.notebook.run(fullcopy_notebook, 3600, { "RawPath" : 'raw/' + i.DestinationFolder + '/' + i.TableSourceName + '/' + actual_date_time + '/' + i.TableSourceName + '.parquet', "DestinationFolder": str(i.DestinationFolder or '') }) if (i.IngestionType == "FullCopy") else dbutils.notebook.run(incremental_notebook, 3600 , { "RawPath" : 'raw/' + i.DestinationFolder + '/' + i.TableSourceName + '/' + actual_date_time + '/' + i.TableSourceName + '.parquet', "IdRow": str(i.DataPipelineId or ''), "ColumnId": str(i.ColumnId or ''), "DatetimeDaysAgo": str(i.DatetimeDaysAgo or ''), "IncrementalColumn": str(i.IncrementalColumn or ''), "WatermarkTable": watermark_tablename, "DestinationFolder": str(i.DestinationFolder or '') })
           , data_datapipeline)

# COMMAND ----------

if df_count > 0:
  pool.terminate()