# Databricks notebook source
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

cluster_sql_notebook = 'Cluster SQL Ingestion'
cluster_delta_notebook = 'Cluster Delta Ingestion'

print('Site Name: ' + site_name)
print('Ingestion Group: ' + ingestion_group)
print('Actual DateTime: ' + actual_date_time)
print('Watermark Schema: ' + watermark_schema)
print('Watermark Table: ' + watermark_table)

# COMMAND ----------

# DBTITLE 1,Cluster SQL Notebook
dbutils.notebook.run(cluster_sql_notebook, 3600, { "SiteName" : site_name, "IngestionGroup": ingestion_group, "ActualDateTime": actual_date_time }) 

# COMMAND ----------

# DBTITLE 1,Cluster Delta Notebook
dbutils.notebook.run(cluster_delta_notebook, 3600, { "SiteName" : site_name, "IngestionGroup": ingestion_group, "ActualDateTime": actual_date_time, "WatermarkSchema": watermark_schema, "WatermarkTable": watermark_table }) 