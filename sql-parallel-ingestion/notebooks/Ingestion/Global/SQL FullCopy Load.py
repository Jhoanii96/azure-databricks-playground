# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text("ParquetPath", "", "Parquet Path")
dbutils.widgets.text("Schema", "", "Schema")
dbutils.widgets.text("TableName", "", "Table Name")
dbutils.widgets.text("DatabaseTargetName", "", "Database Target Name")

# COMMAND ----------

ParquetPath = dbutils.widgets.get("ParquetPath")
TableName = dbutils.widgets.get("TableName")
Schema = dbutils.widgets.get("Schema")
DatabaseTargetName = dbutils.widgets.get("DatabaseTargetName")

# COMMAND ----------

if Schema == '' or DatabaseTargetName == '':
  dbutils.notebook.exit("finished reading")

# COMMAND ----------

ParquetPath = '/mnt/' + ParquetPath
Schema_Tablename = "[" + Schema + "].[" + TableName + "]"
ParquetPath,Schema_Tablename

# COMMAND ----------

df = spark.read.parquet(ParquetPath)
parquet_row_count = df.count()
print("Row count: " + str(parquet_row_count))

# COMMAND ----------

column_names = df.schema.names
for column in column_names:
  df = df.withColumnRenamed(column, column.replace('-', '_').replace('(', '_').replace(')', '').replace(' ', '_'))

display(df)

# COMMAND ----------

df_repartitioned = df.repartition(25)

# COMMAND ----------

url = dbutils.secrets.get(scope="AkvSecretScope01", key='azure-sql-dw-conn-str')
df_repartitioned.write.format('jdbc').mode('overwrite').option('url', url).option('dbtable', Schema_Tablename).save()