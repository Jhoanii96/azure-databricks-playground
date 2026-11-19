# Databricks notebook source
destination_folder = dbutils.widgets.get("DestinationFolder")
TableName = dbutils.widgets.get("TableName")
Schema = dbutils.widgets.get("Schema")
DatabaseTargetName = dbutils.widgets.get("DatabaseTargetName")

# COMMAND ----------

if DatabaseTargetName == '':
  dbutils.notebook.exit("finished reading")

# COMMAND ----------

root_path = '/mnt/bronze/'
file_path = root_path+destination_folder+"/"+TableName+".delta"
file_path

# COMMAND ----------

df = spark.read.format('delta').load(file_path)

# COMMAND ----------

url = dbutils.secrets.get(scope="AkvSecretScope01", key='azure-sql-dw-conn-str')

# COMMAND ----------

dbtable = Schema+"."+TableName
dbtable

# COMMAND ----------

df.write.format('jdbc').mode('overwrite').option('url', url).option('dbtable', dbtable).save()