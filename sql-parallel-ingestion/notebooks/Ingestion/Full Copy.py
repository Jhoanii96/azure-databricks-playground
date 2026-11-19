# Databricks notebook source
import os
from delta.tables import *

# COMMAND ----------

raw_path = dbutils.widgets.get("RawPath")
destination_folder = dbutils.widgets.get("DestinationFolder")

print('Raw Path: ', raw_path)
print('Destination Folder: ', destination_folder)

# COMMAND ----------

#root_path = '/dbfs/mnt/raw-v2/PV6/'
root_path = '/dbfs/mnt/'
file_path = root_path + raw_path
file_path

# COMMAND ----------

file_extension = file_path.split('/')[-1].split('.')[-1]
file_path_load = str(file_path[5:])
new_root_path = str(root_path[5:])
table_name = os.path.basename(file_path_load.replace(file_extension, "delta"))
new_path = new_root_path + "bronze/" + destination_folder + "/" + table_name
file_extension,file_path_load,new_path

# COMMAND ----------

df_temp = spark.read.format(file_extension).load(file_path_load)
df_temp.write.format('delta').mode('overwrite').option("overwriteSchema","True").save(new_path)

# COMMAND ----------

data_delta_new = DeltaTable.forPath(spark, new_path)
data_delta_new.toDF().count()