# Databricks notebook source
## Data Ingestion ##

# COMMAND ----------

import glob
import os
import pyarrow.parquet as pq
import pyodbc
from pyspark.sql.functions import *
from pyspark.sql import DataFrame
from pathlib import Path
from datetime import datetime, timedelta
from delta.tables import *

# COMMAND ----------

dbutils.widgets.text("RawPath", "", "Raw Path")
dbutils.widgets.text("IdRow", "", "Id Row")
dbutils.widgets.text("ColumnId", "", "Column Id")
dbutils.widgets.text("DatetimeDaysAgo", "", "Datetime Days Ago")
dbutils.widgets.text("IncrementalColumn", "", "Incremental Column")
dbutils.widgets.text("WatermarkTable", "", "Watermark Table")
dbutils.widgets.text("DestinationFolder", "", "Destination Folder")

# COMMAND ----------

## Reading parameters from ADF
raw_path = dbutils.widgets.get("RawPath")
id_row = dbutils.widgets.get("IdRow")
column_id = dbutils.widgets.get("ColumnId")
datetime_days_ago = dbutils.widgets.get("DatetimeDaysAgo")
incremental_column = dbutils.widgets.get("IncrementalColumn")
watermark_table = dbutils.widgets.get("WatermarkTable")
destination_folder = dbutils.widgets.get("DestinationFolder")

print('Raw Path: ', raw_path)
print('Id Row: ', id_row)
print('Column Id: ', column_id)
print('Datetime Days Ago: ', datetime_days_ago)
print('Incremental Column: ', incremental_column)
print('Watermark Table: ', watermark_table)
print('Destination Folder: ', destination_folder)

# COMMAND ----------

# Read path location
internal_path = '/dbfs/mnt'
#/dbfs/mnt
root_path0 = internal_path + '/raw'
#/dbfs/mnt/raw
file_path = internal_path + '/' + raw_path
#/dbfs/mnt/{container+directory}/file.parquet
print('File path: ', file_path)

# COMMAND ----------

# Detect if the Delta file exists in the incremental mode
def path_exists(path):
    try:
        dbutils.fs.ls(path)
        return True
    except Exception as e:
        if 'java.io.FileNotFoundException' in str(e):
            return False
        else:
            raise

# COMMAND ----------

# Validate if is datetime format
def isDatetime(datetime_text):
    try:
        datetime.strptime(datetime_text, "%Y-%m-%d %H:%M:%S")
        return True
    except ValueError:
        try:
            datetime.strptime(datetime_text, "%Y-%m-%d %H:%M:%S.%f")
            return True
        except ValueError:
            return False

# COMMAND ----------

# Convert to datetime
def StringToDatetime(datetime_text):
    try:
        todatetime = datetime.strptime(datetime_text, "%Y-%m-%d %H:%M:%S")
        return todatetime
    except ValueError:
        try:
            todatetime = datetime.strptime(datetime_text, "%Y-%m-%d %H:%M:%S.%f")
            return todatetime
        except ValueError:
            return 'null'

# COMMAND ----------

#Detect file extension
file_extension = file_path.split('/')[-1].split('.')[-1]
file_path_load = str(file_path[5:])
new_internal_path = str(internal_path[5:])

#Setup Delta file path
table_name = os.path.basename(file_path_load.replace(file_extension, "delta"))
new_path = new_internal_path + "/bronze/" + destination_folder + "/" + table_name
if path_exists(new_path) == False:
    df_temp = spark.read.format(file_extension).load(file_path_load)
    df_temp.write.format('delta').mode('overwrite').save(new_path)

# Detects Bronze path
bronze_path = new_path
print('Bronze path: ', bronze_path)

# COMMAND ----------

# Row count of Delta table before the merge
old_deltaTable = DeltaTable.forPath(spark, bronze_path)
old_deltaTable.toDF().count()

# COMMAND ----------

# Read parquet from incremental dataset
newIncrementalData = spark.read.parquet('/mnt/' + raw_path)
newIncrementalData.createOrReplaceTempView('IncrementalTable')
count_rows_updates = newIncrementalData.count()

# Detect max value from incremental dataset
if count_rows_updates > 0:
    max_incremental_value = None
    identifiers = ''
    
    try:
        max_incremental_value = str(spark.sql("SELECT max(" + incremental_column + ") AS Result FROM IncrementalTable").first().Result)
    except Exception:
        max_incremental_value = None
  
    list_columns_insert = []
    list_columns_update = []
    for col in newIncrementalData.dtypes:
        list_columns_insert.append((col[0], 'newData.' + col[0]))
        if col[0] not in column_id:
            list_columns_update.append((col[0], 'newData.' + col[0]))

    insert_merge_columns = dict(list_columns_insert)
    update_merge_columns = dict(list_columns_update)
    
    splited_columns_id = column_id.split(',')
    
    for col_id in splited_columns_id:
        identifiers = identifiers + "oldData." + col_id + " = newData." + col_id + " AND "
    
    if identifiers == '':
        identifiers = "oldData." + column_id + " = newData." + column_id
    else:
        identifiers = identifiers.strip()
        identifiers = identifiers[:-3]
        identifiers = identifiers.strip()
        
    print(insert_merge_columns)
    print(update_merge_columns)
    
    # Execute merge
    (old_deltaTable
        .alias("oldData")
        .merge(newIncrementalData.alias("newData"), identifiers)
        .whenMatchedUpdate(set = update_merge_columns)
        .whenNotMatchedInsert(values = insert_merge_columns)
        .execute()
    )
    
spark.catalog.dropTempView('IncrementalTable')
if count_rows_updates > 0:
  print(max_incremental_value)

# COMMAND ----------

# Count total rows after merge
data_parquet = DeltaTable.forPath(spark, bronze_path)
data_parquet.toDF().count()

# COMMAND ----------

# Detect max business key value to setup the new watermark value
watermark_val = ""
if count_rows_updates > 0:
    if datetime_days_ago is None or datetime_days_ago == "":
        if isDatetime(max_incremental_value) == True:
            watermark_val = StringToDatetime(max_incremental_value).strftime("%Y-%m-%d %H:%M:%S")
        else:
            watermark_val = max_incremental_value
    else:
        if isDatetime(max_incremental_value) == True:
            watermark_val = (StringToDatetime(max_incremental_value) - timedelta(days=int(float(datetime_days_ago)))).strftime("%Y-%m-%d %H:%M:%S")
        else:
            watermark_val = max_incremental_value
        
watermark_val

# COMMAND ----------

# Update sql datapipeline
if count_rows_updates > 0:
    url = dbutils.secrets.get(scope="AkvSecretScope01", key='azure-sql-dw-odbc-conn-str')
    try:
        cnxn = pyodbc.connect(url)
        cursor = cnxn.cursor()
        
        print("Watermark table: " + watermark_table)
        print("New watermark value: " + watermark_val)
        print("Data Id: " + str(id_row))
        
        sqlStatement = "UPDATE " + watermark_table + " SET [WatermarkVal] = '" + watermark_val + "' WHERE [DataPipelineId] = " + id_row + ";"
        
        # Execute query
        cursor.execute(sqlStatement).commit()
        print("Row successfully updated")
        
        cnxn.close()
    except Exception as e:
        print(e)