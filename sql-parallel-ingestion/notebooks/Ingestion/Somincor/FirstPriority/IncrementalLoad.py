# Databricks notebook source
# DBTITLE 1,Libraries
#================
#Import libraries
#================
import os
import pyodbc
from pyspark.sql.functions import *
from pyspark.sql import DataFrame
from delta.tables import *

# COMMAND ----------

# DBTITLE 1,Parameters
## Reading parameters from ADF
folder_root_path = dbutils.widgets.get("FolderRootPath")
#folder_root_path = 'Somincor/HSE'
actual_datetime = dbutils.widgets.get("ActualDateTime")
#actual_datetime = '2022-07-09'
table_name = dbutils.widgets.get("TableName")
#table_name = 'Event'
column_id = dbutils.widgets.get("ColumnId")
#column_id = 'event_id'
row_id = dbutils.widgets.get("RowId")
#row_id = 'event_id'
incremental_column = dbutils.widgets.get("IncrementalColumn")
#incremental_column = 'event_datetime'
watermark_table = dbutils.widgets.get("WatermarkTable")
#watermark_table = 'null'

print('Folder Root Path:', folder_root_path)
print('Actual DateTime:', actual_datetime)
print('Table Name:', table_name)
print('Column Id:', column_id)

# COMMAND ----------

# DBTITLE 1,File Path
## Create raw and bronze with path
full_folder_path = '/mnt/raw/' + folder_root_path + '/' + table_name
bronze_path = '/mnt/bronze/' + folder_root_path + '/' + table_name + '.delta'
print('Full Folder Path:', full_folder_path)
print('Bronze Path:', bronze_path)

# COMMAND ----------

# DBTITLE 1,Functions
# Retrive directories path
def get_dir_content(ls_path):
    dir_paths = dbutils.fs.ls(ls_path)
    subdir_paths = [get_dir_content(p.path) for p in dir_paths if p.isDir() and p.path != ls_path]
    flat_subdir_paths = [p for subdir in subdir_paths for p in subdir]
    return list(map(lambda p: p.path, dir_paths)) + flat_subdir_paths

# COMMAND ----------

# Detect if the Delta file exists
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

# DBTITLE 1,Start Code
## Flag Path if exists set to True
flag_path_exists = True
if path_exists(bronze_path) == False:
    flag_path_exists = False

print(flag_path_exists)

# COMMAND ----------

## If bronze table doesn't exists then execute full copy
if flag_path_exists == False:
    paths = get_dir_content(full_folder_path)
    path_list = list()
    for parquet_path in paths:
        if os.path.basename(parquet_path).endswith('.parquet'):
            path_list.append(parquet_path)
    
    print(path_list)

# COMMAND ----------

## If bronze table doesn't exists then execute full copy
if flag_path_exists == False:
    for parquet_path in path_list:
        if flag_path_exists == False:
            df_temp = spark.read.format('parquet').load(parquet_path)
            df_temp.write.format('delta').mode('overwrite').save(bronze_path)
            flag_path_exists = True
        else:
            # Row count of Delta table before the merge
            old_deltaTable = DeltaTable.forPath(spark, bronze_path)

            # Read parquet from incremental dataset
            newIncrementalData = spark.read.parquet(parquet_path)
            newIncrementalData.createOrReplaceTempView('IncrementalTable')
            count_rows_updates = newIncrementalData.count()

            list_columns_insert = []
            list_columns_update = []
            for col in newIncrementalData.dtypes:
                list_columns_insert.append((col[0], 'newData.' + col[0]))
                if col[0] != column_id:
                    list_columns_update.append((col[0], 'newData.' + col[0]))

            insert_merge_columns = dict(list_columns_insert)
            update_merge_columns = dict(list_columns_update)
            
            condition_columns = "oldData." + column_id + " = newData." + column_id
            print(condition_columns)
            
            # Execute merge
            (old_deltaTable
                .alias("oldData") 
                .merge(newIncrementalData.alias("newData"), condition_columns)
                .whenMatchedUpdate(set = update_merge_columns)
                .whenNotMatchedInsert(values = insert_merge_columns)
                .execute()
            )
else:
    old_deltaTable = DeltaTable.forPath(spark, bronze_path)
    
    # Read parquet from incremental dataset
    parquet_path = full_folder_path + '/' + actual_datetime.split('-')[0] + '/' + actual_datetime.split('-')[1] + '/' + actual_datetime.split('_')[0].split('-')[2] + '/' + actual_datetime.split('_')[1] + '/' + table_name + '_' + actual_datetime.replace('-', '') + '.parquet'
    print(parquet_path)
    if path_exists(parquet_path) == True:
        print('File parquet detected:', parquet_path)
        newIncrementalData = spark.read.parquet(parquet_path)
        newIncrementalData.createOrReplaceTempView('IncrementalTable')

        list_columns_insert = []
        list_columns_update = []
        for col in newIncrementalData.dtypes:
            list_columns_insert.append((col[0], 'newData.' + col[0]))
            if col[0] != column_id:
                list_columns_update.append((col[0], 'newData.' + col[0]))

        insert_merge_columns = dict(list_columns_insert)
        update_merge_columns = dict(list_columns_update)

        # Execute merge
        (old_deltaTable
            .alias("oldData") 
            .merge(newIncrementalData.alias("newData"), "oldData." + column_id + " = newData." + column_id)
            .whenMatchedUpdate(set = update_merge_columns)
            .whenNotMatchedInsert(values = insert_merge_columns)
            .execute()
        )
        print('Merge Completed')
    else:
        print('File parquet does not exists')

# COMMAND ----------

## Display table
table_results = DeltaTable.forPath(spark, bronze_path)
count_rows_updates = table_results.toDF().count()
print('Total Rows', count_rows_updates)
display(table_results.toDF())

# COMMAND ----------

table_results.toDF().createOrReplaceTempView('TableResults')
if count_rows_updates > 0:
    max_incremental_value = None
    try:
        max_incremental_value = str(spark.sql("SELECT max(" + incremental_column + ") AS Result FROM TableResults").first().Result)
    except Exception:
        max_incremental_value = None
    
    print(max_incremental_value)

# COMMAND ----------

# Update sql datapipeline
if count_rows_updates > 0:
    url = dbutils.secrets.get(scope="AkvSecretScope01", key='azure-sql-dw-odbc-conn-str')
    try:
        cnxn = pyodbc.connect(url)
        cursor = cnxn.cursor()
        
        print("Watermark table: " + watermark_table)
        print("New watermark value: " + max_incremental_value)
        print("Data Id: " + str(row_id))
        
        sqlStatement = "UPDATE " + watermark_table + " SET [WatermarkVal] = '" + max_incremental_value + "' WHERE [DataPipelineId] = " + row_id + ";"
        
        # Execute query
        cursor.execute(sqlStatement).commit()
        print("Row successfully updated")
        
        cnxn.close()
    except Exception as e:
        print(e)