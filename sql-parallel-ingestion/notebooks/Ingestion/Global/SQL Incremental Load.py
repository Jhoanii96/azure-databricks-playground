# Databricks notebook source
# DBTITLE 1,Libraries
import os
import pyodbc
import pandas as pd
import datetime
from dateutil import parser
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.functions import udf

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text("ParquetPath", "", "Parquet Path")
dbutils.widgets.text("Schema", "", "Schema")
dbutils.widgets.text("Tablename", "", "Tablename")
dbutils.widgets.text("Watermark", "", "Watermark")
dbutils.widgets.text("ColumnId", "", "Column Id")
dbutils.widgets.text("DatabaseTargetName", "", "Database Target Name")
dbutils.widgets.text("IngestionType", "", "Ingestion Type")
dbutils.widgets.text("IncrementalColumn", "", "Incremental Column")

# COMMAND ----------

# Get value parameters
ParquetPath = dbutils.widgets.get("ParquetPath")
Schema = dbutils.widgets.get("Schema")
Tablename = dbutils.widgets.get("Tablename")
Watermark = dbutils.widgets.get("Watermark")
ColumnId = dbutils.widgets.get("ColumnId")
DatabaseTargetName = dbutils.widgets.get("DatabaseTargetName")
IngestionType = dbutils.widgets.get("IngestionType")
IncrementalColumn = dbutils.widgets.get("IncrementalColumn")
# Print parameters
print("Parquet Path: " + ParquetPath)
print("Schema: " + Schema)
print("Table Name: " + Tablename)
print("Watermark: " + Watermark)
print("Column Id: " + ColumnId)
print("Database Target Name: " + DatabaseTargetName)
print("Ingestion Type: " + IngestionType)
print("Incremental Column: " + IncrementalColumn)

# COMMAND ----------

# DBTITLE 1,Parameters data validation
if Schema == '' or DatabaseTargetName == '' or IngestionType == 'FullCopy' or IngestionType == '':
  dbutils.notebook.exit("Exit Notebook due to incorrect input parameters")

# COMMAND ----------

# DBTITLE 1,Set variables
url_sql = dbutils.secrets.get(scope="AkvSecretScope01", key='azure-sql-dw-odbc-conn-str')
url_jdbc = dbutils.secrets.get(scope="AkvSecretScope01", key='azure-sql-dw-conn-str')
ParquetPath = '/mnt/' + ParquetPath
Schema_Tablename = "[" + Schema + "].[" + Tablename + "]"
ParquetPath,Schema_Tablename

# COMMAND ----------

# DBTITLE 1,Function
# Detect if the file exists
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

# DBTITLE 1,Main Ingestion code
# Check if parquet file exists
if path_exists(ParquetPath) == False:
  dbutils.notebook.exit("Parquet file does not exists.")

# COMMAND ----------

# Read parquet from incremental parquet dataset and count number of records
newIncrementalData = spark.read.parquet(ParquetPath)
parquet_row_count = newIncrementalData.count()
print("Row count: " + str(parquet_row_count))

# Check if parquet has rows
if parquet_row_count == 0:
  dbutils.notebook.exit("Parquet file is empty.")

# COMMAND ----------

# Renaming column special characters
column_names = newIncrementalData.schema.names
for column in column_names:
  newIncrementalData = newIncrementalData.withColumnRenamed(column, column.replace('-', '_').replace('(', '_').replace(')', '').replace(' ', '_'))

# COMMAND ----------

# if watermark is blank, do a full copy and exit notebook
if Watermark == "":
  newIncrementalData_repartitioned = newIncrementalData.repartition(25)
  newIncrementalData_repartitioned.write.format('jdbc').mode('overwrite').option('url', url_jdbc).option('dbtable', Schema_Tablename).save()
  dbutils.notebook.exit("Table " + Schema_Tablename + " copied to database: " + DatabaseTargetName + ".")

# COMMAND ----------

# Get a total of rows count of the table
conn = pyodbc.connect(url_sql)
tableResultCount = pd.DataFrame()
try:
  print("Getting row count")
  queryCount = ("SELECT COUNT(*) AS [row_count] FROM " + Schema_Tablename)
  tableResultCount = pd.read_sql(queryCount, conn)
except Exception as ex:
  print("Error: " + str(ex))
print("The table: " + Schema_Tablename + " has " + str(tableResultCount['row_count'].iloc[0]) + " rows.")
TableRowCount = int(tableResultCount['row_count'].iloc[0])

# COMMAND ----------

# Getting column names to array
new_column_names = newIncrementalData.schema.names
print("Column count: " + str(len(new_column_names)))

# COMMAND ----------

# if row count is grather than 1 million rows then we will truncate and insert new rows.
if ((IngestionType == "Incremental" and TableRowCount >= 1000000) or (len(new_column_names) > 50)):
  conn = pyodbc.connect(url_sql)
  cursor = conn.cursor()
  
  # Do the insert
  print("Data cleansing")
  delete_query = "DELETE FROM " + Schema_Tablename + " WHERE [" + IncrementalColumn + "] > '" + Watermark + "'"
  print(delete_query)
  cursor.execute(delete_query)
  # commit the transaction
  conn.commit()
  print("The query execution was succesful. Total rows affected (" + str(cursor.rowcount) + ")")
  # close connection
  conn.close()
  
  print("Appending new rows...")
  newIncrementalData.write.format("jdbc").mode("append").option("url", url_jdbc).option("dbtable", Schema_Tablename).save()
  dbutils.notebook.exit("Table " + Schema_Tablename + " new records has been appened to: " + DatabaseTargetName + ".")

# COMMAND ----------

# DBTITLE 1,Starting merge code if less than 1 million
# Change decimal to double type column
select_expr = [
    col(c).cast("double") if "decimal" in t else col(c) for c, t in newIncrementalData.dtypes
]
newIncrementalData = newIncrementalData.select(*select_expr)

# COMMAND ----------

# Format datetime and replace string 
has_binary = False
for cl, tps in newIncrementalData.dtypes:
  if "string" in tps:
    newIncrementalData = newIncrementalData.withColumn(cl, regexp_replace(cl, "'", "''"))
  if "timestamp" in tps:
    newIncrementalData = newIncrementalData.withColumn(cl, F.date_format(cl, 'yyyy-MM-dd HH:mm:ss'))
  if "binary" in tps:
    has_binary = True

print("The table has binary column?: " + str(has_binary))
display(newIncrementalData)

# COMMAND ----------

# Building the start part of merge query
query_data_first_1 = "MERGE [" + Schema + "].[" + Tablename + "] AS Target " + os.linesep
query_data_first_1 += "USING ( VALUES ";
query_data_middle_1 = os.linesep;
# Append columns update and insert method
list_columns = []
list_columns_update = []
matched_update = ""
for ind, column in enumerate(new_column_names):
  list_columns.append(column)
  if column not in ColumnId:
    matched_update = matched_update + "Target.[" + column + "] = Source.[" + column + "], " + os.linesep

columns_string = "[" + "], [".join(list_columns) + "]"
columns_value = "Source.[" + "], Source.[".join(list_columns) + "]"
columns_mapping = "'{" + "}', '{".join(list_columns) + "}'"
matched_update = matched_update.strip()
matched_update = matched_update[:-1]

# COMMAND ----------

# Build the last part of merge query
query_data_last_1 = ") AS Source (" + columns_string + ") " + os.linesep;
query_data_last_1 += "ON"

for column_id in ColumnId.split(','):
  query_data_last_1 += " Source.[" + column_id + "] = Target.[" + column_id + "] and"

query_data_last_1 = query_data_last_1.strip()
query_data_last_1 = query_data_last_1[:-3]
query_data_last_1 += os.linesep;
query_data_last_1 += "WHEN MATCHED THEN " + os.linesep + "UPDATE SET ";
query_data_last_1 += matched_update;
query_data_last_1 += os.linesep;
query_data_last_1 += "WHEN NOT MATCHED BY Target THEN " + os.linesep + "INSERT (" + columns_string + ")";
query_data_last_1 += os.linesep;
query_data_last_1 += "VALUES (" + columns_value + ");";

# COMMAND ----------

# Build the data part of merge query and execute merging query
print("The query may take a long time to build if the table contains many columns. Only for binary cases.")
query_data_middle_1 = ""
full_query = ""
iteration_indx = 0
records_number = 5000
data_table = newIncrementalData.rdd.collect()
complete_execution = False
count_attemps=0
while complete_execution == False and count_attemps < 5:
  try:
    for row in data_table:
      if (iteration_indx % records_number == 0 and iteration_indx > 0) or (iteration_indx == parquet_row_count-1):
        if has_binary == True:
          query_data_middle_1 += "(";
          for ind, column in enumerate(new_column_names):
            if type(row[column]) is bytearray:
              query_data_middle_1 += "0x" + bytes(row[column]).hex().upper() + ",";
            else:
              query_data_middle_1 += "'" + str(row[column]) + "',";
          query_data_middle_1 = query_data_middle_1.strip()
          query_data_middle_1 = query_data_middle_1[:-1]
          query_data_middle_1 += ")" + os.linesep
        else:
          query_data_middle_1 += "("+ columns_mapping.format_map(row) + ")," + os.linesep;
          query_data_middle_1 = query_data_middle_1.strip()
          query_data_middle_1 = query_data_middle_1[:-1]

        query_data_middle_1 = query_data_middle_1.replace("'nan'", "NULL")
        query_data_middle_1 = query_data_middle_1.replace("'null'", "NULL")
        query_data_middle_1 = query_data_middle_1.replace("'None'", "NULL")

        full_query = query_data_first_1 + os.linesep + query_data_middle_1 + os.linesep + query_data_last_1

        if iteration_indx == parquet_row_count-1:
          print(str(parquet_row_count) + " of " + str(parquet_row_count))
        else:
          print(str(iteration_indx) + " of " + str(parquet_row_count))

        start_datetime = datetime.datetime.now()
        conn = pyodbc.connect(url_sql)
        cursor = conn.cursor()

        # Execute merge
        print("Merge Operation...")
        cursor.execute(full_query)
        # commit the transaction
        conn.commit()
        # close connection
        conn.close()
        end_datetime = datetime.datetime.now()
        total_difference = end_datetime - start_datetime
        print("Query executed succesfully!. Total row affected (" + str(cursor.rowcount) + ") in " + str("%s.%s" % (total_difference.seconds, total_difference.microseconds)) + " seconds.")
        query_data_middle_1 = ""
        full_query = ""
      else:
        if has_binary == True:
          query_data_middle_1 += "(";
          for ind, column in enumerate(new_column_names):
            if type(row[column]) is bytearray:
              query_data_middle_1 += "0x" + bytes(row[column]).hex().upper() + ",";
            else:
              query_data_middle_1 += "'" + str(row[column]) + "',";
          query_data_middle_1 = query_data_middle_1.strip()
          query_data_middle_1 = query_data_middle_1[:-1]
          query_data_middle_1 += ")," + os.linesep;
        else:
          query_data_middle_1 += "("+ columns_mapping.format_map(row) + ")," + os.linesep;

      iteration_indx += 1
    complete_execution = True
  except Exception as e:
    print('Error: '+ str(e))
    query_data_middle_1 = ""
    full_query = ""
    iteration_indx = 0
    records_number = (records_number - 1000)
    count_attemps += 1
    complete_execution = False
print("The merge execution was completed.")

# COMMAND ----------

dbutils.notebook.exit("Table " + Schema_Tablename + ", the merge execution was completed in " + DatabaseTargetName + ".")