# Databricks notebook source
# MAGIC %md
# MAGIC ---
# MAGIC # Bronze to Silver
# MAGIC 
# MAGIC * __*Version :*__ 1.0b
# MAGIC * __*Date :*__ May 30, 2022
# MAGIC ***
# MAGIC * __*Description :*__  
# MAGIC This notebook does the following:  
# MAGIC 1- Loads the Data Pipeline and Config KPI tables to obtain the group and the date and shift format.  
# MAGIC 2- Filter data sources that requires Date Key and Shift Key.  
# MAGIC 3- Fill Null Values.  
# MAGIC 4- Create the DateKey and ShiftKey columns.  
# MAGIC 5- Upload Delta table to silver zone.

# COMMAND ----------

# DBTITLE 1,Libraries
from delta.tables import *

# COMMAND ----------

# DBTITLE 1,Parameters
IngestionGroup_Column = dbutils.widgets.get("IngestionGroup")

#Note: If you want to run manually this notebook please comment the line #1 and uncomment the line below.
#IngestionGroup_Column = '1' # <= For a manual execution uncomment this line and comment the previous one.

# COMMAND ----------

# DBTITLE 1,Functions
# MAGIC %md
# MAGIC * __*create_datekey     :*__ Create the subquery that adds the column DateKey (yyyymmdd) in the data source.
# MAGIC * __*create_shiftkey    :*__ Create the subquery that adds the ShiftKey column in the data source.
# MAGIC * __*concatenate_query  :*__ Creates the query that consults the data source, concatenating the results of the functions "create_datekey" and "create_shiftkey"
# MAGIC * __*file_exists        :*__ Check if a path exists.

# COMMAND ----------

def create_datekey(date_column,date_format):
    if "/" in date_format or "-" in date_format:
        return "DATE_FORMAT("+date_column+", 'yyyyMMdd')"
    else:
        if date_format != '':
            year_iloc = [pos for pos, char in enumerate(date_format) if char == 'y']
            year_len = (year_iloc[1] - year_iloc[0]) + 1
            year_query = "substring("+date_column+","+str(year_iloc[0]+1)+","+str(year_len)+")"
            if year_len ==2:
                year_query = "concat('20',"+year_query+")"
            month_iloc = [pos for pos, char in enumerate(date_format) if char == 'm']
            month_len = (month_iloc[1] - month_iloc[0]) + 1
            month_query = "substring("+date_column+","+str(month_iloc[0]+1)+","+str(month_len)+")"
            
            day_iloc = [pos for pos, char in enumerate(date_format) if char == 'd']
            day_len = (day_iloc[1] - day_iloc[0]) + 1
            day_query = "substring("+date_column+","+str(day_iloc[0]+1)+","+str(day_len)+")"
            return "concat("+str(year_query)+","+str(month_query)+","+str(day_query)+")"
        else:
            return date_column

# COMMAND ----------

def create_shiftkey(shift_column, shift_night, shift_day):
    if str(shift_column) == '0':
        return str('0')
    else:
        if shift_day != '':
            if len(shift_day) == 1:
                query = 'case when ' + str(shift_column) + " like '%" + str(shift_day) \
                        + "' then 1 else case when " + str(shift_column) + " like '%" \
                        + str(shift_night) + "' then 2 else 0 end end "
                return query
            else:
                list_dayshift_start = shift_day.split(':')
                list_dayshift_start = list(map(int, list_dayshift_start))
                dayshift_start = list_dayshift_start[0]
                
                list_nightshift_start = shift_night.split(':')
                list_nightshift_start = list(map(int, list_nightshift_start))
                nightshift_start = list_nightshift_start[0]
                
                query = "case when date_part('HOUR', " + str(shift_column) + ') >=' \
                        + str(dayshift_start) + " and  date_part('HOUR', " + str(shift_column) \
                        + ') < ' + str(nightshift_start) + ' then 1 else 2 end'

            return query
            

# COMMAND ----------

def concatenate_query(query_datekey,query_shiftkey,data_source):
    query_temp = 'SHOW COLUMNS IN '+str(data_source)
    columns_name = spark.sql(query_temp)
    df_temp_date = columns_name[columns_name['col_name'] == 'DateKey']
    df_temp_shift = columns_name[columns_name['col_name'] == 'ShiftKey'] 
    v_date = df_temp_date.count()
    v_shift= df_temp_shift.count()
    
    query = 'Select '
    if v_date == 0:
        query=query+str(query_datekey)+" as Datekey, "
    if v_shift == 0:
        query = query+str(query_shiftkey)+" as ShiftKey, "
    query = query+" * from "+str(data_source)
    return query


# COMMAND ----------

def file_exists(path):
    try:
        dbutils.fs.ls(path)
        return True
    except Exception as e:
        if 'java.io.FileNotFoundException' in str(e):
            return False
        else:
            raise

# COMMAND ----------

# DBTITLE 1,Load Tables
url = dbutils.secrets.get(scope="AkvSecretScope01", key='azure-sql-dw-conn-str')
df_ConfigKPI = spark.read.jdbc(url, "[KPI].[ConfigKPI]")
df_DataPipeline = spark.read.jdbc(url, "[KPI].[DataPipeline]")
df_DataPipeline = df_DataPipeline.filter(df_DataPipeline.DeletedSetDate.isNull())
df_DataPipeline = df_DataPipeline.filter(df_DataPipeline.IngestionGroup == IngestionGroup_Column)

# COMMAND ----------

# DBTITLE 1,Union Tables
df_DataSource= df_ConfigKPI.select('DataSource', 'DateColumn', 'DateFormat','ShiftDayFormat','ShiftNightFormat','ShiftColumn').distinct()
df = df_DataPipeline.join(df_DataSource,df_DataPipeline.TableSourceName == df_DataSource.DataSource)
df = df.select(
    "TableSourceName",
    "IncrementalColumn",
    "IngestionType",
    "ColumnId",
    "WatermarkVal",
    "DestinationFolder",
    "DateColumn",
    "DateFormat",
    "ShiftDayFormat",
    "ShiftNightFormat",
    "ShiftColumn",
)

# COMMAND ----------

# DBTITLE 1,Fill Null Values 
col_names = df.schema.names
for col in col_names:
    dt=str(dict(df.dtypes)[col])
    if dt == 'string':
        df = df.na.fill({col:''})

# COMMAND ----------

# DBTITLE 1,Upload Data sets to Silver zone
# Iterates through the data source table
for row in df.collect():
    try:
        # The path of the data source is located
        path_bz = "/mnt/raw-v2/PV6/bronze/"+row['DestinationFolder']+"/"+row['TableSourceName']+".delta"
        file_bz_exits = file_exists(path_bz)
        if file_bz_exits:
            # Construction of the query
            if row['DateColumn'] != '':
                # Construction of the Datekey query
                datekey_query = create_datekey(str(row['DateColumn']), str(row['DateFormat'])) 
                # Construction of the Shiftkey query
                shift_query = create_shiftkey(str(row['ShiftColumn']), str(row['ShiftNightFormat']), str(row['ShiftDayFormat']))
                # Construction query
                final_query = concatenate_query(datekey_query, shift_query,row['TableSourceName'])
            else:
                final_query = "SELECT * FROM "+row['TableSourceName']
                
            # Read from data source in bronze
            df_temp = spark.read.format('delta').load(path_bz)
            # Path Silver
            path_slv = "/mnt/raw-v2/PV6/silver/"+row['DestinationFolder']+"/"+row['TableSourceName']+".delta"
            file_slv_exits = file_exists(path_slv)
            
            if (row['IngestionType'] == 'Incremental') and (file_slv_exits):
            #---------------------------------------------------------------[1]-----------------------------------------------------------------------
                # Create Silver Table
                spark.sql("DROP TABLE IF EXISTS tbl_slv_"+ row['TableSourceName'])
                query_create = "CREATE TABLE tbl_slv_"+row['TableSourceName']+" USING DELTA LOCATION '"+path_slv+"'"
                spark.sql(query_create)
                # Get the maximum value of the incremental column
                row_max_val_incremental = spark.sql("Select max("+row['IncrementalColumn']+") as max_value from tbl_slv_"+row['TableSourceName'])
                max_val_incremental = row_max_val_incremental.agg({'max_value':'max'})
                max_val_incremental = max_val_incremental.rdd.map(lambda x: x[0]).collect()
                spark.sql("DROP TABLE tbl_slv_"+ row['TableSourceName'])
                
                if str(max_val_incremental[0]) != row['WatermarkVal']:
                #-----------------------------------------------------------[2]-------------------------------------------------------------------------
                    # Create Bronze Table
                    spark.sql("DROP TABLE IF EXISTS tbl_bz_"+ row['TableSourceName'])
                    query_create = "CREATE TABLE tbl_bz_"+row['TableSourceName']+" USING DELTA LOCATION '"+path_bz+"'"
                    spark.sql(query_create)
                    # Read version log
                    query_version = "DESCRIBE HISTORY tbl_bz_"+row['TableSourceName']
                    df_version = spark.sql(query_version)
                    # Identify max version
                    max_version = df_version.agg({'version':'max'})
                    max_version = max_version.rdd.map(lambda x: x[0]).collect()
                    # Get the maximum value of the incremental column
                    qr_max_val_incremental = ("Select max(" + row["IncrementalColumn"] + ") as max_value from tbl_bz_" + row["TableSourceName"]
                                              + " VERSION as Of " + str(max_version[0] - 1) )
                    row_max_val_incremental =spark.sql(qr_max_val_incremental)
                    max_val_incremental = row_max_val_incremental.agg({'max_value':'max'})
                    max_val_incremental = max_val_incremental.rdd.map(lambda x: x[0]).collect()
                        
                    dt = str(dict(df_temp.dtypes)[row["IncrementalColumn"]]) # <= Identifies the type of data
                    # Create Expresion Filter
                    if dt in ['bigint','double','float','int','smallint','tinyint']:
                        expr_filter = " "+row['IncrementalColumn']+" > "+str(max_val_incremental[0])
                    else:
                        expr_filter = " "+row['IncrementalColumn']+" > '"+str(max_val_incremental[0])+"'"
                        
                    # Filter Dataframe
                    df_temp = df_temp.filter(expr_filter)
                   
                    spark.sql("DROP TABLE tbl_bz_"+ row['TableSourceName'])
                    
                    df_temp.createOrReplaceTempView(row['TableSourceName'])
                    
                    df_final = spark.sql(final_query)
                    
                    list_columns_insert = []
                    list_columns_update = []
                    
                    for col in df_final.dtypes:
                        list_columns_insert.append((col[0], 'newData.' + col[0]))
                        if col[0] != row['ColumnId']:
                            list_columns_update.append((col[0], 'newData.' + col[0]))

                    insert_merge_columns = dict(list_columns_insert)
                    update_merge_columns = dict(list_columns_update)
                    
                    old_deltaTable = DeltaTable.forPath(spark, path_slv)
                    # Execute merge
                    (old_deltaTable
                     .alias("oldData") 
                     .merge(df_final.alias("newData"), "oldData." + row['ColumnId'] + " = newData." + row['ColumnId'])
                     .whenMatchedUpdate(set = update_merge_columns)
                     .whenNotMatchedInsert(values = insert_merge_columns)
                     .execute()
                    )
                #-----------------------------------------------------------[2]------------------------------------------------------------------------- 
            else:
                df_temp.createOrReplaceTempView(row['TableSourceName'])
                df_final = spark.sql(final_query)
                df_final.write.format('delta').mode('overwrite').save(path_slv)
            #---------------------------------------------------------------[1]-------------------------------------------------------------------------
            spark.catalog.dropTempView(row['TableSourceName'])
            print(path_slv)
    except Exception as e:
        print(row['TableSourceName'], e)