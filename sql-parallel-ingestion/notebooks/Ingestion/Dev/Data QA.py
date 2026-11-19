# Databricks notebook source
#%sql
#CREATE DATABASE PV6

# COMMAND ----------

#%sql
#CREATE TABLE IF NOT EXISTS PV6.StdShifteqmt
#USING DELTA
#LOCATION '/mnt/raw-v2/PV6/bronze/StdShifteqmt.delta'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT max(id) FROM PV6.SumShiftLoad
# MAGIC --where ShiftId >= 220101001 
# MAGIC --and Id <= 2204280020000036476

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM PV6.StdShifteqmt

# COMMAND ----------

# MAGIC %sql
# MAGIC 	select count(1)
# MAGIC   FROM PV6.SumShiftLoad
# MAGIC   where id <=2205030020013063908

# COMMAND ----------

# MAGIC %sql
# MAGIC   select count(*)
# MAGIC   FROM PV6.StdShiftState
# MAGIC   where id <= 2205030020013069596
# MAGIC   --order by id desc