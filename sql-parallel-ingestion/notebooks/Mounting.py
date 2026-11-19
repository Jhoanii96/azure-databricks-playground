# Databricks notebook source
storage_name = dbutils.secrets.get(scope = "AkvSecretScope01", key = "azure-blob-storage-acct-name")

def mount_blob(container):
  if not any(mount.mountPoint == f"/mnt/{container}" for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(
      source = f"wasbs://{container}@{storage_name}.blob.core.windows.net",
      mount_point = f"/mnt/{container}",
      extra_configs = {f"fs.azure.account.key.{storage_name}.blob.core.windows.net":dbutils.secrets.get(scope = "AkvSecretScope01", key = "azure-blob-storage-key")})

# COMMAND ----------

mount_blob("raw")
mount_blob("bronze")
mount_blob("silver")
mount_blob("gold")
mount_blob("temp-files")

mount_blob("config-v2")
mount_blob("curated-v2")
mount_blob("pipeline-logs-v2")
mount_blob("raw-v2")
mount_blob("staging-v2")

# COMMAND ----------

dbutils.secrets.list("AkvSecretScope01")

# COMMAND ----------

display(dbutils.fs.mounts())
