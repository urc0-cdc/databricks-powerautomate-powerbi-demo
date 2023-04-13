# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS cases_directory_snapshot
# MAGIC (
# MAGIC   path STRING,
# MAGIC   name STRING,
# MAGIC   size LONG,
# MAGIC   modtime LONG
# MAGIC )

# COMMAND ----------

storage_account_name = 'avajmdstorage'
blob_container = 'avajmd-container'

spark.conf.set(
    "fs.azure.account.key." + storage_account_name + ".dfs.core.windows.net",
    dbutils.secrets.get(scope="keyvaultsecretscope", key="secretKey"))

case_file_root_path = "abfss://" + blob_container + "@" + storage_account_name + ".dfs.core.windows.net"
case_file_staging_path = "abfss://" + blob_container + "@" + storage_account_name + ".dfs.core.windows.net/casefile_staging"
case_file_archive_path = "abfss://" + blob_container + "@" + storage_account_name + ".dfs.core.windows.net/casefile_archive"

# COMMAND ----------

from pyspark.sql.functions import lit, col, concat
from pyspark.sql.types import StructType, StructField, LongType, StringType

schema = StructType(
  [
    StructField('path', StringType()),
    StructField('name', StringType()),
    StructField('size', LongType()),
    StructField('modtime', LongType())
  ]
)

root_df = (spark.createDataFrame(dbutils.fs.ls(case_file_root_path), schema)
  .withColumn("path", lit(case_file_root_path)))

staging_df = (spark.createDataFrame(dbutils.fs.ls(case_file_staging_path), schema)
  .withColumn("path", lit(case_file_staging_path))
  .withColumn("name", concat(lit("casefile_staging/"), col("name"))))

archive_df = (spark.createDataFrame(dbutils.fs.ls(case_file_archive_path), schema)
  .withColumn("path", lit(case_file_archive_path))
  .withColumn("name", concat(lit("casefile_archive/"), col("name"))))

all_files_df = root_df.union(staging_df).union(archive_df)

display(all_files_df)

# COMMAND ----------

all_files_df.write.mode("overwrite").format("delta").saveAsTable("cases_directory_snapshot")
