# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS stg_cases
# MAGIC (
# MAGIC   load_id BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   load_timestamp TIMESTAMP,
# MAGIC   case_id INT,
# MAGIC   state STRING,
# MAGIC   county STRING,
# MAGIC   institution_id INT
# MAGIC )

# COMMAND ----------

from pyspark.sql.types import StringType, IntegerType, StructType, StructField

case_file_schema = StructType([
    StructField("case_id", IntegerType(), True),
    StructField("state", StringType(), True),
    StructField("county", StringType(), True),
    StructField("institution_id", IntegerType(), True)
])

# COMMAND ----------

storage_account_name = 'avajmdstorage'
blob_container = 'avajmd-container'

spark.conf.set(
    "fs.azure.account.key." + storage_account_name + ".dfs.core.windows.net",
    dbutils.secrets.get(scope="keyvaultsecretscope", key="secretKey"))

case_file_staging_path = "abfss://" + blob_container + "@" + storage_account_name + ".dfs.core.windows.net/casefile_staging"
case_file_archive_path = "abfss://" + blob_container + "@" + storage_account_name + ".dfs.core.windows.net/casefile_archive"

# COMMAND ----------

cases_df = spark.read.option("header", True).schema(case_file_schema).csv(case_file_staging_path)

# COMMAND ----------

display(cases_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp 

cases_stg_df = cases_df.withColumn("load_timestamp", current_timestamp())

display(cases_stg_df)

# COMMAND ----------

cases_stg_df.write.mode("append").format("delta").saveAsTable("stg_cases")

# COMMAND ----------

# MAGIC %sql select * from stg_cases order by load_id

# COMMAND ----------

files = dbutils.fs.ls(case_file_staging_path)
csv_files = [x.name for x in files if x.path.endswith(".csv")]
for file in csv_files:
    dbutils.fs.mv(case_file_staging_path + "/" + file, case_file_archive_path + "/" + file)

# COMMAND ----------

# MAGIC %run ./Get_CaseFile_FileInfo
