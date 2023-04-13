# Databricks notebook source
from datetime import datetime

case_file = "dat_" + datetime.now().strftime("%Y_%m_%d-%k%M%S")

display("We will create " + case_file)

# COMMAND ----------

storage_account_name = 'avajmdstorage'
blob_container = 'avajmd-container'

spark.conf.set(
    "fs.azure.account.key." + storage_account_name + ".dfs.core.windows.net",
    dbutils.secrets.get(scope="keyvaultsecretscope", key="secretKey"))

case_file_directory = "abfss://" + blob_container + "@" + storage_account_name + ".dfs.core.windows.net/casefile_staging"
case_file_path = case_file_directory + "/" + case_file
case_file_subdirectory = case_file_path + "/"

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH fips_random_rows AS
# MAGIC (
# MAGIC   SELECT FLOOR(RAND() * (SELECT COUNT(*) FROM fips_by_state)) AS fips_id
# MAGIC   FROM numbers
# MAGIC   WHERE number < RAND() * 100
# MAGIC ),
# MAGIC fips_with_row_ids AS (
# MAGIC   SELECT ROW_NUMBER() OVER(ORDER BY fips) AS row_id, * FROM fips_by_state
# MAGIC ),
# MAGIC random_dataset AS (
# MAGIC   SELECT row_id, fips, 
# MAGIC     CASE WHEN RAND() > 0.9 THEN NULL ELSE name END as county, 
# MAGIC     CASE WHEN RAND() > 0.9 THEN NULL ELSE state END as state,
# MAGIC     FLOOR(RAND() * 9999) as case_id,
# MAGIC     CASE WHEN RAND() > 0.95 THEN NULL ELSE FLOOR(RAND() * 999999) END as institution_id,
# MAGIC     CASE WHEN RAND() > 0.9 THEN 1 ELSE 0 END AS duplicate
# MAGIC   FROM fips_random_rows r
# MAGIC   INNER JOIN fips_with_row_ids f on r.fips_id = f.row_id
# MAGIC )
# MAGIC SELECT case_id, state, county, institution_id FROM random_dataset
# MAGIC UNION ALL
# MAGIC SELECT case_id, state, county, institution_id FROM random_dataset WHERE duplicate = 1

# COMMAND ----------

random_data_df = _sqldf
display(random_data_df)

# COMMAND ----------

(random_data_df
  .coalesce(1)
  .write
  .mode("overwrite")
  .option("header", "true")
  .format("csv")
  .save(case_file_path))

# COMMAND ----------

data_location = case_file_subdirectory

files = dbutils.fs.ls(data_location)
csv_file = [x.path for x in files if x.path.endswith(".csv")][0]
dbutils.fs.mv(csv_file, data_location.rstrip('/') + ".csv")
dbutils.fs.rm(data_location, recurse = True)

# COMMAND ----------

# MAGIC %run ./Get_CaseFile_FileInfo
