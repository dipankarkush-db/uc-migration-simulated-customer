# Databricks notebook source
# MAGIC %run ./SharedNotebookToRun

# COMMAND ----------

entity_name = "people_parquet"
stgloc = get_uc_mount_target('/mnt/meijermount', normalize=True) + "/dkushari/dkushari_db/ADRMParquet/{0}Stg".format(entity_name)
display(spark.read.format("parquet").load(stgloc))

# COMMAND ----------

from pyspark.sql.functions import col
peopleid = spark.read.parquet(get_uc_mount_target('/mnt/meijermount', normalize=True) + "/dkushari/dkushari_db/ADRMParquet/people_parquetStg").filter(col("id")==6280266)

# COMMAND ----------

table = "people_parquetStg2"
tgtlocExt = get_uc_mount_target('/mnt/meijermount', normalize=True) + "/dkushari/dkushari_db/ADRMParquet/{0}".format(table)
tgtlocExt

# COMMAND ----------

display(spark.read.parquet(tgtlocExt))

# COMMAND ----------

dbutils.fs.rm(get_uc_mount_target('/mnt/meijermount', normalize=True) + '/dkushari/dkushari_db/ADRMParquet/people_parquetStg1',True);

# COMMAND ----------

people = spark.read.parquet(get_uc_mount_target('/mnt/meijermount', normalize=True) + "/dkushari/dkushari_db/ADRMParquet/people_parquetStg")
display(people)

# COMMAND ----------

sourceSystem = "ADRMParquet"
source_table_name = "people_parquetStg/part-00000-tid-1383576846752282503-c6933315-90e2-4a66-9cc2-8bf8d1a4c200-239-1-c000.snappy"
metadata_filepath=get_uc_mount_target('/mnt/meijermount', normalize=True) + "/dkushari/dkushari_db/{}/{}.parquet".format(sourceSystem,source_table_name)
display(spark.read.parquet(metadata_filepath))

# COMMAND ----------

mountPoint = get_uc_mount_target('/mnt/meijermount', normalize=True) + ""

# COMMAND ----------

clnsmountPoint = get_uc_mount_target('/mnt/meijermount', normalize=True) + ""

list_of_mounts = [get_uc_mount_target('/mnt/meijermount', normalize=True) + "", "/mnt/meijermount2"]

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE some_table
# MAGIC LOCATION '/mnt/meijermount/dkushari/dkushari_db/some_table'
# MAGIC AS
# MAGIC SELECT "a"

# COMMAND ----------


