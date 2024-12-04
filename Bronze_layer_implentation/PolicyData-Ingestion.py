# Databricks notebook source
from pyspark.sql.functions import lit
schema="policy_id int,policy_type string,customer_id int,start_date timestamp,end_date timestamp,premium int,coverage_amount string"


df_Policy=spark.read.format("json").load("dbfs:/mnt/data-processedlayer/Policy/11-28-2024/",schema=schema)
df_Policy=df_Policy.withColumn("Merge_Flag",lit("False"))

# COMMAND ----------

df_Policy.display()

# COMMAND ----------

df_Policy.write.mode("overwrite").option("mergeSchema","True").format("delta").save("dbfs:/mnt/data_bronzelayer/PolicyData")

#df.write.format("parquet").option("path","dbfs:/mnt/data_bronzelayer/Claim").mode("append").save()

# COMMAND ----------

df = spark.read.format("delta").load("dbfs:/mnt/data_bronzelayer/PolicyData")
df.display()

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/data-processedlayer/Policy/11-28-2024/

# COMMAND ----------

from datetime import datetime

current_date=datetime.now().strftime("%m-%d-%Y")

#print(current_date)

new_folder='/mnt/data-processedlayer/Policy/'+current_date
#print(new_folder)

dbutils.fs.mv('/mnt/data_landing/Policy',new_folder,True)


# COMMAND ----------


