# Databricks notebook source
schema = "agent_id integer, agent_name string, agent_email string,agent_phone string, branch_id integer, create_timestamp timestamp"
df = spark.read.option("header", "true").option("inferSchema", "false").schema(schema).parquet("dbfs:/mnt/data_landing/Full/2024/11/29/Agent/Agent.parquet")
#df.write.format("delta").option("path","/mnt/bronzelayer/a2").mode("append").saveAsTable("Agent")



# COMMAND ----------

#df = df.dropDuplicates()
df.write.format("delta").mode("overwrite").save("/mnt/data_bronzelayer/Agent")

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.functions import col
#df=spark.read.format("delta").load("/mnt/data_bronzelayer/Agent")
df.display()

# COMMAND ----------

from datetime import datetime

current_date=datetime.now().strftime("%m-%d-%Y")

#print(current_date)

new_folder='/mnt/data-processedlayer/AgentData/'+current_date
#print(new_folder)

dbutils.fs.mv('/mnt/data_landing/Full/2024/11/29/Agent/',new_folder,True)


# COMMAND ----------


