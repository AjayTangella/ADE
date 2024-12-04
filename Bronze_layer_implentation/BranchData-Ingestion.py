# Databricks notebook source
schema ="branch_id int,branch_country string,branch_city string"
df=spark.read.format("parquet").schema(schema).load("dbfs:/mnt/data_landing/Full/2024/11/28/Branch")


# COMMAND ----------

from pyspark.sql.functions import lit
df=spark.read.format("parquet").load("/mnt/data-processedlayer/BrachData/11-28-2024/")
df_Brach=df.withColumn("Flag",lit("False"))
df_Brach.show()

# COMMAND ----------

# df.write.format("delta").option("path","dbfs:/mnt/data_bronzelayer/Branch").mode("append").save()

df_Brach.write.format("delta") \
    .option("path", "dbfs:/mnt/data_bronzelayer/Branch") \
    .option("mergeSchema", "true") \
    .mode("append") \
    .save()

# COMMAND ----------

df_Brach.show()

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/data-processedlayer/BrachData/11-28-2024/

# COMMAND ----------

from datetime import datetime

current_date=datetime.now().strftime("%m-%d-%Y")

#print(current_date)

new_folder='/mnt/data-processedlayer/BrachData/'+current_date
#print(new_folder)

dbutils.fs.mv('/mnt/data_landing/Full/2024/11/28/Branch/',new_folder,True)


# COMMAND ----------

from datetime import datetime
def getfilePathWithDate(filepath):
    #get the current date
    current_date=datetime.now().strftime("%m-%d-%Y")
    new_folder=filepath+current_date
    return new_folder


# COMMAND ----------

getfilePathWithDate("/mnt/data_landing/Full/")

# COMMAND ----------


