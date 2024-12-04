# Databricks notebook source
from pyspark.sql.functions import  lit
schema="customer_id int,first_name string,last_name string,email string,phone string,country string,city string,registration_date timestamp,date_of_birth timestamp,gender string"

df_customer=spark.read.format("csv").option("header","True").load("dbfs:/mnt/data_landing/Customer")
df_customer=df_customer.withColumn("Flag",lit("False"))
#df_customer.display()

# COMMAND ----------

df_customer.write.mode("overwrite").format("delta").option("mergeSchema","true").save("dbfs:/mnt/data_bronzelayer/CustomerData")

#df_customer.write.mode("overwrite").format("delta").option("overwriteSchema", "true").save("dbfs:/mnt/data_bronzelayer/CustomerData")

#df.write.format("parquet").option("path","dbfs:/mnt/data_bronzelayer/Claim").mode("append").save()

# COMMAND ----------

df_customer.createOrReplaceTempView("customer")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customer

# COMMAND ----------

df = spark.read.format("delta").load("dbfs:/mnt/data_bronzelayer/CustomerData")
df.display()

# COMMAND ----------


