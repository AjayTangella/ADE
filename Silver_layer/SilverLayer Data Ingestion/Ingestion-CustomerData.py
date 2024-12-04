# Databricks notebook source
# MAGIC %md
# MAGIC <b>Remove all where Customer Id not null
# MAGIC

# COMMAND ----------

# df_customer=spark.read.format("delta").load("dbfs:/mnt/data_bronzelayer/CustomerData")
# df_customer=df_customer.filter((df_customer.customer_id.isNotNull()) & ((df_customer.gender=="Female")|(df_customer.gender=="Male")))
# display(df_customer)

# COMMAND ----------

# MAGIC %md
# MAGIC <b> 
# MAGIC Remove records where gender is other than Male/Female
# MAGIC

# COMMAND ----------

df_customer=spark.read.format("delta").load("dbfs:/mnt/data_bronzelayer/CustomerData")
df_customer=df_customer.filter((df_customer.customer_id.isNotNull()) & ((df_customer.gender=="Female")|(df_customer.gender=="Male")))
# display(df_customer)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Outlier check at some registration_date > DOb.
# MAGIC

# COMMAND ----------

df_customer=spark.read.format("delta").load("dbfs:/mnt/data_bronzelayer/CustomerData")
df_customer=df_customer.filter((df_customer.customer_id.isNotNull()) & ((df_customer.gender=="Female")|(df_customer.gender=="Male")))
df_customer=df_customer.filter((df_customer.registration_date)>(df_customer.date_of_birth))


#display(df_customer)

# COMMAND ----------

df_customer.createOrReplaceTempView("customer")

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Merge the data into silver layer while adding current_timestamp
# MAGIC

# COMMAND ----------

# %sql
# merge into silverlayer.customer T using customer S on
#  T.customer_id=S.customer_id when matched then 
#  update set  T.customer_id=S.customer_id ,
#  T.first_name=S.first_name ,
#  T.last_name=S.last_name ,
#  T.email =S.email,
#  T.phone =S.phone,
#  T.country=S.country ,
#  T.city=S.city ,
#  T.registration_date=S.registration_date ,
#  T.date_of_birth =S.date_of_birth,
#  T.gender=S.gender,
#  T.merge_timstamp=current_timestamp() 
#  when not matched then 
#  insert (customer_id ,first_name ,last_name ,email ,phone ,country ,city ,registration_date ,date_of_birth ,gender,merge_timstamp) 
#  values(S.customer_id ,S.first_name ,S.last_name ,S.email ,S.phone ,S.country ,S.city ,S.registration_date ,S.date_of_birth ,S.gender,current_timestamp())
 

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into komatsu_ade.silverlayer.customer T using customer S on T.customer_id=S.customer_id when matched then update set T.customer_id=S.customer_id, T.first_name=S.first_name, T.last_name=S.last_name, T.email=S.email, T.phone=S.phone, T.country=S.country, T.city=S.city, T.registration_date=S.registration_date, T.date_of_birth=S.date_of_birth, T.gender=S.gender, T.merge_timstamp=current_timestamp() when not matched then insert (customer_id, first_name, last_name, email, phone, country, city, registration_date, date_of_birth, gender, merge_timstamp) values(S.customer_id, S.first_name, S.last_name, S.email, S.phone, S.country, S.city, S.registration_date, S.date_of_birth, S.gender, current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Update the flag in the bronze layer

# COMMAND ----------

from pyspark.sql.functions import when
df_customer=df_customer.withColumn("Flag",when(df_customer.Flag=="False","True").otherwise(df_customer.Flag))

df_customer.write.format("delta").mode("overwrite").save("dbfs:/mnt/data_bronzelayer/CustomerData")


df_customer.display()


# COMMAND ----------

# MAGIC %md
# MAGIC <b> Extension: You can add the test cases for all the steps
