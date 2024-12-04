# Databricks notebook source
# MAGIC %md
# MAGIC <b>Remove all the rows where Customer Id,Policy ID is null
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import lit
df_policy = spark.read.format("delta").load("dbfs:/mnt/data_bronzelayer/PolicyData")
#df_policy.display()
#df_policy.createOrReplaceTempView("policy")
df_policy=df_policy.filter((df_policy.policy_id.isNotNull())&(df_policy.customer_id.isNotNull()))
#df_policy.display()





# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC Remove all rows where Customer Id not exist in Customer table
# MAGIC

# COMMAND ----------

df_customer=spark.read.format("delta").load("dbfs:/mnt/data_bronzelayer/CustomerData")

# COMMAND ----------

df_policy=df_policy.filter((df_policy.policy_id.isNotNull())&(df_policy.customer_id.isNotNull()))
Joined_policy_df=df_policy.join(df_customer,"customer_id")
Joined_policy_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC Every policy must have preminum & covergae amount >0
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col
Joined_policy_df=df_policy.join(df_customer,"customer_id")\
                 .filter((col("premium")>0)&(col("coverage_amount")>0))

Joined_policy_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC Validate end_date>start_date
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col
Joined_policy_df=df_policy.join(df_customer,"customer_id")\
                 .filter((col("premium")>0)&(col("coverage_amount")>0)&(df_policy.end_date>df_policy.start_date))\
                 .select(df_policy["*"])

Joined_policy_df.display()        
Joined_policy_df.createOrReplaceTempView("cleaned_policy")

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC Merged the table with merged_date_timestamp as current timesatmp

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into silverlayer.policy T
# MAGIC using cleaned_policy S
# MAGIC on S.policy_id = T.policy_id
# MAGIC when matched then
# MAGIC   update set
# MAGIC     T.policy_type = S.policy_type,
# MAGIC     T.customer_id = S.customer_id,
# MAGIC     T.start_date = S.start_date,
# MAGIC     T.end_date = S.end_date,
# MAGIC     T.premium = S.premium,
# MAGIC     T.coverage_amount = S.coverage_amount
# MAGIC when not matched then
# MAGIC   insert (policy_id, policy_type, customer_id, start_date, end_date, premium, coverage_amount, merge_timestamp)
# MAGIC   values (S.policy_id, S.policy_type, S.customer_id, S.start_date, S.end_date, S.premium, S.coverage_amount, current_timestamp)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC Update the merged_flag in the bronze layer

# COMMAND ----------

from pyspark.sql.functions import when
Joined_policy_df=Joined_policy_df.withColumn("Merge_Flag" ,when(Joined_policy_df.Merge_Flag=="False","True").otherwise(Joined_policy_df.Merge_Flag))

Joined_policy_df.write.mode("append").save("dbfs:/mnt/data_bronzelayer/PolicyData")

Joined_policy_df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC <b> Extension: You can add the test cases for all the steps

# COMMAND ----------

Joined_policy_df.display()

# COMMAND ----------


