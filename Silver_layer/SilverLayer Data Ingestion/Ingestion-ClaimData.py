# Databricks notebook source
# MAGIC %md
# MAGIC <b>Remove all where claim_id, policy_id is ,claim status,claim_amount,lastupdated null
# MAGIC

# COMMAND ----------

df_claim=spark.read.format("delta").load("dbfs:/mnt/data_bronzelayer/Claim")
#df_claim.display()
df_claim=df_claim.filter((df_claim.claim_id.isNotNull())&(df_claim.policy_id.isNotNull())&(df_claim.claim_status.isNotNull()) & (df_claim.claim_amount.isNotNull()) & (df_claim.LastUpdatedTimeStamp.isNotNull()))
#df_claim.display()

# COMMAND ----------

df_claim.createOrReplaceTempView("claim")

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #Policy Table

# COMMAND ----------

df_Policy = spark.read.format("delta").load("dbfs:/mnt/data_bronzelayer/PolicyData")
df_Policy.createOrReplaceTempView("Policy")

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Remove all rows where policy_id Id not exist in Policy table
# MAGIC

# COMMAND ----------

# %sql
# select C.claim_id, C.policy_id, date_format(C.date_of_claim, 'MM-dd-yyyy') as date_of_claim, C.claim_amount, C.claim_status, C.LastUpdatedTimeStamp 
# from silverlayer.claim C
# inner join Policy P on C.policy_id = P.policy_id

# COMMAND ----------

remove_policy_id =df_claim.join(df_Policy,"policy_id")

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Convert date_of_claim to Date column with formate (MM-dd-yyyy)
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import to_date,date_format
remove_policy_id =df_claim.join(df_Policy,"policy_id")\
                  .withColumn("date_of_claimas", date_format(to_date(df_claim["date_of_claim"], "MM-dd-yyyy"),"MM-dd-yyyy"))

display(remove_policy_id)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Ensure  claim amount is >0

# COMMAND ----------

from pyspark.sql.functions import to_date,date_format
remove_policy_id =df_claim.join(df_Policy,"policy_id")\
                  .withColumn("date_of_claimas", date_format(to_date(df_claim["date_of_claim"], "MM-dd-yyyy"),"MM-dd-yyyy"))
df_claim_cleaned=remove_policy_id.filter(remove_policy_id.claim_amount>0)

# COMMAND ----------

df_claim_cleaned.createOrReplaceTempView("claim_cleaned")

# COMMAND ----------

df_claim_cleaned = df_claim_cleaned.withColumn("date_of_claim", to_date(df_claim_cleaned["date_of_claim"], "MM-dd-yyyy"))
df_claim_cleaned.printSchema()
df_claim_cleaned.display()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC <b>Add the merged_date_timestamp (current timesatmp)

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into silverlayer.claim T using claim_cleaned S on T.claim_id=S.claim_id when matched then update set 
# MAGIC T.policy_id=S.policy_id,T.date_of_claim=S.date_of_claim,T.claim_amount=S.claim_amount,
# MAGIC T.claim_status=S.claim_status,T.LastUpdatedTimeStamp=S.LastUpdatedTimeStamp,T.merge_timestamp=current_timestamp()
# MAGIC when not matched then 
# MAGIC insert (claim_id,policy_id,date_of_claim,claim_amount,claim_status,LastUpdatedTimeStamp,merge_timestamp)
# MAGIC values(S.claim_id,S.policy_id,S.date_of_claim,S.claim_amount,S.claim_status,S.LastUpdatedTimeStamp,current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC <b> Update Flag

# COMMAND ----------

from pyspark.sql.functions import when

# Update the Merge_Flag column
df_claim = df_claim.withColumn("Merge_Flag", when(df_claim["Merge_Flag"] == "False", "True").otherwise(df_claim["Merge_Flag"]))


#df_claim.show()

# Write the updated DataFrame to Delta format
df_claim.write.format("delta").option("path", "dbfs:/mnt/data_bronzelayer/Claim").mode("overwrite").save()
df_claim.show()

# COMMAND ----------


