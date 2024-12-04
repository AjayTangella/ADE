# Databricks notebook source
# MAGIC %sql
# MAGIC create or Replace table silverlayer.claim (
# MAGIC   claim_id int,
# MAGIC   policy_id int,
# MAGIC   date_of_claim string,
# MAGIC   claim_amount double,
# MAGIC   claim_status string,
# MAGIC   LastUpdatedTimeStamp timestamp,
# MAGIC   merge_timestamp timestamp
# MAGIC )using delta

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silverlayer.claim

# COMMAND ----------


