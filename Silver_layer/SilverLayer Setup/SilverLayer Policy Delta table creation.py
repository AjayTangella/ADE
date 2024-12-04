# Databricks notebook source
# MAGIC %sql
# MAGIC create or replace table silverlayer.policy(
# MAGIC   policy_id int,
# MAGIC   policy_type string,
# MAGIC   customer_id int,
# MAGIC   start_date timestamp,
# MAGIC   end_date timestamp,
# MAGIC   premium int,
# MAGIC   coverage_amount string,
# MAGIC   merge_timestamp timestamp
# MAGIC ) using delta

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silverlayer.policy
