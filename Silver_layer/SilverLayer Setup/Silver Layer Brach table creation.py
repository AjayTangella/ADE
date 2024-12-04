# Databricks notebook source
# MAGIC %sql
# MAGIC create or Replace table silverlayer.branch(
# MAGIC   branch_id int,
# MAGIC   branch_country string,
# MAGIC   branch_city string,
# MAGIC   Merge_timestamp timestamp
# MAGIC )using delta

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from silverlayer.branch
