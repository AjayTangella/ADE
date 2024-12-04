# Databricks notebook source
# MAGIC %sql
# MAGIC create or Replace table goldlayer.sales_by_policy_type_month(
# MAGIC   policy_type string,
# MAGIC   sale_month string,
# MAGIC   total_Premium string,
# MAGIC   updated_timestamp timestamp
# MAGIC )using delta

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from goldlayer.sales_by_policy_type_status 
# MAGIC As
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create or Replace table goldlayer.claim_by_policy_type_month(
# MAGIC   policy_type string,
# MAGIC   claim_status string,
# MAGIC   total_claim string,
# MAGIC   total_claim_amount string,
# MAGIC   updated_timestamp timestamp
# MAGIC )using delta

# COMMAND ----------

# MAGIC %sql
# MAGIC create or Replace table goldlayer.claim_analysis(
# MAGIC   policy_type string,
# MAGIC   avg_claim_amount string,
# MAGIC   max_claim_amount string,
# MAGIC   min_claim_amount string,
# MAGIC   total_claim string,
# MAGIC   updated_timestamp timestamp
# MAGIC )using delta

# COMMAND ----------


