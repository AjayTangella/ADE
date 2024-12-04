# Databricks notebook source
# MAGIC %md
# MAGIC <b>Remove all where brnach_id not null
# MAGIC

# COMMAND ----------

#Write your code here
df_branch=spark.read.format("delta").load("dbfs:/mnt/data_bronzelayer/Branch")
df_branch.createOrReplaceTempView("branch")

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Remove all the leading and trailing spaces in Brnach Country and covert it into UPPER CASE

# COMMAND ----------

# %sql
# select branch_id,branch_city, upper(trim(branch_country)) from Branch where branch_id is not null and Flag="False"

from pyspark.sql.functions import upper, trim
filtered_df_branch=df_branch.filter(df_branch.branch_id.isNotNull() &(df_branch.Flag=="False"))\
                    .select("branch_id","branch_city",upper(trim("branch_country")).alias("branch_country"))

# filtered_df = df.filter((df.branch_id.isNotNull()) & (df.Flag == "False")) \
#                 .select("branch_id", "branch_city", upper(trim(df.branch_country)).alias("branch_country"))

display(filtered_df_branch)

# COMMAND ----------

filtered_df_branch.createOrReplaceTempView2bfr6gt("cleaned_branch")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleaned_branch

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Merge into Silver layer table

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into komatsu_ade.silverlayer.branch T using cleaned_branch S on S.branch_id=T.branch_id when matched then 
# MAGIC update set T.branch_country=S.branch_country, T.branch_city=S.branch_city ,T.Merge_timestamp=current_timestamp() when not matched then
# MAGIC insert (branch_id, branch_city, branch_country, Merge_timestamp) values (S.branch_id, S.branch_city, S.branch_country, current_timestamp()) 

# COMMAND ----------

# MAGIC %md <b>
# MAGIC Update the merged_flag in the bronzelayer table

# COMMAND ----------

# MAGIC %sql
# MAGIC update branch set flag="True" where Flag="False"

# COMMAND ----------

# MAGIC %md
# MAGIC <b> Extension: You can add the test cases for all the steps

# COMMAND ----------

#Write your code here

