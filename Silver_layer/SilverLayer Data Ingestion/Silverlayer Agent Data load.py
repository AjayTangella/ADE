# Databricks notebook source
# MAGIC %sql
# MAGIC create  or replace  table  silverlayer.agent(
# MAGIC   agent_id integer, 
# MAGIC   agent_name string, 
# MAGIC   agent_email string,
# MAGIC   agent_phone string,
# MAGIC   branch_id integer, 
# MAGIC   create_timestamp timestamp,
# MAGIC   Flag integer,
# MAGIC   merge_timestamp timestamp
# MAGIC )
# MAGIC using delta 
# MAGIC -- location '/mnt/data_silverlayer'
# MAGIC
# MAGIC --select * from silverlayer.agent
# MAGIC

# COMMAND ----------

df_BranchData= spark.read.format("delta").load("dbfs:/mnt/data_bronzelayer/Branch/")
df_AgentData=spark.read.format("delta").load("dbfs:/mnt/data_bronzelayer/Agent")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from agent where agent_name like 'Test%'

# COMMAND ----------


# df_BranchData.dropDuplicates()
#df_AgentData.dropDuplicates()
# display(df_BranchData.limit(10))
# display(df_AgentData.limit(10))

df_AgentData.display()

# COMMAND ----------

df_AgentData.createOrReplaceTempView("agent")
df_BranchData.createOrReplaceTempView("branch")

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from agent where agent_id=2003;
# MAGIC
# MAGIC select count(*) from silverlayer.agent  

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO agent (agent_id, agent_name, agent_email, agent_phone, branch_id, create_timestamp) VALUES
# MAGIC (2003, 'Test1', 'bcreeber2@sitemeter.com', '3509759144', 2138, '2021-12-05'),
# MAGIC (2006, 'Test2', 'sdrissell5@cyberchimps.com', '2432547349', 2464, '2021-06-08')
# MAGIC --select * from silverlayer.agent where agent_name like 'Test%'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select a.agent_id,a.agent_name,a.agent_email,
# MAGIC -- --ifnull(a.agent_phone,"test@gmail.com"),
# MAGIC -- coalesce(nullif(a.agent_email,''),'test@gnail.com') as agent_email,
# MAGIC -- a.branch_id,a.create_timestamp
# MAGIC --  from agent A join branch B  on A.branch_id=B.branch_id
# MAGIC -- where length(A.agent_phone)=10  
# MAGIC
# MAGIC with cte as(
# MAGIC select a.agent_id,a.agent_name,a.agent_email,
# MAGIC --ifnull(a.agent_phone,"test@gmail.com"),
# MAGIC --regexp_replace(a.agent_email,"",'test@gmail.com') as new_agent_email,
# MAGIC a.branch_id,a.create_timestamp
# MAGIC  from agent A join branch B  on A.branch_id=B.branch_id
# MAGIC where length(A.agent_phone)=10 )
# MAGIC
# MAGIC select agent_id,agent_name,regexp_replace(cte.agent_email," ",'test@gmail.com')as agent_email,branch_id,create_timestamp from cte

# COMMAND ----------

from pyspark.sql.functions import col, length,regexp_replace

df_result=df_AgentData.join(df_BranchData,"branch_id")
df_result_1=df_result.filter(length(col("agent_phone"))==10)

#replace null values with test@gmail.com
df_remove_null_result=df_result_1.na.fill({"agent_email":"test@gmail.com"})

#replace empty value to test@gmail.com
df_final_result=df_remove_null_result.withColumn("agent_email",regexp_replace(col("agent_email"),"^$","test@gmail.com"))
display(df_final_result)

# COMMAND ----------

df_final_result.createOrReplaceTempView("cleaned_data")


# COMMAND ----------

# MAGIC %sql
# MAGIC -- UPDATE T
# MAGIC -- SET t.Flag = 0, 
# MAGIC --     t.merge_timestamp = CURRENT_TIMESTAMP
# MAGIC -- --SELECT T.*  
# MAGIC -- FROM silverlayer.agent T 
# MAGIC -- INNER JOIN cleaned_data s ON s.branch_id = T.branch_id
# MAGIC -- WHERE s.agent_id = T.agent_id
# MAGIC -- AND (s.agent_name <> T.agent_name OR 
# MAGIC --      s.agent_email <> T.agent_email OR 
# MAGIC --      s.agent_phone <> T.agent_phone OR 
# MAGIC --      s.agent_id <> T.agent_id) 
# MAGIC
# MAGIC
# MAGIC
# MAGIC MERGE INTO silverlayer.agent T
# MAGIC USING cleaned_data s
# MAGIC ON s.branch_id = T.branch_id AND s.agent_id = T.agent_id
# MAGIC WHEN MATCHED AND (s.agent_name <> T.agent_name OR 
# MAGIC                   s.agent_email <> T.agent_email OR 
# MAGIC                   s.agent_phone <> T.agent_phone OR 
# MAGIC                   s.agent_id <> T.agent_id)
# MAGIC THEN UPDATE SET 
# MAGIC     T.Flag = 0, 
# MAGIC     T.merge_timestamp = CURRENT_TIMESTAMP

# COMMAND ----------

# MAGIC %sql
# MAGIC -- UPDATE silverlayer.agent T
# MAGIC -- SET T.Flag = 0, 
# MAGIC --     T.merge_timestamp = CURRENT_TIMESTAMP
# MAGIC select * from silverlayer.agent T
# MAGIC WHERE T.agent_id IN (
# MAGIC     SELECT s.agent_id
# MAGIC     FROM cleaned_data s
# MAGIC     WHERE s.branch_id = T.branch_id
# MAGIC     AND (s.agent_name <> T.agent_name OR 
# MAGIC          s.agent_email <> T.agent_email OR 
# MAGIC          s.agent_phone <> T.agent_phone OR 
# MAGIC          s.agent_id <> T.agent_id)
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC #SCD Type 2 in Delta table

# COMMAND ----------

# MAGIC %sql
# MAGIC --Insert the new records 
# MAGIC INSERT INTO silverlayer.agent (agent_id, agent_name, agent_email, agent_phone, branch_id, create_timestamp, Flag, merge_timestamp)
# MAGIC SELECT s.agent_id, s.agent_name, s.agent_email, s.agent_phone, s.branch_id, s.create_timestamp, 1, current_date()
# MAGIC FROM cleaned_data s
# MAGIC LEFT JOIN silverlayer.agent T ON s.branch_id = t.branch_id
# MAGIC WHERE t.branch_id IS NULL;
# MAGIC
# MAGIC
# MAGIC --update the existing records not working
# MAGIC -- UPDATE T
# MAGIC -- SET t.Flag = 0, 
# MAGIC --     t.merge_timestamp = CURRENT_TIMESTAMP
# MAGIC -- --SELECT T.*
# MAGIC -- FROM silverlayer.agent T 
# MAGIC -- INNER JOIN cleaned_data s ON s.branch_id = T.branch_id
# MAGIC -- WHERE s.agent_id = T.agent_id
# MAGIC -- AND (s.agent_name <> T.agent_name OR 
# MAGIC --      s.agent_email <> T.agent_email OR 
# MAGIC --      s.agent_phone <> T.agent_phone OR 
# MAGIC --      s.agent_id <> T.agent_id) 
# MAGIC
# MAGIC --update the existing records
# MAGIC UPDATE silverlayer.agent T
# MAGIC SET T.Flag = 0, 
# MAGIC     T.merge_timestamp = CURRENT_TIMESTAMP
# MAGIC WHERE T.agent_id IN (
# MAGIC     SELECT s.agent_id
# MAGIC     FROM cleaned_data s
# MAGIC     WHERE s.branch_id = T.branch_id
# MAGIC     AND (s.agent_name <> T.agent_name OR 
# MAGIC          s.agent_email <> T.agent_email OR 
# MAGIC          s.agent_phone <> T.agent_phone OR 
# MAGIC          s.agent_id <> T.agent_id)
# MAGIC );
# MAGIC
# MAGIC
# MAGIC --select * from [dbo].[merge_Agent]  where agent_name like '%test%'
# MAGIC
# MAGIC --Update records need to insert again 
# MAGIC INSERT INTO silverlayer.agent (agent_id, agent_name, agent_email, agent_phone, branch_id, create_timestamp, Flag, merge_timestamp)
# MAGIC SELECT s.agent_id, s.agent_name, s.agent_email, s.agent_phone, s.branch_id, s.create_timestamp, 1, CURRENT_TIMESTAMP
# MAGIC FROM cleaned_data s
# MAGIC LEFT JOIN silverlayer.agent t ON s.agent_id = t.agent_id AND s.branch_id = t.branch_id
# MAGIC WHERE t.agent_id IS NULL OR 
# MAGIC       (s.agent_name <> t.agent_name OR 
# MAGIC        s.agent_email <> t.agent_email OR 
# MAGIC        s.agent_phone <> t.agent_phone) 
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silverlayer.agent where agent_email='test@gmail.com'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silverlayer.agent  where agent_name='Test1'

# COMMAND ----------

# MAGIC %md
# MAGIC #using Merge

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silverlayer.agent AS target
# MAGIC USING cleaned_data AS source
# MAGIC ON target.branch_id = source.branch_id
# MAGIC WHEN MATCHED AND (
# MAGIC     target.agent_name <> source.agent_name OR 
# MAGIC     target.agent_email <> source.agent_email OR 
# MAGIC     target.agent_phone <> source.agent_phone OR 
# MAGIC     target.agent_id <> source.agent_id
# MAGIC ) THEN
# MAGIC     -- Update the existing record to mark it as inactive
# MAGIC     UPDATE SET 
# MAGIC         target.Flag = 0, 
# MAGIC         target.merge_timestamp = CURRENT_TIMESTAMP
# MAGIC WHEN MATCHED THEN
# MAGIC     -- Optionally, you can handle cases where no changes are needed
# MAGIC     -- (this part is optional and can be omitted if not needed)
# MAGIC     UPDATE SET 
# MAGIC         target.merge_timestamp = CURRENT_TIMESTAMP
# MAGIC WHEN NOT MATCHED BY TARGET THEN
# MAGIC     -- Insert new records
# MAGIC     INSERT (agent_id, agent_name, agent_email, agent_phone, branch_id, create_timestamp, Flag, merge_timestamp)
# MAGIC     VALUES (source.agent_id, source.agent_name, source.agent_email, source.agent_phone, source.branch_id, source.create_timestamp, 1, CURRENT_TIMESTAMP);
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #not working this scenario 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH deduplicated_source AS (
# MAGIC     SELECT 
# MAGIC         *,
# MAGIC         ROW_NUMBER() OVER (PARTITION BY branch_id ORDER BY create_timestamp DESC) AS row_num
# MAGIC     FROM 
# MAGIC         cleaned_data
# MAGIC )
# MAGIC MERGE INTO silverlayer.agent AS target
# MAGIC USING (
# MAGIC     SELECT * 
# MAGIC     FROM deduplicated_source 
# MAGIC     WHERE row_num = 1
# MAGIC ) AS source
# MAGIC ON target.branch_id = source.branch_id
# MAGIC WHEN MATCHED AND (
# MAGIC     target.agent_name <> source.agent_name OR 
# MAGIC     target.agent_email <> source.agent_email OR 
# MAGIC     target.agent_phone <> source.agent_phone OR 
# MAGIC     target.agent_id <> source.agent_id
# MAGIC ) THEN
# MAGIC     -- Update the existing record to mark it as inactive
# MAGIC     UPDATE SET 
# MAGIC         target.Flag = 0, 
# MAGIC         target.merge_timestamp = CURRENT_TIMESTAMP
# MAGIC WHEN MATCHED THEN
# MAGIC     -- Optionally, you can handle cases where no changes are needed
# MAGIC     -- (this part is optional and can be omitted if not needed)
# MAGIC     UPDATE SET 
# MAGIC         target.merge_timestamp = CURRENT_TIMESTAMP
# MAGIC WHEN NOT MATCHED BY TARGET THEN
# MAGIC     -- Insert new records
# MAGIC     INSERT (agent_id, agent_name, agent_email, agent_phone, branch_id, create_timestamp, Flag, merge_timestamp)
# MAGIC     VALUES (source.agent_id, source.agent_name, source.agent_email, source.agent_phone, source.branch_id, source.create_timestamp, 1, CURRENT_TIMESTAMP);

# COMMAND ----------


