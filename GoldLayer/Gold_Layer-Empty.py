# Databricks notebook source
# MAGIC %md
# MAGIC <b> Sales By Policy Type and Month: </b>
# MAGIC This table would contain the total sales for each policy type and each month. It would be used to analyze the performance of different policy types over time.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT P.policy_type,
# MAGIC        EXTRACT(MONTH FROM TO_DATE(C.date_of_claim, 'yyyy-MM-dd')) AS sale_month, 
# MAGIC        COUNT(C.claim_id) AS total_sales
# MAGIC FROM silverlayer.policy P 
# MAGIC JOIN silverlayer.claim C 
# MAGIC ON P.policy_id = C.policy_id
# MAGIC WHERE C.date_of_claim IS NOT NULL  -- Ensure date_of_claim is not NULL
# MAGIC GROUP BY P.policy_type, EXTRACT(MONTH FROM TO_DATE(C.date_of_claim, 'yyyy-MM-dd'))
# MAGIC ORDER BY P.policy_type, sale_month;
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view vw_sales_by_policy_type_month as
# MAGIC select P.policy_type,
# MAGIC        month(C.date_of_claim) as sale_month, 
# MAGIC        count(C.claim_id) as total_sales
# MAGIC from silverlayer.policy P 
# MAGIC left join silverlayer.claim C 
# MAGIC on P.policy_id = C.policy_id
# MAGIC WHERE C.claim_id IS NULL
# MAGIC group by P.policy_type, month(C.date_of_claim)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vw_sales_by_policy_type_month

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Premiums By Agent and Branch: </b>
# MAGIC This table would contain the total premiums earned by each agent and each branch. It would be used to identify the top-performing agents and branches and to allocate resources accordingly.

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Claims By Policy Type and Status:</b>
# MAGIC  This table would contain the number and amount of claims by policy type and claim status. It would be used to monitor the claims process and identify any trends or issues.

# COMMAND ----------

# MAGIC %sql
# MAGIC --  select * from goldlayer.claim_by_policy_type_month;
# MAGIC
# MAGIC
# MAGIC create or replace temp view vw_claim_by_policy_type_month
# MAGIC as
# MAGIC select P.policy_type,
# MAGIC        C.claim_status,
# MAGIC        count(C.claim_id) as total_claim,
# MAGIC        sum(C.claim_amount) as total_claim_amount
# MAGIC from silverlayer.claim C
# MAGIC join silverlayer.policy P
# MAGIC on C.policy_id = P.policy_id
# MAGIC group by P.policy_type, C.claim_status
# MAGIC having P.policy_type is not null

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from goldlayer.claim_by_policy_type_month

# COMMAND ----------

# MAGIC %md
# MAGIC #merge claim_by_policy_type_month

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into goldlayer.claim_by_policy_type_month T using vw_claim_by_policy_type_month S
# MAGIC on T.policy_type=S.policy_type and T.claim_status=S.claim_status
# MAGIC when matched then 
# MAGIC update set T.claim_status=s.claim_status,
# MAGIC            T.total_claim=S.total_claim,
# MAGIC            T.total_claim_amount=S.total_claim_amount,
# MAGIC            T.updated_timestamp=current_timestamp
# MAGIC when not matched then 
# MAGIC Insert (policy_type,claim_status,total_claim,total_claim_amount,updated_timestamp)
# MAGIC values(
# MAGIC        S.policy_type,
# MAGIC        S.claim_status,
# MAGIC        S.total_claim,
# MAGIC        S.total_claim_amount,
# MAGIC        current_timestamp()
# MAGIC )
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #Analyze the claim data based on the policy type like AVG, MAX, MIN, Count of claim.

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view vm_claim_analysis
# MAGIC as
# MAGIC select policy_type,
# MAGIC       avg(claim_amount) as avg_claim_amount,
# MAGIC       max(claim_amount) as max_claim_amount,
# MAGIC       min(claim_amount) as min_claim_amount,
# MAGIC       count(distinct claim_id) as total_claims
# MAGIC    from silverlayer.claim C join silverlayer.policy P
# MAGIC on C.policy_id=P.policy_id
# MAGIC group by P.policy_type
# MAGIC having P.policy_type is not null

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vm_claim_analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into goldlayer.claim_analysis T using vm_claim_analysis S on T.policy_type=S.policy_type when matched then
# MAGIC update set T.policy_type=S.policy_type,
# MAGIC            T.avg_claim_amount=S.avg_claim_amount,
# MAGIC            T.max_claim_amount=S.max_claim_amount,
# MAGIC            T.min_claim_amount=S.min_claim_amount,
# MAGIC            T.total_claim=S.total_claims,
# MAGIC            T.updated_timestamp=current_timestamp() 
# MAGIC when not matched then
# MAGIC Insert(policy_type,avg_claim_amount,max_claim_amount,min_claim_amount,total_claim,updated_timestamp)
# MAGIC values(S.policy_type,S.avg_claim_amount,S.max_claim_amount,S.min_claim_amount,S.total_claims,current_timestamp())
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vm_claim_analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from goldlayer.claim_analysis

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Sales By Agent and Branch: </b>
# MAGIC This table would contain the total sales for each agent and each branch. It would be used to identify the top-performing agents and branches and to allocate resources accordingly.
