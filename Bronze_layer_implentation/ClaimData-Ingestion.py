# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType, LongType, StringType
from pyspark.sql.functions import lit

schema = StructType([
    StructField("claim_id", IntegerType(), True),
    StructField("policy_id", IntegerType(), True),
    StructField("date_of_claim", TimestampType(), True),
    StructField("claim_amount", LongType(), True),  # Changed from DoubleType to LongType
    StructField("claim_status", StringType(), True),
    StructField("LastUpdatedTimeStamp", TimestampType(), True)
])

df_claim = spark.read.format("parquet").schema(schema).load("dbfs:/mnt/data_landing/Incremetal/2024/11/28/Claim")
df_claim=df_claim.withColumn("Merge_Flag",lit("False"))
#df_claim.show()


# COMMAND ----------

df_claim.write.format("delta").option("path","dbfs:/mnt/data_bronzelayer/Claim").mode("overwrite").save()

# COMMAND ----------

df_claim.createOrReplaceTempView("claim")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from claim

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 1: Create a temporary table with the updated values
# MAGIC CREATE OR REPLACE TEMP VIEW updated_claim AS
# MAGIC SELECT *,
# MAGIC        CASE WHEN Merge_Flag = 'False' THEN 'True' ELSE Merge_Flag END AS new_Merge_Flag
# MAGIC FROM claim;
# MAGIC
# MAGIC -- Step 2: Create or replace the Delta table with the updated values
# MAGIC CREATE OR REPLACE TABLE claim_delta AS
# MAGIC SELECT claim_id, policy_id, date_of_claim, claim_amount, claim_status, LastUpdatedTimeStamp, new_Merge_Flag AS Merge_Flag
# MAGIC FROM updated_claim;
# MAGIC
# MAGIC -- -- Step 3: Convert the original table to Delta format
# MAGIC -- -- Ensure the table exists before converting
# MAGIC -- CONVERT TO DELTA komatsu_ade.default.claim;
# MAGIC
# MAGIC -- -- Step 4: Delete the old values from the original table
# MAGIC -- DELETE FROM komatsu_ade.default.claim WHERE Merge_Flag = 'False';
# MAGIC
# MAGIC -- -- Step 5: Insert the updated values back into the original table
# MAGIC -- INSERT INTO komatsu_ade.default.claim
# MAGIC -- SELECT claim_id, policy_id, date_of_claim, claim_amount, claim_status, LastUpdatedTimeStamp, Merge_Flag
# MAGIC -- FROM claim_delta;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table komatsu_ade.default.claim

# COMMAND ----------

from datetime import datetime

current_date=datetime.now().strftime("%m-%d-%Y")

#print(current_date)

new_folder='/mnt/data-processedlayer/Claim/'+current_date
#print(new_folder)

dbutils.fs.mv('/mnt/data_landing/Incremetal/2024/11/28/Claim/',new_folder,True)


# COMMAND ----------

from datetime import datetime
def getfilePathWithDate(filepath):
    #get the current date
    current_date=datetime.now().strftime("%m-%d-%Y")
    new_folder=filepath+current_date
    return new_folder


# COMMAND ----------

getfilePathWithDate("/mnt/data_landing/Full/")

# COMMAND ----------


