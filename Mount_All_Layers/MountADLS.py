# Databricks notebook source
# MAGIC %md
# MAGIC <b> Mount the Bronze Layer Container:
# MAGIC   

# COMMAND ----------

# Container name
# Stoage account name
# Storage account SAS token
# Mount point name (it could be anything /mnt/.....)

#code Template


dbutils.fs.mount( source = 'wasbs://bronzelayer@cignainsurance.blob.core.windows.net', 
                 mount_point= '/mnt/data_bronzelayer', extra_configs ={'fs.azure.sas.bronzelayer.cignainsurance.blob.core.windows.net':'sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2025-02-07T17:15:56Z&st=2024-11-28T09:15:56Z&spr=https&sig=eJAcEIqoCLMjccsyCJ%2B7mHs7aEsPypJYDdQnwwhSPwc%3D'})



# COMMAND ----------

# MAGIC %md
# MAGIC <b> Checking Bronze Layer Mount point

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/data_bronzelayer

# COMMAND ----------

# MAGIC %md
# MAGIC <b> Mount the Landing Container:

# COMMAND ----------

dbutils.fs.mount( source = 'wasbs://landing@cignainsurance.blob.core.windows.net', 
                 mount_point= '/mnt/data_landing', extra_configs ={'fs.azure.sas.landing.cignainsurance.blob.core.windows.net':'sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2025-03-22T17:06:09Z&st=2024-11-28T09:06:09Z&spr=https&sig=2IoqV8MFrNFJvFOruNboQk1zMcbXtI7SBKWwhFzRTvM%3D'})

# COMMAND ----------

# MAGIC %md
# MAGIC <b> Checking landing Mount point

# COMMAND ----------

# MAGIC %fs
# MAGIC ls mnt/landing

# COMMAND ----------

# MAGIC %md
# MAGIC <b> Mount the Processed Container:

# COMMAND ----------

dbutils.fs.mount( source = 'wasbs://processedlayer@cignainsurance.blob.core.windows.net', 
                 mount_point= '/mnt/data-processedlayer', extra_configs ={'fs.azure.sas.processedlayer.cignainsurance.blob.core.windows.net':'sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2025-01-10T17:47:54Z&st=2024-11-28T09:47:54Z&spr=https&sig=hOMyWe7kjuduRnznLMponGRMXqAWgwVbnfQ8SJ9DpdI%3D'})

# COMMAND ----------

# MAGIC %md
# MAGIC <b> Checking processed Mount point

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/data-processedlayer/

# COMMAND ----------

# MAGIC %md
# MAGIC <b> Create Silver Layer Mount Point

# COMMAND ----------

dbutils.fs.mount( source = 'wasbs://silverlayer@cignainsurance.blob.core.windows.net', 
                 mount_point= '/mnt/data_silverlayer', extra_configs ={'fs.azure.sas.silverlayer.cignainsurance.blob.core.windows.net':'sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2025-01-11T00:21:59Z&st=2024-11-28T16:21:59Z&spr=https&sig=p8z%2F9qWf%2Fcf9BBPN2DjokkKvp3EERdlYbXUrQJfrfcw%3D'})

# COMMAND ----------

# MAGIC %md
# MAGIC <b> Check Silver Layer Mount Point

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/data_silverlayer

# COMMAND ----------

# MAGIC %md
# MAGIC <b> Create Golder Layer Mount Point

# COMMAND ----------

# MAGIC %md
# MAGIC <b> Check Golden Layer Mount Point

# COMMAND ----------

# MAGIC %md
# MAGIC <b> Create and Load Agent Table in bronze layer

# COMMAND ----------

 display(dbutils.fs.ls("/mnt/data_bronzelayer/Agent"))

# COMMAND ----------

from datetime import datetime

current_date=datetime.now().strftime("%m-%d-%Y")

#print(current_date)

#new_folder='/mnt/data_processed/AgentData'+current_date
#print(new_folder)

dbutils.fs.mv('/mnt/data-processedlayer/AgentData/','/mnt/data_landing/Full/2024/11/28/Agent/',True)


# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/data_landing/Full/2024/11/28/Agent/

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/data_processed/BranchData

# COMMAND ----------


