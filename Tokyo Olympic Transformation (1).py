# Databricks notebook source
from pyspark.sql.types import StructType , StructField , StringType ,IntegerType,DoubleType,BooleanType,DateType
from pyspark.sql.functions import col 

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "7df78ecb-1140-4745-b0b0-738d2037e9f7",
"fs.azure.account.oauth2.client.secret": 'E5I8Q~j-gNaHEQ-6EVTEsxilyw0iCt0lltRf-b55',
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/3d7a934d-a026-42dc-9e68-f693eb167d47/oauth2/token"}

dbutils.fs.mount(
source = "abfss://tokyoolympicdata@tokyoolympicstorage2580.dfs.core.windows.net", # contrainer@storageac
mount_point = "/mnt/olymicmount2",
extra_configs = configs)


# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/olymicmount2"

# COMMAND ----------

athletes = spark.read.format("csv").load("/mnt/olymicmount2/Raw data/athletes.csv")

# COMMAND ----------

athletes.show()

# COMMAND ----------

athletes = spark.read.format("csv").option("header","true").load("/mnt/olymicmount2/Raw data/athletes.csv")
coaches = spark.read.format("csv").option("header","true").load("/mnt/olymicmount2/Raw data/coaches.csv")
entriesgender= spark.read.format("csv").option("header","true").load("/mnt/olymicmount2/Raw data/entriesgender.csv")
medals= spark.read.format("csv").option("header","true").load("/mnt/olymicmount2/Raw data/medals.csv")
teams = spark.read.format("csv").option("header","true").load("/mnt/olymicmount2/Raw data/teams.csv")

# COMMAND ----------

athletes.show()

# COMMAND ----------

athletes.printSchema()

# COMMAND ----------

coaches.show()

# COMMAND ----------

coaches.printSchema()

# COMMAND ----------

entriesgender.show()

# COMMAND ----------

entriesgender.printSchema()

# COMMAND ----------

entriesgender=entriesgender.withColumn("Female",col("Female").cast("int"))\
    .withColumn("Male",col("Male").cast("int"))\
     .withColumn("Total",col("Total").cast("int"))

# COMMAND ----------

entriesgender.printSchema()

# COMMAND ----------

medals.show()

# COMMAND ----------

medals.printSchema()

# COMMAND ----------

medals=medals.withColumn("Rank",col("Rank").cast("int"))\
    .withColumn("Gold",col("Gold").cast("int"))\
     .withColumn("Silver",col("Silver").cast("int"))\
         .withColumn("Bronze",col("Bronze").cast("int"))\
             .withColumn("Rank by Total",col("Rank by Total").cast("int"))

# COMMAND ----------

medals.printSchema()

# COMMAND ----------

teams.show()

# COMMAND ----------

teams.printSchema()

# COMMAND ----------

#Calculate the average number of entries by gender for each discipline
average_entries_by_gender = entriesgender.withColumn('Avg_Female',entriesgender['Female']/entriesgender['Total']).withColumn('Avg_Male',entriesgender['Male']/entriesgender['Total'])
average_entries_by_gender.show()

# COMMAND ----------

athletes.write.mode("overwrite").option("header","true").csv("mnt/olymicmount2/Transform data/athletes")
coaches.write.mode("overwrite").option("header","true").csv("mnt/olymicmount2/Transform data/coaches")
entriesgender.write.mode("overwrite").option("header","true").csv("mnt/olymicmount2/Transform data/entriesgender")
medals.write.mode("overwrite").option("header","true").csv("mnt/olymicmount2/Transform data/medals")
teams.write.mode("overwrite").option("header","true").csv("mnt/olymicmount2/Transform data/teams")
