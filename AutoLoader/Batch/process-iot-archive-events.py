# Databricks notebook source
# MAGIC %md
# MAGIC #Autoloader Demo - Batch Run
# MAGIC 
# MAGIC This notebook will take in parameters of:
# MAGIC - Where the autoloader should look for new files (i.e. the source loacation)
# MAGIC - Where the autoloader should store the schema (the initial run will try to infer the schema based on the files that are in the source location)
# MAGIC - Where should autoloader store the checkpoint information to help with finding new files that were loaded to your source location
# MAGIC - Where should autoloader store the data that is new. 
# MAGIC 
# MAGIC This notebook will be triggered

# COMMAND ----------

# DBTITLE 1,Parameter Configuration
dbutils.widgets.text("sourceLocationForFiles", "/mnt/andrdatalake001x/bronze/environmentalsensors/*", "Source Location For AutoLoader to find the files: ")
dbutils.widgets.text("autoLoaderSchemaLocation", "/mnt/andrdatalake001x/bronze/autoloader/environmentalsensors/schemalocation", "Where should AutoLoader put the Schema for the files: ")
dbutils.widgets.text("autoLoaderCheckpointLocation", "/mnt/andrdatalake001x/bronze/autoloader/environmentalsensors/checkpoint", "Where should Autoloader store the checkpoint location for the files: ")
dbutils.widgets.text("destinationLocation", "/mnt/andrdatalake001x/silver/environmentalsensors/", "Where should the data be written to: ")

# COMMAND ----------

# DBTITLE 1,Process The Cloud Files
#Enable Metric Collection for Spark Structure Streaming
spark.conf.set("spark.sql.streaming.metricsEnabled", "true")

#Read the Cloud Files from the source location and store the schema from those files in the location for the schema
df = spark.readStream.format("cloudFiles") \
  .option("cloudFiles.format", "json") \
  .option("cloudFiles.inferColumnTypes", "true") \
  .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \
  .option("cloudFiles.schemaLocation", dbutils.widgets.get("autoLoaderSchemaLocation")) \
  .load(dbutils.widgets.get("sourceLocationForFiles")) 


#Write the data in to the delta lake format into the location above
output = df.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", dbutils.widgets.get("autoLoaderCheckpointLocation")) \
  .option("mergeSchema", "true") \
  .trigger(once=True) \
  .start(dbutils.widgets.get("destinationLocation")) 
