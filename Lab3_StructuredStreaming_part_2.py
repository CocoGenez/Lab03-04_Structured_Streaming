# Databricks notebook source
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    BooleanType, LongType, MapType
)
from pyspark.sql.functions import window, col
from datetime import datetime

# COMMAND ----------

# project data catalog defined and created in <placeholder> notebook
catalog = 'wikimedia_db'

# db_schema containing unprocessed/streaming data
uc_schema_raw_events = 'raw_events'

# raw data is saved in a temp volume by yy_mm_day
raw_events_volume_time = datetime.now()
raw_events_volume =  f"events_tmp_{raw_events_volume_time.strftime('%y_%m_%d')}"
raw_data_path = f'/Volumes/{catalog}/{uc_schema_raw_events}/{raw_events_volume}'

# db schema for checkpointing streaming tables
db_schema_checkpoints = 'checkpoints'
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{db_schema_checkpoints}")

# COMMAND ----------


# Meta schema (nested)
meta_schema = StructType([
    StructField("uri", StringType(), True),
    StructField("request_id", StringType(), True),
    StructField("id", StringType(), True),
    StructField("dt", StringType(), True),
    StructField("domain", StringType(), True),
    StructField("stream", StringType(), True)
])

# Length schema (nested)
length_schema = StructType([
    StructField("old", IntegerType(), True),
    StructField("new", IntegerType(), True)
])

# Revision schema (nested)
revision_schema = StructType([
    StructField("old", LongType(), True),
    StructField("new", LongType(), True)
])

# Main recent change schema
recentchange_schema = StructType([
    StructField("$schema", StringType(), True),
    StructField("meta", meta_schema, True),
    StructField("id", LongType(), True),
    StructField("type", StringType(), True),
    StructField("namespace", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("comment", StringType(), True),
    StructField("timestamp", LongType(), True),
    StructField("user", StringType(), True),
    StructField("bot", BooleanType(), True),
    StructField("minor", BooleanType(), True),
    StructField("patrolled", BooleanType(), True),
    StructField("length", length_schema, True),
    StructField("revision", revision_schema, True),
    StructField("server_url", StringType(), True),
    StructField("server_name", StringType(), True),
    StructField("wiki", StringType(), True),
    StructField("parsedcomment", StringType(), True),
])


# COMMAND ----------

# Read data from a file
# Similar to definition of staticInputDF above, just using `readStream` instead of `read`
streamingInputDF = (
  spark
    .readStream                       
    .schema(recentchange_schema)               # Set the schema of the JSON data
    .option("maxFilesPerTrigger", 1)  # Treat a sequence of files as a stream by picking n number of files at a time
    .json(raw_data_path)
)


# COMMAND ----------

# Do some transformations
# Same query as staticInputDF
streamingCountsDF = (
  streamingInputDF
    .groupBy(
      streamingInputDF.bot, # group by edit made by bot boolean
      window(
        col("timestamp").cast("timestamp"), 
        "5 minutes"
      )
    )
    .count()
)


# COMMAND ----------

# temp volume for checkpoint storage
volume = 'tmp_streamingInputDF'
volume_path = f'/Volumes/{catalog}/{db_schema_checkpoints}/{volume}'
volume_name = f'{catalog}.{db_schema_checkpoints}.{volume}'

# drop old temp volume and recreate
spark.sql(f"DROP VOLUME IF EXISTS {volume_name}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {volume_name}")

# Display the streaming dataframe
streamingInputDF.display(checkpointLocation=volume_path)

# COMMAND ----------

# temp volume for checkpoint storage
volume = 'tmp_streamingDF'
volume_path = f'/Volumes/{catalog}/{db_schema_checkpoints}/{volume}'
volume_name = f'{catalog}.{db_schema_checkpoints}.{volume}'

# drop old temp volume and recreate
spark.sql(f"DROP VOLUME IF EXISTS {volume_name}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {volume_name}")

# Display transformed data
streamingCountsDF.display(checkpointLocation=volume_path)