# Databricks notebook source
# MAGIC %md
# MAGIC # Structured Streaming using the Python DataFrames API
# MAGIC
# MAGIC Apache Spark includes a high-level stream processing API, [Structured Streaming](http://spark.apache.org/docs/latest/structured-streaming-programming-guide.html). In this notebook we take a quick look at how to use the DataFrame API to build Structured Streaming applications. We want to compute real-time metrics like running counts and windowed counts on a stream of timestamped actions (e.g. Open, Close, etc).
# MAGIC
# MAGIC To run this notebook, import it and attach it to a Spark cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample Data
# MAGIC We have some sample action data as files in `/databricks-datasets/structured-streaming/events/` which we are going to use to build this appication. Let's take a look at the contents of this directory.

# COMMAND ----------

# Look at the content of the following folder: /databricks-datasets/structured-streaming/events/
data = dbutils.fs.ls("/databricks-datasets/structured-streaming/events/")
display(data)
# What do you see?
#we see the paths, the names of the files, the sizes ans the timestamps of the modification

# COMMAND ----------

# MAGIC %md
# MAGIC There are about 50 JSON files in the directory. Let's see what each JSON file contains.

# COMMAND ----------

# Look at the functions head in dbutils
dbutils.fs.help()
# Open one file
print(dbutils.fs.head("/databricks-datasets/structured-streaming/events/file-0.json", 2000))

# COMMAND ----------

# MAGIC %md
# MAGIC Each line in the file contains JSON record with two fields - `time` and `action`. Let's try to analyze these files interactively.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Batch/Interactive Processing
# MAGIC The usual first step in attempting to process the data is to interactively query the data. Let's define a static DataFrame on the files, and give it a table name.

# COMMAND ----------

from pyspark.sql.types import *

inputPath = "/databricks-datasets/structured-streaming/events/"

# Since we know the data format already, let's define the schema to speed up processing (no need for Spark to infer schema)
jsonSchema = StructType(
  [ StructField("time", TimestampType(), True),
   StructField("action", StringType(), True) ]
)



# COMMAND ----------

# Read all json files, taking into account the defined schema, and display the content 
eventsDF = (
  spark
    .read                       
    .schema(jsonSchema)
    .json(inputPath)
)
eventsDF.display()


# COMMAND ----------

# MAGIC %md
# MAGIC - Compare the dates from the output without schema and with it.
# MAGIC
# MAGIC Without schema, spark leave it in string so we get just the text value of the timestamp (ex :1469501420) while with schema we get the given type, here Timestamp and it is displayed as a date time (ex : 2016-07-28T04:19:31.000+00:00).
# MAGIC - Did you notice that inputPath is a folder?
# MAGIC
# MAGIC Indeed, the path points to a directory that contains multiple JSON files, probably some batchs

# COMMAND ----------

# Calculate the total number of 'Open' and 'Close' actions 
actions_counts = (
    eventsDF
      .groupBy(eventsDF.action)
      .count()
)

display(actions_counts)

# COMMAND ----------

# Determine min and max time
from pyspark.sql.functions import min, max
mn_time = eventsDF.select(min(eventsDF.time)).first()[0]
mx_time = eventsDF.select(max(eventsDF.time)).first()[0]
print(f"min time: {mn_time}")
print(f"max time: {mx_time}")

# COMMAND ----------

# Calculate the number of "open" and "close" actions with one hour windows: staticCountsDF
# Look at groupBy(..., window) function
from pyspark.sql.functions import window
staticCountsDF  = (
    eventsDF
    .groupBy(eventsDF.action, window(eventsDF.time, "1 hour"))
    .count()
    .orderBy("window")
)
staticCountsDF.display()

# COMMAND ----------

# Make this window a sliding window (30 minutes overlap): staticCountsSW
sliding_window = window(eventsDF.time, "1 hour", "30 minutes")
staticCountsSW = (
    eventsDF
    .groupBy(eventsDF.action, sliding_window)
    .count()
    .orderBy("window")
)
staticCountsSW.display()

# COMMAND ----------

# Register staticCountsDF (createOrReplaceTempView) as table 'static_counts'
staticCountsDF.createOrReplaceTempView("static_counts")

# COMMAND ----------

# MAGIC %md
# MAGIC Now we can directly use SQL to query the table.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Count all Open and Close actions in the table static_counts  
# MAGIC SELECT action, SUM(count) FROM static_counts GROUP BY action

# COMMAND ----------

# MAGIC %sql
# MAGIC -- How many actions (Close and Open separately) is within each time window (in the table static_counts)
# MAGIC -- Make a plot
# MAGIC SELECT window.start as window_start, window.end as window_end, action, count FROM static_counts ORDER BY window_start, action
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Note the two ends of the graph. The close actions are generated such that they are after the corresponding open actions, so there are more "opens" in the beginning and more "closes" in the end.

# COMMAND ----------

# MAGIC %md
# MAGIC **NOTE:** Due to the Databricks environment, it is needed to also create temp tables in the unity catalog by running the below cell before starting the streaming processing. Additionally the below cell will need to be re-run to clear out the temp tables prior to re-running any  streaming dfs.

# COMMAND ----------

# Define the name of the new catalog
catalog = 'workspace'

# define variables for the trips data
schema = 'default'
volume = 'checkpoints'

# Path for file operations
path_volume = f'/Volumes/{catalog}/{schema}/{volume}'

# Three-part names for SQL operations
path_table = f'{catalog}.{schema}'
volume_name = f'{catalog}.{schema}.{volume}'

# Drop old temp volume (use three-part name, not path)
spark.sql(f"DROP VOLUME IF EXISTS {volume_name}")

# Create new temp volume
spark.sql(f"CREATE VOLUME IF NOT EXISTS {volume_name}")

# Define tmp dir for each stream
tmp_input = f"{path_volume}/input"
tmp_streaming_counts = f"{path_volume}/streaming_counts"
tmp_streaming_counts_filter = f"{path_volume}/streaming_counts_filter"
tmp_streaming_counts_run = f"{path_volume}/streaming_counts_run"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demo: Stream Processing 
# MAGIC Now that we have analyzed the data interactively, let's convert this to a streaming query that continuously updates as data comes. Since we just have a static set of files, we are going to emulate a stream from them by reading one file at a time, in the chronological order they were created. The query we have to write is pretty much the same as the interactive query above.
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *

# Read data from a file
# Similar to definition of staticInputDF above, just using `readStream` instead of `read`
streamingInputDF = (
  spark
    .readStream                       
    .schema(jsonSchema)               # Set the schema of the JSON data
    .option("maxFilesPerTrigger", 1)  # Treat a sequence of files as a stream by picking one file at a time
    .json(inputPath)
)

# Do some transformations
# Same query as staticInputDF
streamingCountsDF = (                 
  streamingInputDF
    .groupBy(
      streamingInputDF.action, 
      window(streamingInputDF.time, "1 hour"))
    .count()
)

# Is this DF actually a streaming DF?
streamingCountsDF.isStreaming

# COMMAND ----------

# Display input data
streamingInputDF.display(checkpointLocation=tmp_input)

# COMMAND ----------

# Display transformed data
streamingCountsDF.display(checkpointLocation=tmp_streaming_counts)

# COMMAND ----------

# Add aditional filter to transformed dataframe
streamingCountsDF.filter(streamingCountsDF.action == 'Open').display(checkpointLocation=tmp_streaming_counts_filter)

# COMMAND ----------

# MAGIC %md
# MAGIC As you can see, `streamingCountsDF` is a streaming Dataframe (`streamingCountsDF.isStreaming` was `true`). You can start streaming computation, by defining the sink and starting it. 
# MAGIC In our case, we want to interactively query the counts (same queries as above), so we will set the complete set of 1 hour counts to be in a in-memory table.

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "2")  # keep the size of shuffles small

query = (
  streamingCountsDF
    .writeStream
    .format("memory")        # memory = store in-memory table 
    .queryName("counts")     # counts = name of the in-memory table
    .outputMode("complete")  # complete = all the counts should be in the table
    .option("checkpointLocation", tmp_streaming_counts_run)
    .trigger(availableNow=True)
    .start()
)

# COMMAND ----------

# MAGIC %md
# MAGIC `query` is a handle to the streaming query that is running in the background. This query is continuously picking up files and updating the windowed counts. 
# MAGIC
# MAGIC Note the status of query in the above cell. The progress bar shows that the query is active. 
# MAGIC Furthermore, if you expand the `> counts` above, you will find the number of files they have already processed. 
# MAGIC
# MAGIC Let's wait a bit for a few files to be processed and then interactively query the in-memory `counts` table.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM counts

# COMMAND ----------

from time import sleep
sleep(1)  # wait a bit for computation to start

# COMMAND ----------

# MAGIC %sql
# MAGIC select action, date_format(window.end, "MMM-dd HH:mm") as time, count
# MAGIC from counts
# MAGIC order by time, action

# COMMAND ----------

# MAGIC %md
# MAGIC We see the timeline of windowed counts (similar to the static one earlier) building up. If we keep running this interactive query repeatedly, we will see the latest updated counts which the streaming query is updating in the background.

# COMMAND ----------

sleep(1)  # wait a bit more for more data to be computed

# COMMAND ----------

# MAGIC %sql select action, date_format(window.end, "MMM-dd HH:mm") as time, count from counts order by time, action

# COMMAND ----------

sleep(1)  # wait a bit more for more data to be computed

# COMMAND ----------

# MAGIC %sql select action, date_format(window.end, "MMM-dd HH:mm") as time, count from counts order by time, action

# COMMAND ----------

# MAGIC %md
# MAGIC Also, let's see the total number of "opens" and "closes".

# COMMAND ----------

# MAGIC %sql 
# MAGIC select action, sum(count) as total_count 
# MAGIC from counts 
# MAGIC group by action 
# MAGIC order by action

# COMMAND ----------

# MAGIC %md
# MAGIC If you keep running the above query repeatedly, you will always find that the number of "opens" is more than the number of "closes", as expected in a data stream where a "close" always appear after corresponding "open". This shows that Structured Streaming ensures **prefix integrity**. Read the blog posts linked below if you want to know more.
# MAGIC
# MAGIC Note that there are only a few files, so consuming all of them there will be no updates to the counts. Rerun the query if you want to interact with the streaming query again.
# MAGIC
# MAGIC Finally, you can stop the query running in the background, either by clicking on the 'Cancel' link in the cell of the query, or by executing `query.stop()`. Either way, when the query is stopped, the status of the corresponding cell above will automatically update to `TERMINATED`.

# COMMAND ----------

query.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ### IoT data

# COMMAND ----------

# MAGIC %md
# MAGIC Develop a streaming example on `IoT device`dataset:
# MAGIC
# MAGIC - inspect the dataset
# MAGIC - ask yourself couple of questions about the data and try to answer them (eg. how many steps users do, how many calories do they burn...)
# MAGIC - you read the data in streaming fashion (file by file) and keep the data for only one company? Here are some hints:
# MAGIC   - you can find the schema in the readme file 
# MAGIC   - as above, use this option: .option("maxFilesPerTrigger", 1)
# MAGIC   - use user_id or device_id for grouping
# MAGIC   - use timestamp for window definition
# MAGIC   - you can try streaming joins with the user data (/databricks-datasets/iot-stream/data-user/userData.csv). Here is the doc: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#join-operations

# COMMAND ----------

# Iot stream dataset
display(dbutils.fs.ls('/databricks-datasets/iot-stream/'))

# COMMAND ----------

# Read the README file
display(spark.read.text('/databricks-datasets/iot-stream/README.md'))

# COMMAND ----------

# Define the schema (copy from the README) data-device
from pyspark.sql.types import StructType, StructField, LongType, FloatType, StringType

deviceSchema = StructType([
    StructField("id", LongType(), False),
    StructField("user_id", LongType(), True),
    StructField("device_id", LongType(), True),
    StructField("num_steps", LongType(), True),
    StructField("miles_walked", FloatType(), True),
    StructField("calories_burnt", FloatType(), True),
    StructField("timestamp", StringType(), True),
    StructField("value", StringType(), True)
])

# COMMAND ----------

# Open one file to see how the data looks like (as a static dataframe)
inputPath = "/databricks-datasets/iot-stream/"
files = dbutils.fs.ls(inputPath)

first_file = files[1].path
df_static = spark.read.schema(deviceSchema).json(first_file)

display(df_static)
df_static.printSchema()

# COMMAND ----------

# Define your streaming dataframe
streamingDF = (
    spark
    .readStream                      # <-- lecture en streaming
    .schema(deviceSchema)            # appliquer le schéma défini
    .option("maxFilesPerTrigger", 1) # lire un fichier à la fois pour simuler un flux
    .json(inputPath)                 # lire les fichiers JSON
)
streamingDF.printSchema()

# COMMAND ----------

# Define your transformations
from pyspark.sql.functions import avg, sum, count, window

from pyspark.sql.functions import col, to_timestamp

transformedDF = (
    streamingDF
    .withColumn("event_time", to_timestamp(col("timestamp")))  # convertir le champ timestamp
    .select("user_id", "device_id", "num_steps", "miles_walked", "calories_burnt", "event_time")
)

# 2️⃣ Regrouper par utilisateur et fenêtre temporelle (par exemple, toutes les 10 minutes)
aggregatedDF = (
    transformedDF
    .groupBy(
        "user_id",
        window("event_time", "10 minutes")
    )
    .agg(
        avg("miles_walked").alias("avg_miles"),
        sum("num_steps").alias("total_steps"),
        sum("calories_burnt").alias("total_calories")
    )
)

# COMMAND ----------

# Define the sink and start streaming 
catalog = 'workspace'
schema = 'default'
volume = 'checkpoints'

path_volume = f'/Volumes/{catalog}/{schema}/{volume}'

iot_checkpoint = f"{path_volume}/iot_stream_checkpoint"

volume_name = f'{catalog}.{schema}.{volume}'

spark.sql(f"DROP VOLUME IF EXISTS {volume_name}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {volume_name}")

query = (
    aggregatedDF.writeStream
    .outputMode("complete")
    .option("checkpointLocation", iot_checkpoint)
    .queryName("iot_counts")
    .trigger(once=True)
    .toTable("workspace.default.iot_counts")
)

query.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- visualize your streaming analytics
# MAGIC SELECT window.start as start_time,
# MAGIC        window.end as end_time,
# MAGIC        user_id,
# MAGIC        total_steps,
# MAGIC        avg_miles,
# MAGIC        total_calories
# MAGIC FROM iot_counts
# MAGIC ORDER BY start_time, user_id
# MAGIC