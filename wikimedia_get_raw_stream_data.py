# Databricks notebook source
# Install libs not already included in instance
%pip install requests-sse
%pip install pywikibot
%restart_python

# COMMAND ----------

import json
import os
from pywikibot.comms.eventstreams import EventStreams
from datetime import datetime, timedelta


# COMMAND ----------

# Define the name of the new uc_catalog
uc_catalog = 'wikimedia_db'
spark.sql('create catalog if not exists ' + uc_catalog)

# define the raw events schema
uc_schema_raw_events = 'raw_events'
spark.sql('create schema if not exists ' + uc_catalog + '.' + uc_schema_raw_events)

# save the volume time
tmp_volume_time = datetime.now()
tmp_volume =  f'events_tmp_{tmp_volume_time.strftime('%y_%m_%d')}'
spark.sql('create volume if not exists ' + uc_catalog + '.' + uc_schema_raw_events + '.' + tmp_volume)


# COMMAND ----------

# Create uc last_event_cache schema and volume if not exists
uc_schema_last_event_cache = 'last_event_cache'
spark.sql('create schema if not exists ' + uc_catalog + '.' + uc_schema_last_event_cache)

last_event_cache_volume = 'data'
spark.sql('create volume if not exists ' + uc_catalog + '.' + uc_schema_last_event_cache + '.' + last_event_cache_volume)

# COMMAND ----------


# simple helper function for checking if a file exists
def check_file_exists(last_event_cache_path: str) -> bool:
    return os.path.exists(last_event_cache_path)

# set stream object to start from  7 days ago on first run
# and then from the last event id on subsequent runs
def set_stream(last_event_cache_path: str, start_time: datetime) -> EventStreams:
    if not check_file_exists(last_event_cache_path):
        # start from 7 days ago
        stream_start_date_raw = start_time - timedelta(days=8)
        stream_start_date_formatted = stream_start_date_raw.strftime('%Y%m%d')
        return EventStreams(streams=["recentchange", "revision-create"], since=stream_start_date_formatted)
    else:
        # start from last event id
        with open(last_event_cache_path, 'r') as f:
            last_time_stamp = f.read()
            return EventStreams(streams=["recentchange", "revision-create"], since=last_time_stamp)



# COMMAND ----------

# set start time for streaming and temp voume naming
start_time = datetime.now()

# set stop time for streaming
duration = 5
stop_time = start_time + timedelta(minutes=duration)

# set last_event_cache path
last_event_cache_path = f"/Volumes/{uc_catalog}/{uc_schema_last_event_cache}/{last_event_cache_volume}/last_event_cache.txt"

# create the streaming object
stream = set_stream(last_event_cache_path, start_time)


# COMMAND ----------

# example filter to only look for edits to fr.wikipedia
stream.register_filter(server_name='fr.wikipedia.org', type='edit')

# build path for temp volume
raw_volume_path = f"/Volumes/{uc_catalog}/{uc_schema_raw_events}/{tmp_volume}"

# loop to get streaming data
while datetime.now() < stop_time:
    change = next(stream)
    
    # Use a field that definitely exists in the event data
    event_timestamp = change['meta']['dt']  # ISO 8601 timestamp
    revision_id = change.get('revision', {}).get('new', 'unknown')  # More reliable
    
    file = f"{raw_volume_path}/event_{revision_id}.json"

    # write event to file
    with open(file, 'w') as f:
        json.dump(change, f)

    # update last_event_cache with TIMESTAMP (what 'since' actually needs)
    with open(last_event_cache_path, 'w') as f:
        f.write(event_timestamp)