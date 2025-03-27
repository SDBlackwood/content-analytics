# Content Analytics Pipeline - Storage Implementation

See the [dev_setup.md](dev_setup.md) for more details on how to run the prototype.

## The component you chose and why.

I chose to implement the storage component of the system as it contained the most unique aspects to the challenges domain and allowed for experimentation in parquet which would give me more insights into the potential ML ready operations which could be performed on the data.
## How your prototype fits into the overall data pipeline.

This implementation focuses on the storage component of the content analytics pipeline, which processes media events from Kafka and stores them as Parquet files in S3-compatible object storage (MinIO for local development).

The local development environment for the prototype sets up a Kafka cluster and injects 5M records into the `media-events` topic. We also set up a local MinIO instance to store the parquet files into. This setup allows us to test the prototype end to end and validate the assumptions made in the architecture design and test it for performance.
## ML Readiness

Storing data as parquet provides us with the benefit of a data format which is tailored for data exploration and ML tasks. We store many multiple records in a single file with each field as a separate column which can be queried and aggregated upon e.g

Filter by a given event time and /or user
```python
user_plays = spark.read.parquet("s3://content-analytics/events/") \
    .filter("user_id = 'user_123' AND event_type = 'play'")
``` 

Create aggregations to determine the number of play events during a given period for each media item
```python
from pyspark.sql.functions import count

plays_last_month = (spark.read.parquet("s3://content-analytics/events/")
    .filter("event_type = 'play' AND date BETWEEN '2025-02-25' AND '2025-03-25'")
    .groupBy("content_id", "title")
    .agg(count("*").alias("play_count"))
    .orderBy("play_count"))
```

Using parquet we can partion the data in the storage location to allow for more easy reading from disk (as redundant data is skipped when filtering) and for a more human readable data format.  For this purpose we can partion the data per event type e.g `play`, `pause`, `stop` etc.  and `date`.  The stucture of the data would look like so

```
  s3://media-analytics/events/event_type=play/date=2025-03-25/
  s3://media-analytics/events/event_type=pause/date=2025-03-25/
  s3://media-analytics/events/event_type=stop/date=2025-03-25/
  s3://media-analytics/events/event_type=play/date=2025-04-02/
  ...
```

The data has been partitioned this way but it is not required to be done this way.  We would want to analyse the access patterns for the data before making any decisions on the partitioning strategy.

