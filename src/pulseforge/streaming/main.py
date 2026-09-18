import logging

from pyspark.sql import SparkSession

from pulseforge.logging import configure_logging
from pulseforge.streaming.database import ensure_tables, write_event_batch, write_metric_batch
from pulseforge.streaming.settings import StreamingSettings
from pulseforge.streaming.transforms import (
    archived_events,
    classify_events,
    dead_letter_records,
    event_sink_rows,
    minute_metrics,
    raw_archive_rows,
    valid_events,
)

logger = logging.getLogger(__name__)


def build_spark(settings: StreamingSettings) -> SparkSession:
    return (
        SparkSession.builder.appName("pulseforge-streaming")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(settings.shuffle_partitions))
        .config("spark.hadoop.fs.s3a.endpoint", settings.s3_endpoint_url)
        .config("spark.hadoop.fs.s3a.access.key", settings.s3_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", settings.s3_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .getOrCreate()
    )


def kafka_source(spark: SparkSession, settings: StreamingSettings):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", settings.kafka_topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
        .option("maxOffsetsPerTrigger", settings.max_offsets_per_trigger)
        .load()
    )


def run() -> None:
    configure_logging()
    settings = StreamingSettings.from_env()
    spark = build_spark(settings)
    spark.sparkContext.setLogLevel("WARN")
    ensure_tables(settings)

    classified = classify_events(kafka_source(spark, settings))
    accepted = valid_events(classified, settings.watermark_delay)

    queries = [
        (
            raw_archive_rows(classified)
            .writeStream.queryName("pulseforge-raw-archive")
            .format("parquet")
            .partitionBy("ingest_date", "source_topic")
            .option("path", settings.lake_raw_uri)
            .option("checkpointLocation", settings.checkpoint("raw-archive"))
            .outputMode("append")
            .trigger(processingTime=settings.trigger_interval)
            .start()
        ),
        (
            dead_letter_records(classified)
            .writeStream.queryName("pulseforge-dead-letter")
            .format("kafka")
            .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
            .option("topic", settings.kafka_dead_letter_topic)
            .option("checkpointLocation", settings.checkpoint("dead-letter"))
            .outputMode("append")
            .trigger(processingTime=settings.trigger_interval)
            .start()
        ),
        (
            archived_events(accepted)
            .writeStream.queryName("pulseforge-event-archive")
            .format("parquet")
            .partitionBy("event_date", "event_type")
            .option("path", settings.lake_events_uri)
            .option("checkpointLocation", settings.checkpoint("event-archive"))
            .outputMode("append")
            .trigger(processingTime=settings.trigger_interval)
            .start()
        ),
        (
            event_sink_rows(accepted)
            .writeStream.queryName("pulseforge-event-warehouse")
            .foreachBatch(lambda frame, batch_id: write_event_batch(frame, batch_id, settings))
            .option("checkpointLocation", settings.checkpoint("event-warehouse"))
            .outputMode("append")
            .trigger(processingTime=settings.trigger_interval)
            .start()
        ),
        (
            minute_metrics(accepted)
            .writeStream.queryName("pulseforge-minute-metrics")
            .foreachBatch(lambda frame, batch_id: write_metric_batch(frame, batch_id, settings))
            .option("checkpointLocation", settings.checkpoint("minute-metrics"))
            .outputMode("update")
            .trigger(processingTime=settings.trigger_interval)
            .start()
        ),
    ]
    logger.info("streaming_queries_started count=%s", len(queries))
    try:
        spark.streams.awaitAnyTermination()
    finally:
        for query in queries:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    run()
