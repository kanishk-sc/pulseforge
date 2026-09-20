from functools import partial

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from pulseforge.streaming.config import StreamSettings
from pulseforge.streaming.schema import RAW_SCHEMA, VALIDATION_SCHEMA
from pulseforge.streaming.sink import write_batch
from pulseforge.streaming.validation import validate_payload


def start_queries(spark: SparkSession, settings: StreamSettings) -> list[StreamingQuery]:
    queries = []
    try:
        raw_path = settings.path("raw")
        # A fresh bucket has no raw directory yet. Create only its directory marker.
        path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(raw_path)
        path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration()).mkdirs(path)
        kafka = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
            .option("subscribe", settings.kafka_topic)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "true")
            .option("kafka.request.timeout.ms", "10000")
            .option("kafka.default.api.timeout.ms", "15000")
            .option("maxOffsetsPerTrigger", settings.stream_max_offsets)
            .load()
        )
        raw = kafka.select(
            F.col("topic").alias("source_topic"),
            F.col("partition").alias("source_partition"),
            F.col("offset").alias("source_offset"),
            F.col("timestamp").alias("source_timestamp"),
            "key",
            F.col("value").alias("raw_payload"),
            F.current_timestamp().alias("ingested_at"),
        )
        queries.append(
            raw.withColumn("ingest_date", F.to_date("ingested_at"))
            .writeStream.format("parquet")
            .partitionBy("ingest_date")
            .option("path", raw_path)
            .option("checkpointLocation", settings.checkpoint("raw"))
            .queryName("raw")
            .trigger(processingTime=settings.stream_trigger)
            .start()
        )
        archived = (
            spark.readStream.schema(RAW_SCHEMA).option("maxFilesPerTrigger", 20).parquet(raw_path)
        )
        validate = F.udf(validate_payload, VALIDATION_SCHEMA)
        classified = archived.withColumn("validation", validate("raw_payload", "ingested_at"))
        invalid = classified.filter(F.col("validation.error_code").isNotNull())
        dlq = invalid.select(
            F.concat_ws(":", "source_topic", "source_partition", "source_offset").alias("key"),
            F.to_json(
                F.struct(
                    "source_topic",
                    "source_partition",
                    "source_offset",
                    "source_timestamp",
                    "ingested_at",
                    F.col("validation.error_code").alias("error_code"),
                    F.col("validation.error_detail").alias("error_detail"),
                    F.base64("raw_payload").alias("payload_base64"),
                    F.base64("key").alias("key_base64"),
                )
            ).alias("value"),
        )
        queries.append(
            dlq.writeStream.format("kafka")
            .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
            .option("topic", settings.kafka_dlq_topic)
            .option("kafka.acks", "all")
            .option("kafka.delivery.timeout.ms", "30000")
            .option("kafka.request.timeout.ms", "10000")
            .option("checkpointLocation", settings.checkpoint("dlq"))
            .queryName("dlq")
            .trigger(processingTime=settings.stream_trigger)
            .start()
        )
        valid = classified.filter(F.col("validation.error_code").isNull()).select(
            "validation.event.*",
            "source_topic",
            "source_partition",
            "source_offset",
            "source_timestamp",
            "ingested_at",
        )
        deduplicated = valid.withWatermark(
            "event_ts", settings.stream_watermark
        ).dropDuplicatesWithinWatermark(["event_id"])
        queries.append(
            deduplicated.writeStream.foreachBatch(partial(write_batch, settings=settings))
            .option("checkpointLocation", settings.checkpoint("valid-events"))
            .queryName("valid-events")
            .trigger(processingTime=settings.stream_trigger)
            .start()
        )
        return queries
    except BaseException:
        for query in queries:
            query.stop()
        raise
