from glob import glob

from pyspark.sql import SparkSession

from pulseforge.streaming.config import StreamSettings


def spark_session(settings: StreamSettings) -> SparkSession:
    jars = sorted(glob("/opt/connectors/*.jar"))
    if not jars:
        raise RuntimeError("Use the streaming image: Kafka, S3A and JDBC jars are required")
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("PulseForge streaming")
        .config("spark.jars", ",".join(jars))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", settings.stream_shuffle_partitions)
        .config("spark.sql.streaming.stopTimeout", "30000")
        .config("spark.ui.enabled", "false")
        .config("spark.hadoop.fs.s3a.endpoint", settings.s3_endpoint_url)
        .config("spark.hadoop.fs.s3a.endpoint.region", settings.s3_region)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.access.key", settings.minio_root_user)
        .config("spark.hadoop.fs.s3a.secret.key", settings.minio_root_password.get_secret_value())
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "15000")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.hadoop.fs.s3a.connection.timeout", "10000")
        .config("spark.hadoop.fs.s3a.attempts.maximum", "2")
        .config("spark.hadoop.fs.s3a.retry.limit", "2")
        .config("spark.redaction.regex", "(?i)secret|password|token|access[.]?key")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    hadoop = spark.sparkContext._jvm.org.apache.hadoop.util.VersionInfo.getVersion()
    if hadoop != "3.4.1":
        raise RuntimeError(f"S3A dependency mismatch: expected Hadoop 3.4.1, got {hadoop}")
    return spark
