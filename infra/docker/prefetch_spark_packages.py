"""Resolve Spark connector packages into the image's Ivy cache at build time."""

from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("connector-prefetch").getOrCreate()
spark.stop()
