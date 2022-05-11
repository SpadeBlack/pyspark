

from pyspark.sql.functions import *
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master('local').appName('delta1') \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:1.2.1") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

from delta.tables import *
# Open delta table
deltaTable = DeltaTable.forPath(spark,"gs://taxi-dataproc-data-bucket/delta_random")

# Create manifest
deltaTable.generate("symlink_format_manifest")

spark.stop()
