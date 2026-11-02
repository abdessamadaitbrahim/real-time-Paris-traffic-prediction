"""Debug tool: reads the Kafka topic with Spark Structured Streaming and prints it."""
import os
import sys

# Use the current interpreter (works on Windows, Linux, macOS and in a venv)
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 pyspark-shell"
)

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql.functions import from_json, col  # noqa: E402
from pyspark.sql.types import StructType, StructField, DoubleType, StringType  # noqa: E402

schema = StructType([
    StructField("q", DoubleType(), True),
    StructField("k", DoubleType(), True),
    StructField("etat_trafic", StringType(), True),
    StructField("t_1h", StringType(), True),
    StructField("geo_point_2d", StructType([
        StructField("lon", DoubleType(), True),
        StructField("lat", DoubleType(), True),
    ]), True),
])

spark = (
    SparkSession.builder
    .appName("KafkaTrafficConsumer")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

df_raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "trafic_paris")
    .option("startingOffsets", "latest")
    .load()
)

df_parsed = (
    df_raw.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
)

(
    df_parsed.writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", False)
    .start()
    .awaitTermination()
)
