# import pyspark
# import os
# from dotenv import load_dotenv
# from pathlib import Path


# dotenv_path = Path("/opt/app/.env")
# load_dotenv(dotenv_path=dotenv_path)

# spark_hostname = os.getenv("SPARK_MASTER_HOST_NAME")
# spark_port = os.getenv("SPARK_MASTER_PORT")
# kafka_host = os.getenv("KAFKA_HOST")
# kafka_topic = os.getenv("KAFKA_TOPIC_NAME")

# spark_host = f"spark://{spark_hostname}:{spark_port}"

# os.environ[
#     "PYSPARK_SUBMIT_ARGS"
# ] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 org.postgresql:postgresql:42.2.18"

# sparkcontext = pyspark.SparkContext.getOrCreate(
#     conf=(pyspark.SparkConf().setAppName("DibimbingStreaming").setMaster(spark_host))
# )
# sparkcontext.setLogLevel("WARN")
# spark = pyspark.sql.SparkSession(sparkcontext.getOrCreate())

# stream_df = (
#     spark.readStream.format("kafka")
#     .option("kafka.bootstrap.servers", f"{kafka_host}:9092")
#     .option("subscribe", kafka_topic)
#     .option("startingOffsets", "latest")
#     .load()
# )

# (
#     stream_df.selectExpr("CAST(value AS STRING)")
#     .writeStream.format("console")
#     .outputMode("append")
#     .start()
#     .awaitTermination()
# )

import pyspark
import os
from dotenv import load_dotenv
from pathlib import Path
from pyspark.sql.functions import window, col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql import functions as F

# Load environment variables
dotenv_path = Path("/opt/app/.env")
load_dotenv(dotenv_path=dotenv_path)

spark_hostname = os.getenv("SPARK_MASTER_HOST_NAME")
spark_port = os.getenv("SPARK_MASTER_PORT")
kafka_host = os.getenv("KAFKA_HOST")
kafka_topic = os.getenv("KAFKA_TOPIC_NAME")

spark_host = f"spark://{spark_hostname}:{spark_port}"

os.environ[
    "PYSPARK_SUBMIT_ARGS"
] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 org.postgresql:postgresql:42.2.18"

# Initialize SparkContext and SparkSession
sparkcontext = pyspark.SparkContext.getOrCreate(
    conf=(pyspark.SparkConf().setAppName("DibimbingStreaming").setMaster(spark_host))
)
sparkcontext.setLogLevel("WARN")
spark = pyspark.sql.SparkSession(sparkcontext.getOrCreate())

# Define schema for Kafka value
schema = StructType([
    StructField("data", StringType()),
    StructField("timestamp", TimestampType())
])

# Read from Kafka
stream_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", f"{kafka_host}:9092")
    .option("subscribe", kafka_topic)
    .option("startingOffsets", "latest")
    .load()
)

# Convert Kafka value to structured format
stream_df = stream_df.selectExpr("CAST(value AS STRING) as value") \
    .select(F.from_json("value", schema).alias("parsed_value")) \
    .select("parsed_value.*")

# Handle late data with watermarking and windowing
aggregated_df = stream_df \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(window(col("timestamp"), "5 minutes")) \
    .agg(F.count("*").alias("count"))

# Output to console
query = aggregated_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
