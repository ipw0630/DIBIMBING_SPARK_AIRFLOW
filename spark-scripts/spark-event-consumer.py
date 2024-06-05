import pyspark
import os
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables from the .env file
dotenv_path = Path("/opt/app/.env")
load_dotenv(dotenv_path=dotenv_path)

spark_hostname = os.getenv("SPARK_MASTER_HOST_NAME")
spark_port = os.getenv("SPARK_MASTER_PORT")
kafka_host = os.getenv("KAFKA_HOST")
kafka_topic = os.getenv("KAFKA_TOPIC_NAME")

spark_host = f"spark://{spark_hostname}:{spark_port}"

# Set PySpark submit arguments for required packages
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,org.postgresql:postgresql:42.2.18 pyspark-shell"
)

# Initialize Spark context and session
sparkcontext = pyspark.SparkContext.getOrCreate(
    conf=(pyspark.SparkConf().setAppName("DibimbingStreaming").setMaster(spark_host))
)
sparkcontext.setLogLevel("WARN")
spark = pyspark.sql.SparkSession(sparkcontext.getOrCreate())

# Define the schema of the incoming Kafka data
schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("furniture", StringType(), True),
    StructField("color", StringType(), True),
    StructField("price", FloatType(), True),
    StructField("ts", TimestampType(), True)
])

stream_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", f"{kafka_host}:9092")
    .option("subscribe", kafka_topic)
    .option("startingOffsets", "latest")
    .load()
)

(
    stream_df.selectExpr("CAST(value AS STRING)")
    .writeStream.format("console")
    .outputMode("append")
    .start()
    .awaitTermination()
)   

# Parse the JSON messages
parsed_stream_df = (
    stream_df.selectExpr("CAST(value AS STRING)")
    .select(F.from_json(F.col("value"), schema).alias("data"))
    .select("data.*")
)

# Calculate the running total
running_total_df = (
    parsed_stream_df
    .withWatermark("ts", "1 minute")
    .groupBy(F.window("ts", "1 minute"))
    .agg(F.sum("price").alias("running_total"))
    .select("window.start", "window.end", "running_total")
)

# Process and write stream to console with a trigger
query = (
    running_total_df.writeStream.format("console")
    .outputMode("complete")  # or "complete" if you want to see complete results for each window
    .trigger(processingTime='10 seconds')  # Trigger every 10 seconds
    .start()
)

query.awaitTermination()
