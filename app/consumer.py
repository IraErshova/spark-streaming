from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import os
import time

CASSANDRA_KEYSPACE = os.environ.get("CASSANDRA_KEYSPACE", "wiki")
CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "cassandra")
CASSANDRA_PORT = os.environ.get("CASSANDRA_PORT", "9042")
TOPIC = os.environ.get("KAFKA_TOPIC", "pages")
BOOTSTRAP = os.environ.get("BOOTSTRAP_SERVERS", "kafka-server:9092")
TABLE = os.environ.get("CASSANDRA_TABLE", "pages")

schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("domain", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("page_title", StringType(), True)
])

spark = SparkSession \
    .builder \
    .appName("Wiki pages Streaming") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,"
            "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint") \
    .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
    .config("spark.cassandra.connection.port", CASSANDRA_PORT) \
    .getOrCreate()

print("Spark session created successfully")

# Retry logic for Kafka connection
max_retries = 10
retry_delay = 2

for attempt in range(max_retries):
    try:
        print(f"Attempting to connect to Kafka topic '{TOPIC}' (attempt {attempt + 1}/{max_retries})")
        
        df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", BOOTSTRAP) \
            .option("subscribe", TOPIC) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        print(f"Successfully connected to Kafka topic '{TOPIC}'")
        break
        
    except Exception as e:
        print(f"Failed to connect to Kafka topic '{TOPIC}': {e}")
        if attempt < max_retries - 1:
            print(f"Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)
        else:
            print("Failed to connect to Kafka after all retries")
            raise

# convert created_at to timestamp
parsed_df = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select(
    col("data.user_id"),
    col("data.domain"),
    to_timestamp(col("data.created_at")).alias("created_at"),
    col("data.page_title")
)

print("Starting streaming query...")

query = parsed_df.writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .outputMode("append") \
    .options(table=TABLE, keyspace=CASSANDRA_KEYSPACE) \
    .option("checkpointLocation", "/tmp/spark-checkpoint") \
    .start()

print("Streaming query started successfully")
print("Waiting for termination...")

query.awaitTermination()
