from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType

# ------------------------------------
# Spark Session
# ------------------------------------
spark = SparkSession.builder \
    .appName("IPL-Raw-Streaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ------------------------------------
# Kafka Source
# ------------------------------------
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ipl_ball_by_ball") \
    .option("startingOffsets", "latest") \
    .load()

# ------------------------------------
# Schema for RAW IPL delivery
# ------------------------------------
schema = StructType() \
    .add("match_id", StringType()) \
    .add("season", StringType()) \
    .add("batting_team", StringType()) \
    .add("bowling_team", StringType()) \
    .add("over", StringType()) \
    .add("ball", StringType()) \
    .add("batter", StringType()) \
    .add("bowler", StringType()) \
    .add("batsman_runs", StringType()) \
    .add("total_runs", StringType()) \
    .add("is_wicket", StringType())

# ------------------------------------
# Parse Kafka JSON
# ------------------------------------
parsed_df = kafka_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
)

raw_df = parsed_df.select("data.*")

# ------------------------------------
# Write RAW data to HDFS
# ------------------------------------
query = raw_df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", "hdfs://localhost:9000/ipl/raw/streaming_deliveries") \
    .option("checkpointLocation", "hdfs://localhost:9000/ipl/checkpoint/raw_streaming") \
    .start()

query.awaitTermination()

