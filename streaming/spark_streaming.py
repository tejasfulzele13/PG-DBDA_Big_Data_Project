from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum
from pyspark.sql.types import StructType, StringType

# -----------------------------------
# Spark Session
# -----------------------------------
spark = SparkSession.builder \
    .appName("IPL-Kafka-Streaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -----------------------------------
# Kafka Schema
# -----------------------------------
schema = StructType() \
    .add("match_id", StringType()) \
    .add("over", StringType()) \
    .add("ball", StringType()) \
    .add("batsman_runs", StringType()) \
    .add("total_runs", StringType())

# -----------------------------------
# Read from Kafka
# -----------------------------------
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ipl_ball_by_ball") \
    .option("startingOffsets", "latest") \
    .load()

# -----------------------------------
# Parse JSON
# -----------------------------------
parsed_df = kafka_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select(
    col("data.match_id"),
    col("data.total_runs").cast("int")
)

# -----------------------------------
# Aggregation (LIVE SCORE)
# -----------------------------------
runs_per_match = parsed_df.groupBy("match_id") \
    .agg(sum("total_runs").alias("live_total_runs"))

# -----------------------------------
# WRITE USING foreachBatch (KEY FIX)
# -----------------------------------
def write_to_hdfs(batch_df, batch_id):
    batch_df.write \
        .mode("overwrite") \
        .parquet("hdfs://localhost:9000/ipl/streaming/runs_per_match")

query = runs_per_match.writeStream \
    .outputMode("complete") \
    .foreachBatch(write_to_hdfs) \
    .option("checkpointLocation", "hdfs://localhost:9000/ipl/checkpoint/runs_per_match") \
    .start()

query.awaitTermination()

