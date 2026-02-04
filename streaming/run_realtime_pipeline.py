import subprocess
import time
import sys
import os

# ===============================
# CONFIGURATION
# ===============================

SPARK_HOME = "/home/talentum/spark"
PROJECT_HOME = "/home/talentum/ipl_bigdata_project"

PRODUCER_SCRIPT = os.path.join(PROJECT_HOME, "kafka/ipl_producer.py")
STREAMING_RAW_SCRIPT = os.path.join(PROJECT_HOME, "streaming/spark_streaming_raw.py")
STREAMING_AGG_SCRIPT = os.path.join(PROJECT_HOME, "streaming/spark_streaming.py")

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5"

# ===============================
# HELPER FUNCTION
# ===============================

def start_process(cmd, name):
    print("\nStarting " + name + "...")
    try:
        # Using subprocess.Popen to run in background
        return subprocess.Popen(cmd, shell=True)
    except Exception as e:
        print("Failed to start " + name + ": " + str(e))
        sys.exit(1)

# ===============================
# STEP 1: START RAW STREAMING
# ===============================

raw_cmd = (
    SPARK_HOME + "/bin/spark-submit " +
    "--packages " + KAFKA_PACKAGE + " " +
    STREAMING_RAW_SCRIPT
)

raw_process = start_process(raw_cmd, "Spark Streaming RAW")

# Wait for Spark to initialize
time.sleep(10)

# ===============================
# STEP 2: START AGGREGATION STREAMING
# ===============================

agg_cmd = SPARK_HOME + "/bin/spark-submit " + STREAMING_AGG_SCRIPT

agg_process = start_process(agg_cmd, "Spark Streaming AGGREGATION")

# Wait for Spark to initialize
time.sleep(10)

# ===============================
# STEP 3: START KAFKA PRODUCER
# ===============================

producer_cmd = "python3 " + PRODUCER_SCRIPT
producer_process = start_process(producer_cmd, "Kafka Producer")

# ===============================
# KEEP SCRIPT ALIVE & CLEANUP
# ===============================

print("\nReal-time pipeline started successfully.")
print("Press CTRL+C to stop all processes.\n")

try:
    # This keeps the main script running while the producer works
    producer_process.wait()
except KeyboardInterrupt:
    print("\nStopping all processes...")
    
    # Send termination signals to background processes
    producer_process.terminate()
    agg_process.terminate()
    raw_process.terminate()
    
    # Wait for processes to exit to avoid zombie processes
    producer_process.wait()
    agg_process.wait()
    raw_process.wait()
    
    print("Pipeline stopped cleanly.")
