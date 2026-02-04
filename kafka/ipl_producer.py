from kafka import KafkaProducer
import csv
import json
import time

# =========================
# Kafka Configuration
# =========================
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "ipl_ball_by_ball"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# =========================
# Input CSV File
# =========================
CSV_FILE = "/home/talentum/ipl_bigdata_project/data/deliveries.csv"

# =========================
# Send data ball-by-ball
# =========================
with open(CSV_FILE, "r") as file:
    reader = csv.DictReader(file)

    for row in reader:
        producer.send(TOPIC_NAME, value=row)
        print(
            f"Sent | Match:{row['match_id']} Over:{row['over']} Ball:{row['ball']}"
        )
        time.sleep(1)   # 1 ball per second (simulate real-time)

producer.flush()
producer.close()

print(" IPL Kafka Producer finished sending data")

