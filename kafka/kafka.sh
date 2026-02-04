#!/bin/bash

# ================================
# CONFIGURATION
# ================================
KAFKA_HOME=/home/talentum/kafka
TOPIC_NAME=ipl_ball_by_ball
BROKER=localhost:9092
PARTITIONS=1
REPLICATION_FACTOR=1

# ================================
# START ZOOKEEPER
# ================================
echo "Starting ZooKeeper..."
$KAFKA_HOME/bin/zookeeper-server-start.sh \
  -daemon $KAFKA_HOME/config/zookeeper.properties

sleep 5

# ================================
# START KAFKA BROKER
# ================================
echo "Starting Kafka Broker..."
$KAFKA_HOME/bin/kafka-server-start.sh \
  -daemon $KAFKA_HOME/config/server.properties

sleep 5

# ================================
# CREATE KAFKA TOPIC
# ================================
echo "Creating Kafka topic: $TOPIC_NAME"

$KAFKA_HOME/bin/kafka-topics.sh \
  --create \
  --topic $TOPIC_NAME \
  --bootstrap-server $BROKER \
  --partitions $PARTITIONS \
  --replication-factor $REPLICATION_FACTOR

echo "Kafka setup completed successfully !!!"

