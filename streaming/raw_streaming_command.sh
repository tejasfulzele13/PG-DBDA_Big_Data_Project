#!/bin/bash
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3

/home/talentum/spark/bin/spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 \
spark_streaming_raw.py
