# IPL Match Analytics Using Big Data (Lambda Architecture)

## Project Overview
This project implements an end-to-end Big Data analytics system for Indian Premier League (IPL) match data using **Lambda Architecture**. It supports both **batch analytics** for historical insights and **real-time streaming analytics** for live match monitoring. The solution processes large-scale ball-by-ball data using PySpark and Kafka, stores results in HDFS, and visualizes insights using Tableau.

---

##  System Architecture (Lambda Architecture)

The project is designed using **Lambda Architecture**, which consists of:

### 1️⃣ Batch Layer
- Processes complete historical IPL datasets
- Computes accurate, long-term analytics
- Stores results in optimized Parquet format in HDFS

### 2️⃣ Speed (Real-Time) Layer
- Ingests live ball-by-ball data using Kafka
- Processes streaming data using Spark Structured Streaming
- Produces near real-time match-level metrics

### 3️⃣ Serving Layer
- Combines batch and real-time outputs
- Exposes curated datasets for visualization in Tableau

---

## Data Pipeline Flow

###  Data Ingestion
- Raw IPL CSV datasets (matches, deliveries)
- Kafka producer streams ball-by-ball data to Kafka topic

### Data Processing
**Batch Processing (PySpark)**
- Season-wise runs
- Team-wise runs
- Player-wise runs, wickets, fours, sixes
- Fielding metrics (catches, run-outs, stumpings)
- Matches played per player

**Real-Time Processing (Spark Streaming + Kafka)**
- Live total runs per match
- Streaming data checkpointed in HDFS for fault tolerance

### Data Storage
- Raw Layer: `/ipl/raw/`
- Curated Layer: `/ipl/curated/`
- Streaming Layer: `/ipl/streaming/`
- Checkpoints: `/ipl/checkpoint/`

### Data Visualization
- Curated CSV exports consumed by Tableau
- Interactive dashboards for:
  - Top run scorers
  - Top wicket takers
  - Team performance trends
  - Live match score updates

---

## Tools & Technologies Used
- **PySpark**
- **Apache Spark (Batch & Structured Streaming)**
- **Apache Kafka (Producer–Consumer)**
- **HDFS**
- **Python**
- **Shell Scripting**
- **Tableau Public**
- **Linux**
- **Git & GitHub**

---

## 📁 Project Folder Structure


