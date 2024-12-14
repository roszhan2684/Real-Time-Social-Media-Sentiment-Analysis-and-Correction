Real-Time-Social-Media-Sentiment-Analysis-and-Correction
PROJECT 

# Overview

This project aims to analyze TikTok videos in real-time to classify sentiment (positive, negative, neutral) and provide actionable insights for brands. By leveraging Big Data technologies like Apache Kafka, Spark Streaming, HDFS, and Elasticsearch, the system collects, processes, and visualizes sentiment data efficiently.

# Objectives

1. Sentiment Analysis: Classify TikTok videos into positive, negative, or neutral sentiment.
2. Data Visualization: Provide real-time insights through Kibana dashboards.
3. Improved Brand Perception: Suggest strategies for addressing negative sentiments.

# Key Features

- Real-time ingestion of TikTok data using Apache Kafka.
- Preprocessing of text data for sentiment analysis.
- Integration of Spark Streaming for data processing.
- Storage in HDFS and Elasticsearch for scalability and real-time querying.
- Visualization of sentiment trends and engagement metrics in Kibana.

## Architecture

1. Data Collection: TikTok video data is scraped and streamed to Kafka.
2. Preprocessing: Data is changed from JSON to NDJSON ( newline-delimited-JSON ).
3. Real-Time Processing: Spark Streaming processes sentiment data.
4. Storage: Results are stored in HDFS and Elasticsearch for further analysis.
5. Visualization: Kibana dashboards display trends and insights.

# Setup Instructions

## Prerequisites

- Hadoop: Ensure Hadoop and HDFS are installed and configured.
- Kafka: Install Apache Kafka and start the broker.
- Spark: Install Apache Spark with the required packages.
- Python: Install Python with dependencies (`pyspark`, `elasticsearch`, etc.).

### Steps

1. Start Hadoop:

   .bash
   start-pdfs.sh
   start-yarn.sh
  

2. Start Kafka:

   .bash
   bin/zookeeper-server-start.sh config/zookeeper.properties
   bin/kafka-server-start.sh config/server.properties
   

3. Run Data Collector:
   Use a third-party tool (e.g., Apify) to collect TikTok data and send it to Kafka.

4. Start Spark Streaming Job:

   .bash
   spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 process_hadoop_data.py
   

5. View Results in Kibana:
   Ensure Elasticsearch and Kibana are running. Visualize data through pre-configured dashboards.


   step 1: run DATA.py
   step 2: run HDFS.py                  this is when you get real time data and to process it.
   step 3: run spark_streaming.py

   step 1: run FINAL.py                  this is when you have the data in local system ( no live data )

## Results

- Sentiment classification for TikTok videos.
- Real-time play counts for positive, negative, and neutral videos.
- Dashboards displaying sentiment trends and engagement metrics.


## Author

Roszhan Raj Meenakshi Sundhresan
Parasto toosie

