from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf, when, lit
from pyspark.sql.types import StructType, StringType, IntegerType, BooleanType, StructField

# Initialize Spark session
spark = SparkSession.builder \
    .appName("RealTimeKafkaProcessingWithHadoop") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .getOrCreate()

# Schema for JSON structure
data_schema = StructType([
    StructField("id", StringType(), True),
    StructField("text", StringType(), True),
    StructField("authorMeta", StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("verified", BooleanType(), True),
        StructField("region", StringType(), True)
    ]), True),
    StructField("playCount", IntegerType(), True)
])

# Kafka source
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "nike_reviews") \
    .load()

# Parse JSON and apply schema
parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), data_schema).alias("data")) \
    .select("data.*")

# Sentiment analysis using UDF
def analyze_sentiment(text):
    """Mock sentiment analysis based on text content."""
    if not text:
        return "neutral"
    text_lower = text.lower()
    if "love" in text_lower or "great" in text_lower or "excellent" in text_lower:
        return "positive"
    elif "bad" in text_lower or "hate" in text_lower or "terrible" in text_lower:
        return "negative"
    else:
        return "neutral"

sentiment_udf = udf(analyze_sentiment, StringType())

processed_df = parsed_df \
    .withColumn("sentiment", sentiment_udf(col("text"))) \
    .withColumn("sentiment_label", when(col("sentiment") == "positive", lit(1))
                .when(col("sentiment") == "negative", lit(-1))
                .otherwise(lit(0)))

# Write results to separate directories in HDFS
output_dir = "hdfs://localhost:9000/user/roszhanraj/kafka_data/"

processed_df.filter(col("sentiment") == "positive").writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", f"{output_dir}/positive") \
    .option("checkpointLocation", f"{output_dir}/positive_checkpoint") \
    .start()

processed_df.filter(col("sentiment") == "negative").writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", f"{output_dir}/negative") \
    .option("checkpointLocation", f"{output_dir}/negative_checkpoint") \
    .start()

processed_df.filter(col("sentiment") == "neutral").writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", f"{output_dir}/neutral") \
    .option("checkpointLocation", f"{output_dir}/neutral_checkpoint") \
    .start()

# Monitor processing
spark.streams.awaitAnyTermination()
