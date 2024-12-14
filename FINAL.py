from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
from textblob import TextBlob
import json

# Paths for input and output
input_file = "/users/roszhanraj/kafka_data/nike.json"
processed_file = "/users/roszhanraj/kafka_data/nike_processed.json"
hdfs_input_path = "file:///users/roszhanraj/kafka_data/nike_processed.json"
output_path = "file:///users/roszhanraj/processed_data/"

# input_file = "hdfs://localhost:9000/users/roszhanraj/kafka_data/nike.json"
# processed_file = "hdfs://localhost:9000/users/roszhanraj/kafka_data/nike_processed.json"
# hdfs_input_path = "hdfs://localhost:9000/users/roszhanraj/kafka_data/nike_processed.json"  # HDFS input path for Spark
# output_path = "hdfs://localhost:9000/users/roszhanraj/processed_data/"

# Function to perform sentiment analysis
def analyze_sentiment(text):
    try:
        polarity = TextBlob(text).sentiment.polarity
        if polarity > 0:
            return "positive"
        elif polarity < 0:
            return "negative"
        else:
            return "neutral"
    except Exception:
        return "neutral"

# UDF for Spark
sentiment_udf = udf(analyze_sentiment, StringType())

# Preprocess JSON file into NDJSON format
def preprocess_json(input_file, processed_file):
    print(f"Processing JSON file: {input_file}")
    try:
        with open(input_file, 'r') as infile, open(processed_file, 'w') as outfile:
            data = json.load(infile)  # Load the entire JSON file
            for item in data:  # Each item in the list
                json.dump(item, outfile)  # Write each item as a line
                outfile.write('\n')  # Add newline after each JSON object
        print(f"Processed file saved at {processed_file}")
        return True
    except Exception as e:
        print(f"Error during JSON preprocessing: {e}")
        return False

# Spark Processing
def process_data_with_spark(hdfs_input_path, output_path):
    print(f"Reading data from HDFS: {hdfs_input_path}")
    try:
        # Initialize Spark session
        spark = SparkSession.builder \
            .appName("ProcessHadoopData") \
            .getOrCreate()

        # Read JSON file
        df = spark.read.json(hdfs_input_path)
        print("File successfully read. Schema is as follows:")
        df.printSchema()

        # Add sentiment column
        print("Adding sentiment column...")
        df = df.withColumn("sentiment", sentiment_udf(col("text")))

        # Display a sample of the data
        print("Displaying first 10 rows of data:")
        df.show(10, truncate=False)

        # Processing
        print("Processing data...")
        # Count number of reviews per sentiment
        if "sentiment" in df.columns:
            sentiment_count = df.groupBy("sentiment").count()
            print("Sentiment counts:")
            sentiment_count.show()

        # Average play count per sentiment (if `playCount` exists)
        if "playCount" in df.columns:
            avg_play_count = df.groupBy("sentiment").avg("playCount").alias("avg_play_count")
            print("Average play count per sentiment:")
            avg_play_count.show()

        # Save results back to HDFS
        print(f"Saving results to HDFS: {output_path}")
        sentiment_count.write.mode("overwrite").parquet(f"{output_path}/sentiment_count")
        if "playCount" in df.columns:
            avg_play_count.write.mode("overwrite").parquet(f"{output_path}/avg_play_count")

        print("Processing complete. Results saved to HDFS.")
    except Exception as e:
        print(f"Error occurred: {e}")
    finally:
        print("Stopping Spark session...")
        spark.stop()

# Main Execution
if __name__ == "__main__":
    # Preprocess the JSON file
    if preprocess_json(input_file, processed_file):
        # Process data with Spark
        process_data_with_spark(hdfs_input_path, output_path)
    else:
        print("Skipping Spark processing due to preprocessing failure.")
