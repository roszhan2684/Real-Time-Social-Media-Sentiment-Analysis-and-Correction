import requests
from kafka import KafkaProducer
import json
import time
from textblob import TextBlob  # For basic sentiment analysis

# Kafka Producer setup
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Apify API endpoint
apify_url = "https://api.apify.com/v2/actor-runs/1PzGsp7JCMjp97BiM?token=apify_api_yeC9qDabFcPxB9UloTtdzTbs5bUy9D0gJroK"

def analyze_sentiment(text):
    """Classify sentiment as positive, negative, or neutral."""
    polarity = TextBlob(text).sentiment.polarity
    if polarity > 0:
        return "positive"
    elif polarity < 0:
        return "negative"
    else:
        return "neutral"

def fetch_and_send_data():
    """Fetch data from Apify API and send to Kafka."""
    try:
        # Fetch data from Apify API
        response = requests.get(apify_url)
        if response.status_code == 200:
            data = response.json()

            # Process and send each review
            for review in data.get('reviews', []):  # Adjust 'reviews' key based on API response
                review['sentiment'] = analyze_sentiment(review.get('text', ""))
                producer.send('nike_reviews', review)
                print("Sent to Kafka:", review)
        else:
            print(f"Failed to fetch data: {response.status_code}")
    except Exception as e:
        print(f"Error fetching or sending data: {e}")

# Test Kafka Producer functionality
def test_kafka_producer():
    """Send a sample message to Kafka for testing."""
    data = {
        "id": "example_id",
        "message": "Sample data for Kafka",
        "sentiment": "neutral"
    }
    producer.send('nike_reviews', data)
    print("Sent to Kafka (test):", data)

# Run fetch and send in intervals (every 60 seconds)
if __name__ == "__main__":
    test_kafka_producer()  # Test the Kafka setup with a sample message
    while True:
        fetch_and_send_data()
        time.sleep(60)  # Wait 60 seconds before fetching again
