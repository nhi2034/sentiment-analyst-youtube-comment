from confluent_kafka import Consumer, KafkaError
import json
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from pymongo import MongoClient

# Kafka consumer configuration
consumer_config = {
    'bootstrap.servers': 'localhost:9092',  # Change if Kafka broker is running on a different host
    'group.id': 'youtube_sentiment_group',
    'auto.offset.reset': 'earliest'
}

# MongoDB connection
mongo_client = MongoClient('mongodb+srv://bigdatapyspark:bigdatapyspark@bigdata.d648sgf.mongodb.net/')
db = mongo_client['youtube_sentiment']
collection = db['sentiment_results']

# Create Kafka consumer
consumer = Consumer(consumer_config)

# Subscribe to the Kafka topic
topic = 'youtube_comments'
consumer.subscribe([topic])

# Sentiment analyzer
analyzer = SentimentIntensityAnalyzer()

# Continuously poll for new messages
while True:
    msg = consumer.poll(1.0)  # Adjust the timeout as needed

    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            continue
        else:
            print(msg.error())
            break

    # Process the received message
    try:
        youtube_comment = json.loads(msg.value())
        
        # Check if 'text' key is present in youtube_comment
        if 'text' not in youtube_comment:
            continue

        # Perform sentiment analysis on youtube_comment['text']
        text = youtube_comment['text']
        sentiment_score = analyzer.polarity_scores(text)

        if sentiment_score['compound'] >= 0.05:
            sentiment = 'positive'
        elif sentiment_score['compound'] <= -0.05:
            sentiment = 'negative'
        else:
            sentiment = 'neutral'
        # Store sentiment analysis results in MongoDB
        sentiment_entry = {
            'text': text,
            'negative_percentage': sentiment_score['neg'] * 100,
            'neutral_percentage': sentiment_score['neu'] * 100,
            'positive_percentage': sentiment_score['pos'] * 100,
            'compound': sentiment_score['compound'] * 100,
            'sentiment': sentiment  # Add the determined sentiment here
        }
        collection.insert_one(sentiment_entry)

    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")

# Close down consumer to commit final offsets.
consumer.close()
