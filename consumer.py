from confluent_kafka import Consumer, KafkaError
import json
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# Kafka consumer configuration
consumer_config = {
    'bootstrap.servers': 'localhost:9092',  # Change if Kafka broker is running on a different host
    'group.id': 'youtube_sentiment_group',
    'auto.offset.reset': 'earliest'
}

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

        # Print or use the sentiment score as needed
        print("------------------------------")
        print(f"Text:   {youtube_comment['text']}:")
        print("sentence was rated as ", sentiment_score['neg']*100, "% Negative")
        print("sentence was rated as ", sentiment_score['neu']*100, "% Neutral")
        print("sentence was rated as ", sentiment_score['pos']*100, "% Positive")
        
              

    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")

# Close down consumer to commit final offsets.
consumer.close()
