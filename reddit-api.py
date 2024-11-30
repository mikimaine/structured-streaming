import time
from kafka import KafkaProducer
import praw
import json
from dotenv import load_dotenv
from dotenv import dotenv_values

load_dotenv()

config = dotenv_values(".env")

reddit = praw.Reddit(
    client_id=config.get("REDDIT_CLIENT_ID"),
    client_secret=config.get("REDDIT_CLIENT_SECRET"),
    user_agent='Kafka-Streamer/1.0'
)

producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

try:
    subreddit = reddit.subreddit('news')
    print("Connected to subreddit. Streaming comments...")

    for comment in subreddit.stream.comments(skip_existing=True):
        try:
            producer.send('topic1', comment.body)
            print(f"Sent to Kafka: {comment.body}")

        except Exception as kafka_error:
            print(f"Error sending comment to Kafka: {kafka_error}")

        time.sleep(1)

except Exception as e:
    print(f"Failed to fetch or stream comments: {e}")

