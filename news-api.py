from kafka import KafkaProducer
import time
import requests
import json
from dotenv import load_dotenv
from dotenv import dotenv_values

load_dotenv()

config = dotenv_values(".env")

producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

NEWS_API_KEY = config.get("NEWS_API_KEY")

def fetch_real_time_news():
    url = f"https://newsapi.org/v2/top-headlines?country=us&apiKey={NEWS_API_KEY}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json().get('articles', [])
    else:
        print(f"Failed to fetch news: {response.status_code} {response.text}")
        return []

while True:
    articles = fetch_real_time_news()
    for article in articles:
        content = article.get("content", "")
        if content:
            # producer.send("topic1", content)
            print(f"Sent: {content}")
    time.sleep(10)