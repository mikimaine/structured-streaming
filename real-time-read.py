from kafka import KafkaProducer
import time
import requests
import json

producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_real_time_data():
    response = requests.get("https://api.sampleapis.com/futurama/characters")
    return response.json()

while True:
    data = fetch_real_time_data()
    for record in data:
        producer.send("topic1", record['sayings'][0])
        print(f"Sent: {record['sayings'][0]}")
    time.sleep(10)  # Fetch data every 10 seconds