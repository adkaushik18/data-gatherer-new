from kafka import KafkaConsumer
from pymongo import MongoClient
import json

consumer = KafkaConsumer(
    'gathered_data',
    bootstrap_servers='kafka:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

mongo = MongoClient("mongodb://mongo:27017")
collection = mongo["media_db"]["posts"]

for message in consumer:
    post = message.value
    collection.insert_one(post)
    print(f"[MongoWriter] Inserted post: {post['url']}")
