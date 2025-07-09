from kafka import KafkaConsumer
from pymongo import MongoClient
import json
from visible_db_connection import VisibleDbConnection

db_conn = VisibleDbConnection()
consumer = KafkaConsumer(
    'gathered_data',
    bootstrap_servers='kafka:9092',
    api_version=(0,11,5),
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)
for message in consumer:
    posts = message.posts
    project_key = message.project_key
    
    for post in posts:
        db_conn.upload_to_db(
            row=post,
            project=project_key
        )
