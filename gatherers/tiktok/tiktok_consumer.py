# tiktok_consumer.py

from gatherers.tiktok.tiktok_gatherer import gather_tiktok_data
from kafka import KafkaConsumer, KafkaProducer
import json
import time

def simulate_tiktok_scrape(keyword, project_key):
   
    print(f"[TikTok Gatherer] Scraping keyword '{keyword}' for project '{project_key}'...")
    
    # Simulate 3 fake posts
    return [
        {
            "project_key": project_key,
            "keyword": keyword,
            "source": "tiktok",
            "url": f"https://www.tiktok.com/@example_{i}_{keyword}",
            "timestamp": time.time()
        }
        for i in range(3)
    ]

def main():
    print("[TikTok Consumer] Starting...")

    consumer = KafkaConsumer(
        "gather_tasks",
        bootstrap_servers="kafka:9092",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        group_id="tiktok_gatherer_group"
    )

    producer = KafkaProducer(
        bootstrap_servers="kafka:9092",
        value_serializer=lambda x: json.dumps(x).encode("utf-8")
    )

    for message in consumer:
        task = message.value

        if 'tiktok' not in task.get('sources', []):
            continue

        project_key = task["project_key"]
        hashtags = task.get("hashtags", [])
        keywords = task.get("keywords", [])

       
        posts = gather_tiktok_data(hashtags=hashtags, keywords=keywords, project_key=project_key)
        producer.send(
            "gathered_data",
            {
                "project_key": project_key,
                "source": "tiktok",
                "posts": posts
            }
        )
            

if __name__ == "__main__":
    main()
