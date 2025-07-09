from kafka import KafkaProducer
import json
from visible_db_connection import VisibleDbConnection

db_conn = VisibleDbConnection()
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)


def send_gather_tasks():
    all_projects = db_conn.get_all_projects()
    print(f"Found {len(all_projects)} projects to process.")
    tasks = []
    for project_info in all_projects:
        project_key = project_info['project_key']
        project_details = db_conn.get_project_info_byKey(project_key)
        project_ht_list = [ht.lower() for ht in project_details.get('tiktok_hashtags', [])]
        project_keywords_list = [kw.lower() for kw in project_details.get('tiktok_keywords', [])]
        tasks.append({
            "project_key": project_key,
            "hashtags": project_ht_list,
            "keywords": project_keywords_list,
            "sources": ["tiktok"]
        })        
    for task in tasks:
        producer.send("gather_tasks", value=task)
        print(f"Task sent: {task}")

if __name__ == "__main__":
    send_gather_tasks()
