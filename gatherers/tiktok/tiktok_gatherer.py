from asyncio import subprocess
import os
import shlex
import traceback
import requests
from typing import List
from tiktok_apify import TikTokScraper
from kafka import  KafkaProducer

tiktok_manager = TikTokScraper()

producer = KafkaProducer(
        bootstrap_servers="kafka:9092",
        api_version=(0,11,5),
        value_serializer=lambda x: json.dumps(x).encode("utf-8")
    )

def download_videos_from_dataset(download_dir,dataset_id: str):
    dataset_url = f"https://api.apify.com/v2/datasets/{dataset_id}/items?token={self.TOKEN}"
    try:
        res = requests.get(dataset_url)
        res.raise_for_status()
        items = res.json()
    except Exception as e:
        print(f"Error fetching dataset: {e}")
        return []

    paths = []
    for i, item in enumerate(items):
        video_url = item.get("webVideoUrl")
        if not video_url:
            print(f"No video URL found for item {i}")
            continue

        try:
            filename_prefix = f"video_{i}"
            input_label = str(item.get("input", "")).replace(" ", "_").replace("/", "_")
            output_template = os.path.join(download_dir, f"{filename_prefix}_{input_label}.%(ext)s")

            cmd = f'yt-dlp "{video_url}" -o "{output_template}"'
            subprocess.run(shlex.split(cmd), check=True)

            downloaded_files = [
                f for f in os.listdir(download_dir)
                if f.startswith(f"{filename_prefix}_{input_label}")
            ]

            for file in downloaded_files:
                full_path = os.path.join(download_dir, file)
                print(f"Downloaded: {full_path}")
                paths.append(full_path)

        except subprocess.CalledProcessError as e:
            print(f"yt-dlp failed for video {i}: {e}")
        except Exception as e:
            print(f"Unexpected error for video {i}: {e}")

        return paths

def gather_tiktok_data(hashtags: List[str], keywords: List[str]):     

    if not hashtags and not keywords:
        print("No hashtags or keywords provided for TikTok data gathering.")
        return []

    results = tiktok_manager.scrape_all(hashtags=hashtags, search_queries=keywords, results_per_page=2)
    all_items = []

    for result in results:
        all_items.extend(result["items"])

    if not all_items:
        print("No items found for the provided hashtags or keywords.")
        return False

    print(f"Found {len(all_items)} items for the provided hashtags or keywords.")
    posts = []

    for item in all_items:
        post = {
            'media_url': item.get('webVideoUrl', {}),
            'source': 'tiktok',
            'source_type': 'video',
            'caption': item.get('text', ''),
            'hashtags': item.get('hashtags', []),
            'like_count': item.get('diggCount', 0),
            'comments_count': item.get('commentCount', 0),
            'permalink': item.get('webVideoUrl', ''),
            'media_type': 'video',
            'username': item.get('authorMeta', {}).get('name', ''),
            'brand': item.get('authorMeta', {}).get('nickName', ''),
            'created_at': float(item.get('createTime', 0)),
        }
        posts.append(post)

    return posts


def main(params):    
    posts = gather_tiktok_data(hashtags=params.get("hashtags", []),keywords=params.get("keywords", []))
    print(f"[TikTok Gatherer] Gathered {len(posts)} posts for project {params.get('project_key','')}")
    print(f"[TikTok Gatherer] Sending gathered data for project {params.get('project_key','')} to Kafka...")  
    producer.send(
            "gathered_data",
            {
                "project_key": params.get("project_key",''),
                "source": "tiktok",
                "posts": posts
            }
        ) 


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="TikTok Scraper Runner")
    parser.add_argument("--hashtags", nargs="*", default=[], help="List of hashtags to scrape")
    parser.add_argument("--keywords", nargs="*", default=[], help="List of search keywords")
    parser.add_argument("--project_key", nargs="*", default='', help="Project key for the TikTok gatherer")   
  

    args = parser.parse_args()

    params = {
        "hashtags": args.hashtags,
        "keywords": args.keywords,
        "project_key": args.project_key       
    }
    print(f"[TikTok Gatherer] Starting with params: {params}")

    main(params)
