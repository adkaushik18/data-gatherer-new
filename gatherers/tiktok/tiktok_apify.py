import asyncio
from typing import List, Literal, Optional
import os

from apify_client import ApifyClientAsync
from config import APIFY_TOKEN


print(f"Running __name__ = {__name__}")


class TikTokScraper:
    def __init__(self):
        self.TOKEN = APIFY_TOKEN      
        
    async def _run_actor_async(self, input_data):
        client = ApifyClientAsync(token=self.TOKEN)
        actor = client.actor('GdWCkxBtKWOsKjdch')
        run = await actor.call(run_input=input_data)
        dataset_id = run.get('defaultDatasetId')
        dataset = client.dataset(dataset_id)
        items_page = await dataset.list_items()
        return { 
            "dataset_id": dataset_id,
            "items": items_page.items
        }

    def _run_actor_sync(self, input_data):
        return asyncio.run(self._run_actor_async(input_data))

    def scrape_hashtags(self, hashtags: List[str], results_per_page: int = 1):
        return self._run_actor_sync({
            "hashtags": hashtags,
            "resultsPerPage": results_per_page
        })

    def scrape_profiles(self, profiles: List[str], results_per_page: int = 1,
                        scrape_sections=["videos"], sorting="latest"):
        return self._run_actor_sync({
            "profiles": profiles,
            "resultsPerPage": results_per_page,
            "profileScrapeSections": scrape_sections,
            "profileSorting": sorting,
        })

    def scrape_search_queries(self, search_queries: List[str], results_per_page: int = 1,
                              section: Literal["/video", "/user", ""] = ""):
        return self._run_actor_sync({
            "searchQueries": search_queries,
            "resultsPerPage": results_per_page,
            "searchSection": section
        })

    def scrape_post_urls(self, post_urls: List[str], results_per_page: int = 1):
        return self._run_actor_sync({
            "postURLs": post_urls,
            "resultsPerPage": results_per_page
        })

    def scrape_all(self,
                   hashtags: Optional[List[str]] = None,
                   profiles: Optional[List[str]] = None,
                   search_queries: Optional[List[str]] = None,
                   post_urls: Optional[List[str]] = None,
                   results_per_page: int = 1):
        results = []

        if hashtags:
            result = self.scrape_hashtags(hashtags, results_per_page)
            results.append({
                "type": "hashtags",
                "dataset_id": result["dataset_id"],
                "items": result["items"]
            })

        if profiles:
            result = self.scrape_profiles(profiles, results_per_page)
            results.append({
                "type": "profiles",
                "dataset_id": result["dataset_id"],
                "items": result["items"]
            })

        if search_queries:
            result = self.scrape_search_queries(search_queries, results_per_page)
            results.append({
                "type": "searchQueries",
                "dataset_id": result["dataset_id"],
                "items": result["items"]
            })

        if post_urls:
            result = self.scrape_post_urls(post_urls, results_per_page)
            results.append({
                "type": "postURLs",
                "dataset_id": result["dataset_id"],
                "items": result["items"]
            })

        return results

            
    

