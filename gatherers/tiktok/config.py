# config.py
import os
from dotenv import load_dotenv

load_dotenv()  # Automatically loads .env from current directory

APIFY_TOKEN = os.getenv("APIFY_TOKEN")

