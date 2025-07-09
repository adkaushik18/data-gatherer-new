import os
from dotenv import load_dotenv

load_dotenv()
  # Automatically loads .env from current directory
DB_API_URL = os.getenv('DB_API_URL', 'https://visibleapi.gmc2.com')
EMAIL = os.getenv('EMAIL', '')
PASSWORD = os.getenv('PASSWORD', '')

