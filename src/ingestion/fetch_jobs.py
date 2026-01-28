import os
import requests
import json
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

# Load API keys
load_dotenv()
APP_ID = os.getenv("ADZUNA_APP_ID")
APP_KEY = os.getenv("ADZUNA_APP_KEY")

# Bronze layer path
RAW_PATH = Path("data/raw")
RAW_PATH.mkdir(parents=True, exist_ok=True)

# API parameters
BASE_URL = "https://api.adzuna.com/v1/api/jobs/us/search/1"
PARAMS = {
    "app_id": APP_ID,
    "app_key": APP_KEY,
    "results_per_page": 50,
    "what": "data scientist",
    "content-type": "application/json"
}

def fetch_jobs():
    response = requests.get(BASE_URL, params=PARAMS)
    if response.status_code == 200:
        data = response.json()
        date_str = datetime.now().strftime("%Y-%m-%d")
        file_path = RAW_PATH / f"jobs_raw_{date_str}.json"
        with open(file_path, "w") as f:
            json.dump(data, f, indent=2)
        print(f"Saved {len(data.get('results', []))} jobs to {file_path}")
    else:
        print("Failed to fetch data:", response.status_code, response.text)

if __name__ == "__main__":
    fetch_jobs()
