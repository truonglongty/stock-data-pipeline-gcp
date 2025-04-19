import requests
import datetime
from .gcs_helper import upload_json_to_gcs
import os

POLYGON_API_KEY = os.environ.get("POLYGON_API_KEY")
BUCKET_NAME = os.environ.get("BUCKET_NAME")

def crawl_ohlcs():
    date_obj = datetime.date.today() - datetime.timedelta(days=1)
    if date_obj.weekday() >= 5:
        print("Weekend, no data.")
        return

    date_str = date_obj.strftime("%Y-%m-%d")
    url = f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{date_str}"
    params = {
        "adjusted": "true",
        "include_otc": "true",
        "apiKey": POLYGON_API_KEY
    }

    try:
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        data = r.json().get("results", [])
        if not data:
            print(f"No OHLCs data for {date_str}")
            return
        upload_json_to_gcs(BUCKET_NAME, data, "ohlc")
        print(f"Uploaded OHLCs data for {date_str} to GCS")

    except Exception as e:
        print(f"Error fetching OHLCs: {e}")
