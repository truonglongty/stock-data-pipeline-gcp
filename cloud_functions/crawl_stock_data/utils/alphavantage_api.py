import requests
import datetime
import os
from .gcs_helper import upload_json_to_gcs

ALPHAVANTAGE_API_KEY = os.environ.get("ALPHAVANTAGE_API_KEY")
BUCKET_NAME = os.environ.get("BUCKET_NAME")

def get_time_range(tz):
    yesterday = datetime.date.today() - datetime.timedelta(days=1)
    if tz == 1:
        return (yesterday.strftime("%Y%m%dT0000"), yesterday.strftime("%Y%m%dT2359"))
    elif tz == 2:
        return (yesterday.strftime("%Y%m%dT0000"), yesterday.strftime("%Y%m%dT1200"))
    else:
        return (yesterday.strftime("%Y%m%dT1201"), yesterday.strftime("%Y%m%dT2359"))

def crawl_news():
    url = "https://www.alphavantage.co/query"
    params = {
        "function": "NEWS_SENTIMENT",
        "sort": "LATEST",
        "limit": "100",
        "apikey": ALPHAVANTAGE_API_KEY
    }

    all_news = []
    for tz in [1, 2, 3]:
        params["time_from"], params["time_to"] = get_time_range(tz)
        try:
            r = requests.get(url, params=params, timeout=10)
            r.raise_for_status()
            news = r.json().get("feed", [])
            all_news += news
            if len(all_news) >= 1000:
                break
        except Exception as e:
            print(f"News API error: {e}")
            continue

    if all_news:
        upload_json_to_gcs(BUCKET_NAME, all_news, "news")
        print(f"Uploaded news data to GCS: {len(all_news)} records")