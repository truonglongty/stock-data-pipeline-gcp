import datetime
import os
import json
import pandas as pd
from google.cloud import storage, bigquery

def transform_to_bq_entrypoint(request):
    date_str = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y_%m_%d")
    print(f"Transforming data for date: {date_str}")

    bucket_name = os.environ.get("BUCKET_NAME")
    dataset_id = os.environ.get("BQ_DATASET")

    storage_client = storage.Client()
    bq_client = bigquery.Client()
    bucket = storage_client.bucket(bucket_name)

    messages = []

    # === Load News ===
    try:
        print("Reading News from GCS...")
        news_path = f"stock_data/news/news_{date_str}.json"
        news_blob = bucket.blob(news_path)

        if news_blob.exists():
            news_json = json.loads(news_blob.download_as_text())
            df_news = pd.DataFrame(news_json)
            df_news["published_at"] = pd.to_datetime(
                df_news["time_published"], format="%Y%m%dT%H%M%S", errors="coerce"
            ).dt.date

            keep_cols = [
                "title", "summary", "url", "published_at", "overall_sentiment_score",
                "overall_sentiment_label", "source", "authors", "topics", "ticker_sentiment"
            ]
            df_news = df_news[keep_cols]

            bq_client.load_table_from_dataframe(df_news, f"{dataset_id}.news").result()
            messages.append("Uploaded News.")
        else:
            messages.append(f"News file not found: {news_path}")
    except Exception as e:
        messages.append(f"Error processing News: {str(e)}")

    # === Load OHLCs ===
    try:
        print("Reading OHLCs from GCS...")
        ohlc_path = f"stock_data/ohlc/ohlc_{date_str}.json"
        ohlcs_blob = bucket.blob(ohlc_path)

        if ohlcs_blob.exists():
            ohlcs_json = json.loads(ohlcs_blob.download_as_text())
            df_ohlcs = pd.DataFrame(ohlcs_json)

            df_ohlcs["timestamp"] = pd.to_datetime(df_ohlcs["t"], unit="ms").dt.date
            df_ohlcs = df_ohlcs.rename(columns={
                "T": "ticker", "o": "open", "h": "high", "l": "low", "c": "close", "v": "volume"
            })
            df_ohlcs = df_ohlcs[["ticker", "timestamp", "open", "high", "low", "close", "volume"]]

            bq_client.load_table_from_dataframe(df_ohlcs, f"{dataset_id}.ohlcs").result()
            messages.append("Uploaded OHLCs.")
        else:
            messages.append(f"OHLC file not found: {ohlc_path}")
    except Exception as e:
        messages.append(f"Error processing OHLCs: {str(e)}")

    print("Transformation Summary:")
    for msg in messages:
        print(msg)

    return "\n".join(messages), 200 if any("Uploaded" in msg for msg in messages) else 500