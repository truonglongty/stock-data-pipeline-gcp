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

    # GCS + BigQuery clients
    storage_client = storage.Client()
    bq_client = bigquery.Client()

    # === Read OHLCs ===
    print("Reading OHLCs from GCS...")
    ohlcs_blob = storage_client.bucket(bucket_name).blob(f"stock_data/ohlc/ohlc_{date_str}.json")
    ohlcs_json = json.loads(ohlcs_blob.download_as_text())
    df_ohlcs = pd.DataFrame(ohlcs_json)

    # Convert timestamp + rename columns
    df_ohlcs["timestamp"] = pd.to_datetime(df_ohlcs["t"], unit="ms").dt.date.astype(str)
    df_ohlcs = df_ohlcs.rename(columns={"T": "ticker", "o": "open", "h": "high", "l": "low", "c": "close", "v": "volume"})
    df_ohlcs = df_ohlcs[["ticker", "timestamp", "open", "high", "low", "close", "volume"]]

    # === Read News ===
    print("Reading News from GCS...")
    news_blob = storage_client.bucket(bucket_name).blob(f"stock_data/news/news_{date_str}.json")
    news_json = json.loads(news_blob.download_as_text())
    df_news = pd.DataFrame(news_json)

    df_news["published_at"] = pd.to_datetime(df_news["time_published"], format="%Y%m%dT%H%M%S", errors="coerce").dt.date.astype(str)
    keep_cols = ["title", "summary", "url", "published_at", "overall_sentiment_score",
                 "overall_sentiment_label", "source", "authors", "topics", "ticker_sentiment"]
    df_news = df_news[keep_cols]

    # === Upload to BigQuery ===
    print("Uploading to BigQuery...")
    table_ohlcs = f"{dataset_id}.ohlcs"
    table_news = f"{dataset_id}.news"

    bq_client.load_table_from_dataframe(df_ohlcs, table_ohlcs).result()
    bq_client.load_table_from_dataframe(df_news, table_news).result()

    return f"Loaded data for {date_str} to BigQuery successfully!"
