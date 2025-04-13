from google.cloud import storage
import datetime
import json

def upload_json_to_gcs(bucket_name, data, prefix):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    date_str = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y_%m_%d")
    blob_path = f"stock_data/{prefix}/{prefix}_{date_str}.json"
    blob = bucket.blob(blob_path)
    blob.upload_from_string(json.dumps(data), content_type='application/json')
    print(f"Uploaded to GCS: {blob_path}")
