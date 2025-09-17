"""
Lambda function to be triggered by S3 object creation events (claims files).
- Validates the incoming file key
- Moves or copies file into raw prefix (if needed)
- Writes metadata to DynamoDB table for tracking
This function assumes the S3 Put event triggers it; AWS will pass event records with bucket/key.
"""

import os
import json
import boto3
from datetime import datetime

DDB_TABLE = os.getenv("METADATA_TABLE", "claims-metadata")
RAW_BUCKET = os.getenv("RAW_BUCKET", "my-claims-bucket")
RAW_PREFIX = os.getenv("RAW_PREFIX", "raw/")
REGION = os.getenv("AWS_REGION", "us-east-1")

dynamodb = boto3.resource("dynamodb", region_name=REGION)
table = dynamodb.Table(DDB_TABLE)
s3 = boto3.client("s3", region_name=REGION)

def write_metadata(item):
    """Write metadata record to DynamoDB"""
    table.put_item(Item=item)

def lambda_handler(event, context):
    # event: S3 Put notification
    processed = []
    for record in event.get("Records", []):
        s3_info = record.get("s3", {})
        bucket = s3_info.get("bucket", {}).get("name")
        key = s3_info.get("object", {}).get("key")
        # Only process .csv or .json
        if not key.endswith((".csv", ".json")):
            print(f"Skipping non-claims file: {key}")
            continue

        # Construct raw key (prefix by date)
        datepart = datetime.utcnow().strftime("%Y/%m/%d")
        raw_key = f"{RAW_PREFIX}{datepart}/{key.split('/')[-1]}"

        # If object already under raw prefix do nothing; otherwise copy
        if not key.startswith(RAW_PREFIX):
            copy_source = {'Bucket': bucket, 'Key': key}
            s3.copy_object(Bucket=RAW_BUCKET, CopySource=copy_source, Key=raw_key)
            # Optionally delete original if needed: s3.delete_object(Bucket=bucket, Key=key)
        else:
            raw_key = key  # already good

        # Write metadata
        metadata = {
            "file_key": raw_key,
            "bucket": RAW_BUCKET,
            "ingested_at": datetime.utcnow().isoformat(),
            "status": "INGESTED"
        }
        write_metadata(metadata)
        processed.append(raw_key)
        print(f"Ingested file -> {raw_key}")

    return {"processed_files": processed}
