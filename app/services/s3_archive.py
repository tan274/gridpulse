import json
import boto3
from app.config import settings


def upload_run_archive(run_id: int, payload: list[dict]) -> str:
    """Upload raw payload JSON to S3 and return the object key."""
    key = f"archives/electricity-retail-sales/{run_id}.json"
    client = boto3.client(
        "s3",
        region_name=settings.aws_region,
        aws_access_key_id=settings.aws_access_key_id or None,
        aws_secret_access_key=settings.aws_secret_access_key or None,
    )
    client.put_object(
        Bucket=settings.s3_bucket_name,
        Key=key,
        Body=json.dumps(payload),
        ContentType="application/json",
    )
    return key
