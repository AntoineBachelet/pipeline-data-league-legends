from dagster import ConfigurableResource
import boto3
import json
from datetime import datetime, date

class S3Resource(ConfigurableResource):
    bucket_name: str
    aws_access_key_id: str
    aws_secret_access_key: str
    region_name: str = "eu-west-3"

    def get_client(self):
        return boto3.client(
            "s3",
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.region_name,
        )

    def upload_json(self, data: list[dict], key: str) -> str:
        client = self.get_client()

        def default_serializer(obj):
            if isinstance(obj, (datetime, date)):
                return obj.isoformat()
            raise TypeError(f"Type {type(obj)} non sérialisable")

        client.put_object(
            Bucket=self.bucket_name,
            Key=key,
            Body=json.dumps(data, ensure_ascii=False, default=default_serializer),
            ContentType="application/json",
        )
        return f"s3://{self.bucket_name}/{key}"

    def get_latest_key(self, prefix: str) -> str:
        """Returns the most recent S3 key under a prefix, sorted by folder name (YYYY-MM-DD)."""
        client = self.get_client()
        response = client.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix)
        objects = response.get("Contents", [])
        if not objects:
            raise FileNotFoundError(f"No files found under s3://{self.bucket_name}/{prefix}")
        latest = sorted(objects, key=lambda o: o["Key"], reverse=True)[0]
        return latest["Key"]

    def download_json(self, key: str) -> list[dict]:
        """Downloads and parses a JSON file from S3."""
        client = self.get_client()
        response = client.get_object(Bucket=self.bucket_name, Key=key)
        return json.loads(response["Body"].read().decode("utf-8"))