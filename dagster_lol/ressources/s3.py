from dagster import ConfigurableResource
import boto3
import json
from datetime import datetime, date
import io
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd

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

    def get_latest_key(self, prefix: str, extension: str | None = None) -> str:
        """Returns the most recent S3 key under a prefix, sorted by folder name (YYYY-MM-DD)."""
        client = self.get_client()
        response = client.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix)
        objects = response.get("Contents", [])
        if extension:
            objects = [o for o in objects if o["Key"].endswith(extension)]
        if not objects:
            raise FileNotFoundError(f"No files found under s3://{self.bucket_name}/{prefix}")
        return sorted(objects, key=lambda o: o["Key"], reverse=True)[0]["Key"]

    def download_json(self, key: str) -> list[dict]:
        """Downloads and parses a JSON file from S3."""
        client = self.get_client()
        response = client.get_object(Bucket=self.bucket_name, Key=key)
        return json.loads(response["Body"].read().decode("utf-8"))
    
    def upload_parquet(self, records: list[dict], s3_key: str) -> str:
        """Convertit une liste de dicts en Parquet et uploade sur S3."""
        # Conversion via pyarrow (plus léger que pandas pour ce cas)
        table = pa.Table.from_pylist(records)
        
        buffer = io.BytesIO()
        pq.write_table(
            table,
            buffer,
            compression="snappy",       # bon compromis taille/vitesse
            write_statistics=True,       # utile pour DuckDB/Snowflake
        )
        buffer.seek(0)
        
        self.get_client().put_object(
            Bucket=self.bucket_name,
            Key=s3_key,
            Body=buffer.getvalue(),
        )
        return f"s3://{self.bucket_name}/{s3_key}"
    
    def download_parquet(self, key: str) -> pd.DataFrame:
        """Télécharge un fichier Parquet depuis S3 et retourne un DataFrame."""
        response = self.get_client().get_object(Bucket=self.bucket_name, Key=key)
        buffer = io.BytesIO(response["Body"].read())
        return pd.read_parquet(buffer)