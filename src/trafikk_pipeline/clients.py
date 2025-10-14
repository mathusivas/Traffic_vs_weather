from __future__ import annotations
import io
import json
import requests
import pandas as pd
from azure.storage.blob import BlobServiceClient
from trafikk_pipeline import config

def gql(query: str) -> dict:
    r = requests.post(
        config.API_URL,
        headers=config.HEADERS,
        json={"query": query},
        timeout=60,
    )
    r.raise_for_status()
    return r.json()

def blob_container():
    url = f"https://{config.AZURE_ACCOUNT}.blob.core.windows.net"
    return BlobServiceClient(account_url=url, credential=config.AZURE_KEY).get_container_client(
        config.AZURE_CONTAINER
    )

def upload_parquet(path: str, df: pd.DataFrame) -> None:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)
    blob_container().upload_blob(name=path, data=buf.getvalue(), overwrite=True)

def upload_json(path: str, obj: dict) -> None:
    blob_container().upload_blob(
        name=path,
        data=json.dumps(obj, ensure_ascii=False).encode("utf-8"),
        overwrite=True,
    )
