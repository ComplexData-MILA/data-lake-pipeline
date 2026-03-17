from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Iterator

import boto3
import smart_open
from tqdm import tqdm


class S3Storage:
    def __init__(
        self,
        bucket: str,
        prefix: str = "",
        *,
        endpoint_url: str | None = None,
        access_key: str | None = None,
        secret_key: str | None = None,
    ):
        self.bucket = bucket
        self.prefix = prefix.rstrip("/")
        self.endpoint_url = endpoint_url

        if access_key and secret_key:
            self.client = boto3.client(
                "s3",
                endpoint_url=endpoint_url,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
            )
        else:
            self.client = boto3.client("s3", endpoint_url=endpoint_url)

    def _full_key(self, key: str) -> str:
        key = key.lstrip("/")
        if self.prefix:
            return f"{self.prefix}/{key}"
        return key

    def get_full_key(self, key: str) -> str:
        return f"s3://{self.bucket}/{self._full_key(key)}"

    def stream_jsonl(self, key: str) -> Iterator[dict]:
        full_key = self._full_key(key)
        uri = f"s3://{self.bucket}/{full_key}"
        transport_params = self._get_transport_params()
        with smart_open.open(uri, "r", encoding="utf-8", transport_params=transport_params) as f:
            for line in f:
                line = line.strip()
                if line:
                    yield json.loads(line)

    def _get_transport_params(self) -> dict:
        params = {"client": self.client}
        if self.endpoint_url:
            params["client_kwargs"] = {"endpoint_url": self.endpoint_url}
        return params

    def append_jsonl(self, key: str, records: Iterator[dict]) -> int:
        full_key = self._full_key(key)
        uri = f"s3://{self.bucket}/{full_key}"
        transport_params = self._get_transport_params()

        existing = []
        if self.object_exists(key):
            with tqdm(desc="Reading existing records", unit="rows") as pbar:
                with smart_open.open(uri, "r", encoding="utf-8", transport_params=transport_params) as f:
                    for line in f:
                        existing.append(line.rstrip("\n"))
                        pbar.update(1)

        count = 0
        with tqdm(desc="Writing records to S3", unit="rows") as pbar:
            with smart_open.open(uri, "w", encoding="utf-8", transport_params=transport_params) as f:
                for line in existing:
                    f.write(line + "\n")
                for record in records:
                    f.write(json.dumps(record, ensure_ascii=False) + "\n")
                    count += 1
                    pbar.update(1)
        return count

    def put_json(self, key: str, data: dict, if_none_match: bool = False) -> bool:
        full_key = self._full_key(key)
        kwargs = {
            "Bucket": self.bucket,
            "Key": full_key,
            "Body": json.dumps(data, ensure_ascii=False).encode("utf-8"),
            "ContentType": "application/json",
        }
        if if_none_match:
            kwargs["IfNoneMatch"] = "*"
        try:
            self.client.put_object(**kwargs)
            return True
        except self.client.exceptions.ConditionalRequestFailed:
            return False
        except Exception as e:
            if "ConditionalRequestFailed" in str(e) or "412" in str(e):
                return False
            raise

    def get_json(self, key: str) -> dict | None:
        full_key = self._full_key(key)
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=full_key)
            body = response["Body"].read().decode("utf-8")
            return json.loads(body)
        except self.client.exceptions.NoSuchKey:
            return None
        except Exception as e:
            if "NoSuchKey" in str(e) or "404" in str(e):
                return None
            raise

    def copy_object(self, src: str, dst: str) -> None:
        src_key = self._full_key(src)
        dst_key = self._full_key(dst)
        copy_source = {"Bucket": self.bucket, "Key": src_key}
        self.client.copy_object(Bucket=self.bucket, CopySource=copy_source, Key=dst_key)

    def delete_object(self, key: str) -> None:
        full_key = self._full_key(key)
        self.client.delete_object(Bucket=self.bucket, Key=full_key)

    def list_objects(self, prefix: str, suffix: str = "") -> list[str]:
        full_prefix = self._full_key(prefix)
        keys = []
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=full_prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if self.prefix:
                    key = key[len(self.prefix) + 1 :]
                if not suffix or key.endswith(suffix):
                    keys.append(key)
        return keys

    def get_object_age_seconds(self, key: str) -> int:
        full_key = self._full_key(key)
        response = self.client.head_object(Bucket=self.bucket, Key=full_key)
        last_modified = response["LastModified"]
        now = datetime.now(timezone.utc)
        age = (now - last_modified).total_seconds()
        return int(age)

    def object_exists(self, key: str) -> bool:
        full_key = self._full_key(key)
        try:
            self.client.head_object(Bucket=self.bucket, Key=full_key)
            return True
        except Exception:
            return False

    def read_bytes(self, key: str) -> bytes:
        uri = self.get_full_key(key)
        transport_params = self._get_transport_params()
        with smart_open.open(uri, "rb", transport_params=transport_params) as f:
            return f.read()

    def write_bytes(self, key: str, data: bytes) -> None:
        uri = self.get_full_key(key)
        transport_params = self._get_transport_params()
        with smart_open.open(uri, "wb", transport_params=transport_params) as f:
            f.write(data)
