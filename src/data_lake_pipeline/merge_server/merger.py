from __future__ import annotations

import io
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from data_lake_pipeline.storage.s3 import S3Storage

logger = logging.getLogger(__name__)


@dataclass
class MergeStatus:
    filters_to_merge: list[str]
    no_filters: bool = False


@dataclass
class MergeResult:
    merged_count: int
    filters_added: list[str]
    nothing_to_merge: bool = False


class BatchMerger:
    def __init__(
        self,
        storage: S3Storage,
        batch_id: str,
        annotations_prefix: str = "annotations",
        id_column: str = "id",
    ):
        self.storage = storage
        self.batch_id = batch_id
        self.annotations_prefix = annotations_prefix
        self.id_column = id_column
        self.base_path = f"{annotations_prefix}/{batch_id}"

    async def get_merge_status(self) -> MergeStatus:
        filter_files = self.storage.list_objects(
            f"{self.base_path}/filters", ".parquet"
        )
        merged_exists = self.storage.object_exists(f"{self.base_path}/merged.parquet")

        if not filter_files:
            return MergeStatus(filters_to_merge=[], no_filters=True)

        filter_names = [self._parse_filter_name(f) for f in filter_files]

        if not merged_exists:
            return MergeStatus(filters_to_merge=filter_names)

        merged_columns = await self._get_merged_columns()
        filters_to_merge = [f for f in filter_names if f not in merged_columns]

        return MergeStatus(filters_to_merge=filters_to_merge)

    async def merge(self, filter_names: list[str] | None = None) -> MergeResult:
        status = await self.get_merge_status()

        if filter_names:
            status.filters_to_merge = [
                f for f in status.filters_to_merge if f in filter_names
            ]

        if not status.filters_to_merge:
            return MergeResult(merged_count=0, filters_added=[], nothing_to_merge=True)

        merged_df = await self._load_base_or_merged()

        for filter_name in status.filters_to_merge:
            filter_df = await self._load_filter_output(filter_name)
            if filter_df is not None and len(filter_df) > 0:
                merged_df = merged_df.merge(filter_df, on=self.id_column, how="left")

        await self._write_merged_atomically(merged_df)

        return MergeResult(
            merged_count=len(merged_df),
            filters_added=status.filters_to_merge,
        )

    def _parse_filter_name(self, key: str) -> str:
        filename = key.split("/")[-1]
        return filename.replace(".parquet", "")

    async def _get_merged_columns(self) -> set[str]:
        merged_key = f"{self.base_path}/merged.parquet"
        try:
            data = self.storage.read_bytes(merged_key)
            df = pd.read_parquet(io.BytesIO(data))
            columns = set()
            for col in df.columns:
                if col.endswith("_passed"):
                    name = col.rsplit("_passed", 1)[0]
                    columns.add(name)
            return columns
        except Exception as e:
            logger.warning("Failed to read merged.parquet: %s", e)
            return set()

    async def _load_base_or_merged(self) -> pd.DataFrame:
        merged_key = f"{self.base_path}/merged.parquet"
        base_key = f"{self.base_path}/base.parquet"

        if self.storage.object_exists(merged_key):
            data = self.storage.read_bytes(merged_key)
            return pd.read_parquet(io.BytesIO(data))

        if self.storage.object_exists(base_key):
            data = self.storage.read_bytes(base_key)
            return pd.read_parquet(io.BytesIO(data))

        return pd.DataFrame(columns=[self.id_column])

    async def _load_filter_output(self, filter_name: str) -> pd.DataFrame | None:
        filter_key = f"{self.base_path}/filters/{filter_name}.parquet"
        try:
            data = self.storage.read_bytes(filter_key)
            return pd.read_parquet(io.BytesIO(data))
        except Exception as e:
            logger.warning("Failed to read filter %s: %s", filter_name, e)
            return None

    async def _write_merged_atomically(self, df: pd.DataFrame):
        temp_key = f"{self.base_path}/.merged_tmp.parquet"
        final_key = f"{self.base_path}/merged.parquet"

        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)

        self.storage.write_bytes(temp_key, buffer.read())

        self.storage.copy_object(temp_key, final_key)
        self.storage.delete_object(temp_key)
