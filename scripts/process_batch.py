#!/usr/bin/env python
from data_lake_pipeline.config import Settings
from data_lake_pipeline.processing.batch_processor import process_pending_batches


def main() -> int:
    settings = Settings.from_env()
    summary = process_pending_batches(settings=settings)
    print(summary.model_dump_json())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
