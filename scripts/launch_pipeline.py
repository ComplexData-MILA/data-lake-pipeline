#!/usr/bin/env python
import argparse

from data_lake_pipeline.config import Settings
from data_lake_pipeline.orchestration.batcher import promote_stable_landing_files, submit_slurm_if_needed


def main() -> int:
    parser = argparse.ArgumentParser(description="Promote stable landing files into the processing queue.")
    parser.add_argument(
        "--min-age-minutes",
        type=int,
        default=None,
        help="Only move landing files older than this many minutes.",
    )
    args = parser.parse_args()

    settings = Settings.from_env()
    min_age = args.min_age_minutes if args.min_age_minutes is not None else settings.stable_file_age_minutes
    promoted = promote_stable_landing_files(settings=settings, min_age_minutes=min_age)
    print(f"Promoted {len(promoted)} files to pending batches.")

    if promoted:
        submit_slurm_if_needed(settings=settings)
    else:
        print("No stable files found. Nothing submitted.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
