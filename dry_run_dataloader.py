"""Validate classifier features

(test data split.)
"""

import asyncio
import json

from tqdm.asyncio import tqdm

from s3_data_tool import S3DataTool, RawDuckFilter


async def main():
    count = 0
    async with S3DataTool().filter_for_annotation(
        name="posts",
        annotator_name="dry_run_data_export_001",
        base_columns=["id", "text"],
        annotator_columns={"feasibility_001": ["is_feasible"]},
        annotator_filters={
            "feasibility_001": RawDuckFilter(sql="is_feasible IS NOT NULL"),
        },
    ) as annotator_view:
        async for _row in annotator_view:
            count += 1 
            if count % 10 == 0:
                print(count)


if __name__ == "__main__":
    asyncio.run(main())
