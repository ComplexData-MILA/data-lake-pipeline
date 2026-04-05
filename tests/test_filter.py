"""Tests for Filter DSL compilation and execution."""
import io
import os
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from s3_data_tool.filter import (
    AllFilter,
    AnyFilter,
    BooleanFilter,
    FilterNode,
    RawDuckFilter,
)


def _configure_duckdb_s3(conn):
    """Configure DuckDB with S3 credentials from environment."""
    endpoint = os.environ.get("S3_ENDPOINT_URL", "")
    if endpoint.startswith("https://"):
        endpoint_host = endpoint[8:].rstrip("/")
        use_ssl = True
    elif endpoint.startswith("http://"):
        endpoint_host = endpoint[7:].rstrip("/")
        use_ssl = False
    else:
        endpoint_host = endpoint.rstrip("/")
        use_ssl = False

    conn.execute(f"SET s3_endpoint='{endpoint_host}'")
    conn.execute(f"SET s3_access_key_id='{os.environ.get('S3_ACCESS_KEY', '')}'")
    conn.execute(f"SET s3_secret_access_key='{os.environ.get('S3_SECRET_KEY', '')}'")
    conn.execute(f"SET s3_use_ssl={str(use_ssl).lower()}")
    conn.execute("SET s3_url_style='path'")


class TestBooleanFilterCompilation:
    """Tests for BooleanFilter compilation."""

    def test_compile_with_existing_dataset_column(self):
        """Test compilation when dataset column exists."""
        f = BooleanFilter(field="is_valid", value=True)
        result = f.compile({"is_valid", "text", "source_id"})
        assert result == "is_valid = True"

    def test_compile_with_missing_column_returns_false(self):
        """Test compilation returns FALSE for missing column."""
        f = BooleanFilter(field="is_valid", value=True)
        result = f.compile({"text", "source_id"})
        assert result == "FALSE"

    def test_compile_false_value(self):
        """Test compilation with False value."""
        f = BooleanFilter(field="is_valid", value=False)
        result = f.compile({"is_valid"})
        assert result == "is_valid = False"

    def test_compile_empty_available_columns(self):
        """Test compilation with empty column set."""
        f = BooleanFilter(field="is_valid", value=True)
        result = f.compile(set())
        assert result == "FALSE"


class TestAllFilterCompilation:
    """Tests for AllFilter (AND) compilation."""

    def test_compile_empty_filters_returns_true(self):
        """Test empty AND returns TRUE."""
        f = AllFilter(filters=[])
        result = f.compile({"any_column"})
        assert result == "TRUE"

    def test_compile_single_filter(self):
        """Test single filter in AND."""
        f = AllFilter(filters=[
            BooleanFilter(field="is_valid", value=True)
        ])
        result = f.compile({"is_valid"})
        assert result == "(is_valid = True)"

    def test_compile_multiple_filters(self):
        """Test multiple filters combined with AND."""
        f = AllFilter(filters=[
            BooleanFilter(field="is_valid", value=True),
            BooleanFilter(field="is_duplicate", value=False),
        ])
        result = f.compile({"is_valid", "is_duplicate"})
        assert result == "(is_valid = True AND is_duplicate = False)"

    def test_compile_with_missing_column(self):
        """Test AND with one missing column."""
        f = AllFilter(filters=[
            BooleanFilter(field="is_valid", value=True),
            BooleanFilter(field="missing_field", value=True),
        ])
        result = f.compile({"is_valid"})
        assert result == "(is_valid = True AND FALSE)"

    def test_compile_all_missing_columns(self):
        """Test AND with all missing columns."""
        f = AllFilter(filters=[
            BooleanFilter(field="field1", value=True),
            BooleanFilter(field="field2", value=False),
        ])
        result = f.compile({"other_column"})
        assert result == "(FALSE AND FALSE)"


class TestAnyFilterCompilation:
    """Tests for AnyFilter (OR) compilation."""

    def test_compile_empty_filters_returns_false(self):
        """Test empty OR returns FALSE."""
        f = AnyFilter(filters=[])
        result = f.compile({"any_column"})
        assert result == "FALSE"

    def test_compile_single_filter(self):
        """Test single filter in OR."""
        f = AnyFilter(filters=[
            BooleanFilter(field="is_valid", value=True)
        ])
        result = f.compile({"is_valid"})
        assert result == "(is_valid = True)"

    def test_compile_multiple_filters(self):
        """Test multiple filters combined with OR."""
        f = AnyFilter(filters=[
            BooleanFilter(field="is_valid", value=True),
            BooleanFilter(field="is_featured", value=True),
        ])
        result = f.compile({"is_valid", "is_featured"})
        assert result == "(is_valid = True OR is_featured = True)"

    def test_compile_with_missing_column(self):
        """Test OR with one missing column."""
        f = AnyFilter(filters=[
            BooleanFilter(field="is_valid", value=True),
            BooleanFilter(field="missing_field", value=True),
        ])
        result = f.compile({"is_valid"})
        assert result == "(is_valid = True OR FALSE)"

    def test_compile_all_missing_columns(self):
        """Test OR with all missing columns."""
        f = AnyFilter(filters=[
            BooleanFilter(field="field1", value=True),
            BooleanFilter(field="field2", value=False),
        ])
        result = f.compile({"other_column"})
        assert result == "(FALSE OR FALSE)"


class TestNestedFilters:
    """Tests for nested filter structures."""

    def test_all_containing_any(self):
        """Test nested OR within AND."""
        f = AllFilter(filters=[
            AnyFilter(filters=[
                BooleanFilter(field="is_valid", value=True),
                BooleanFilter(field="is_featured", value=True),
            ]),
            BooleanFilter(field="priority", value=True),
        ])
        cols = {"is_valid", "is_featured", "priority"}
        result = f.compile(cols)
        assert result == "((is_valid = True OR is_featured = True) AND priority = True)"

    def test_any_containing_all(self):
        """Test nested AND within OR."""
        f = AnyFilter(filters=[
            AllFilter(filters=[
                BooleanFilter(field="is_valid", value=True),
                BooleanFilter(field="priority", value=True),
            ]),
            BooleanFilter(field="is_featured", value=True),
        ])
        cols = {"is_valid", "priority", "is_featured"}
        result = f.compile(cols)
        assert result == "((is_valid = True AND priority = True) OR is_featured = True)"

    def test_deeply_nested(self):
        """Test deeply nested filter structures."""
        f = AllFilter(filters=[
            BooleanFilter(field="a", value=True),
            AnyFilter(filters=[
                BooleanFilter(field="b", value=True),
                AllFilter(filters=[
                    BooleanFilter(field="c", value=True),
                    BooleanFilter(field="d", value=False),
                ]),
            ]),
        ])
        cols = {"a", "b", "c", "d"}
        result = f.compile(cols)
        expected = "(a = True AND (b = True OR (c = True AND d = False)))"
        assert result == expected

    def test_nested_with_missing_columns(self):
        """Test nested filters with missing columns."""
        f = AllFilter(filters=[
            AnyFilter(filters=[
                BooleanFilter(field="exists", value=True),
                BooleanFilter(field="missing", value=True),
            ]),
            BooleanFilter(field="also_missing", value=False),
        ])
        cols = {"exists"}
        result = f.compile(cols)
        assert result == "((exists = True OR FALSE) AND FALSE)"


class TestRawDuckFilter:
    """Tests for RawDuckFilter."""

    def test_compile_returns_sql(self):
        """Test raw SQL pass-through."""
        f = RawDuckFilter(sql="length(text) > 100")
        result = f.compile({"text", "other"})
        assert result == "length(text) > 100"

    def test_compile_ignores_available_columns(self):
        """Test that raw SQL doesn't check columns."""
        f = RawDuckFilter(sql="custom_function()")
        result = f.compile(set())
        assert result == "custom_function()"

    def test_combined_with_boolean(self):
        """Test combining raw SQL with boolean filters."""
        f = AllFilter(filters=[
            BooleanFilter(field="is_valid", value=True),
            RawDuckFilter(sql="length(text) > 100"),
        ])
        cols = {"is_valid", "text"}
        result = f.compile(cols)
        assert result == "(is_valid = True AND length(text) > 100)"


class TestFilterSerialization:
    """Tests for Pydantic serialization/deserialization."""

    def test_boolean_filter_serialize_deserialize(self):
        """Test round-trip serialization."""
        f = BooleanFilter(field="is_valid", value=True)
        data = f.model_dump()
        f2 = BooleanFilter(**data)
        assert f2.field == "is_valid"
        assert f2.value is True

    def test_nested_filter_serialize_deserialize(self):
        """Test nested filter serialization."""
        f = AllFilter(filters=[
            BooleanFilter(field="a", value=True),
            AnyFilter(filters=[
                BooleanFilter(field="b", value=False),
            ]),
        ])
        data = f.model_dump()
        f2 = AllFilter(**data)
        assert isinstance(f2.filters[1], AnyFilter)


# ============================================================================
# LIVE S3 PARQUET TESTS
# ============================================================================

@pytest.fixture
def s3_client():
    """Create S3 client from environment variables."""
    import aioboto3

    endpoint_url = os.environ.get("S3_ENDPOINT_URL")
    access_key = os.environ.get("S3_ACCESS_KEY")
    secret_key = os.environ.get("S3_SECRET_KEY")

    session = aioboto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )

    kwargs: dict[str, Any] = {}
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url

    return session, kwargs


@pytest.fixture
def test_bucket():
    """Get test bucket name from environment."""
    return os.environ.get("S3_BUCKET", "test-bucket")


@pytest.fixture
def test_prefix():
    """Get test prefix from environment."""
    return os.environ.get("S3_PREFIX", "datasets")


@pytest.fixture
async def sample_parquet_data():
    """Create sample parquet data for testing."""
    rows = [
        {"id": "1", "text": "Valid text one", "is_valid": True, "priority": True, "_batch": "batch1"},
        {"id": "2", "text": "Invalid text", "is_valid": False, "priority": False, "_batch": "batch1"},
        {"id": "3", "text": "Valid text two", "is_valid": True, "priority": False, "_batch": "batch1"},
        {"id": "4", "text": "Another valid", "is_valid": True, "priority": True, "_batch": "batch1"},
        {"id": "5", "text": "Not valid", "is_valid": False, "priority": True, "_batch": "batch1"},
    ]
    return rows


def create_parquet_buffer(rows: list[dict]) -> io.BytesIO:
    """Create parquet file in memory from rows."""
    if not rows:
        table = pa.table({})
    else:
        table = pa.Table.from_pylist(rows)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)
    return buf


@pytest.mark.asyncio
class TestLiveS3ParquetQueries:
    """Live S3 tests for filter execution on parquet files."""

    async def test_boolean_filter_on_dataset_column(
        self, s3_client, test_bucket, test_prefix, sample_parquet_data
    ):
        """Test boolean filter on dataset column with live parquet."""
        import duckdb
        import uuid

        session, kwargs = s3_client
        test_id = str(uuid.uuid4())[:8]
        key = f"{test_prefix}/filter_test_{test_id}/batch1/merged.parquet"

        async with session.client("s3", **kwargs) as client:
            try:
                buf = create_parquet_buffer(sample_parquet_data)
                await client.put_object(
                    Bucket=test_bucket,
                    Key=key,
                    Body=buf.read(),
                )

                conn = duckdb.connect()
                _configure_duckdb_s3(conn)
                conn.execute(f"""
                    CREATE TABLE dataset AS 
                    SELECT * FROM read_parquet('s3://{test_bucket}/{key}')
                """)

                available_columns = set(conn.execute(
                    "SELECT column_name FROM information_schema.columns WHERE table_name = 'dataset'"
                ).fetchall())
                available_columns = {col[0] for col in available_columns}

                filter = BooleanFilter(field="is_valid", value=True)
                where_clause = filter.compile(available_columns)

                result = conn.execute(f"SELECT * FROM dataset WHERE {where_clause}").fetchall()

                assert len(result) == 3
                for row in result:
                    assert row[2] is True

            finally:
                await client.delete_object(Bucket=test_bucket, Key=key)

    async def test_combined_filter_on_dataset(
        self, s3_client, test_bucket, test_prefix, sample_parquet_data
    ):
        """Test combined AND filter with live parquet."""
        import duckdb
        import uuid

        session, kwargs = s3_client
        test_id = str(uuid.uuid4())[:8]
        key = f"{test_prefix}/filter_test_{test_id}/batch1/merged.parquet"

        async with session.client("s3", **kwargs) as client:
            try:
                buf = create_parquet_buffer(sample_parquet_data)
                await client.put_object(
                    Bucket=test_bucket,
                    Key=key,
                    Body=buf.read(),
                )

                conn = duckdb.connect()
                _configure_duckdb_s3(conn)
                conn.execute(f"""
                    CREATE TABLE dataset AS 
                    SELECT * FROM read_parquet('s3://{test_bucket}/{key}')
                """)

                available_columns = set(conn.execute(
                    "SELECT column_name FROM information_schema.columns WHERE table_name = 'dataset'"
                ).fetchall())
                available_columns = {col[0] for col in available_columns}

                filter = AllFilter(filters=[
                    BooleanFilter(field="is_valid", value=True),
                    BooleanFilter(field="priority", value=True),
                ])
                where_clause = filter.compile(available_columns)

                result = conn.execute(f"SELECT * FROM dataset WHERE {where_clause}").fetchall()

                assert len(result) == 2
                for row in result:
                    assert row[2] is True
                    assert row[3] is True

            finally:
                await client.delete_object(Bucket=test_bucket, Key=key)

    async def test_filter_with_missing_column(
        self, s3_client, test_bucket, test_prefix, sample_parquet_data
    ):
        """Test filter with missing column returns no results."""
        import duckdb
        import uuid

        session, kwargs = s3_client
        test_id = str(uuid.uuid4())[:8]
        key = f"{test_prefix}/filter_test_{test_id}/batch1/merged.parquet"

        async with session.client("s3", **kwargs) as client:
            try:
                buf = create_parquet_buffer(sample_parquet_data)
                await client.put_object(
                    Bucket=test_bucket,
                    Key=key,
                    Body=buf.read(),
                )

                conn = duckdb.connect()
                _configure_duckdb_s3(conn)
                conn.execute(f"""
                    CREATE TABLE dataset AS 
                    SELECT * FROM read_parquet('s3://{test_bucket}/{key}')
                """)

                available_columns = set(conn.execute(
                    "SELECT column_name FROM information_schema.columns WHERE table_name = 'dataset'"
                ).fetchall())
                available_columns = {col[0] for col in available_columns}

                filter = AllFilter(filters=[
                    BooleanFilter(field="is_valid", value=True),
                    BooleanFilter(field="missing_column", value=True),
                ])
                where_clause = filter.compile(available_columns)

                result = conn.execute(f"SELECT * FROM dataset WHERE {where_clause}").fetchall()

                assert len(result) == 0

            finally:
                await client.delete_object(Bucket=test_bucket, Key=key)

    async def test_any_filter_with_partial_missing(
        self, s3_client, test_bucket, test_prefix, sample_parquet_data
    ):
        """Test OR filter where one column is missing."""
        import duckdb
        import uuid

        session, kwargs = s3_client
        test_id = str(uuid.uuid4())[:8]
        key = f"{test_prefix}/filter_test_{test_id}/batch1/merged.parquet"

        async with session.client("s3", **kwargs) as client:
            try:
                buf = create_parquet_buffer(sample_parquet_data)
                await client.put_object(
                    Bucket=test_bucket,
                    Key=key,
                    Body=buf.read(),
                )

                conn = duckdb.connect()
                _configure_duckdb_s3(conn)
                conn.execute(f"""
                    CREATE TABLE dataset AS 
                    SELECT * FROM read_parquet('s3://{test_bucket}/{key}')
                """)

                available_columns = set(conn.execute(
                    "SELECT column_name FROM information_schema.columns WHERE table_name = 'dataset'"
                ).fetchall())
                available_columns = {col[0] for col in available_columns}

                filter = AnyFilter(filters=[
                    BooleanFilter(field="is_valid", value=True),
                    BooleanFilter(field="missing_column", value=True),
                ])
                where_clause = filter.compile(available_columns)

                result = conn.execute(f"SELECT * FROM dataset WHERE {where_clause}").fetchall()

                assert len(result) == 3

            finally:
                await client.delete_object(Bucket=test_bucket, Key=key)

    async def test_raw_duck_filter(
        self, s3_client, test_bucket, test_prefix, sample_parquet_data
    ):
        """Test raw DuckDB SQL filter."""
        import duckdb
        import uuid

        session, kwargs = s3_client
        test_id = str(uuid.uuid4())[:8]
        key = f"{test_prefix}/filter_test_{test_id}/batch1/merged.parquet"

        async with session.client("s3", **kwargs) as client:
            try:
                buf = create_parquet_buffer(sample_parquet_data)
                await client.put_object(
                    Bucket=test_bucket,
                    Key=key,
                    Body=buf.read(),
                )

                conn = duckdb.connect()
                _configure_duckdb_s3(conn)
                conn.execute(f"""
                    CREATE TABLE dataset AS 
                    SELECT * FROM read_parquet('s3://{test_bucket}/{key}')
                """)

                available_columns = {"id", "text", "is_valid", "priority"}

                filter = AllFilter(filters=[
                    BooleanFilter(field="is_valid", value=True),
                    RawDuckFilter(sql="length(text) > 10"),
                ])
                where_clause = filter.compile(available_columns)

                result = conn.execute(f"SELECT * FROM dataset WHERE {where_clause}").fetchall()

                assert len(result) == 3

            finally:
                await client.delete_object(Bucket=test_bucket, Key=key)

    async def test_nested_filter_complex(
        self, s3_client, test_bucket, test_prefix, sample_parquet_data
    ):
        """Test complex nested filter with live parquet."""
        import duckdb
        import uuid

        session, kwargs = s3_client
        test_id = str(uuid.uuid4())[:8]
        key = f"{test_prefix}/filter_test_{test_id}/batch1/merged.parquet"

        async with session.client("s3", **kwargs) as client:
            try:
                buf = create_parquet_buffer(sample_parquet_data)
                await client.put_object(
                    Bucket=test_bucket,
                    Key=key,
                    Body=buf.read(),
                )

                conn = duckdb.connect()
                _configure_duckdb_s3(conn)
                conn.execute(f"""
                    CREATE TABLE dataset AS 
                    SELECT * FROM read_parquet('s3://{test_bucket}/{key}')
                """)

                available_columns = {"id", "text", "is_valid", "priority"}

                filter = AllFilter(filters=[
                    AnyFilter(filters=[
                        AllFilter(filters=[
                            BooleanFilter(field="is_valid", value=True),
                            BooleanFilter(field="priority", value=True),
                        ]),
                        BooleanFilter(field="is_valid", value=False),
                    ]),
                    RawDuckFilter(sql="id != '5'"),
                ])
                where_clause = filter.compile(available_columns)

                result = conn.execute(f"SELECT * FROM dataset WHERE {where_clause}").fetchdf()

                assert len(result) == 3
                assert "5" not in result["id"].tolist()

            finally:
                await client.delete_object(Bucket=test_bucket, Key=key)


@pytest.mark.asyncio
class TestSchemaDiscovery:
    """Tests for schema discovery with live S3 parquet files."""

    async def test_discover_columns_from_parquet(
        self, s3_client, test_bucket, test_prefix, sample_parquet_data
    ):
        """Test reading columns from parquet file."""
        import uuid

        session, kwargs = s3_client
        test_id = str(uuid.uuid4())[:8]
        key = f"{test_prefix}/filter_test_{test_id}/batch1/merged.parquet"

        async with session.client("s3", **kwargs) as client:
            try:
                buf = create_parquet_buffer(sample_parquet_data)
                await client.put_object(
                    Bucket=test_bucket,
                    Key=key,
                    Body=buf.read(),
                )

                from s3_data_tool.s3_utils import s3_object_exists, read_parquet_columns

                exists = await s3_object_exists(client, test_bucket, key)
                assert exists is True

                columns = await read_parquet_columns(client, test_bucket, key)
                assert columns == {"id", "text", "is_valid", "priority"}

            finally:
                await client.delete_object(Bucket=test_bucket, Key=key)

    async def test_discover_columns_empty_parquet(
        self, s3_client, test_bucket, test_prefix
    ):
        """Test reading columns from empty parquet file."""
        import uuid

        session, kwargs = s3_client
        test_id = str(uuid.uuid4())[:8]
        key = f"{test_prefix}/filter_test_{test_id}/batch1/merged.parquet"

        async with session.client("s3", **kwargs) as client:
            try:
                buf = create_parquet_buffer([])
                await client.put_object(
                    Bucket=test_bucket,
                    Key=key,
                    Body=buf.read(),
                )

                from s3_data_tool.s3_utils import read_parquet_columns

                columns = await read_parquet_columns(client, test_bucket, key)
                assert columns == set()

            finally:
                await client.delete_object(Bucket=test_bucket, Key=key)

    async def test_parallel_column_discovery(
        self, s3_client, test_bucket, test_prefix
    ):
        """Test parallel discovery of columns from multiple parquet files."""
        import uuid

        session, kwargs = s3_client
        test_id = str(uuid.uuid4())[:8]

        datasets = [
            {
                "key": f"{test_prefix}/filter_test_{test_id}/batch1/merged.parquet",
                "rows": [
                    {"id": "1", "text": "a", "is_valid": True, "_batch": "batch1"},
                    {"id": "2", "text": "b", "is_valid": False, "_batch": "batch1"},
                ],
            },
            {
                "key": f"{test_prefix}/filter_test_{test_id}/batch2/merged.parquet",
                "rows": [
                    {"id": "3", "text": "c", "priority": True, "_batch": "batch2"},
                    {"id": "4", "text": "d", "priority": False, "_batch": "batch2"},
                ],
            },
            {
                "key": f"{test_prefix}/filter_test_{test_id}/batch3/merged.parquet",
                "rows": [
                    {"id": "5", "text": "e", "is_valid": True, "priority": True, "_batch": "batch3"},
                ],
            },
        ]

        async with session.client("s3", **kwargs) as client:
            try:
                for ds in datasets:
                    buf = create_parquet_buffer(ds["rows"])
                    await client.put_object(
                        Bucket=test_bucket,
                        Key=ds["key"],
                        Body=buf.read(),
                    )

                from s3_data_tool.s3_utils import read_parquet_columns

                keys = [ds["key"] for ds in datasets]
                import asyncio

                results = await asyncio.gather(
                    *[read_parquet_columns(client, test_bucket, key) for key in keys]
                )

                all_columns = set().union(*results)
                assert all_columns == {"id", "text", "is_valid", "priority"}

            finally:
                for ds in datasets:
                    await client.delete_object(Bucket=test_bucket, Key=ds["key"])
