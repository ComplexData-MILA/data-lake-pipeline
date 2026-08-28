"""Tests for DuckDB query builder."""

import pytest

from viewer.backend import duckdb_query


TEST_BASE_PATHS = [
    "s3://bucket/prefix/test_ds/batch1/merged.parquet",
    "s3://bucket/prefix/test_ds/batch2/merged.parquet",
]
TEST_ANNOT_PATHS = {
    "annotator1": [
        "s3://bucket/prefix/test_ds/annotations/annotator1/batch1/merged.parquet",
        "s3://bucket/prefix/test_ds/annotations/annotator1/batch2/merged.parquet",
    ],
}


class TestBuildQuery:
    """Tests for build_query function."""

    @pytest.mark.asyncio
    async def test_query_with_annotator_filter_excludes_non_matching_rows(self):
        """Test that annotator filters exclude non-matching base rows.

        When an annotator filter is applied, rows from base that don't have
        matching annotator data should NOT be returned at all (not with NULLs).
        """
        # annotator1 has rows: id=1 (label='positive'), id=2 (label='negative')
        # When we filter for label='positive', only id=1 should be returned
        filter_data = {
            "annotators": {
                "annotator1": {"field": "label", "op": "eq", "value": "positive"}
            }
        }
        filters = duckdb_query.FilterSpec(filter_data)
        query, params, columns, annotator_columns = duckdb_query.build_query(
            columns=["id", "text"],
            annotators=["annotator1"],
            filters=filters,
            base_parquet_paths=TEST_BASE_PATHS,
            annot_parquet_paths=TEST_ANNOT_PATHS,
            annotator_columns={"annotator1": ["label", "is_valid"]},
            offset=0,
            limit=50,
        )
        assert "INNER JOIN" in query, "Filtered annotators should use INNER JOIN to exclude non-matching rows"

    @pytest.mark.asyncio
    async def test_query_with_annotator_filter_count_excludes_non_matching(self):
        """Test that count query with annotator filters excludes non-matching rows."""
        filter_data = {
            "annotators": {
                "annotator1": {"field": "label", "op": "eq", "value": "positive"}
            }
        }
        filters = duckdb_query.FilterSpec(filter_data)
        query, params = duckdb_query.build_count_query(
            filters=filters,
            base_parquet_paths=TEST_BASE_PATHS,
            annot_parquet_paths=TEST_ANNOT_PATHS,
            annotators=["annotator1"],
            annotator_columns={"annotator1": ["label"]},
        )
        assert "INNER JOIN" in query, "Count query should use INNER JOIN for filtered annotators"

    @pytest.mark.asyncio
    async def test_basic_query_no_filters(self):
        """Test basic query without filters."""
        filters = duckdb_query.FilterSpec()
        query, params, columns, annotator_columns = duckdb_query.build_query(
            columns=["id", "text"],
            annotators=[],
            filters=filters,
            base_parquet_paths=TEST_BASE_PATHS,
            annot_parquet_paths={},
            offset=0,
            limit=50,
        )
        assert "WITH" in query
        assert "base" in query
        assert "text" in query

    @pytest.mark.asyncio
    async def test_query_with_base_filter(self):
        """Test query with base filter."""
        filter_data = {"base": {"field": "annotator_name", "op": "eq", "value": "test"}}
        filters = duckdb_query.FilterSpec(filter_data)
        query, params, columns, annotator_columns = duckdb_query.build_query(
            columns=["id", "text"],
            annotators=[],
            filters=filters,
            base_parquet_paths=TEST_BASE_PATHS,
            annot_parquet_paths={},
            offset=0,
            limit=50,
        )
        assert '"base"."annotator_name"' in query
        assert "WHERE" in query
        assert params == ["test"], "filter value must be bound, not interpolated"

    @pytest.mark.asyncio
    async def test_query_with_annotator_join(self):
        """Test query with annotator join."""
        filters = duckdb_query.FilterSpec()
        query, params, columns, annotator_columns = duckdb_query.build_query(
            columns=["id", "text"],
            annotators=["annotator1"],
            filters=filters,
            base_parquet_paths=TEST_BASE_PATHS,
            annot_parquet_paths=TEST_ANNOT_PATHS,
            annotator_columns={"annotator1": ["label", "is_valid"]},
            offset=0,
            limit=50,
        )
        assert "annotator1" in query
        assert "LEFT JOIN" in query
        assert '"annotator1.label"' in query, "Annotator columns should be aliased with {annotator}.{col} format in SQL"
        assert '"annotator1.is_valid"' in query, "Annotator columns should be aliased with {annotator}.{col} format in SQL"
        assert "annotator1.label" in columns, "Returned columns list should include prefixed annotator column names"
        assert "annotator1.is_valid" in columns, "Returned columns list should include prefixed annotator column names"
        assert "annotator1" in annotator_columns

    @pytest.mark.asyncio
    async def test_query_column_aliasing(self):
        """Test that column aliasing in CTE uses correct syntax."""
        filters = duckdb_query.FilterSpec()
        query, params, columns, annotator_columns = duckdb_query.build_query(
            columns=["id", "text", "score"],
            annotators=[],
            filters=filters,
            base_parquet_paths=TEST_BASE_PATHS,
            annot_parquet_paths={},
            offset=0,
            limit=50,
        )
        assert "ANY_VALUE" in query
        assert "GROUP BY" in query


class TestBuildCountQuery:
    """Tests for build_count_query function."""

    @pytest.mark.asyncio
    async def test_count_query_no_filters(self):
        """Test count query without filters."""
        filters = duckdb_query.FilterSpec()
        query, params = duckdb_query.build_count_query(
            filters=filters,
            base_parquet_paths=TEST_BASE_PATHS,
            annot_parquet_paths={},
            annotators=[],
            annotator_columns={},
        )
        assert "SELECT COUNT" in query

    @pytest.mark.asyncio
    async def test_count_query_with_filters(self):
        """Test count query with filters."""
        filter_data = {"base": {"field": "value", "op": "gt", "value": 10}}
        filters = duckdb_query.FilterSpec(filter_data)
        query, params = duckdb_query.build_count_query(
            filters=filters,
            base_parquet_paths=TEST_BASE_PATHS,
            annot_parquet_paths={},
            annotators=[],
            annotator_columns={},
        )
        assert "WHERE" in query
        assert params == [10]


class TestFilterSpecCompile:
    """Tests for FilterSpec.compile method."""

    def test_compile_eq_string(self):
        """Test eq compile with string value (bound parameter)."""
        fs = duckdb_query.FilterSpec()
        clause, params = fs.compile("base", {"field": "name", "op": "eq", "value": "test"})
        assert clause == '"base"."name" = ?'
        assert params == ["test"]

    def test_compile_eq_bool(self):
        """Test eq compile with bool value (bound parameter)."""
        fs = duckdb_query.FilterSpec()
        clause, params = fs.compile("base", {"field": "flag", "op": "eq", "value": True})
        assert clause == '"base"."flag" = ?'
        assert params == [True]

    def test_compile_gt(self):
        """Test gt compile (bound parameter)."""
        fs = duckdb_query.FilterSpec()
        clause, params = fs.compile("base", {"field": "score", "op": "gt", "value": 10})
        assert clause == '"base"."score" > ?'
        assert params == [10]

    def test_compile_contains(self):
        """Test contains compile."""
        fs = duckdb_query.FilterSpec()
        clause, params = fs.compile("base", {"field": "text", "op": "contains", "value": "foo"})
        assert "LIKE ?" in clause
        assert params == ["%foo%"]

    def test_compile_escapes_like_wildcards(self):
        """Test contains escapes LIKE wildcards in the bound value."""
        fs = duckdb_query.FilterSpec()
        clause, params = fs.compile("base", {"field": "text", "op": "contains", "value": "a%b_c"})
        assert params == ["%a\\%b\\_c%"]

    def test_compile_empty(self):
        """Test compile with no filter spec."""
        fs = duckdb_query.FilterSpec()
        clause, params = fs.compile("base", None)
        assert clause == ""
        assert params == []

    def test_compile_rejects_invalid_field(self):
        """Test compile rejects non-identifier fields (SQL injection guard)."""
        fs = duckdb_query.FilterSpec()
        with pytest.raises(ValueError):
            fs.compile("base", {"field": "x; DROP TABLE", "op": "eq", "value": "test"})
        with pytest.raises(ValueError):
            fs.compile("base", {"field": ".field", "op": "eq", "value": "test"})

    def test_compile_does_not_interpolate_values(self):
        """Test string values never appear interpolated in the clause."""
        fs = duckdb_query.FilterSpec()
        clause, params = fs.compile("base", {"field": "name", "op": "eq", "value": "' OR 1=1 --"})
        assert "OR 1=1" not in clause
        assert params == ["' OR 1=1 --"]

    def test_compile_unsupported_op(self):
        """Test unsupported ops raise ValueError."""
        fs = duckdb_query.FilterSpec()
        with pytest.raises(ValueError):
            fs.compile("base", {"field": "name", "op": "drop", "value": "x"})


class TestFormatParquetPaths:
    """Tests for _format_parquet_paths helper."""

    def test_format_single_path(self):
        """Test formatting single path."""
        paths = ["s3://bucket/prefix/dataset/batch1/merged.parquet"]
        result = duckdb_query._format_parquet_paths(paths)
        assert result == "['s3://bucket/prefix/dataset/batch1/merged.parquet']"

    def test_format_multiple_paths(self):
        """Test formatting multiple paths."""
        paths = [
            "s3://bucket/prefix/dataset/batch1/merged.parquet",
            "s3://bucket/prefix/dataset/batch2/merged.parquet",
        ]
        result = duckdb_query._format_parquet_paths(paths)
        assert "['s3://bucket/prefix/dataset/batch1/merged.parquet', 's3://bucket/prefix/dataset/batch2/merged.parquet']" in result

    def test_format_empty_paths(self):
        """Test formatting empty list raises AssertionError."""
        with pytest.raises(AssertionError):
            duckdb_query._format_parquet_paths([])
