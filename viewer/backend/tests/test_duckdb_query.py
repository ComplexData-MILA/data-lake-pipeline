"""Tests for DuckDB query builder."""

import pytest

from viewer.backend import duckdb_query


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
        query, columns, annotator_columns = duckdb_query.build_query(
            dataset_name="test_ds",
            columns=["id", "text"],
            annotators=["annotator1"],
            annotator_columns={"annotator1": ["label", "is_valid"]},
            filters=filters,
            offset=0,
            limit=50,
        )
        # The query should use INNER JOIN for filtered annotators, not LEFT JOIN
        # This ensures non-matching rows are excluded
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
        query = duckdb_query.build_count_query(
            dataset_name="test_ds",
            annotators=["annotator1"],
            filters=filters,
            annotator_columns={"annotator1": ["label"]},
        )
        # Count query should also use INNER JOIN for filtered annotators
        assert "INNER JOIN" in query, "Count query should use INNER JOIN for filtered annotators"

    @pytest.mark.asyncio
    async def test_basic_query_no_filters(self):
        """Test basic query without filters."""
        filters = duckdb_query.FilterSpec()
        query, columns, annotator_columns = duckdb_query.build_query(
            dataset_name="test_ds",
            columns=["id", "text"],
            annotators=[],
            filters=filters,
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
        query, columns, annotator_columns = duckdb_query.build_query(
            dataset_name="test_ds",
            columns=["id", "text"],
            annotators=[],
            filters=filters,
            offset=0,
            limit=50,
        )
        assert "base.annotator_name" in query
        assert "WHERE" in query

    @pytest.mark.asyncio
    async def test_query_with_annotator_join(self):
        """Test query with annotator join."""
        filters = duckdb_query.FilterSpec()
        query, columns, annotator_columns = duckdb_query.build_query(
            dataset_name="test_ds",
            columns=["id", "text"],
            annotators=["annotator1"],
            annotator_columns={"annotator1": ["label", "is_valid"]},
            filters=filters,
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
        query, columns, annotator_columns = duckdb_query.build_query(
            dataset_name="test_ds",
            columns=["id", "text", "score"],
            annotators=[],
            filters=filters,
            offset=0,
            limit=50,
        )
        # Check that ANY_VALUE is used for aggregation
        assert "ANY_VALUE" in query
        # Check that GROUP BY uses correct syntax (without base. prefix inside CTE)
        assert "GROUP BY id" in query


class TestBuildCountQuery:
    """Tests for build_count_query function."""

    @pytest.mark.asyncio
    async def test_count_query_no_filters(self):
        """Test count query without filters."""
        filters = duckdb_query.FilterSpec()
        query = duckdb_query.build_count_query(
            dataset_name="test_ds",
            annotators=[],
            filters=filters,
            annotator_columns={},
        )
        assert "SELECT COUNT" in query

    @pytest.mark.asyncio
    async def test_count_query_with_filters(self):
        """Test count query with filters."""
        filter_data = {"base": {"field": "value", "op": "gt", "value": 10}}
        filters = duckdb_query.FilterSpec(filter_data)
        query = duckdb_query.build_count_query(
            dataset_name="test_ds",
            annotators=[],
            filters=filters,
            annotator_columns={},
        )
        assert "WHERE" in query


class TestFilterSpecCompile:
    """Tests for FilterSpec.compile method."""

    def test_compile_eq_string(self):
        """Test eq compile with string value."""
        fs = duckdb_query.FilterSpec()
        result = fs.compile("base", {"field": "name", "op": "eq", "value": "test"})
        assert result == "base.name = 'test'"

    def test_compile_eq_bool(self):
        """Test eq compile with bool value."""
        fs = duckdb_query.FilterSpec()
        result = fs.compile("base", {"field": "flag", "op": "eq", "value": True})
        assert result == "base.flag = true"

    def test_compile_gt(self):
        """Test gt compile."""
        fs = duckdb_query.FilterSpec()
        result = fs.compile("base", {"field": "score", "op": "gt", "value": 10})
        assert result == "base.score > 10"

    def test_compile_contains(self):
        """Test contains compile."""
        fs = duckdb_query.FilterSpec()
        result = fs.compile("base", {"field": "text", "op": "contains", "value": "foo"})
        assert "LIKE" in result

    def test_compile_empty(self):
        """Test compile with no filter spec."""
        fs = duckdb_query.FilterSpec()
        result = fs.compile("base", None)
        assert result == ""

    def test_compile_leading_dot_in_field(self):
        """Test compile handles leading dot in field (edge case)."""
        fs = duckdb_query.FilterSpec()
        # This simulates potential user input issue
        result = fs.compile("base", {".field": "value", "op": "eq", "value": "test"})
        # Should not produce double dots
        assert ".." not in result