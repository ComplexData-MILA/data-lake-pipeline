"""Unit tests for viewer.backend.charts SQL builders (no S3 access)."""

import pytest

from viewer.backend import charts


def test_created_at_expr_normalizes_and_casts():
    expr = charts._created_at_expr("base")
    assert "json_extract_string" in expr
    assert "TRY_CAST" in expr
    assert "AS JSON" in expr
    assert "AS TIMESTAMPTZ" in expr
    assert '"base"."_created_at"' in expr


def test_value_expr_normalizes_and_quotes():
    expr = charts._value_expr("base", "label")
    assert '"base"."label"' in expr
    assert "AS JSON" in expr


def test_value_expr_rejects_invalid_identifier():
    with pytest.raises(ValueError):
        charts._value_expr("base", "x; DROP TABLE")
    with pytest.raises(ValueError):
        charts._value_expr("base", "has space")


def test_activity_query_shape():
    query, params = charts.build_activity_query(
        ["s3://b/p.parquet"],
        ["s3://b/c.jsonl"],
        "1 minute",
        "2026-08-27T00:00:00+00:00",
        "2026-08-28T00:00:00+00:00",
    )
    assert "read_parquet" in query
    assert "read_json_auto" in query
    assert "time_bucket(INTERVAL '1 minute', ct)" in query
    assert 'GROUP BY "id", "_batch"' in query
    assert "ct IS NOT NULL" in query
    assert "TRY_CAST(? AS TIMESTAMPTZ)" in query
    assert params == [
        "2026-08-27T00:00:00+00:00",
        "2026-08-28T00:00:00+00:00",
    ]


def test_activity_query_no_window():
    query, params = charts.build_activity_query(
        ["s3://b/p.parquet"], [], "1 hour", None, "2026-08-28T00:00:00+00:00"
    )
    assert "ct >= " not in query
    assert params == []


def test_activity_query_empty_source():
    query, params = charts.build_activity_query(
        [], [], "1 minute", None, None
    )
    assert query == ""
    assert params == []


def test_categorical_counts_query_shape():
    query, params = charts.build_categorical_counts_query(
        ["s3://b/p.parquet"],
        [],
        "label",
        20,
        None,
        None,
    )
    assert '"src"."label"' in query
    assert "SUM(cnt) OVER () AS total" in query
    assert "COUNT(*) OVER () AS distinct_count" in query
    assert "LIMIT ?" in query
    assert params == [20]


def test_categorical_trend_query_shape():
    query, params = charts.build_categorical_trend_query(
        ["s3://b/p.parquet"],
        ["s3://b/c.jsonl"],
        "label",
        "1 hour",
        8,
        "2026-08-27T00:00:00+00:00",
        "2026-08-28T00:00:00+00:00",
    )
    assert "time_bucket(INTERVAL '1 hour', ct)" in query
    assert "ELSE 'other'" in query
    assert "row_number() OVER (ORDER BY total_cnt DESC, v ASC) <= ?" in query
    assert params == [
        "2026-08-27T00:00:00+00:00",
        "2026-08-28T00:00:00+00:00",
        8,
    ]
