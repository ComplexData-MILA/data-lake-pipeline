"""Unit tests for viewer cache key helpers and invalidation mapping."""

from viewer.backend import cache


def test_invalidate_keys_rows_ingested():
    keys = cache.invalidate_keys_for_event(
        {"type": "rows_ingested", "dataset": "d1"}
    )
    assert cache.datasets_key() in keys
    assert cache.files_key("d1") in keys
    assert cache.schema_keys_set("d1") in keys
    assert cache.count_keys_set("d1") in keys
    assert cache.activity_keys_set() in keys
    assert cache.categorical_keys_set("d1") in keys


def test_invalidate_keys_batch_merged():
    keys = cache.invalidate_keys_for_event(
        {"type": "batch_merged", "dataset": "d1"}
    )
    assert cache.annotators_key("d1") in keys
    assert cache.index_meta_key("d1") in keys
    assert cache.activity_keys_set() in keys
    assert cache.categorical_keys_set("d1") in keys


def test_invalidate_keys_conversion_progress():
    from viewer.backend.cache import conversion_key, invalidate_keys_for_event

    keys = invalidate_keys_for_event(
        {"type": "conversion_progress", "dataset": "d1"}
    )
    assert conversion_key("d1") in keys


def test_invalidate_keys_annotation_updated():
    keys = cache.invalidate_keys_for_event(
        {"type": "annotation_updated", "dataset": "d1"}
    )
    assert cache.annotators_key("d1") in keys
    assert cache.schema_keys_set("d1") in keys
    assert cache.count_keys_set("d1") in keys
    # Categorical charts query base data only.
    assert cache.categorical_keys_set("d1") not in keys


def test_invalidate_keys_without_dataset():
    keys = cache.invalidate_keys_for_event({"type": "batch_merged"})
    assert keys == [cache.datasets_key()]


def test_count_key_stable_and_specific():
    filters_a = {"base": {"field": "f", "op": "eq", "value": "v"}}
    filters_b = {"base": {"field": "f", "op": "eq", "value": "w"}}
    k1 = cache.count_key("d", {"a": ["x"]}, filters_a)
    k2 = cache.count_key("d", {"a": ["x"]}, filters_a)
    k3 = cache.count_key("d", {"a": ["x"]}, filters_b)
    assert k1 == k2
    assert k1 != k3


def test_schema_key_stable_and_annotator_specific():
    k1 = cache.schema_key("d", ["a1", "a2"])
    k2 = cache.schema_key("d", ["a2", "a1"])
    k3 = cache.schema_key("d", ["a1"])
    assert k1 == k2, "annotator order should not matter"
    assert k1 != k3


def test_activity_key_stable_and_param_specific():
    k1 = cache.activity_key("1m", 1440)
    k2 = cache.activity_key("1m", 1440)
    k3 = cache.activity_key("1m", 60)
    k4 = cache.activity_key("1h", 1440)
    assert k1 == k2
    assert k1 != k3
    assert k1 != k4


def test_categorical_key_stable_and_column_specific():
    k1 = cache.categorical_key("d", "label", "counts", "1h", 20, None)
    k2 = cache.categorical_key("d", "label", "counts", "1h", 20, None)
    k3 = cache.categorical_key("d", "other", "counts", "1h", 20, None)
    k4 = cache.categorical_key("d", "label", "trend", "1h", 20, None)
    assert k1 == k2
    assert k1 != k3
    assert k1 != k4
